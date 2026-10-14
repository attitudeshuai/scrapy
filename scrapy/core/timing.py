from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from time import monotonic
from typing import TYPE_CHECKING, Generic, Self, TypeVar, overload

from scrapy.utils.httpobj import urlparse_cached

if TYPE_CHECKING:
    from twisted.python.failure import Failure

    from scrapy.crawler import Crawler
    from scrapy.http import Request, Response

#: Time between entering the scheduler and leaving it.
SCHEDULER_WAIT = "scheduler_wait"
#: Time spent in the process_request chain of downloader middlewares.
DOWNLOADER_REQUEST_MIDDLEWARE = "downloader_request_middleware"
#: Time spent waiting in a downloader slot (concurrency limit and delay).
DOWNLOADER_SLOT_WAIT = "downloader_slot_wait"
#: Time spent downloading the response, matching the download_latency meta key.
DOWNLOAD = "download"
#: Time spent in the process_response chain of downloader middlewares.
DOWNLOADER_RESPONSE_MIDDLEWARE = "downloader_response_middleware"
#: Time between receiving the response and the callback or errback starting.
SCRAPER_WAIT = "scraper_wait"
#: Time spent running the callback or errback.
CALLBACK = "callback"
#: Time spent processing an item in the item pipelines.
ITEM_PIPELINE = "item_pipeline"

#: Request meta key that holds the per-request stage durations, in seconds.
REQUEST_TIMING_META_KEY = "request_timing"

_ENQUEUED = "enqueued"
_DEQUEUED = "dequeued"
_FETCH_STARTED = "fetch_started"
_REACHED_DOWNLOADER = "reached_downloader"
_DOWNLOAD_STARTED = "download_started"
_RESPONSE_DOWNLOADED = "response_downloaded"
_RESPONSE_RECEIVED = "response_received"
_SCRAPE_STARTED = "scrape_started"

_T = TypeVar("_T")


@dataclass(slots=True)
class _Record:
    """Timestamps and accumulated timings of a single request."""

    points: dict[str, float] = field(default_factory=dict)
    callback_time: float = 0.0
    callback_started: bool = False
    download: float | None = None


class _TimedIterable(Generic[_T]):
    """Iterable wrapper adding the time spent stepping *iterable* to a record."""

    def __init__(
        self, iterable: Iterable[_T], timing: _RequestTiming, request_id: int
    ) -> None:
        self._iterator = iter(iterable)
        self._timing = timing
        self._request_id = request_id

    def __iter__(self) -> _TimedIterable[_T]:
        return self

    def __next__(self) -> _T:
        start = monotonic()
        try:
            return next(self._iterator)
        finally:
            self._timing._add_callback_time(self._request_id, monotonic() - start)


class _TimedAsyncIterable(Generic[_T]):
    """Async iterable wrapper adding the time spent stepping *iterable* to a
    record."""

    def __init__(
        self, iterable: AsyncIterator[_T], timing: _RequestTiming, request_id: int
    ) -> None:
        self._iterator = iterable
        self._timing = timing
        self._request_id = request_id

    def __aiter__(self) -> _TimedAsyncIterable[_T]:
        return self

    async def __anext__(self) -> _T:
        start = monotonic()
        try:
            return await self._iterator.__anext__()
        finally:
            self._timing._add_callback_time(self._request_id, monotonic() - start)


class _RequestTiming:
    """Records per-request lifecycle stage durations and aggregates them in
    the stats collector.

    A record is created when a request is enqueued, keyed by the unique
    :attr:`~scrapy.http.Request.id`, so concurrent requests never share
    state. Requests derived through retries or redirects get their own
    records and link to the original through
    :attr:`~scrapy.http.Request.parent_id`.
    """

    def __init__(self, crawler: Crawler) -> None:
        self._stats = crawler.stats
        self._records: dict[int, _Record] = {}

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(crawler)

    def _mark(self, request: Request, point: str) -> None:
        record = self._records.get(request.id)
        if record is not None:
            record.points[point] = monotonic()

    def _add_callback_time(self, request_id: int, seconds: float) -> None:
        # The record is dropped only after the callback output is consumed.
        record = self._records[request_id]
        record.callback_time += seconds

    def enqueued(self, request: Request) -> None:
        """Record the request entering the scheduler."""
        self._records[request.id] = _Record(points={_ENQUEUED: monotonic()})

    def dequeued(self, request: Request) -> None:
        """Record the request leaving the scheduler."""
        self._mark(request, _DEQUEUED)

    def fetch_started(self, request: Request) -> None:
        """Record the request entering the downloader."""
        self._mark(request, _FETCH_STARTED)

    def reached_downloader(self, request: Request) -> None:
        """Record the request reaching a downloader slot."""
        self._mark(request, _REACHED_DOWNLOADER)

    def download_started(self, request: Request) -> None:
        """Record the download handler starting for the request."""
        self._mark(request, _DOWNLOAD_STARTED)

    def response_downloaded(self, request: Request) -> None:
        """Record the download handler returning a response.

        The download stage reuses the value of the ``download_latency``
        request meta key when the handler set it, so its semantics stay
        identical to those of the meta key.
        """
        record = self._records.get(request.id)
        if record is None:
            return
        record.points[_RESPONSE_DOWNLOADED] = monotonic()
        latency = request.meta.get("download_latency")
        if isinstance(latency, (int, float)):
            record.download = float(latency)

    def response_received(self, request: Request) -> None:
        """Record the engine receiving the response from the downloader."""
        self._mark(request, _RESPONSE_RECEIVED)

    def scrape_started(self, request: Request) -> None:
        """Record the callback or errback starting in the scraper."""
        self._mark(request, _SCRAPE_STARTED)

    @overload
    def time_callback_output(
        self, request: Request, output: AsyncIterator[_T], started: float
    ) -> AsyncIterator[_T]: ...

    @overload
    def time_callback_output(
        self, request: Request, output: Iterable[_T], started: float
    ) -> Iterable[_T]: ...

    def time_callback_output(
        self,
        request: Request,
        output: Iterable[_T] | AsyncIterator[_T],
        started: float,
    ) -> Iterable[_T] | AsyncIterator[_T]:
        """Return *output* wrapped so the time spent producing its elements
        is added to the callback stage of the request record.

        *started* is the time right before the callback or errback was
        invoked, which covers the call itself and awaiting its result.
        """
        # Only scheduled requests reach the scraper, so they all have a record.
        record = self._records[request.id]
        record.callback_started = True
        record.callback_time += monotonic() - started
        if isinstance(output, AsyncIterator):
            return _TimedAsyncIterable(output, self, request.id)
        return _TimedIterable(output, self, request.id)

    def finish_request(self, request: Request) -> None:
        """Aggregate the recorded stages of the request and drop its record."""
        record = self._records.pop(request.id, None)
        if record is None:
            return
        points = record.points
        stages: dict[str, float] = {}

        def span(stage: str, start: str, end: str) -> None:
            if start in points and end in points:
                stages[stage] = points[end] - points[start]

        span(SCHEDULER_WAIT, _ENQUEUED, _DEQUEUED)
        if _REACHED_DOWNLOADER in points:
            span(
                DOWNLOADER_REQUEST_MIDDLEWARE,
                _FETCH_STARTED,
                _REACHED_DOWNLOADER,
            )
        else:
            # A downloader middleware provided the response (e.g. an HTTP
            # cache hit), so the request never reached a slot.
            span(
                DOWNLOADER_REQUEST_MIDDLEWARE,
                _FETCH_STARTED,
                _RESPONSE_RECEIVED,
            )
        span(DOWNLOADER_SLOT_WAIT, _REACHED_DOWNLOADER, _DOWNLOAD_STARTED)
        if record.download is not None:
            stages[DOWNLOAD] = record.download
        else:
            span(DOWNLOAD, _DOWNLOAD_STARTED, _RESPONSE_DOWNLOADED)
        span(
            DOWNLOADER_RESPONSE_MIDDLEWARE,
            _RESPONSE_DOWNLOADED,
            _RESPONSE_RECEIVED,
        )
        span(SCRAPER_WAIT, _RESPONSE_RECEIVED, _SCRAPE_STARTED)
        if record.callback_started:
            stages[CALLBACK] = record.callback_time
        # Every finalized request was dequeued, so at least that span exists.
        assert stages
        domain = self._domain(request)
        for stage, seconds in stages.items():
            self._store(domain, stage, seconds)
        request.meta[REQUEST_TIMING_META_KEY] = stages

    def discard_request(self, request: Request) -> None:
        """Drop the record of a request without aggregating it."""
        self._records.pop(request.id, None)

    def record_item_pipeline(
        self, response: Response | Failure | None, seconds: float
    ) -> None:
        """Record the time an item spent in the item pipelines."""
        request = getattr(response, "request", None)
        domain = self._domain(request) if request is not None else ""
        self._store(domain, ITEM_PIPELINE, seconds)

    @staticmethod
    def _domain(request: Request) -> str:
        slot = request.meta.get("download_slot")
        if slot is not None:
            return str(slot)
        return urlparse_cached(request).hostname or ""

    def _store(self, domain: str, stage: str, seconds: float) -> None:
        stats = self._stats
        for prefix in (
            f"timing/stage/{stage}",
            f"timing/domain/{domain}/{stage}",
        ):
            stats.inc_value(f"{prefix}/count")
            stats.set_value(
                f"{prefix}/total", stats.get_value(f"{prefix}/total", 0.0) + seconds
            )
            stats.min_value(f"{prefix}/min", seconds)
            stats.max_value(f"{prefix}/max", seconds)
