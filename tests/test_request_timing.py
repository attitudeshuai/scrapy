from __future__ import annotations

import shutil
import tempfile
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from scrapy import Request, Spider, signals
from scrapy.utils.asyncio import sleep
from scrapy.utils.defer import deferred_from_coro, maybe_deferred_to_future
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from twisted.python.failure import Failure

    from scrapy.http import Response
    from tests.mockserver.http import MockServer


# Stages attributed to a request, in the order they are expected to happen.
REQUEST_STAGES = (
    "scheduler_wait",
    "downloader_request_middleware",
    "downloader_slot_wait",
    "download",
    "downloader_response_middleware",
    "scraper_wait",
    "callback",
)
METRICS = ("count", "total", "min", "max")


class TimingSpider(Spider):
    name = "timing"

    def __init__(
        self, mockserver: MockServer, paths: Sequence[str] = ("/text",), **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.mockserver = mockserver
        self.paths = list(paths)

    async def start(self) -> AsyncIterator[Request]:
        for path in self.paths:
            yield Request(self.mockserver.url(path), errback=self.on_error)

    def parse(self, response: Response) -> Iterable[dict[str, int]]:
        # Deterministic work inside the (generator) callback, so that the
        # callback stage measures the generator body rather than ~0.
        total = 0
        for value in range(20000):
            total += value
        yield {"sum": total}

    async def on_error(self, failure: Failure) -> AsyncIterator[dict[str, bool]]:
        yield {"error_handled": True}


def _domain(mockserver: MockServer) -> str:
    return urlparse(mockserver.url("/")).hostname or ""


def _stage_metrics(
    stats: dict[str, Any], stage: str, domain: str | None = None
) -> dict[str, float]:
    if domain is None:
        prefix = f"timing/stage/{stage}/"
    else:
        prefix = f"timing/domain/{domain}/{stage}/"
    return {metric: stats[prefix + metric] for metric in METRICS}


async def _crawl(
    mockserver: MockServer,
    settings: dict[str, Any] | None = None,
    paths: Sequence[str] = ("/text",),
    spider_cls: type[Spider] = TimingSpider,
) -> tuple[Any, list[dict[str, Any]], dict[str, Request]]:
    # The per-request timing is written into request.meta after the item has
    # been processed, so keep references to the meta dicts and read them once
    # the crawl has finished.
    item_metas: list[dict[str, Any]] = []
    requests_seen: dict[str, Request] = {}

    crawler = get_crawler(spider_cls, settings)

    def on_item_scraped(response: Any) -> None:
        request = getattr(response, "request", None)
        if request is not None:
            item_metas.append(request.meta)

    def on_reached_downloader(request: Request) -> None:
        path = request.url.removeprefix(mockserver.url(""))
        if "503" in path and request.meta.get("retry_times"):
            requests_seen.setdefault("retry", request)
        else:
            requests_seen.setdefault(path, request)

    crawler.signals.connect(on_item_scraped, signals.item_scraped)
    crawler.signals.connect(on_reached_downloader, signals.request_reached_downloader)
    await maybe_deferred_to_future(
        crawler.crawl(mockserver=mockserver, paths=list(paths))
    )
    return crawler, item_metas, requests_seen


@coroutine_test
async def test_disabled_by_default(mockserver: MockServer) -> None:
    crawler, item_metas, _ = await _crawl(mockserver)
    assert crawler.request_timing is None
    assert all(not key.startswith("timing/") for key in crawler.stats.get_stats())
    assert len(item_metas) == 1
    assert "request_timing" not in item_metas[0]


@coroutine_test
async def test_enabled_stages_and_redirect_lineage(
    mockserver: MockServer,
) -> None:
    settings = {
        "REQUEST_TIMING_ENABLED": True,
        "ITEM_PIPELINES": {"tests.pipelines.DelayPipeline": 300},
    }
    crawler, item_metas, requests_seen = await _crawl(
        mockserver, settings=settings, paths=("/text", "/redirect")
    )
    stats = crawler.stats.get_stats()
    domain = _domain(mockserver)

    # The redirect and its target are scheduled and downloaded separately,
    # but only the target reaches a callback; each page yields one item.
    assert _stage_metrics(stats, "scheduler_wait")["count"] == 3
    assert _stage_metrics(stats, "download")["count"] == 3
    assert _stage_metrics(stats, "callback")["count"] == 2
    assert _stage_metrics(stats, "item_pipeline")["count"] == 2
    assert _stage_metrics(stats, "item_pipeline")["min"] >= 0.05

    for stage in REQUEST_STAGES:
        metrics = _stage_metrics(stats, stage)
        assert metrics["count"] >= 1
        assert metrics["total"] >= 0
        assert 0 <= metrics["min"] <= metrics["total"] / metrics["count"]
        assert metrics["total"] / metrics["count"] <= metrics["max"]
        assert _stage_metrics(stats, stage, domain) == metrics

    # Every item carries the complete stage breakdown, and the download stage
    # reuses the download_latency value of the same request.
    assert len(item_metas) == 2
    for meta in item_metas:
        timing = meta["request_timing"]
        assert set(timing) == set(REQUEST_STAGES)
        assert timing["download"] == meta["download_latency"]

    # The redirected request links to the original and was timed on its own.
    assert requests_seen["/redirected"].parent_id == requests_seen["/redirect"].id
    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}


@coroutine_test
async def test_scheduler_and_slot_waits(mockserver: MockServer) -> None:
    settings = {
        "REQUEST_TIMING_ENABLED": True,
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 0.1,
        "DOWNLOAD_DELAY_JITTER": 0,
    }
    crawler, _, _ = await _crawl(
        mockserver,
        settings=settings,
        paths=("/status?n=200&i=0", "/status?n=200&i=1", "/status?n=200&i=2"),
    )
    stats = crawler.stats.get_stats()
    domain = _domain(mockserver)

    assert _stage_metrics(stats, "scheduler_wait")["count"] == 3
    assert _stage_metrics(stats, "scheduler_wait", domain)["max"] > 0
    assert _stage_metrics(stats, "downloader_slot_wait")["count"] == 3
    assert _stage_metrics(stats, "downloader_slot_wait", domain)["max"] >= 0.09


@coroutine_test
async def test_callback_time_does_not_include_pipeline(
    mockserver: MockServer,
) -> None:
    settings = {
        "REQUEST_TIMING_ENABLED": True,
        "ITEM_PIPELINES": {"tests.pipelines.DelayPipeline": 300},
    }
    crawler, _, _ = await _crawl(mockserver, settings=settings)
    stats = crawler.stats.get_stats()

    # The pipeline sleeps 0.05s per item; the callback does not, so the two
    # stages must not be attributed to each other.
    assert _stage_metrics(stats, "callback")["max"] < 0.05
    assert _stage_metrics(stats, "item_pipeline")["min"] >= 0.05


@coroutine_test
async def test_retry_lineage(mockserver: MockServer) -> None:
    settings = {"REQUEST_TIMING_ENABLED": True, "RETRY_TIMES": 1}
    crawler, item_metas, requests_seen = await _crawl(
        mockserver, settings=settings, paths=("/status?n=503",)
    )
    stats = crawler.stats.get_stats()
    domain = _domain(mockserver)

    # The original attempt and its retry both went through the scheduler.
    assert requests_seen["retry"].parent_id == requests_seen["/status?n=503"].id
    assert _stage_metrics(stats, "scheduler_wait")["count"] == 2
    assert _stage_metrics(stats, "scheduler_wait", domain)["count"] == 2

    # The errback handled the exhausted retry, and no record was left behind.
    assert len(item_metas) == 1
    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}


class RawUrlTimingSpider(TimingSpider):
    name = "timing-raw"

    async def start(self) -> AsyncIterator[Request]:
        for url in self.paths:
            yield Request(url)


class CachedTimingSpider(TimingSpider):
    name = "timing-cached"

    async def start(self) -> AsyncIterator[Request]:
        yield Request(self.mockserver.url("/text"))

    def parse(self, response: Response) -> Iterable[Any]:
        yield {"sum": 1}
        # Issued after the first response has been cached, so it is served
        # from the cache without reaching the downloader. A different
        # callback avoids issuing the same request again forever.
        yield Request(
            self.mockserver.url("/text"),
            dont_filter=True,
            callback=self.parse_cached,
        )

    def parse_cached(self, response: Response) -> Iterable[dict[str, int]]:
        yield {"sum": 2}


class AsyncCallbackTimingSpider(TimingSpider):
    name = "timing-async"

    async def parse(self, response: Response) -> Any:
        total = 0
        for value in range(20000):
            total += value
        yield {"sum": total}


class StartItemTimingSpider(Spider):
    name = "timing-start-item"

    async def start(self) -> AsyncIterator[dict[str, str]]:
        yield {"name": "test item"}


class NoErrbackTimingSpider(TimingSpider):
    name = "timing-no-errback"

    async def start(self) -> AsyncIterator[Request]:
        for path in self.paths:
            yield Request(self.mockserver.url(path))


@coroutine_test
async def test_no_errback_records_no_callback_stage(mockserver: MockServer) -> None:
    crawler = get_crawler(NoErrbackTimingSpider, {"REQUEST_TIMING_ENABLED": True})
    await maybe_deferred_to_future(
        crawler.crawl(mockserver=mockserver, paths=["/status?n=404"])
    )
    stats = crawler.stats.get_stats()

    # The request still went through the downloader, but no callback or
    # errback ran, so no callback stage is recorded and no record leaked.
    assert "timing/stage/callback/count" not in stats
    assert _stage_metrics(stats, "scheduler_wait")["count"] == 1
    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}


@coroutine_test
async def test_download_stage_without_download_latency(
    mockserver: MockServer,
) -> None:
    # The data: URI handler does not set download_latency, so the download
    # stage is computed from the download checkpoints instead.
    crawler = get_crawler(RawUrlTimingSpider, {"REQUEST_TIMING_ENABLED": True})
    await maybe_deferred_to_future(
        crawler.crawl(mockserver=mockserver, paths=["data:text/plain,hello"])
    )
    stats = crawler.stats.get_stats()

    assert _stage_metrics(stats, "download")["count"] == 1
    assert _stage_metrics(stats, "download", "")["count"] == 1
    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}


@coroutine_test
async def test_cache_hit_skips_download_stages(mockserver: MockServer) -> None:
    tmpdir = tempfile.mkdtemp()
    settings = {
        "REQUEST_TIMING_ENABLED": True,
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_DIR": tmpdir,
        "HTTPCACHE_POLICY": "scrapy.extensions.httpcache.DummyPolicy",
        "HTTPCACHE_STORAGE": "scrapy.extensions.httpcache.FilesystemCacheStorage",
        "HTTPCACHE_IGNORE_HTTP_CODES": [],
    }
    try:
        crawler, item_metas, _ = await _crawl(
            mockserver, settings=settings, spider_cls=CachedTimingSpider
        )
    finally:
        shutil.rmtree(tmpdir)
    stats = crawler.stats.get_stats()

    # Only the first request is downloaded; the second one is served from
    # the cache before reaching a downloader slot.
    assert _stage_metrics(stats, "download")["count"] == 1
    assert _stage_metrics(stats, "downloader_slot_wait")["count"] == 1
    assert _stage_metrics(stats, "downloader_request_middleware")["count"] == 2
    cached_timing = item_metas[1]["request_timing"]
    assert "download" not in cached_timing
    assert "downloader_slot_wait" not in cached_timing
    # The cached request never reached a slot, so its domain comes from its
    # URL rather than from the download_slot meta key.
    assert _stage_metrics(stats, "callback", _domain(mockserver))["count"] == 2


@coroutine_test
async def test_async_callback_timing(mockserver: MockServer) -> None:
    crawler, _, _ = await _crawl(
        mockserver,
        settings={"REQUEST_TIMING_ENABLED": True},
        spider_cls=AsyncCallbackTimingSpider,
    )
    stats = crawler.stats.get_stats()

    assert _stage_metrics(stats, "callback")["count"] == 1
    assert _stage_metrics(stats, "callback")["max"] > 0


@coroutine_test
async def test_start_item_pipeline_timing(mockserver: MockServer) -> None:
    crawler = get_crawler(StartItemTimingSpider, {"REQUEST_TIMING_ENABLED": True})
    await maybe_deferred_to_future(crawler.crawl(mockserver=mockserver))
    stats = crawler.stats.get_stats()

    # Items yielded from start() are not tied to a request, so their pipeline
    # stage is attributed to an empty domain.
    assert _stage_metrics(stats, "item_pipeline", "")["count"] == 1
    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}


@coroutine_test
async def test_internal_download_has_no_record(mockserver: MockServer) -> None:
    # Fetching robots.txt goes through engine.download() without scheduling,
    # so it must not create or alter any timing record.
    crawler, item_metas, _ = await _crawl(
        mockserver, settings={"REQUEST_TIMING_ENABLED": True, "ROBOTSTXT_OBEY": True}
    )
    stats = crawler.stats.get_stats()

    assert len(item_metas) == 1
    assert _stage_metrics(stats, "scheduler_wait")["count"] == 1
    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}


@coroutine_test
async def test_fast_stop_discards_records(mockserver: MockServer) -> None:
    crawler = get_crawler(TimingSpider, {"REQUEST_TIMING_ENABLED": True})
    crawl_dfd = deferred_from_coro(
        crawler.crawl_async(mockserver=mockserver, paths=["/delay?n=10"])
    )
    await crawler.signals.wait_for(signals.headers_received)
    # Let the download handler start waiting for the response body.
    await sleep(0)
    await crawler.stop_async(mode="fast")
    await maybe_deferred_to_future(crawl_dfd)

    assert crawler.request_timing is not None
    assert crawler.request_timing._records == {}
