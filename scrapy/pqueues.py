from __future__ import annotations

import hashlib
import heapq
import logging
import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

from scrapy.utils.misc import build_from_crawler

if TYPE_CHECKING:
    from collections.abc import Iterable

    from scrapy import Request
    from scrapy.core.downloader import Downloader
    from scrapy.crawler import Crawler

# (enqueue time in time.time() seconds, dequeue rounds at enqueue)
_AgeEntry = tuple[float, int]

logger = logging.getLogger(__name__)


def _path_safe(text: str) -> str:
    """
    Return a filesystem-safe version of a string ``text``

    >>> _path_safe('simple.org').startswith('simple.org')
    True
    >>> _path_safe('dash-underscore_.org').startswith('dash-underscore_.org')
    True
    >>> _path_safe('some@symbol?').startswith('some_symbol_')
    True
    """
    pathable_slot = "".join([c if c.isalnum() or c in "-._" else "_" for c in text])
    # as we replace some letters we can get collision for different slots
    # add we add unique part
    unique_slot = hashlib.md5(text.encode("utf8")).hexdigest()  # noqa: S324
    return f"{pathable_slot}-{unique_slot}"


class QueueProtocol(Protocol):
    """Protocol for downstream queues of ``ScrapyPriorityQueue``."""

    def push(self, request: Request) -> None: ...

    def pop(self) -> Request | None: ...

    def close(self) -> None: ...

    def __len__(self) -> int: ...


class PriorityQueueProtocol(Protocol):
    """Interface of :setting:`SCHEDULER_PRIORITY_QUEUE` classes.

    :attr:`changed`, :attr:`last_pop_age`, :attr:`last_pop_boost`,
    :meth:`next_priority` and :meth:`state` are optional.
    """

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        downstream_queue_cls: type[QueueProtocol],
        key: str,
        startprios: Any = (),
        *,
        start_queue_cls: type[QueueProtocol] | None = None,
    ) -> Self:
        """Return a priority queue that stores requests in instances of
        *downstream_queue_cls*, or of *start_queue_cls* for :ref:`start
        requests <start-requests>` if it is not ``None``.

        *key* is the directory where the queue persists its requests, or an
        empty string for an in-memory queue. *startprios* is what
        :meth:`close` returned the last time the job stopped.
        """

    def push(self, request: Request) -> None:
        """Store *request*."""

    def pop(self) -> Request | None:
        """Remove and return the next request, or return ``None`` if the queue
        is empty.
        """

    def close(self) -> Any:
        """Close the queue and return a JSON-serializable state to pass as
        *startprios* to :meth:`from_crawler` when resuming the job.
        """

    def __len__(self) -> int:
        """Return the number of stored requests."""

    changed: bool
    """Whether the return value of :meth:`state` changed since this
    attribute was last set to ``False``.

    With :setting:`JOBDIR_SYNC_EVERY`, the scheduler persists the state while
    the job runs, and resets this attribute after persisting it.
    """

    last_pop_boost: int
    """Priority points added to the request returned by the last
    :meth:`pop` through request starvation protection, or ``0`` if the
    request kept its declared priority.

    Queues that do not implement starvation protection do not need this
    attribute; the scheduler then assumes ``0``.
    """

    last_pop_age: float | None
    """Seconds the request returned by the last :meth:`pop` spent queued, or
    ``None`` if it is unknown (e.g. the queue was resumed from a state
    without queuing-age data).

    Queues that do not track queuing age do not need this attribute; the
    scheduler then assumes ``None``.
    """

    def next_priority(self) -> int | None:
        """Return the priority of the request that :meth:`pop` would return
        next, or ``None`` if the queue is empty.

        The scheduler pops requests from whichever of its memory and disk
        queues returns the lowest value, or from its memory queue on a tie, so
        any scale works as long as lower values pop first. Without this
        method, the scheduler pops memory requests first.
        """

    def state(self) -> Any:
        """Return the same as :meth:`close`, without closing the queue."""


class ScrapyPriorityQueue:
    """Priority queue that pops requests from highest to lowest
    :attr:`~scrapy.Request.priority`, in :ref:`request order <request-order>`
    within a priority.

    Request starvation protection
    ==============================

    When :setting:`SCHEDULER_PRIORITY_BOOST_AGE` or
    :setting:`SCHEDULER_PRIORITY_BOOST_ROUNDS` is set, requests of a given
    priority that wait too long are scheduled at a higher effective priority.
    Every additional threshold interval adds another
    :setting:`SCHEDULER_PRIORITY_BOOST_STEP` priority points, up to
    :setting:`SCHEDULER_PRIORITY_BOOST_CAP`. Boosting is virtual: the
    :attr:`~scrapy.Request.priority` attribute of requests is never modified,
    and the boost no longer applies once a request is popped. Requests
    re-enqueued later (e.g. retries) are new requests that start waiting from
    zero.

    Requests share one downstream queue per declared priority, so the whole
    priority starts being boosted as soon as its oldest waiting request
    crosses a threshold; the request order inside that queue does not change.
    :ref:`Start requests <start-requests>` are never boosted. With both
    thresholds left at their default of ``0``, the dequeue order is the same
    as without starvation protection.

    Disk persistence
    ================

    .. warning:: The files that this class generates on disk are an
        implementation detail, and may change without a warning in a future
        version of Scrapy. Do not rely on the following information for
        anything other than debugging purposes.

    When a component instantiates this class with a non-empty *key* argument,
    *key* is used as a persistence directory.

    For every request enqueued, this class checks:

    -   Whether the request is a :ref:`start request <start-requests>` or not.

    -   The :data:`~scrapy.Request.priority` of the request.

    For each combination of the above seen, this class creates an instance of
    *downstream_queue_cls* (or *start_queue_cls* for start requests if it was
    passed) with *key* set to a subdirectory of the persistence directory,
    named as the negated request priority (e.g. ``-1``), with an ``s`` suffix
    in case of a start request (e.g. ``-1s``).

    When request starvation protection is enabled, the state returned by
    :meth:`state` also stores the queuing age of every request (enqueue time,
    dequeue rounds at enqueue, the current dequeue-round counter), so that it
    resumes when the job is resumed.
    """

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        downstream_queue_cls: type[QueueProtocol],
        key: str,
        startprios: Iterable[int] | dict[str, Any] = (),
        *,
        start_queue_cls: type[QueueProtocol] | None = None,
    ) -> Self:
        return cls(
            crawler,
            downstream_queue_cls,
            key,
            startprios,
            start_queue_cls=start_queue_cls,
        )

    def __init__(
        self,
        crawler: Crawler,
        downstream_queue_cls: type[QueueProtocol],
        key: str,
        startprios: Iterable[int] | dict[str, Any] = (),
        *,
        start_queue_cls: type[QueueProtocol] | None = None,
    ):
        self.crawler: Crawler = crawler
        self.downstream_queue_cls: type[QueueProtocol] = downstream_queue_cls
        self._start_queue_cls: type[QueueProtocol] | None = start_queue_cls
        self.key: str = key
        self.queues: dict[int, QueueProtocol] = {}
        self._start_queues: dict[int, QueueProtocol] = {}
        self.curprio: int | None = None
        self.changed: bool = False
        settings = crawler.settings
        self._boost_age: float = settings.getfloat("SCHEDULER_PRIORITY_BOOST_AGE")
        self._boost_rounds: int = settings.getint("SCHEDULER_PRIORITY_BOOST_ROUNDS")
        self._boost_step: int = settings.getint("SCHEDULER_PRIORITY_BOOST_STEP")
        self._boost_cap: int = settings.getint("SCHEDULER_PRIORITY_BOOST_CAP")
        if self._boost_age < 0 or self._boost_rounds < 0:
            raise ValueError(
                "SCHEDULER_PRIORITY_BOOST_AGE and "
                "SCHEDULER_PRIORITY_BOOST_ROUNDS must not be negative"
            )
        if self._boost_step < 1 or self._boost_cap < 1:
            raise ValueError(
                "SCHEDULER_PRIORITY_BOOST_STEP and "
                "SCHEDULER_PRIORITY_BOOST_CAP must be greater than 0"
            )
        self._boost_enabled: bool = self._boost_age > 0 or self._boost_rounds > 0
        self._max_boost_levels: int = self._boost_cap // self._boost_step
        # Per priority bucket, request id -> (enqueue time, rounds at
        # enqueue) for non-start requests. The request id heap gives the
        # oldest entry: ids start with the enqueue time in milliseconds.
        self._ages: dict[int, dict[int, _AgeEntry]] = {}
        self._age_heaps: dict[int, list[int]] = {}
        self._rounds: int = 0
        # Information about the last pop(), read by the scheduler for stats.
        self.last_pop_boost: int = 0
        self.last_pop_age: float | None = None
        self.init_prios(startprios)

    def init_prios(self, startprios: Iterable[int] | dict[str, Any]) -> None:
        if not startprios:
            return

        ages: dict[str, Any] = {}
        if isinstance(startprios, dict):
            prios: Iterable[int] = startprios.get("p", ())
            ages = startprios.get("a", {})
            self._rounds = int(startprios.get("r", 0))
        else:
            prios = startprios

        for priority in prios:
            q = self.qfactory(priority)
            if q:
                self.queues[priority] = q
            else:
                q.close()
                self.changed = True
            if self._start_queue_cls:
                q = self._sqfactory(priority)
                if q:
                    self._start_queues[priority] = q
                else:
                    q.close()
                    self.changed = True

        for raw_key, entries in ages.items():
            bucket_key = int(raw_key)
            if bucket_key not in self.queues:
                # The downstream queue for these requests no longer exists.
                continue
            bucket_ages = {
                int(request_id): (float(enqueue_time), int(enqueue_round))
                for request_id, enqueue_time, enqueue_round in entries
            }
            self._ages[bucket_key] = bucket_ages
            heap = list(bucket_ages)
            heapq.heapify(heap)
            self._age_heaps[bucket_key] = heap

    def qfactory(self, key: int) -> QueueProtocol:
        return build_from_crawler(
            self.downstream_queue_cls,
            self.crawler,
            self.key + "/" + str(key),
        )

    def _sqfactory(self, key: int) -> QueueProtocol:
        assert self._start_queue_cls is not None
        return build_from_crawler(
            self._start_queue_cls,
            self.crawler,
            f"{self.key}/{key}s",
        )

    def priority(self, request: Request) -> int:
        return -request.priority

    def push(self, request: Request) -> None:
        priority = self.priority(request)
        is_start_request = request.meta.get("is_start_request", False)
        if is_start_request and self._start_queue_cls:
            if priority not in self._start_queues:
                self._start_queues[priority] = self._sqfactory(priority)
                self.changed = True
            q = self._start_queues[priority]
            q.push(request)  # this may fail (eg. serialization error)
            return
        if priority not in self.queues:
            self.queues[priority] = self.qfactory(priority)
            self.changed = True
        q = self.queues[priority]
        q.push(request)  # this may fail (eg. serialization error)
        # Bookkeeping only after the request is stored downstream, so a
        # failed push (e.g. a serialization error) leaves no age entry.
        self._ages.setdefault(priority, {})[request.id] = (
            time.time(),
            self._rounds,
        )
        heapq.heappush(self._age_heaps.setdefault(priority, []), request.id)
        if self._boost_enabled:
            # The aging state is part of the persisted state.
            self.changed = True

    def pop(self) -> Request | None:
        while True:
            selection = self._select_next()
            if selection is None:
                self.curprio = None
                return None
            is_start, key, effective_key = selection
            self.curprio = effective_key
            q = self._start_queues.get(key) if is_start else self.queues.get(key)
            # _select_next() only lists keys of queues that hold requests.
            assert q is not None
            request = q.pop()
            if request is not None:
                # Account before the drained bucket drops its age ledger.
                self._after_pop(is_start, key, request)
            if not q:
                if is_start:
                    del self._start_queues[key]
                else:
                    del self.queues[key]
                    self._ages.pop(key, None)
                    self._age_heaps.pop(key, None)
                q.close()
                self.changed = True
            if request is not None:
                return request

    def _select_next(self) -> tuple[bool, int, int] | None:
        """Return ``(is_start, declared key, effective key)`` for the next
        request to pop, or ``None`` when all queues are empty.

        Normal queues pop at their starvation-boosted effective key, and take
        precedence over start queues on a tie. Start queues always use their
        declared key, as start requests are never boosted.
        """
        now = time.time()
        normal_effective_key: int | None = None
        normal_key: int | None = None
        for key in self.queues:
            effective_key = key - self._bucket_level(key, now) * self._boost_step
            if (
                normal_effective_key is None
                or effective_key < normal_effective_key
                or (
                    effective_key == normal_effective_key
                    and (normal_key is None or key < normal_key)
                )
            ):
                normal_effective_key = effective_key
                normal_key = key
        start_key = min(self._start_queues) if self._start_queues else None
        if (
            normal_effective_key is not None
            and normal_key is not None
            and (start_key is None or normal_effective_key <= start_key)
        ):
            return False, normal_key, normal_effective_key
        if start_key is not None:
            return True, start_key, start_key
        return None

    def _oldest_entry(self, key: int) -> _AgeEntry | None:
        ages = self._ages.get(key)
        if not ages:
            return None
        heap = self._age_heaps[key]
        while heap and heap[0] not in ages:
            heapq.heappop(heap)
        if not heap:
            return None
        return ages[heap[0]]

    def _bucket_level(self, key: int, now: float) -> int:
        if not self._boost_enabled:
            return 0
        oldest = self._oldest_entry(key)
        if oldest is None:
            return 0
        enqueue_time, enqueue_round = oldest
        level = 0
        if self._boost_age:
            waited_age = now - enqueue_time
            if waited_age >= self._boost_age:
                level = int(waited_age // self._boost_age)
        if self._boost_rounds:
            waited_rounds = self._rounds - enqueue_round
            if waited_rounds >= self._boost_rounds:
                level = max(level, waited_rounds // self._boost_rounds)
        return min(level, self._max_boost_levels)

    def _after_pop(self, is_start: bool, key: int, request: Request) -> None:
        now = time.time()
        self.last_pop_boost = 0
        self.last_pop_age = None
        if not is_start:
            ages = self._ages.get(key)
            if ages is not None:
                self.last_pop_boost = self._bucket_level(key, now) * self._boost_step
                entry = ages.pop(request.id, None)
                if entry is not None:
                    self.last_pop_age = max(0.0, now - entry[0])
            if self._boost_enabled:
                # The aging state is part of the persisted state.
                self.changed = True
        self._rounds += 1

    def peek(self) -> Request | None:
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        selection = self._select_next()
        if selection is None:
            return None
        is_start, key, _effective_key = selection
        queue = self._start_queues.get(key) if is_start else self.queues.get(key)
        assert queue is not None
        # Protocols can't declare optional members
        return cast("Request", queue.peek())  # type: ignore[attr-defined]

    def next_priority(self) -> int | None:
        selection = self._select_next()
        if selection is None:
            self.curprio = None
            return None
        self.curprio = selection[2]
        return self.curprio

    def state(self) -> list[int] | dict[str, Any]:
        prios = sorted(self.queues.keys() | self._start_queues.keys())
        if not self._boost_enabled:
            return prios
        ages = {
            str(key): sorted(
                [request_id, enqueue_time, enqueue_round]
                for request_id, (enqueue_time, enqueue_round) in bucket.items()
            )
            for key, bucket in self._ages.items()
            if bucket
        }
        return {"p": prios, "a": ages, "r": self._rounds}

    def close(self) -> list[int] | dict[str, Any]:
        active = self.state()
        for queues in (self.queues, self._start_queues):
            for q in queues.values():
                q.close()
        return active

    def __len__(self) -> int:
        return (
            sum(
                len(x)
                for queues in (self.queues, self._start_queues)
                for x in queues.values()
            )
            if self.queues or self._start_queues
            else 0
        )


class DownloaderInterface:
    def __init__(self, crawler: Crawler):
        self.downloader: Downloader = crawler.engine.downloader

    def stats(self, possible_slots: Iterable[str]) -> list[tuple[int, str]]:
        return [(self._active_downloads(slot), slot) for slot in possible_slots]

    def get_slot_key(self, request: Request) -> str:
        return self.downloader.get_slot_key(request)

    def _active_downloads(self, slot: str) -> int:
        """Return a number of requests in a Downloader for a given slot"""
        if slot not in self.downloader.slots:
            return 0
        return len(self.downloader.slots[slot].active)


class DownloaderAwarePriorityQueue:
    """PriorityQueue which takes Downloader activity into account:
    domains (slots) with the least amount of active downloads are dequeued
    first.

    Disk persistence
    ================

    .. warning:: The files that this class generates on disk are an
        implementation detail, and may change without a warning in a future
        version of Scrapy. Do not rely on the following information for
        anything other than debugging purposes.

    When a component instantiates this class with a non-empty *key* argument,
    *key* is used as a persistence directory, and inside that directory this
    class creates a subdirectory per download slot (domain).

    Those subdirectories are named after the corresponding download slot, with
    path-unsafe characters replaced by underscores and an MD5 hash suffix to
    avoid collisions.

    For each download slot, this class creates an instance of
    :class:`ScrapyPriorityQueue` with the download slot subdirectory as *key*
    and its own *downstream_queue_cls*.

    Request starvation protection (see
    :setting:`SCHEDULER_PRIORITY_BOOST_AGE`) is applied independently inside
    every slot queue: the dequeue-round threshold counts requests dequeued
    from the same slot. Slot selection still follows downloader activity.
    """

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        downstream_queue_cls: type[QueueProtocol],
        key: str,
        startprios: dict[str, Any] | None = None,
        *,
        start_queue_cls: type[QueueProtocol] | None = None,
    ) -> Self:
        return cls(
            crawler,
            downstream_queue_cls,
            key,
            startprios,
            start_queue_cls=start_queue_cls,
        )

    def __init__(
        self,
        crawler: Crawler,
        downstream_queue_cls: type[QueueProtocol],
        key: str,
        slot_startprios: dict[str, Any] | None = None,
        *,
        start_queue_cls: type[QueueProtocol] | None = None,
    ):
        if crawler.settings.getint("CONCURRENT_REQUESTS_PER_IP") != 0:
            raise ValueError(
                f'"{self.__class__}" does not support CONCURRENT_REQUESTS_PER_IP'
            )

        if slot_startprios and not isinstance(slot_startprios, dict):
            raise ValueError(
                "DownloaderAwarePriorityQueue accepts "
                "``slot_startprios`` as a dict; "
                f"{slot_startprios.__class__!r} instance "
                "is passed. Most likely, it means the state is "
                "created by an incompatible priority queue. "
                "Only a crawl started with the same priority "
                "queue class can be resumed."
            )

        self._downloader_interface: DownloaderInterface = DownloaderInterface(crawler)
        self.downstream_queue_cls: type[QueueProtocol] = downstream_queue_cls
        self._start_queue_cls: type[QueueProtocol] | None = start_queue_cls
        self.key: str = key
        self.crawler: Crawler = crawler

        self.pqueues: dict[str, ScrapyPriorityQueue] = {}  # slot -> priority queue
        self._last_selected_slot: str | None = None
        self.changed: bool = False
        # Information about the last pop(), read by the scheduler for stats.
        self.last_pop_boost: int = 0
        self.last_pop_age: float | None = None
        if slot_startprios:
            for slot, startprios in slot_startprios.items():
                self.pqueues[slot] = self.pqfactory(slot, startprios)
                self._collect_change(self.pqueues[slot])

    def _next_slot(self, stats: list[tuple[int, str]], *, update_state: bool) -> str:
        last = self._last_selected_slot
        min_active: int | None = None
        best_slot: str | None = None
        best_slot_after_last: str | None = None
        for active, slot in stats:
            if min_active is None or active < min_active:
                min_active = active
                best_slot = slot
                best_slot_after_last = None
                if last is not None and slot > last:
                    best_slot_after_last = slot
            elif active == min_active:
                if best_slot is None or slot < best_slot:
                    best_slot = slot
                if (
                    last is not None
                    and slot > last
                    and (best_slot_after_last is None or slot < best_slot_after_last)
                ):
                    best_slot_after_last = slot
        assert best_slot is not None
        slot = best_slot_after_last if best_slot_after_last is not None else best_slot
        if update_state:
            self._last_selected_slot = slot
        return slot

    def pqfactory(
        self, slot: str, startprios: Iterable[int] | dict[str, Any] = ()
    ) -> ScrapyPriorityQueue:
        return ScrapyPriorityQueue(
            self.crawler,
            self.downstream_queue_cls,
            self.key + "/" + _path_safe(slot),
            startprios,
            start_queue_cls=self._start_queue_cls,
        )

    def _collect_change(self, queue: ScrapyPriorityQueue) -> None:
        if queue.changed:
            queue.changed = False
            self.changed = True

    def pop(self) -> Request | None:
        stats = self._downloader_interface.stats(self.pqueues)

        self.last_pop_boost = 0
        self.last_pop_age = None
        if not stats:
            return None

        slot = self._next_slot(stats, update_state=True)
        queue = self.pqueues[slot]
        request = queue.pop()
        self.last_pop_boost = queue.last_pop_boost
        self.last_pop_age = queue.last_pop_age
        self._collect_change(queue)
        if len(queue) == 0:
            del self.pqueues[slot]
            queue.close()
            self.changed = True
            if self.key:
                # Reclaim the slot directory; rmdir leaves it alone if the
                # downstream queues did not remove all their files.
                with suppress(OSError):
                    Path(self.key, _path_safe(slot)).rmdir()
        return request

    def push(self, request: Request) -> None:
        slot = self._downloader_interface.get_slot_key(request)
        if slot not in self.pqueues:
            self.pqueues[slot] = self.pqfactory(slot)
            self.changed = True
        queue = self.pqueues[slot]
        queue.push(request)
        self._collect_change(queue)

    def peek(self) -> Request | None:
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        stats = self._downloader_interface.stats(self.pqueues)
        if not stats:
            return None
        slot = self._next_slot(stats, update_state=False)
        queue = self.pqueues[slot]
        return queue.peek()

    def next_priority(self) -> int | None:
        if not self.pqueues:
            return None
        stats = self._downloader_interface.stats(self.pqueues)
        slot = self._next_slot(stats, update_state=False)
        return self.pqueues[slot].next_priority()

    def state(self) -> dict[str, Any]:
        return {slot: queue.state() for slot, queue in self.pqueues.items()}

    def close(self) -> dict[str, Any]:
        active = self.state()
        for queue in self.pqueues.values():
            queue.close()
        self.pqueues.clear()
        return active

    def __len__(self) -> int:
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0

    def __contains__(self, slot: str) -> bool:
        return slot in self.pqueues
