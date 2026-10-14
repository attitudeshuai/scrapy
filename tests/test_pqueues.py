from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest

from scrapy.core.downloader import Downloader
from scrapy.http.request import Request
from scrapy.pqueues import (
    DownloaderAwarePriorityQueue,
    QueueProtocol,
    ScrapyPriorityQueue,
    _path_safe,
)
from scrapy.spiders import Spider
from scrapy.squeues import (
    FifoMemoryQueue,
    LifoMemoryQueue,
    PickleFifoDiskQueue,
    PickleLifoDiskQueue,
)
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.test import get_crawler
from tests.utils.downloader import MockDownloader

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from scrapy.crawler import Crawler


def _pop(queue: ScrapyPriorityQueue | DownloaderAwarePriorityQueue) -> Request:
    request = queue.pop()
    assert request is not None
    return request


def _peek(queue: ScrapyPriorityQueue | DownloaderAwarePriorityQueue) -> Request:
    request = queue.peek()
    assert request is not None
    return request


class TestPriorityQueue:
    @pytest.fixture
    def crawler(self) -> Crawler:
        return get_crawler(Spider)

    def test_queue_push_pop_one(self, crawler: Crawler, tmp_path: Path) -> None:
        queue = build_from_crawler(
            ScrapyPriorityQueue, crawler, FifoMemoryQueue, str(tmp_path)
        )
        assert queue.pop() is None
        assert len(queue) == 0
        req1 = Request("https://example.org/1", priority=1)
        queue.push(req1)
        assert len(queue) == 1
        dequeued = _pop(queue)
        assert len(queue) == 0
        assert dequeued.url == req1.url
        assert dequeued.priority == req1.priority
        assert not queue.close()

    def test_peek(self, crawler: Crawler, tmp_path: Path) -> None:
        queue = build_from_crawler(
            ScrapyPriorityQueue, crawler, FifoMemoryQueue, str(tmp_path)
        )
        assert len(queue) == 0
        assert queue.peek() is None
        req1 = Request("https://example.org/1")
        req2 = Request("https://example.org/2")
        req3 = Request("https://example.org/3")
        queue.push(req1)
        queue.push(req2)
        queue.push(req3)
        assert len(queue) == 3
        assert _peek(queue).url == req1.url
        assert _pop(queue).url == req1.url
        assert len(queue) == 2
        assert _peek(queue).url == req2.url
        assert _pop(queue).url == req2.url
        assert len(queue) == 1
        assert _peek(queue).url == req3.url
        assert _pop(queue).url == req3.url
        assert not queue.close()

    def test_init_prios_with_start_queue(
        self, crawler: Crawler, tmp_path: Path
    ) -> None:
        queue = build_from_crawler(
            ScrapyPriorityQueue,
            crawler,
            PickleFifoDiskQueue,
            str(tmp_path),
            start_queue_cls=PickleFifoDiskQueue,
        )
        req = Request("https://example.org/", meta={"is_start_request": True})
        queue.push(req)
        startprios = queue.close()

        queue2 = build_from_crawler(
            ScrapyPriorityQueue,
            crawler,
            PickleFifoDiskQueue,
            str(tmp_path),
            startprios,
            start_queue_cls=PickleFifoDiskQueue,
        )
        assert len(queue2) == 1
        assert _pop(queue2).url == req.url
        queue2.close()

    def test_queue_push_pop_priorities(self, crawler: Crawler, tmp_path: Path) -> None:
        queue = build_from_crawler(
            ScrapyPriorityQueue, crawler, FifoMemoryQueue, str(tmp_path), [-1, -2, -3]
        )
        assert queue.pop() is None
        assert len(queue) == 0
        req1 = Request("https://example.org/1", priority=1)
        req2 = Request("https://example.org/2", priority=2)
        req3 = Request("https://example.org/3", priority=3)
        queue.push(req1)
        queue.push(req2)
        queue.push(req3)
        assert len(queue) == 3
        dequeued = _pop(queue)
        assert len(queue) == 2
        assert dequeued.url == req3.url
        assert dequeued.priority == req3.priority
        assert set(queue.close()) == {-1, -2}

    def test_next_priority_empty_startprios(self, crawler: Crawler) -> None:
        queue = build_from_crawler(
            ScrapyPriorityQueue, crawler, FifoMemoryQueue, "", [-1]
        )
        assert queue.next_priority() is None

    def test_next_priority_with_start_queue(self, crawler: Crawler) -> None:
        queue = build_from_crawler(
            ScrapyPriorityQueue,
            crawler,
            FifoMemoryQueue,
            "",
            start_queue_cls=FifoMemoryQueue,
        )
        queue.push(Request("https://example.org/1", meta={"is_start_request": True}))
        queue.push(Request("https://example.org/2", priority=1))
        assert queue.next_priority() == -1
        queue.pop()
        assert queue.next_priority() == 0
        queue.pop()
        assert queue.next_priority() is None


class TestDownloaderAwarePriorityQueue:
    @pytest.fixture
    def downloader(self) -> MockDownloader:
        return MockDownloader()

    @pytest.fixture
    def queue(
        self, downloader: MockDownloader
    ) -> Generator[DownloaderAwarePriorityQueue]:
        crawler = get_crawler(Spider)
        crawler.engine = Mock(downloader=downloader)
        queue = build_from_crawler(
            DownloaderAwarePriorityQueue,
            crawler,
            downstream_queue_cls=FifoMemoryQueue,
            key="foo/bar",
        )
        try:
            yield queue
        finally:
            queue.close()

    def test_push_pop(self, queue: DownloaderAwarePriorityQueue) -> None:
        assert len(queue) == 0
        assert queue.pop() is None
        req1 = Request("http://www.example.com/1")
        req2 = Request("http://www.example.com/2")
        req3 = Request("http://www.example.com/3")
        queue.push(req1)
        queue.push(req2)
        queue.push(req3)
        assert len(queue) == 3
        assert _pop(queue).url == req1.url
        assert len(queue) == 2
        assert _pop(queue).url == req2.url
        assert len(queue) == 1
        assert _pop(queue).url == req3.url
        assert len(queue) == 0
        assert queue.pop() is None

    def test_peek(self, queue: DownloaderAwarePriorityQueue) -> None:
        assert len(queue) == 0
        req1 = Request("https://example.org/1")
        req2 = Request("https://example.org/2")
        req3 = Request("https://example.org/3")
        queue.push(req1)
        queue.push(req2)
        queue.push(req3)
        assert len(queue) == 3
        assert _peek(queue).url == req1.url
        assert _pop(queue).url == req1.url
        assert len(queue) == 2
        assert _peek(queue).url == req2.url
        assert _pop(queue).url == req2.url
        assert len(queue) == 1
        assert _peek(queue).url == req3.url
        assert _pop(queue).url == req3.url
        assert queue.peek() is None

    def test_tie_breaking_rotates_slots(
        self, queue: DownloaderAwarePriorityQueue
    ) -> None:
        # No active downloads are tracked in the downloader, so every slot has
        # the same score and tie-breaking must not starve a slot.
        req_a1 = Request("https://example.org/a1")
        req_a1.meta[Downloader.DOWNLOAD_SLOT] = "slot-a"
        req_b1 = Request("https://example.org/b1")
        req_b1.meta[Downloader.DOWNLOAD_SLOT] = "slot-b"
        req_a2 = Request("https://example.org/a2")
        req_a2.meta[Downloader.DOWNLOAD_SLOT] = "slot-a"
        req_b2 = Request("https://example.org/b2")
        req_b2.meta[Downloader.DOWNLOAD_SLOT] = "slot-b"

        for request in (req_a1, req_b1, req_a2, req_b2):
            queue.push(request)

        slots = [
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
        ]

        assert slots == ["slot-a", "slot-b", "slot-a", "slot-b"]

    def test_tie_breaking_keeps_rotation_after_selected_slot_is_deleted(
        self, queue: DownloaderAwarePriorityQueue
    ) -> None:
        # If the selected slot becomes empty, rotation should continue from
        # that slot marker to avoid restarting from the smallest slot.
        req_a1 = Request("https://example.org/a1")
        req_a1.meta[Downloader.DOWNLOAD_SLOT] = "slot-a"
        req_a2 = Request("https://example.org/a2")
        req_a2.meta[Downloader.DOWNLOAD_SLOT] = "slot-a"
        req_b1 = Request("https://example.org/b1")
        req_b1.meta[Downloader.DOWNLOAD_SLOT] = "slot-b"
        req_c1 = Request("https://example.org/c1")
        req_c1.meta[Downloader.DOWNLOAD_SLOT] = "slot-c"

        for request in (req_a1, req_a2, req_b1, req_c1):
            queue.push(request)

        slots = [
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
            _pop(queue).meta[Downloader.DOWNLOAD_SLOT],
        ]

        assert slots == ["slot-a", "slot-b", "slot-c", "slot-a"]

    def test_pop_prefers_slot_with_fewer_active_downloads(
        self, downloader: MockDownloader, queue: DownloaderAwarePriorityQueue
    ) -> None:
        req_a = Request("https://example.org/a")
        req_a.meta[Downloader.DOWNLOAD_SLOT] = "slot-a"
        req_b = Request("https://example.org/b")
        req_b.meta[Downloader.DOWNLOAD_SLOT] = "slot-b"
        req_c = Request("https://example.org/c")
        req_c.meta[Downloader.DOWNLOAD_SLOT] = "slot-c"

        for req in (req_a, req_b, req_c):
            queue.push(req)

        downloader.increment("slot-a")
        downloader.increment("slot-c")

        popped = _pop(queue)
        assert popped.url == req_b.url

    def test_contains(self, queue: DownloaderAwarePriorityQueue) -> None:
        req = Request("https://example.org/")
        req.meta[Downloader.DOWNLOAD_SLOT] = "example-slot"
        assert "example-slot" not in queue
        queue.push(req)
        assert "example-slot" in queue
        assert "other-slot" not in queue


def test_slot_directory_removed_when_slot_drains(tmp_path: Path) -> None:
    crawler = get_crawler(Spider)
    crawler.spider = crawler._create_spider("foo")
    crawler.engine = Mock(downloader=MockDownloader())
    queue = build_from_crawler(
        DownloaderAwarePriorityQueue,
        crawler,
        downstream_queue_cls=PickleFifoDiskQueue,
        key=str(tmp_path),
    )
    request = Request("https://example.org/1")
    slot_dir = tmp_path / _path_safe("example.org")

    queue.push(request)
    assert slot_dir.is_dir()

    assert _pop(queue).url == request.url
    assert not slot_dir.exists()

    queue.push(request)
    assert slot_dir.is_dir()
    queue.close()


@pytest.mark.parametrize(
    ("input_", "output"),
    [
        # By default, start requests are FIFO, other requests are LIFO.
        ([{}, {}], [2, 1]),
        ([{"start": True}, {"start": True}], [1, 2]),
        # Priority matters.
        ([{"priority": 1}, {"start": True}], [1, 2]),
        ([{}, {"start": True, "priority": 1}], [2, 1]),
        # For the same priority, start requests pop last.
        ([{}, {"start": True}], [1, 2]),
        ([{"start": True}, {}], [2, 1]),
    ],
)
def test_pop_order(input_: list[dict[str, Any]], output: list[int]) -> None:
    def make_url(index: int) -> str:
        return f"https://toscrape.com/{index}"

    def make_request(index: int, data: dict[str, Any]) -> Request:
        meta: dict[str, Any] = {}
        if data.get("start", False):
            meta["is_start_request"] = True
        return Request(
            url=make_url(index),
            priority=data.get("priority", 0),
            meta=meta,
        )

    input_requests = [
        make_request(index, data) for index, data in enumerate(input_, start=1)
    ]
    expected_output_urls = [make_url(index) for index in output]

    crawler = get_crawler(Spider)
    settings = crawler.settings
    queue = build_from_crawler(
        ScrapyPriorityQueue,
        crawler,
        downstream_queue_cls=load_object(settings["SCHEDULER_MEMORY_QUEUE"]),
        key="",
        start_queue_cls=load_object(settings["SCHEDULER_START_MEMORY_QUEUE"]),
    )

    for request in input_requests:
        queue.push(request)

    actual_output_urls = []
    while popped := queue.pop():
        actual_output_urls.append(popped.url)

    assert actual_output_urls == expected_output_urls


_BOOST_SETTINGS = {
    "SCHEDULER_PRIORITY_BOOST_ROUNDS": 3,
    "SCHEDULER_PRIORITY_BOOST_STEP": 2,
}


def _aging_crawler(settings: dict[str, Any] | None = None) -> Crawler:
    crawler = get_crawler(Spider, settings_dict=settings or {})
    crawler.engine = Mock(downloader=MockDownloader())
    return crawler


def _aging_queue(
    tmp_path: Path,
    downstream_queue_cls: type[QueueProtocol],
    settings: dict[str, Any],
    *,
    start_queue_cls: type[QueueProtocol] | None = None,
    state: Any = (),
) -> ScrapyPriorityQueue:
    return build_from_crawler(
        ScrapyPriorityQueue,
        _aging_crawler(settings),
        downstream_queue_cls,
        str(tmp_path),
        state,
        start_queue_cls=start_queue_cls,
    )


def _pop_with_boost_info(
    queue: ScrapyPriorityQueue | DownloaderAwarePriorityQueue,
) -> tuple[Request, int, float | None]:
    request = queue.pop()
    assert request is not None
    return request, queue.last_pop_boost, queue.last_pop_age


@pytest.mark.parametrize("downstream_queue_cls", [FifoMemoryQueue, LifoMemoryQueue])
class TestPriorityBoost:
    def test_round_threshold_boosts_bucket(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
    ) -> None:
        queue = _aging_queue(tmp_path, downstream_queue_cls, _BOOST_SETTINGS)
        low = Request("https://example.org/low", priority=-5)
        queue.push(low)
        # High-priority requests keep being produced, like a crawl whose
        # callbacks keep yielding higher-priority requests.
        highs = [Request(f"https://example.org/h{i}", priority=0) for i in range(12)]

        popped: list[Request] = []
        boosts: dict[str, int] = {}
        for request in highs:
            queue.push(request)
            popped_request = queue.pop()
            assert popped_request is not None
            popped.append(popped_request)
            boosts[popped_request.url] = queue.last_pop_boost
        low_request = queue.pop()
        assert low_request is not None
        popped.append(low_request)
        boosts[low_request.url] = queue.last_pop_boost
        while remaining := queue.pop():
            popped.append(remaining)
            boosts[remaining.url] = queue.last_pop_boost

        assert [request.url for request in popped[:9]] == [
            request.url for request in highs[:9]
        ]
        assert popped[9].url == low.url
        assert popped[9].priority == -5
        assert boosts[low.url] == 6
        assert {request.url for request in popped} == {
            request.url for request in highs
        } | {low.url}
        queue.close()

    def test_boost_keeps_order_within_bucket(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
    ) -> None:
        queue = _aging_queue(tmp_path, downstream_queue_cls, _BOOST_SETTINGS)
        low1 = Request("https://example.org/low1", priority=-5)
        low2 = Request("https://example.org/low2", priority=-5)
        queue.push(low1)
        queue.push(low2)

        popped: list[str] = []
        for index in range(12):
            queue.push(Request(f"https://example.org/h{index}", priority=0))
            request = queue.pop()
            assert request is not None
            popped.append(request.url)
        while request := queue.pop():
            popped.append(request.url)

        expected_lows = (
            [low1.url, low2.url]
            if downstream_queue_cls is FifoMemoryQueue
            else [low2.url, low1.url]
        )
        assert popped[9:11] == expected_lows
        queue.close()

    def test_cap_limits_boost(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
    ) -> None:
        settings = {
            "SCHEDULER_PRIORITY_BOOST_ROUNDS": 1,
            "SCHEDULER_PRIORITY_BOOST_STEP": 10,
            "SCHEDULER_PRIORITY_BOOST_CAP": 15,
        }
        queue = _aging_queue(tmp_path, downstream_queue_cls, settings)
        low = Request("https://example.org/low", priority=-20)
        queue.push(low)

        popped: list[str] = []
        boosts: dict[str, int] = {}
        for index in range(5):
            request = Request(f"https://example.org/h{index}", priority=0)
            queue.push(request)
            popped_request = queue.pop()
            assert popped_request is not None
            popped.append(popped_request.url)
            boosts[popped_request.url] = queue.last_pop_boost
        low_request = queue.pop()
        assert low_request is not None
        popped.append(low_request.url)
        boosts[low_request.url] = queue.last_pop_boost

        assert popped == [f"https://example.org/h{index}" for index in range(5)] + [
            low.url
        ]
        assert boosts[low.url] == 10
        queue.close()

    def test_age_threshold_boosts_bucket(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        current_time = 0.0
        monkeypatch.setattr("scrapy.pqueues.time.time", lambda: current_time)
        settings = {
            "SCHEDULER_PRIORITY_BOOST_AGE": 30,
            "SCHEDULER_PRIORITY_BOOST_STEP": 2,
        }
        queue = _aging_queue(tmp_path, downstream_queue_cls, settings)
        low = Request("https://example.org/low", priority=-5)
        queue.push(low)

        popped: list[str] = []
        boosts: dict[str, int] = {}
        ages: dict[str, float | None] = {}
        for index in range(12):
            queue.push(Request(f"https://example.org/h{index}", priority=0))
            current_time += 10
            request, boost, age = _pop_with_boost_info(queue)
            popped.append(request.url)
            boosts[request.url] = boost
            ages[request.url] = age
        while remaining := queue.pop():
            popped.append(remaining.url)

        assert popped[:8] == [f"https://example.org/h{index}" for index in range(8)]
        assert popped[8] == low.url
        assert boosts[low.url] == 6
        assert ages[low.url] == 90
        queue.close()

    def test_age_and_rounds_use_higher_level(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        current_time = 0.0
        monkeypatch.setattr("scrapy.pqueues.time.time", lambda: current_time)
        settings = {
            "SCHEDULER_PRIORITY_BOOST_AGE": 30,
            "SCHEDULER_PRIORITY_BOOST_ROUNDS": 3,
            "SCHEDULER_PRIORITY_BOOST_STEP": 2,
        }
        queue = _aging_queue(tmp_path, downstream_queue_cls, settings)
        # The low-priority request waits for 90 seconds without any pop
        # happening, so only the age threshold grants boost levels.
        low = Request("https://example.org/low", priority=-5)
        queue.push(low)
        current_time = 90.0
        high = Request("https://example.org/high", priority=0)
        queue.push(high)

        assert _pop_with_boost_info(queue)[0].url == low.url
        queue.close()

    def test_start_requests_never_boosted(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
    ) -> None:
        settings = {
            "SCHEDULER_PRIORITY_BOOST_ROUNDS": 1,
            "SCHEDULER_PRIORITY_BOOST_STEP": 1,
        }
        queue = _aging_queue(
            tmp_path,
            downstream_queue_cls,
            settings,
            start_queue_cls=FifoMemoryQueue,
        )
        start1 = Request("https://example.org/s1", meta={"is_start_request": True})
        start2 = Request("https://example.org/s2", meta={"is_start_request": True})
        normal1 = Request("https://example.org/n1")
        normal2 = Request("https://example.org/n2")
        highs = [Request(f"https://example.org/h{i}", priority=10) for i in range(5)]
        for request in (start1, start2, normal1, normal2, *highs):
            queue.push(request)

        popped: list[str] = []
        boosts: dict[str, int] = {}
        while remaining := queue.pop():
            popped.append(remaining.url)
            boosts[remaining.url] = queue.last_pop_boost

        assert popped[-2:] == [start1.url, start2.url]
        assert set(popped[:-2]) == {
            request.url for request in (*highs, normal1, normal2)
        }
        assert boosts[start1.url] == 0
        assert boosts[start2.url] == 0
        queue.close()

    def test_wait_time_tracked_without_boost_threshold(
        self,
        tmp_path: Path,
        downstream_queue_cls: type[QueueProtocol],
    ) -> None:
        queue = _aging_queue(tmp_path, downstream_queue_cls, {})
        queue.push(Request("https://example.org/1"))
        queue.pop()
        assert queue.last_pop_boost == 0
        assert queue.last_pop_age is not None
        assert queue.last_pop_age >= 0
        queue.close()


@pytest.mark.parametrize(
    "downstream_queue_cls",
    [FifoMemoryQueue, LifoMemoryQueue, PickleFifoDiskQueue, PickleLifoDiskQueue],
)
def test_aging_state_format(
    tmp_path: Path, downstream_queue_cls: type[QueueProtocol]
) -> None:
    disabled = _aging_queue(tmp_path, downstream_queue_cls, {})
    request = Request("https://example.org/1", priority=1)
    disabled.push(request)
    assert disabled.close() == [-1]

    enabled = _aging_queue(tmp_path, downstream_queue_cls, _BOOST_SETTINGS)
    enabled.push(request)
    state = enabled.close()
    assert isinstance(state, dict)
    assert state["p"] == [-1]
    assert state["r"] == 0
    assert list(state["a"]) == ["-1"]
    [[request_id, enqueue_time, enqueue_round]] = state["a"]["-1"]
    assert request_id == request.id
    assert isinstance(enqueue_time, float)
    assert enqueue_round == 0


@pytest.mark.parametrize(
    "downstream_queue_cls", [PickleFifoDiskQueue, PickleLifoDiskQueue]
)
def test_round_age_restored_from_state(
    tmp_path: Path, downstream_queue_cls: type[QueueProtocol]
) -> None:
    low = Request("https://example.org/low", priority=-5)
    first_queue = _aging_queue(tmp_path, downstream_queue_cls, _BOOST_SETTINGS)
    first_queue.push(low)
    for index in range(9):
        first_queue.push(Request(f"https://example.org/h{index}", priority=0))
    for _ in range(9):
        first_queue.pop()
    state = first_queue.close()
    assert isinstance(state, dict)
    assert state["r"] == 9

    second_queue = _aging_queue(
        tmp_path, downstream_queue_cls, _BOOST_SETTINGS, state=state
    )
    # A new high-priority request does not overtake the restored low one:
    # it already waited nine rounds, i.e. three boost levels.
    second_queue.push(Request("https://example.org/high", priority=0))
    assert _pop_with_boost_info(second_queue)[0].url == low.url
    second_queue.close()


@pytest.mark.parametrize(
    "downstream_queue_cls", [PickleFifoDiskQueue, PickleLifoDiskQueue]
)
def test_time_age_restored_from_state(
    tmp_path: Path,
    downstream_queue_cls: type[QueueProtocol],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_time = 100.0
    monkeypatch.setattr("scrapy.pqueues.time.time", lambda: current_time)
    settings = {
        "SCHEDULER_PRIORITY_BOOST_AGE": 30,
        "SCHEDULER_PRIORITY_BOOST_STEP": 10,
    }
    low = Request("https://example.org/low", priority=-5)
    first_queue = _aging_queue(tmp_path, downstream_queue_cls, settings)
    first_queue.push(low)
    state = first_queue.close()

    current_time = 200.0
    second_queue = _aging_queue(tmp_path, downstream_queue_cls, settings, state=state)
    second_queue.push(Request("https://example.org/high", priority=0))
    request, boost, age = _pop_with_boost_info(second_queue)
    assert request.url == low.url
    assert boost == 30
    assert age == 100.0
    second_queue.close()


@pytest.mark.parametrize(
    "downstream_queue_cls", [PickleFifoDiskQueue, PickleLifoDiskQueue]
)
def test_legacy_list_state_is_accepted(
    tmp_path: Path, downstream_queue_cls: type[QueueProtocol]
) -> None:
    request = Request("https://example.org/1")

    # A state produced with boosting disabled loads with it enabled, but the
    # restored requests have no queuing age.
    old_queue = _aging_queue(tmp_path, downstream_queue_cls, {})
    old_queue.push(request)
    state = old_queue.close()
    assert state == [0]
    resumed = _aging_queue(tmp_path, downstream_queue_cls, _BOOST_SETTINGS, state=state)
    popped, boost, age = _pop_with_boost_info(resumed)
    assert popped.url == request.url
    assert boost == 0
    assert age is None
    resumed.push(Request("https://example.org/2"))
    _pop_with_boost_info(resumed)
    resumed.close()

    # A state produced with boosting enabled loads with it disabled, and the
    # queue reports states in the old format again.
    enabled_queue = _aging_queue(tmp_path, downstream_queue_cls, _BOOST_SETTINGS)
    enabled_queue.push(request)
    enabled_state = enabled_queue.close()
    assert isinstance(enabled_state, dict)
    disabled_queue = _aging_queue(
        tmp_path, downstream_queue_cls, {}, state=enabled_state
    )
    assert _pop_with_boost_info(disabled_queue)[0].url == request.url
    disabled_queue.push(request)
    assert disabled_queue.state() == [0]
    disabled_queue.close()


@pytest.mark.parametrize(
    "downstream_queue_cls",
    [FifoMemoryQueue, LifoMemoryQueue, PickleFifoDiskQueue, PickleLifoDiskQueue],
)
def test_ledger_matches_pushed_requests(
    tmp_path: Path, downstream_queue_cls: type[QueueProtocol]
) -> None:
    settings = {
        "SCHEDULER_PRIORITY_BOOST_ROUNDS": 2,
        "SCHEDULER_PRIORITY_BOOST_STEP": 1,
    }
    queue = _aging_queue(tmp_path, downstream_queue_cls, settings)
    pushed: list[int] = []
    popped: list[int] = []
    for index in range(60):
        request = Request(f"https://example.org/{index}", priority=index % 5 - 2)
        queue.push(request)
        pushed.append(request.id)
        if index % 3 == 0:
            popped.append(_pop(queue).id)
    while queue:
        popped.append(_pop(queue).id)

    assert sorted(popped) == sorted(pushed)
    assert len(popped) == len(set(popped))
    state = queue.close()
    assert isinstance(state, dict)
    assert state["a"] == {}


@pytest.mark.parametrize(
    "settings",
    [
        {"SCHEDULER_PRIORITY_BOOST_AGE": -1.0},
        {"SCHEDULER_PRIORITY_BOOST_ROUNDS": -1},
        {"SCHEDULER_PRIORITY_BOOST_STEP": 0},
        {"SCHEDULER_PRIORITY_BOOST_CAP": 0},
    ],
)
def test_invalid_boost_settings_raise(settings: dict[str, Any]) -> None:
    crawler = _aging_crawler(settings)
    with pytest.raises(ValueError, match="SCHEDULER_PRIORITY_BOOST"):
        build_from_crawler(ScrapyPriorityQueue, crawler, FifoMemoryQueue, "")


class TestDownloaderAwarePriorityBoost:
    @pytest.fixture
    def queue(self) -> Generator[DownloaderAwarePriorityQueue]:
        crawler = _aging_crawler(_BOOST_SETTINGS)
        queue = build_from_crawler(
            DownloaderAwarePriorityQueue,
            crawler,
            downstream_queue_cls=FifoMemoryQueue,
            key="",
        )
        try:
            yield queue
        finally:
            queue.close()

    @staticmethod
    def _slot(request: Request) -> None:
        request.meta[Downloader.DOWNLOAD_SLOT] = "slot-a"

    def test_round_threshold_boosts_within_slot(
        self, queue: DownloaderAwarePriorityQueue
    ) -> None:
        low = Request("https://example.org/low", priority=-5)
        self._slot(low)
        queue.push(low)

        popped: list[str] = []
        boosts: dict[str, int] = {}
        ages: dict[str, float | None] = {}
        for index in range(12):
            request = Request(f"https://example.org/h{index}", priority=0)
            self._slot(request)
            queue.push(request)
            popped_request, boost, age = _pop_with_boost_info(queue)
            popped.append(popped_request.url)
            boosts[popped_request.url] = boost
            ages[popped_request.url] = age
        low_request, low_boost, _ = _pop_with_boost_info(queue)
        popped.append(low_request.url)
        boosts[low_request.url] = low_boost
        while remaining := queue.pop():
            popped.append(remaining.url)

        assert popped[:9] == [f"https://example.org/h{index}" for index in range(9)]
        assert popped[9] == low.url
        assert boosts[low.url] == 6
        assert ages[popped[0]] is not None

    def test_next_priority_reflects_boost(
        self, queue: DownloaderAwarePriorityQueue
    ) -> None:
        low = Request("https://example.org/low", priority=-5)
        self._slot(low)
        queue.push(low)
        first_high = Request("https://example.org/h0", priority=0)
        self._slot(first_high)
        queue.push(first_high)
        assert queue.next_priority() == 0
        queue.pop()
        for index in range(1, 10):
            request = Request(f"https://example.org/h{index}", priority=0)
            self._slot(request)
            queue.push(request)
            if index < 9:
                queue.pop()
        assert queue.next_priority() == -1

    def test_state_contains_aging_state(
        self, queue: DownloaderAwarePriorityQueue
    ) -> None:
        request = Request("https://example.org/1", priority=1)
        self._slot(request)
        queue.push(request)
        state = queue.state()
        assert set(state) == {"slot-a"}
        assert set(state["slot-a"]) == {"p", "a", "r"}

    def test_slot_rotation_unchanged_below_threshold(self) -> None:
        crawler = _aging_crawler({"SCHEDULER_PRIORITY_BOOST_ROUNDS": 1000})
        queue = build_from_crawler(
            DownloaderAwarePriorityQueue,
            crawler,
            downstream_queue_cls=FifoMemoryQueue,
            key="",
        )
        requests = []
        for slot, index in (("a", 1), ("b", 1), ("a", 2), ("b", 2)):
            request = Request(f"https://example.org/{slot}{index}")
            request.meta[Downloader.DOWNLOAD_SLOT] = f"slot-{slot}"
            requests.append(request)
            queue.push(request)
        slots = [_pop(queue).meta[Downloader.DOWNLOAD_SLOT] for _ in range(4)]
        assert slots == ["slot-a", "slot-b", "slot-a", "slot-b"]
        queue.close()
