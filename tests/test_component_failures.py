from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
from twisted.python.failure import Failure

from scrapy import Spider
from scrapy.core.downloader.middleware import DownloaderMiddlewareManager
from scrapy.core.spidermw import SpiderMiddlewareManager
from scrapy.exceptions import DropItem, IgnoreRequest, ScrapyDeprecationWarning
from scrapy.extension import ExtensionManager
from scrapy.http import Request, Response
from scrapy.middleware import MiddlewareManager
from scrapy.pipelines import ItemPipelineManager
from scrapy.utils.asyncgen import as_async_generator
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.python import global_object_name
from scrapy.utils.test import get_crawler
from tests.spiders import MockServerSpider
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from tests.mockserver.http import MockServer


# Components used by the assembly failure tests.


class _FailingExtension:
    def __init__(self, crawler: Crawler) -> None:
        raise RuntimeError("the extension service is unavailable")

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> _FailingExtension:
        return cls(crawler)


class _FailingDownloaderMW:
    def __init__(self, crawler: Crawler) -> None:
        raise RuntimeError("the downloader service is unavailable")

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> _FailingDownloaderMW:
        return cls(crawler)


class _FailingSpiderMW:
    def __init__(self, crawler: Crawler) -> None:
        raise RuntimeError("the spider service is unavailable")

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> _FailingSpiderMW:
        return cls(crawler)


class _FailingPipeline:
    def __init__(self, crawler: Crawler) -> None:
        raise RuntimeError("the pipeline service is unavailable")

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> _FailingPipeline:
        return cls(crawler)


# Components used by the run-time isolation tests.


class _FailingRequestMW:
    calls = 0

    def __init__(self, crawler: Crawler) -> None:
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> _FailingRequestMW:
        return cls(crawler)

    def process_request(self, request: Request) -> None:
        type(self).calls += 1
        raise RuntimeError("the request hook is broken")


class _IgnoringRequestMW:
    calls = 0

    def process_request(self, request: Request) -> None:
        type(self).calls += 1
        raise IgnoreRequest("dropped on purpose")


class _FailingProcessResponseMW:
    calls = 0

    def process_response(self, request: Request, response: Response) -> Response:
        type(self).calls += 1
        raise RuntimeError("the response hook is broken")


class _FailingPipelineRuntime:
    calls = 0

    def process_item(self, item: Any) -> Any:
        type(self).calls += 1
        raise RuntimeError("the item hook is broken")


class _DroppingPipeline:
    calls = 0

    def process_item(self, item: Any) -> Any:
        type(self).calls += 1
        raise DropItem("dropped on purpose")


class _FailingInputMW:
    calls = 0

    def process_spider_input(self, response: Response) -> None:
        type(self).calls += 1
        raise RuntimeError("the input hook is broken")


class _FailingExceptionMW:
    calls = 0

    def process_exception(self, request: Request, exception: Exception) -> None:
        type(self).calls += 1
        raise RuntimeError("the exception hook is broken")


class _FailingOutputMW:
    calls = 0

    async def process_spider_output(self, response: Response, result: Any) -> Any:
        type(self).calls += 1
        async for item in result:
            yield item
        raise RuntimeError("the output hook is broken")


class _ManagerWithoutSettingName(MiddlewareManager):
    component_name = "custom"

    @classmethod
    def _get_mwlist_from_settings(cls, settings: Any) -> list[Any]:
        raise ValueError("the component list cannot be normalized")


def _settings(middleware_setting: str, component: type, **extra: Any) -> dict[str, Any]:
    return {
        f"{middleware_setting}_BASE": {},
        middleware_setting: {component: 100},
        **extra,
    }


@pytest.fixture
def crawler() -> Crawler:
    return get_crawler(Spider)


def test_no_failure_without_crawler() -> None:
    """Managers built without a crawler behave exactly as before."""
    with pytest.warns(ScrapyDeprecationWarning, match="without the crawler"):
        mwman = DownloaderMiddlewareManager()
    assert mwman._component_failures is None


@pytest.mark.parametrize(
    ("manager_cls", "setting_name", "component"),
    [
        (ExtensionManager, "EXTENSIONS", _FailingExtension),
        (
            DownloaderMiddlewareManager,
            "DOWNLOADER_MIDDLEWARES",
            _FailingDownloaderMW,
        ),
        (SpiderMiddlewareManager, "SPIDER_MIDDLEWARES", _FailingSpiderMW),
        (ItemPipelineManager, "ITEM_PIPELINES", _FailingPipeline),
    ],
)
def test_assembly_failure_skipped_with_policy(
    manager_cls: type, setting_name: str, component: type
) -> None:
    path = global_object_name(component)
    crawler = get_crawler(
        Spider,
        _settings(
            setting_name,
            component,
            COMPONENT_FAILURE_POLICIES={path: "skip"},
        ),
    )

    manager = build_from_crawler(manager_cls, crawler)

    assert manager.middlewares == ()
    failures = crawler.get_component_failures()
    assert len(failures) == 1
    record = failures[0]
    assert record.path == path
    assert record.kind == manager.component_name
    assert isinstance(record.error, RuntimeError)
    assert "unavailable" in str(record.error)
    assert "RuntimeError: the" in record.traceback_text
    assert record.time is not None


@pytest.mark.parametrize(
    ("manager_cls", "setting_name", "component"),
    [
        (ExtensionManager, "EXTENSIONS", _FailingExtension),
        (
            DownloaderMiddlewareManager,
            "DOWNLOADER_MIDDLEWARES",
            _FailingDownloaderMW,
        ),
        (SpiderMiddlewareManager, "SPIDER_MIDDLEWARES", _FailingSpiderMW),
        (ItemPipelineManager, "ITEM_PIPELINES", _FailingPipeline),
    ],
)
def test_assembly_failure_raises_without_policy(
    manager_cls: type, setting_name: str, component: type
) -> None:
    def assemble() -> None:
        # Extensions are assembled inside get_crawler(), the other
        # managers are assembled explicitly below.
        crawler = get_crawler(Spider, _settings(setting_name, component))
        build_from_crawler(manager_cls, crawler)

    with pytest.raises(RuntimeError, match="unavailable"):
        assemble()


def test_unloadable_path_skipped_with_policy() -> None:
    path = "tests.test_component_failures.DoesNotExist"
    crawler = get_crawler(
        Spider,
        {
            "DOWNLOADER_MIDDLEWARES_BASE": {},
            "DOWNLOADER_MIDDLEWARES": {path: 100},
            "COMPONENT_FAILURE_POLICIES": {path: "skip"},
        },
    )

    manager = build_from_crawler(DownloaderMiddlewareManager, crawler)

    assert manager.middlewares == ()
    failures = crawler.get_component_failures()
    assert [record.path for record in failures] == [path]
    assert isinstance(failures[0].error, (ImportError, NameError))


def test_unloadable_path_raises_without_policy() -> None:
    path = "tests.test_component_failures.DoesNotExist"
    crawler = get_crawler(
        Spider,
        {
            "DOWNLOADER_MIDDLEWARES_BASE": {},
            "DOWNLOADER_MIDDLEWARES": {path: 100},
        },
    )

    with pytest.raises(ValueError, match="Could not load"):
        build_from_crawler(DownloaderMiddlewareManager, crawler)


def test_invalid_policy_value_raises() -> None:
    path = global_object_name(_FailingDownloaderMW)

    def assemble() -> None:
        crawler = get_crawler(
            Spider,
            _settings(
                "DOWNLOADER_MIDDLEWARES",
                _FailingDownloaderMW,
                COMPONENT_FAILURE_POLICIES={path: "ignore"},
            ),
        )
        build_from_crawler(DownloaderMiddlewareManager, crawler)

    with pytest.raises(ValueError, match="Invalid value"):
        assemble()


async def _download(mwman: DownloaderMiddlewareManager) -> Response | Request:
    request = Request("http://example.com/")

    async def download_func(request: Request) -> Response:
        return Response(request.url)

    return await mwman.download_async(download_func, request)


@coroutine_test
async def test_downloader_middleware_isolation_and_reset() -> None:
    _FailingRequestMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "DOWNLOADER_MIDDLEWARES",
            _FailingRequestMW,
            COMPONENT_FAILURE_MAX_FAILURES=2,
            COMPONENT_FAILURE_WINDOW=60.0,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(DownloaderMiddlewareManager, crawler)

    # The first two failures keep propagating exactly as before, but the
    # second one reaches the threshold and isolates the component.
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    assert crawler.get_isolated_components() == []
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    assert _FailingRequestMW.calls == 2

    isolated = crawler.get_isolated_components()
    assert len(isolated) == 1
    record = isolated[0]
    assert record.path == global_object_name(_FailingRequestMW)
    assert record.kind == "downloader middleware"
    assert record.failure_count == 2
    assert record.isolated
    assert record.isolated_at is not None
    assert isinstance(record.last_error, RuntimeError)
    assert record.skipped_calls == 0

    # From now on the hook is skipped and the download goes through.
    response = await _download(mwman)
    assert isinstance(response, Response)
    assert _FailingRequestMW.calls == 2
    assert crawler.get_isolated_components()[0].skipped_calls == 1

    # Resetting the isolation re-engages the component.
    released = crawler.reset_component_isolation(_FailingRequestMW)
    assert released == [global_object_name(_FailingRequestMW)]
    assert crawler.get_isolated_components() == []
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    assert _FailingRequestMW.calls == 3

    # Reset also works with the import path and with the component instance.
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    assert crawler.reset_component_isolation(global_object_name(_FailingRequestMW)) == [
        global_object_name(_FailingRequestMW)
    ]
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    assert crawler.reset_component_isolation(mwman.middlewares[0]) == [
        global_object_name(_FailingRequestMW)
    ]
    # Nothing isolated left to reset.
    assert crawler.reset_component_isolation(_FailingRequestMW) == []


@coroutine_test
async def test_process_response_isolation() -> None:
    _FailingProcessResponseMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "DOWNLOADER_MIDDLEWARES",
            _FailingProcessResponseMW,
            COMPONENT_FAILURE_MAX_FAILURES=2,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(DownloaderMiddlewareManager, crawler)

    with pytest.raises(RuntimeError, match="response hook"):
        await _download(mwman)
    with pytest.raises(RuntimeError, match="response hook"):
        await _download(mwman)
    response = await _download(mwman)
    assert isinstance(response, Response)
    assert _FailingProcessResponseMW.calls == 2
    assert len(crawler.get_isolated_components()) == 1


@coroutine_test
async def test_ignore_request_does_not_count() -> None:
    _IgnoringRequestMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "DOWNLOADER_MIDDLEWARES",
            _IgnoringRequestMW,
            COMPONENT_FAILURE_MAX_FAILURES=1,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(DownloaderMiddlewareManager, crawler)

    for _ in range(3):
        with pytest.raises(IgnoreRequest, match="dropped on purpose"):
            await _download(mwman)
    assert _IgnoringRequestMW.calls == 3
    assert crawler.get_isolated_components() == []


@coroutine_test
async def test_pipeline_isolation() -> None:
    _FailingPipelineRuntime.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "ITEM_PIPELINES",
            _FailingPipelineRuntime,
            COMPONENT_FAILURE_MAX_FAILURES=2,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(ItemPipelineManager, crawler)

    with pytest.raises(RuntimeError, match="item hook"):
        await mwman.process_item_async({"x": 1})
    with pytest.raises(RuntimeError, match="item hook"):
        await mwman.process_item_async({"x": 2})
    result = await mwman.process_item_async({"x": 3})
    assert result == {"x": 3}
    assert _FailingPipelineRuntime.calls == 2
    isolated = crawler.get_isolated_components()
    assert len(isolated) == 1
    assert isolated[0].path == global_object_name(_FailingPipelineRuntime)
    assert isolated[0].skipped_calls == 1


@coroutine_test
async def test_drop_item_does_not_count() -> None:
    _DroppingPipeline.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "ITEM_PIPELINES",
            _DroppingPipeline,
            COMPONENT_FAILURE_MAX_FAILURES=1,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(ItemPipelineManager, crawler)

    for _ in range(3):
        with pytest.raises(DropItem, match="dropped on purpose"):
            await mwman.process_item_async({"x": 1})
    assert _DroppingPipeline.calls == 3
    assert crawler.get_isolated_components() == []


@coroutine_test
async def test_spider_input_isolation() -> None:
    _FailingInputMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "SPIDER_MIDDLEWARES",
            _FailingInputMW,
            COMPONENT_FAILURE_MAX_FAILURES=2,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(SpiderMiddlewareManager, crawler)
    request = Request("http://example.com/")
    response = Response("http://example.com/")
    received: list[str] = []

    async def scrape_func(result: Response | Failure, request: Request) -> Any:
        received.append("failure" if isinstance(result, Failure) else "response")
        return as_async_generator(iter([{"ok": 1}]))

    await mwman.scrape_response_async(scrape_func, response, request)
    await mwman.scrape_response_async(scrape_func, response, request)
    await mwman.scrape_response_async(scrape_func, response, request)

    assert _FailingInputMW.calls == 2
    assert received == ["failure", "failure", "response"]
    isolated = crawler.get_isolated_components()
    assert len(isolated) == 1
    assert isolated[0].path == global_object_name(_FailingInputMW)


@coroutine_test
async def test_threshold_disabled_by_default() -> None:
    _FailingRequestMW.calls = 0
    crawler = get_crawler(
        Spider, _settings("DOWNLOADER_MIDDLEWARES", _FailingRequestMW)
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(DownloaderMiddlewareManager, crawler)

    for _ in range(5):
        with pytest.raises(RuntimeError, match="request hook"):
            await _download(mwman)
    assert _FailingRequestMW.calls == 5
    assert crawler.get_isolated_components() == []


def test_signal_handler_exception_is_not_isolated(crawler: Crawler) -> None:
    """The catch-and-log semantics of signal sending is unchanged."""
    calls = 0

    def receiver() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("the signal handler is broken")

    crawler.signals.connect(receiver, signal="test_signal")
    results = crawler.signals.send_catch_log(signal="test_signal")

    assert calls == 1
    assert len(results) == 1
    assert isinstance(results[0][1], Failure)
    assert isinstance(results[0][1].value, RuntimeError)
    assert crawler.get_isolated_components() == []


@coroutine_test
async def test_process_exception_isolation() -> None:
    _FailingExceptionMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "DOWNLOADER_MIDDLEWARES",
            _FailingExceptionMW,
            COMPONENT_FAILURE_MAX_FAILURES=2,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(DownloaderMiddlewareManager, crawler)

    async def failing_download(request: Request) -> Response:
        raise RuntimeError("the download itself failed")

    request = Request("http://example.com/")
    with pytest.raises(RuntimeError, match="exception hook"):
        await mwman.download_async(failing_download, request)
    with pytest.raises(RuntimeError, match="exception hook"):
        await mwman.download_async(failing_download, request)
    assert _FailingExceptionMW.calls == 2
    assert len(crawler.get_isolated_components()) == 1

    # The isolated process_exception() hook is skipped, so the original
    # download exception propagates instead.
    with pytest.raises(RuntimeError, match="download itself failed"):
        await mwman.download_async(failing_download, request)
    assert _FailingExceptionMW.calls == 2


@coroutine_test
async def test_spider_output_isolation() -> None:
    _FailingOutputMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "SPIDER_MIDDLEWARES",
            _FailingOutputMW,
            COMPONENT_FAILURE_MAX_FAILURES=1,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(SpiderMiddlewareManager, crawler)
    request = Request("http://example.com/")
    response = Response("http://example.com/")

    async def scrape_func(result: Response | Failure, request: Request) -> Any:
        return as_async_generator(iter([{"ok": 1}]))

    chain = await mwman.scrape_response_async(scrape_func, response, request)
    with pytest.raises(RuntimeError, match="output hook"):
        async for _ in chain:
            pass
    assert len(crawler.get_isolated_components()) == 1

    # The isolated output hook is skipped, the callback output passes through.
    chain = await mwman.scrape_response_async(scrape_func, response, request)
    items = [item async for item in chain]
    assert items == [{"ok": 1}]
    assert _FailingOutputMW.calls == 1


@coroutine_test
async def test_failures_outside_window_do_not_isolate() -> None:
    _FailingRequestMW.calls = 0
    crawler = get_crawler(
        Spider,
        _settings(
            "DOWNLOADER_MIDDLEWARES",
            _FailingRequestMW,
            COMPONENT_FAILURE_MAX_FAILURES=2,
            COMPONENT_FAILURE_WINDOW=0.05,
        ),
    )
    crawler.spider = crawler._create_spider("foo")
    mwman = build_from_crawler(DownloaderMiddlewareManager, crawler)

    for _ in range(3):
        with pytest.raises(RuntimeError, match="request hook"):
            await _download(mwman)
        await asyncio.sleep(0.1)
    assert _FailingRequestMW.calls == 3
    assert crawler.get_isolated_components() == []


@coroutine_test
async def test_unregistered_component_is_never_isolated() -> None:
    """Hooks of managers built directly still run without a registry entry."""
    _FailingRequestMW.calls = 0
    crawler = get_crawler(Spider, {"COMPONENT_FAILURE_MAX_FAILURES": 1})
    crawler.spider = crawler._create_spider("foo")
    mwman = DownloaderMiddlewareManager(_FailingRequestMW(crawler), crawler=crawler)

    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    with pytest.raises(RuntimeError, match="request hook"):
        await _download(mwman)
    assert _FailingRequestMW.calls == 2
    assert crawler.get_isolated_components() == []


def test_normalization_error_reraised_without_setting_name() -> None:
    crawler = get_crawler(Spider)

    with pytest.raises(ValueError, match="cannot be normalized"):
        build_from_crawler(_ManagerWithoutSettingName, crawler)


@pytest.mark.parametrize(
    ("setting", "value"),
    [
        ("COMPONENT_FAILURE_MAX_FAILURES", -1),
        ("COMPONENT_FAILURE_WINDOW", -1.0),
    ],
)
def test_invalid_isolation_settings_raise(setting: str, value: Any) -> None:
    def assemble() -> None:
        crawler = get_crawler(Spider, {setting: value})
        build_from_crawler(DownloaderMiddlewareManager, crawler)

    with pytest.raises(ValueError, match="COMPONENT_FAILURE"):
        assemble()


# End-to-end crawl against the mock server.


class _E2EUnavailableExtension:
    def __init__(self) -> None:
        raise RuntimeError("the extension service is unavailable")


class _E2EFailingRequestMW:
    calls = 0

    def process_request(self, request: Request) -> None:
        type(self).calls += 1
        raise RuntimeError("the request hook is broken")


class _E2ESpider(MockServerSpider):
    name = "component_failures"
    custom_settings = {
        "EXTENSIONS": {_E2EUnavailableExtension: 10},
        "DOWNLOADER_MIDDLEWARES_BASE": {},
        "DOWNLOADER_MIDDLEWARES": {_E2EFailingRequestMW: 100},
        "COMPONENT_FAILURE_POLICIES": {
            global_object_name(_E2EUnavailableExtension): "skip"
        },
        "COMPONENT_FAILURE_MAX_FAILURES": 2,
    }

    async def start(self) -> Any:
        assert self.mockserver is not None
        for _ in range(5):
            yield Request(self.mockserver.url("/status?n=200"), dont_filter=True)

    async def parse(self, response: Response) -> Any:
        yield {"status": response.status}


@coroutine_test
async def test_crawl_skips_failed_extension_and_isolates_middleware(
    mockserver: MockServer,
) -> None:
    _E2EFailingRequestMW.calls = 0
    crawler = get_crawler(_E2ESpider)

    await crawler.crawl_async(mockserver=mockserver)

    failures = crawler.get_component_failures()
    assert [record.path for record in failures] == [
        global_object_name(_E2EUnavailableExtension)
    ]
    isolated = crawler.get_isolated_components()
    assert [record.path for record in isolated] == [
        global_object_name(_E2EFailingRequestMW)
    ]
    assert isolated[0].failure_count == 2
    assert _E2EFailingRequestMW.calls == 2
    assert crawler.stats.get_value("item_scraped_count") == 3
    assert (
        crawler.stats.get_value(
            f"component_failures/runtime/{global_object_name(_E2EFailingRequestMW)}"
        )
        == 2
    )

    released = crawler.reset_component_isolation(_E2EFailingRequestMW)
    assert released == [global_object_name(_E2EFailingRequestMW)]
    assert crawler.get_isolated_components() == []
