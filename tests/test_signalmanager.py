from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy.signalmanager import SignalManager
from scrapy.utils.asyncio import sleep as async_sleep
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.python import global_object_name
from scrapy.utils.spider import DefaultSpider
from scrapy.utils.test import get_crawler
from tests.spiders import FollowAllSpider
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    import pytest

    from tests.mockserver.http import MockServer

WARN_THRESHOLD = 0.02
HANDLER_TIMEOUT = 0.05
SLOW_DELAY = 0.05


def _governed_manager(**overrides: Any) -> SignalManager:
    settings = Settings(
        {
            "SIGNALS_GOVERNANCE_ENABLED": True,
            "SIGNALS_HANDLER_WARN_THRESHOLD": WARN_THRESHOLD,
            "SIGNALS_HANDLER_TIMEOUT": HANDLER_TIMEOUT,
            **overrides,
        }
    )
    return Crawler(DefaultSpider, settings).signals


def slow_sync_handler() -> str:
    time.sleep(SLOW_DELAY)
    return "slow"


def failing_sync_handler() -> None:
    raise ZeroDivisionError


def ok_sync_handler() -> str:
    return "ok"


class _CustomSignal:
    pass


custom_signal = _CustomSignal()


def test_disconnect_all() -> None:
    signal = object()
    sender = object()
    sm = SignalManager(sender)

    calls = []

    def handler():
        calls.append(1)

    sm.connect(handler, signal)
    sm.send_catch_log(signal)
    assert calls == [1]

    sm.disconnect_all(signal)
    sm.send_catch_log(signal)
    assert calls == [1]  # handler no longer called after disconnect_all


def test_governance_disabled_by_default() -> None:
    sm = SignalManager(object())
    assert sm.get_handler_stats() == {}

    sm.connect(ok_sync_handler, signals.spider_opened)
    sm.connect(failing_sync_handler, signals.spider_opened)
    results = sm.send_catch_log(signals.spider_opened)

    assert [receiver for receiver, _result in results] == [
        ok_sync_handler,
        failing_sync_handler,
    ]
    assert results[0][1] == "ok"
    assert isinstance(results[1][1], Failure)
    assert sm.get_handler_stats() == {}


def test_crawler_enables_governance_from_settings() -> None:
    disabled = get_crawler()
    disabled.signals.connect(ok_sync_handler, signals.engine_started)
    disabled.signals.send_catch_log(signals.engine_started)
    assert disabled.signals.get_handler_stats() == {}

    enabled = get_crawler(
        settings_dict={
            "SIGNALS_GOVERNANCE_ENABLED": True,
            "SIGNALS_HANDLER_WARN_THRESHOLD": WARN_THRESHOLD,
            "SIGNALS_HANDLER_TIMEOUT": HANDLER_TIMEOUT,
        }
    )
    assert enabled.signals.get_handler_stats() == {}
    enabled.signals.connect(ok_sync_handler, signals.engine_started)
    enabled.signals.send_catch_log(signals.engine_started)
    stats = enabled.signals.get_handler_stats()
    assert stats["engine_started"][global_object_name(ok_sync_handler)]["calls"] == 1


def test_sync_handler_timed_warned_and_not_cancelled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = _governed_manager(SIGNALS_HANDLER_TIMEOUT=WARN_THRESHOLD)
    manager.connect(slow_sync_handler, signals.spider_opened)
    manager.connect(ok_sync_handler, signals.spider_opened)

    caplog.set_level(logging.WARNING)
    started = time.perf_counter()
    results = manager.send_catch_log(signals.spider_opened)
    elapsed = time.perf_counter() - started

    # the synchronous slow handler cannot be cancelled, it runs to completion
    assert elapsed >= SLOW_DELAY
    assert results == [(slow_sync_handler, "slow"), (ok_sync_handler, "ok")]

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "spider_opened" in message
    assert global_object_name(slow_sync_handler) in message

    stats = manager.get_handler_stats()
    slow_stats = stats["spider_opened"][global_object_name(slow_sync_handler)]
    assert slow_stats["calls"] == 1
    assert slow_stats["failures"] == 0
    assert slow_stats["timeouts"] == 0
    assert slow_stats["max_time"] >= SLOW_DELAY * 0.8
    ok_stats = stats["spider_opened"][global_object_name(ok_sync_handler)]
    assert ok_stats["calls"] == 1
    assert ok_stats["failures"] == 0
    assert ok_stats["max_time"] < WARN_THRESHOLD


def test_sync_failure_counted(caplog: pytest.LogCaptureFixture) -> None:
    manager = _governed_manager(SIGNALS_HANDLER_WARN_THRESHOLD=None)
    manager.connect(failing_sync_handler, signals.spider_closed)

    caplog.set_level(logging.WARNING)
    results = manager.send_catch_log(signals.spider_closed)

    assert len(results) == 1
    assert results[0][0] is failing_sync_handler
    assert isinstance(results[0][1], Failure)
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "failing_sync_handler" in errors[0].getMessage()
    assert not [r for r in caplog.records if r.levelname == "WARNING"]

    stats = manager.get_handler_stats()["spider_closed"][
        global_object_name(failing_sync_handler)
    ]
    assert stats["calls"] == 1
    assert stats["failures"] == 1
    assert stats["timeouts"] == 0


def test_stats_aggregated_per_signal_and_handler() -> None:
    manager = _governed_manager(SIGNALS_HANDLER_WARN_THRESHOLD=None)
    manager.connect(ok_sync_handler, signals.spider_opened)
    manager.connect(slow_sync_handler, signals.engine_started)

    manager.send_catch_log(signals.spider_opened)
    manager.send_catch_log(signals.spider_opened)
    manager.send_catch_log(signals.engine_started)

    stats = manager.get_handler_stats()
    assert set(stats) == {"spider_opened", "engine_started"}

    ok_stats = stats["spider_opened"][global_object_name(ok_sync_handler)]
    assert ok_stats["calls"] == 2
    assert ok_stats["failures"] == 0
    assert ok_stats["timeouts"] == 0
    assert 0.0 <= float(ok_stats["max_time"]) <= float(ok_stats["total_time"])

    slow_stats = stats["engine_started"][global_object_name(slow_sync_handler)]
    assert slow_stats["calls"] == 1
    assert float(slow_stats["max_time"]) >= SLOW_DELAY * 0.8
    assert float(slow_stats["total_time"]) >= float(slow_stats["max_time"])


def test_custom_signal_names_in_warning(caplog: pytest.LogCaptureFixture) -> None:
    plain_signal = object()
    manager = _governed_manager()
    manager.connect(slow_sync_handler, custom_signal)
    manager.connect(ok_sync_handler, plain_signal)

    caplog.set_level(logging.WARNING)
    manager.send_catch_log(custom_signal)
    manager.send_catch_log(plain_signal)

    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(messages) == 1
    assert global_object_name(_CustomSignal) in messages[0]
    assert global_object_name(slow_sync_handler) in messages[0]

    stats = manager.get_handler_stats()
    assert global_object_name(_CustomSignal) in stats
    plain_key = next(k for k in stats if k.startswith("<object object"))
    plain_handler_stats = stats[plain_key][global_object_name(ok_sync_handler)]
    assert plain_handler_stats["calls"] == 1


@coroutine_test
async def test_async_handler_timed_and_warned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def slow_async_handler() -> str:
        await async_sleep(SLOW_DELAY)
        return "slow"

    async def ok_async_handler() -> str:
        return "ok"

    manager = _governed_manager(SIGNALS_HANDLER_TIMEOUT=None)
    manager.connect(slow_async_handler, signals.item_scraped)
    manager.connect(ok_async_handler, signals.item_scraped)

    caplog.set_level(logging.WARNING)
    results = await manager.send_catch_log_async(signals.item_scraped)

    assert [receiver for receiver, _result in results] == [
        slow_async_handler,
        ok_async_handler,
    ]
    by_handler = dict(results)
    assert by_handler[slow_async_handler] == "slow"
    assert by_handler[ok_async_handler] == "ok"

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "item_scraped" in message
    assert "slow_async_handler" in message

    stats = manager.get_handler_stats()["item_scraped"]
    slow_stats = stats[global_object_name(slow_async_handler)]
    assert slow_stats["calls"] == 1
    assert slow_stats["failures"] == 0
    assert slow_stats["timeouts"] == 0
    assert slow_stats["max_time"] >= SLOW_DELAY * 0.8
    assert stats[global_object_name(ok_async_handler)]["failures"] == 0


@coroutine_test
async def test_async_handler_timeout_cancels_and_isolates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cancelled: list[bool] = []

    async def hanging_handler() -> None:
        try:
            await maybe_deferred_to_future(Deferred())
        finally:
            cancelled.append(True)

    async def failing_async_handler() -> None:
        raise ZeroDivisionError

    async def ok_async_handler() -> str:
        return "ok"

    manager = _governed_manager(
        SIGNALS_HANDLER_WARN_THRESHOLD=None, SIGNALS_HANDLER_TIMEOUT=HANDLER_TIMEOUT
    )
    manager.connect(hanging_handler, signals.response_received)
    manager.connect(failing_async_handler, signals.response_received)
    manager.connect(ok_async_handler, signals.response_received)

    caplog.set_level(logging.WARNING)
    results = await manager.send_catch_log_async(signals.response_received)

    assert [receiver for receiver, _result in results] == [
        hanging_handler,
        failing_async_handler,
        ok_async_handler,
    ]
    by_handler = dict(results)
    assert cancelled == [True]
    assert isinstance(by_handler[hanging_handler], BaseException)
    assert isinstance(by_handler[failing_async_handler], ZeroDivisionError)
    assert by_handler[ok_async_handler] == "ok"

    messages = [r.getMessage() for r in caplog.records]
    timeout_messages = [m for m in messages if "timed out" in m]
    assert len(timeout_messages) == 1
    assert "hanging_handler" in timeout_messages[0]
    assert "response_received" in timeout_messages[0]
    assert any("failing_async_handler" in m for m in messages)

    stats = manager.get_handler_stats()["response_received"]
    hanging_stats = stats[global_object_name(hanging_handler)]
    assert hanging_stats["timeouts"] == 1
    assert hanging_stats["failures"] == 1
    failing_stats = stats[global_object_name(failing_async_handler)]
    assert failing_stats["failures"] == 1
    assert failing_stats["timeouts"] == 0
    assert stats[global_object_name(ok_async_handler)]["failures"] == 0
    assert stats[global_object_name(ok_async_handler)]["timeouts"] == 0


@coroutine_test
async def test_async_dont_log_failure_still_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def ignored_failure_handler() -> None:
        raise ZeroDivisionError

    manager = _governed_manager(SIGNALS_HANDLER_WARN_THRESHOLD=None)
    manager.connect(ignored_failure_handler, signals.item_error)

    caplog.set_level(logging.WARNING)
    results = await manager.send_catch_log_async(
        signals.item_error, dont_log=ZeroDivisionError
    )

    assert len(results) == 1
    assert isinstance(results[0][1], ZeroDivisionError)
    assert not caplog.records
    stats = manager.get_handler_stats()["item_error"][
        global_object_name(ignored_failure_handler)
    ]
    assert stats["calls"] == 1
    assert stats["failures"] == 1
    assert stats["timeouts"] == 0


@coroutine_test
async def test_async_dispatch_unchanged_when_governance_disabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def failing_async_handler() -> None:
        raise ZeroDivisionError

    async def ok_async_handler() -> str:
        return "ok"

    manager = SignalManager(object())
    manager.connect(failing_async_handler, signals.item_dropped)
    manager.connect(ok_async_handler, signals.item_dropped)

    caplog.set_level(logging.WARNING)
    results = await manager.send_catch_log_async(signals.item_dropped)

    assert [receiver for receiver, _result in results] == [
        failing_async_handler,
        ok_async_handler,
    ]
    by_handler = dict(results)
    assert isinstance(by_handler[failing_async_handler], ZeroDivisionError)
    assert by_handler[ok_async_handler] == "ok"
    assert not [r for r in caplog.records if r.levelname == "WARNING"]
    assert len([r for r in caplog.records if r.levelname == "ERROR"]) == 1
    assert manager.get_handler_stats() == {}


@coroutine_test
async def test_crawl_records_handler_stats(mockserver: MockServer) -> None:
    crawler = get_crawler(
        FollowAllSpider,
        settings_dict={
            "SIGNALS_GOVERNANCE_ENABLED": True,
            # do not flood the crawl log, assertions below use the stats
            "SIGNALS_HANDLER_WARN_THRESHOLD": 100.0,
        },
    )
    await crawler.crawl_async(mockserver=mockserver)

    stats = crawler.signals.get_handler_stats()
    assert "spider_opened" in stats
    assert "spider_closed" in stats
    recorded = 0
    for handlers in stats.values():
        for handler_stats in handlers.values():
            recorded += int(handler_stats["calls"])
            assert handler_stats["failures"] == 0
            assert handler_stats["timeouts"] == 0
    assert recorded > 0
