"""Helper functions for working with signals"""

from __future__ import annotations

import asyncio
import logging
import time
import warnings
from collections.abc import Awaitable, Callable, Generator, Sequence
from dataclasses import dataclass
from typing import Any as TypingAny
from typing import cast

from pydispatch.dispatcher import (
    Anonymous,
    Any,
    disconnect,
    getAllReceivers,
    liveReceivers,
)
from pydispatch.robustapply import robustApply
from twisted.internet.defer import (
    CancelledError,
    Deferred,
    DeferredList,
    inlineCallbacks,
)
from twisted.python.failure import Failure

from scrapy.exceptions import ScrapyDeprecationWarning, StopDownload
from scrapy.utils.asyncio import CallLaterResult, call_later, is_asyncio_available
from scrapy.utils.defer import (
    _maybeDeferred_coro,
    ensure_awaitable,
    maybe_deferred_to_future,
)
from scrapy.utils.log import failure_to_exc_info
from scrapy.utils.python import global_object_name

logger = logging.getLogger(__name__)


@dataclass
class _HandlerGovernanceStats:
    calls: int = 0
    failures: int = 0
    timeouts: int = 0
    total_time: float = 0.0
    max_time: float = 0.0


class _SignalGovernance:
    """Records per-signal, per-handler dispatch timings and failures.

    Warns about handlers slower than *warn_threshold* seconds and about
    asynchronous handlers cancelled after *handler_timeout* seconds. A
    ``None`` threshold disables the corresponding check.
    """

    def __init__(
        self,
        *,
        warn_threshold: float | None = None,
        handler_timeout: float | None = None,
    ) -> None:
        self.warn_threshold: float | None = warn_threshold
        self.handler_timeout: float | None = handler_timeout
        self._stats: dict[tuple[str, str], _HandlerGovernanceStats] = {}

    def observe(
        self,
        signal_name: str,
        receiver_name: str,
        elapsed: float,
        *,
        failure: bool,
        timed_out: bool = False,
        spider: TypingAny = None,
    ) -> None:
        """Account for one finished handler call."""
        stats = self._stats.setdefault(
            (signal_name, receiver_name), _HandlerGovernanceStats()
        )
        stats.calls += 1
        stats.total_time += elapsed
        stats.max_time = max(stats.max_time, elapsed)
        if failure:
            stats.failures += 1
        if timed_out:
            stats.timeouts += 1
            logger.warning(
                f"Signal handler {receiver_name} timed out after {elapsed:.3f} s"
                f" while handling signal {signal_name} and was cancelled",
                extra={"spider": spider},
            )
        elif self.warn_threshold is not None and elapsed > self.warn_threshold:
            logger.warning(
                f"Slow signal handler {receiver_name} took {elapsed:.3f} s"
                f" (threshold {self.warn_threshold:.3f} s)"
                f" while handling signal {signal_name}",
                extra={"spider": spider},
            )

    def get_stats(self) -> dict[str, dict[str, dict[str, int | float]]]:
        """Return the aggregated timings, keyed by signal name and handler name."""
        stats_by_signal: dict[str, dict[str, dict[str, int | float]]] = {}
        for (signal_name, receiver_name), stats in self._stats.items():
            stats_by_signal.setdefault(signal_name, {})[receiver_name] = {
                "calls": stats.calls,
                "failures": stats.failures,
                "timeouts": stats.timeouts,
                "total_time": stats.total_time,
                "max_time": stats.max_time,
            }
        return stats_by_signal


def _signal_name(signal: TypingAny) -> str:
    from scrapy import signals as scrapy_signals  # noqa: PLC0415

    for name, value in vars(scrapy_signals).items():
        if value is signal:
            return name
    try:
        return global_object_name(signal)
    except AttributeError:
        # signal is an instance whose class carries the meaningful name
        signal_type = type(signal)
        if signal_type is not object:
            return global_object_name(signal_type)
        return repr(signal)


def send_catch_log(
    signal: TypingAny = Any,
    sender: TypingAny = Anonymous,
    *arguments: TypingAny,
    _governance: _SignalGovernance | None = None,
    **named: TypingAny,
) -> list[tuple[Callable[..., TypingAny], TypingAny]]:
    """Like ``pydispatch.robust.sendRobust()`` but it also logs errors and returns
    Failures instead of exceptions.
    """
    dont_log = named.pop("dont_log", ())
    dont_log = tuple(dont_log) if isinstance(dont_log, Sequence) else (dont_log,)
    dont_log += (StopDownload,)
    spider = named.get("spider")
    signal_name = _signal_name(signal) if _governance is not None else ""
    responses: list[tuple[Callable[..., TypingAny], TypingAny]] = []
    for receiver in liveReceivers(getAllReceivers(sender, signal)):
        result: TypingAny
        # synchronous handlers cannot be interrupted, so the timeout does not
        # apply to them; their duration is still recorded
        started_at = time.perf_counter() if _governance is not None else 0.0
        try:
            response = robustApply(
                receiver, *arguments, signal=signal, sender=sender, **named
            )
            if isinstance(response, Deferred):
                logger.error(
                    "Cannot return deferreds from signal handler: %(receiver)s",
                    {"receiver": receiver},
                    extra={"spider": spider},
                )
        except dont_log:
            result = Failure()
        except Exception:
            result = Failure()
            logger.exception(
                "Error caught on signal handler: %(receiver)s",
                {"receiver": receiver},
                extra={"spider": spider},
            )
        else:
            result = response
        if _governance is not None:
            _governance.observe(
                signal_name,
                global_object_name(receiver),
                time.perf_counter() - started_at,
                failure=isinstance(result, Failure),
                spider=spider,
            )
        responses.append((receiver, result))
    return responses


def send_catch_log_deferred(
    signal: TypingAny = Any,
    sender: TypingAny = Anonymous,
    *arguments: TypingAny,
    _governance: _SignalGovernance | None = None,
    **named: TypingAny,
) -> Deferred[list[tuple[Callable[..., TypingAny], TypingAny]]]:
    """Like :func:`send_catch_log` but supports :ref:`asynchronous signal handlers
    <signal-deferred>`.

    Returns a deferred that gets fired once all signal handlers have finished.
    """
    warnings.warn(
        "send_catch_log_deferred() is deprecated, use send_catch_log_async() instead",
        ScrapyDeprecationWarning,
        stacklevel=2,
    )
    return _send_catch_log_deferred(
        signal, sender, *arguments, _governance=_governance, **named
    )


def _govern_deferred(
    d: Deferred[TypingAny],
    receiver: Callable[..., TypingAny],
    signal_name: str,
    governance: _SignalGovernance,
    dont_log: TypingAny,
    spider: TypingAny,
) -> Deferred[tuple[Callable[..., TypingAny], TypingAny]]:
    """Time *d*, cancel it past the handler timeout, and record the outcome."""
    started_at = time.perf_counter()
    timed_out = False
    timeout_call: CallLaterResult | None = None

    def cancel() -> None:
        nonlocal timed_out
        timed_out = True
        d.cancel()

    if governance.handler_timeout is not None:
        timeout_call = call_later(governance.handler_timeout, cancel)

    def logerror(failure: Failure, recv: TypingAny) -> Failure:
        if timed_out and failure.check(CancelledError):
            # the timeout is logged by governance.observe() once it settles
            return failure
        if dont_log is None or not isinstance(failure.value, dont_log):
            logger.error(
                "Error caught on signal handler: %(receiver)s",
                {"receiver": recv},
                exc_info=failure_to_exc_info(failure),
                extra={"spider": spider},
            )
        return failure

    def record(result: TypingAny) -> TypingAny:
        if timeout_call is not None:
            timeout_call.cancel()
        did_timeout = (
            timed_out
            and isinstance(result, Failure)
            and result.check(CancelledError) is not None
        )
        governance.observe(
            signal_name,
            global_object_name(receiver),
            time.perf_counter() - started_at,
            failure=did_timeout or isinstance(result, Failure),
            timed_out=did_timeout,
            spider=spider,
        )
        return result

    d.addErrback(logerror, receiver)
    d.addBoth(record)
    return d.addBoth(lambda result, recv: (recv, result), receiver)


@inlineCallbacks
def _send_catch_log_deferred(
    signal: TypingAny,
    sender: TypingAny,
    *arguments: TypingAny,
    _governance: _SignalGovernance | None = None,
    **named: TypingAny,
) -> Generator[
    Deferred[TypingAny], TypingAny, list[tuple[Callable[..., TypingAny], TypingAny]]
]:
    def logerror(failure: Failure, recv: TypingAny) -> Failure:
        if dont_log is None or not isinstance(failure.value, dont_log):
            logger.error(
                "Error caught on signal handler: %(receiver)s",
                {"receiver": recv},
                exc_info=failure_to_exc_info(failure),
                extra={"spider": spider},
            )
        return failure

    dont_log = named.pop("dont_log", None)
    spider = named.get("spider")
    signal_name = _signal_name(signal) if _governance is not None else ""
    dfds: list[Deferred[tuple[TypingAny, TypingAny]]] = []
    for receiver in liveReceivers(getAllReceivers(sender, signal)):
        d: Deferred[TypingAny] = _maybeDeferred_coro(
            robustApply,
            True,
            receiver,
            *arguments,
            signal=signal,
            sender=sender,
            **named,
        )
        if _governance is None:
            d.addErrback(logerror, receiver)

            d2: Deferred[tuple[TypingAny, TypingAny]] = d.addBoth(
                lambda result, recv: (recv, result), receiver
            )
        else:
            d2 = _govern_deferred(
                d, receiver, signal_name, _governance, dont_log, spider
            )

        dfds.append(d2)

    results = yield DeferredList(dfds)
    return [result[1] for result in results]


async def send_catch_log_async(
    signal: TypingAny = Any,
    sender: TypingAny = Anonymous,
    *arguments: TypingAny,
    _governance: _SignalGovernance | None = None,
    **named: TypingAny,
) -> list[tuple[Callable[..., TypingAny], TypingAny]]:
    """Like :func:`send_catch_log` but supports :ref:`asynchronous signal handlers
    <signal-deferred>`.

    Returns a coroutine that completes once all signal handlers have finished.

    .. versionadded:: 2.14
    """
    # note that this returns exceptions instead of Failures in the second tuple member
    if is_asyncio_available():
        return await _send_catch_log_asyncio(
            signal, sender, *arguments, _governance=_governance, **named
        )
    results = await maybe_deferred_to_future(
        _send_catch_log_deferred(
            signal, sender, *arguments, _governance=_governance, **named
        )
    )
    return [
        (receiver, result.value if isinstance(result, Failure) else result)
        for receiver, result in results
    ]


async def _send_catch_log_asyncio(
    signal: TypingAny = Any,
    sender: TypingAny = Anonymous,
    *arguments: TypingAny,
    _governance: _SignalGovernance | None = None,
    **named: TypingAny,
) -> list[tuple[Callable[..., TypingAny], TypingAny]]:
    """Like :func:`send_catch_log` but supports :ref:`asynchronous signal handlers
    <signal-deferred>`.

    Returns a coroutine that completes once all signal handlers have finished.

    This function requires an installed asyncio reactor or a running asyncio
    event loop.

    .. versionadded:: 2.14
    """
    dont_log = named.pop("dont_log", ())
    dont_log = tuple(dont_log) if isinstance(dont_log, Sequence) else (dont_log,)
    spider = named.get("spider")
    handler_timeout = _governance.handler_timeout if _governance is not None else None
    signal_name = _signal_name(signal) if _governance is not None else ""
    handlers: list[Awaitable[TypingAny]] = []
    for receiver in liveReceivers(getAllReceivers(sender, signal)):

        async def handler(
            receiver: Callable[..., TypingAny],
        ) -> tuple[Callable[..., TypingAny], TypingAny]:
            result: TypingAny
            timed_out = False
            started_at = time.perf_counter() if _governance is not None else 0.0
            try:
                response = ensure_awaitable(
                    robustApply(
                        receiver, *arguments, signal=signal, sender=sender, **named
                    ),
                    _warn=global_object_name(receiver),
                )
                if handler_timeout is not None:
                    try:
                        result = await asyncio.wait_for(response, handler_timeout)
                    except TimeoutError as ex:
                        # wait_for() has already cancelled the hanging handler
                        timed_out = True
                        result = ex
                else:
                    result = await response
            except dont_log as ex:  # pylint: disable=catching-non-exception
                result = ex
            except Exception as ex:
                logger.exception(
                    "Error caught on signal handler: %(receiver)s",
                    {"receiver": receiver},
                    extra={"spider": spider},
                )
                result = ex
            if _governance is not None:
                _governance.observe(
                    signal_name,
                    global_object_name(receiver),
                    time.perf_counter() - started_at,
                    failure=timed_out or isinstance(result, BaseException),
                    timed_out=timed_out,
                    spider=spider,
                )
            return (receiver, result)

        handlers.append(handler(receiver))

    return cast(
        "list[tuple[Callable[..., TypingAny], TypingAny]]",
        await asyncio.gather(*handlers, return_exceptions=True),
    )


def disconnect_all(signal: TypingAny = Any, sender: TypingAny = Any) -> None:
    """Disconnect all signal handlers. Useful for cleaning up after running
    tests.
    """
    for receiver in liveReceivers(getAllReceivers(sender, signal)):
        disconnect(receiver, signal=signal, sender=sender)
