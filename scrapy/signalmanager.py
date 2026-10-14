from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from pydispatch import dispatcher
from twisted.internet.defer import Deferred

from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.utils import signal as _signal
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.signal import _SignalGovernance

if TYPE_CHECKING:
    from collections.abc import Callable

    from scrapy.crawler import Crawler


def _optional_float_setting(settings: Any, name: str) -> float | None:
    # Settings.get() turns an explicit None into the provided default, read
    # the raw value so None can disable the threshold
    value = settings.attributes[name].value
    return None if value is None else float(value)


class SignalManager:
    def __init__(self, sender: object = dispatcher.Anonymous):
        self.sender: object = sender
        self._governance: _SignalGovernance | None = None

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> SignalManager:
        manager = cls(crawler)
        if crawler.settings.getbool("SIGNALS_GOVERNANCE_ENABLED"):
            manager._governance = _SignalGovernance(
                warn_threshold=_optional_float_setting(
                    crawler.settings, "SIGNALS_HANDLER_WARN_THRESHOLD"
                ),
                handler_timeout=_optional_float_setting(
                    crawler.settings, "SIGNALS_HANDLER_TIMEOUT"
                ),
            )
        return manager

    def connect(
        self, receiver: Callable[..., Any], signal: object, **kwargs: Any
    ) -> None:
        """
        Connect a receiver function to a signal.

        The signal can be any object, although Scrapy comes with some
        predefined signals that are documented in the :ref:`topics-signals`
        section.

        :param receiver: the function to be connected
        :type receiver: collections.abc.Callable

        :param signal: the signal to connect to
        :type signal: object
        """
        kwargs.setdefault("sender", self.sender)
        dispatcher.connect(receiver, signal, **kwargs)

    def disconnect(
        self, receiver: Callable[..., Any], signal: object, **kwargs: Any
    ) -> None:
        """
        Disconnect a receiver function from a signal. This has the
        opposite effect of the :meth:`connect` method, and the arguments
        are the same.
        """
        kwargs.setdefault("sender", self.sender)
        dispatcher.disconnect(receiver, signal, **kwargs)

    def send_catch_log(
        self, signal: object, **kwargs: Any
    ) -> list[tuple[Callable[..., Any], Any]]:
        """
        Send a signal, catch exceptions and log them.

        The keyword arguments are passed to the signal handlers (connected
        through the :meth:`connect` method).
        """
        kwargs.setdefault("sender", self.sender)
        return _signal.send_catch_log(signal, _governance=self._governance, **kwargs)

    def send_catch_log_deferred(
        self, signal: object, **kwargs: Any
    ) -> Deferred[list[tuple[Callable[..., Any], Any]]]:  # pragma: no cover
        """
        Like :meth:`send_catch_log` but supports :ref:`asynchronous signal
        handlers <signal-deferred>`.

        Returns a Deferred that gets fired once all signal handlers
        have finished. Send a signal, catch exceptions and log them.

        The keyword arguments are passed to the signal handlers (connected
        through the :meth:`connect` method).
        """
        kwargs.setdefault("sender", self.sender)
        warnings.warn(
            "send_catch_log_deferred() is deprecated, use send_catch_log_async() instead",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return _signal._send_catch_log_deferred(
            signal, _governance=self._governance, **kwargs
        )

    async def send_catch_log_async(
        self, signal: object, **kwargs: Any
    ) -> list[tuple[Callable[..., Any], Any]]:
        """
        Like :meth:`send_catch_log` but supports :ref:`asynchronous signal
        handlers <signal-deferred>`.

        Returns a coroutine that completes once all signal handlers
        have finished. Send a signal, catch exceptions and log them.

        The keyword arguments are passed to the signal handlers (connected
        through the :meth:`connect` method).

        .. versionadded:: 2.14
        """
        # note that this returns exceptions instead of Failures in the second tuple member
        kwargs.setdefault("sender", self.sender)
        return await _signal.send_catch_log_async(
            signal, _governance=self._governance, **kwargs
        )

    def disconnect_all(self, signal: object, **kwargs: Any) -> None:
        """
        Disconnect all receivers from the given signal.

        :param signal: the signal to disconnect from
        :type signal: object
        """
        kwargs.setdefault("sender", self.sender)
        _signal.disconnect_all(signal, **kwargs)

    def get_handler_stats(self) -> dict[str, dict[str, dict[str, int | float]]]:
        """Return aggregated handler call timings and failure counts.

        The outer key is the signal name and the inner key is the handler
        object name, as used in the warning log messages. Each value reports
        the number of calls, failures and timeouts and the total and maximum
        handler duration in seconds. The mapping is empty unless signal
        governance is enabled with
        :setting:`SIGNALS_GOVERNANCE_ENABLED`.

        .. versionadded:: 2.19.0
        """
        if self._governance is None:
            return {}
        return self._governance.get_stats()

    async def wait_for(self, signal: object) -> None:
        """Await the next *signal*.

        See :ref:`start-requests-lazy` for an example.
        """
        d: Deferred[None] = Deferred()

        def handle() -> None:
            self.disconnect(handle, signal)
            d.callback(None)

        self.connect(handle, signal)
        await maybe_deferred_to_future(d)
