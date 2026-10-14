from __future__ import annotations

import logging
import time
import traceback
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from scrapy.exceptions import (
    CloseSpider,
    DontCloseSpider,
    DropItem,
    IgnoreRequest,
    NotConfigured,
    StopDownload,
)

if TYPE_CHECKING:
    from scrapy.crawler import Crawler

logger = logging.getLogger(__name__)

# Exceptions that components raise as part of the normal request and item
# flow. They do not signal a malfunctioning component, so they never count
# towards its run-time failure threshold.
_CONTROL_FLOW_EXCEPTIONS = (
    NotConfigured,
    IgnoreRequest,
    DropItem,
    CloseSpider,
    DontCloseSpider,
    StopDownload,
)


@dataclass(frozen=True)
class _AssemblyFailureRecord:
    """A component that could not be loaded or instantiated when its manager
    was assembled."""

    path: str
    kind: str
    error: BaseException
    traceback_text: str
    time: datetime


@dataclass(frozen=True)
class _IsolationRecord:
    """A point-in-time view of the run-time failure state of a component."""

    component: object
    path: str
    kind: str
    failure_count: int
    isolated: bool
    isolated_at: datetime | None
    last_error: BaseException | None
    skipped_calls: int


class _ComponentRuntimeState:
    __slots__ = (
        "component",
        "failure_count",
        "failure_times",
        "isolated",
        "isolated_at",
        "kind",
        "last_error",
        "path",
        "skipped_calls",
    )

    def __init__(self, component: object, path: str, kind: str) -> None:
        self.component = component
        self.path = path
        self.kind = kind
        self.failure_times: deque[float] = deque()
        self.failure_count = 0
        self.isolated = False
        self.isolated_at: datetime | None = None
        self.last_error: BaseException | None = None
        self.skipped_calls = 0

    def as_record(self) -> _IsolationRecord:
        return _IsolationRecord(
            component=self.component,
            path=self.path,
            kind=self.kind,
            failure_count=self.failure_count,
            isolated=self.isolated,
            isolated_at=self.isolated_at,
            last_error=self.last_error,
            skipped_calls=self.skipped_calls,
        )


class _ComponentFailureRegistry:
    """Keeps the assembly failures and the run-time failure state of the
    components managed by a crawler.

    A single registry is shared by every component manager of a crawler, so
    that the skipped and isolated components can be listed and reset from one
    place.
    """

    def __init__(self, crawler: Crawler) -> None:
        self._crawler = crawler
        self._configured = False
        self._policies: dict[str, str] = {}
        self._max_failures = 0
        self._window = 60.0
        self._assembly_failures: dict[tuple[str, str], _AssemblyFailureRecord] = {}
        self._states: dict[object, _ComponentRuntimeState] = {}

    def _configure(self) -> None:
        if self._configured:
            return
        settings = self._crawler.settings
        policies = settings.getdict("COMPONENT_FAILURE_POLICIES")
        for path, policy in policies.items():
            if policy not in ("raise", "skip"):
                raise ValueError(
                    f"Invalid value {policy!r} for component {path!r} in the"
                    " COMPONENT_FAILURE_POLICIES setting, expected 'raise' or"
                    " 'skip'."
                )
            self._policies[str(path)] = policy
        self._max_failures = settings.getint("COMPONENT_FAILURE_MAX_FAILURES")
        if self._max_failures < 0:
            raise ValueError(
                "The COMPONENT_FAILURE_MAX_FAILURES setting must be 0 or a"
                " positive number."
            )
        self._window = settings.getfloat("COMPONENT_FAILURE_WINDOW")
        if self._window < 0:
            raise ValueError(
                "The COMPONENT_FAILURE_WINDOW setting must be 0 or a positive"
                " number of seconds."
            )
        self._configured = True

    def _inc_stat(self, key: str) -> None:
        stats = getattr(self._crawler, "_stats", None)
        if stats is not None:
            stats.inc_value(key)

    def register(self, component: object, path: str, kind: str) -> None:
        """Associate a successfully assembled component with its path."""
        self._configure()
        self._states.setdefault(
            component, _ComponentRuntimeState(component, path, kind)
        )

    def record_assembly_failure(
        self, path: str, kind: str, error: BaseException
    ) -> bool:
        """Record that *path* failed to load or instantiate.

        Return whether the declared failure policy is to skip the component
        instead of aborting the crawl.
        """
        self._configure()
        record = _AssemblyFailureRecord(
            path=path,
            kind=kind,
            error=error,
            traceback_text="".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
            time=datetime.now(UTC),
        )
        self._assembly_failures[(kind, path)] = record
        self._inc_stat(f"component_failures/assembly/{path}")
        if self._policies.get(path, "raise") != "skip":
            return False
        logger.error(
            f"Skipping failed {kind} {path}: {type(error).__name__}: {error}",
            exc_info=error,
            extra={"crawler": self._crawler},
        )
        return True

    def get_assembly_failures(self) -> list[_AssemblyFailureRecord]:
        return list(self._assembly_failures.values())

    def record_runtime_failure(self, component: object, error: BaseException) -> None:
        """Account for an exception raised by a component hook.

        When the number of failures recorded for the component within the
        configured time window reaches the configured threshold, the
        component is isolated until its isolation is reset.
        """
        self._configure()
        if not self._max_failures:
            return
        state = self._states.get(component)
        if state is None or state.isolated:
            return
        if isinstance(error, _CONTROL_FLOW_EXCEPTIONS):
            return
        now = time.monotonic()
        cutoff = now - self._window
        while state.failure_times and state.failure_times[0] < cutoff:
            state.failure_times.popleft()
        state.failure_times.append(now)
        state.failure_count += 1
        state.last_error = error
        self._inc_stat(f"component_failures/runtime/{state.path}")
        if len(state.failure_times) >= self._max_failures:
            state.isolated = True
            state.isolated_at = datetime.now(UTC)
            self._inc_stat(f"component_failures/isolated/{state.path}")
            logger.warning(
                f"Isolating {state.kind} {state.path} after"
                f" {len(state.failure_times)} failures within {self._window}"
                f" seconds. Its hooks will be skipped until the isolation is"
                f" reset with Crawler.reset_component_isolation(). Last error:"
                f" {type(error).__name__}: {error}",
                exc_info=error,
                extra={"crawler": self._crawler},
            )

    def is_isolated(self, component: object) -> bool:
        state = self._states.get(component)
        return state is not None and state.isolated

    def record_skipped_hook(self, component: object) -> None:
        state = self._states.get(component)
        if state is None:  # pragma: no cover
            # Only components checked by is_isolated() can reach this method.
            return
        state.skipped_calls += 1
        self._inc_stat(f"component_failures/skipped/{state.path}")

    def get_isolated_components(self) -> list[_IsolationRecord]:
        return [state.as_record() for state in self._states.values() if state.isolated]

    def reset(self, target: type | str | object) -> list[str]:
        """Lift the isolation of every component matching *target*.

        *target* is a component instance, a component class, or the import
        path of a component. The failures accounted within the time window
        are cleared so the component does not get isolated again immediately.
        Return the import paths of the components whose isolation was lifted.
        """
        released: list[str] = []
        for state in self._states.values():
            if not state.isolated:
                continue
            if isinstance(target, str):
                matched = state.path == target
            elif isinstance(target, type):
                matched = isinstance(state.component, target)
            else:
                matched = state.component is target
            if not matched:
                continue
            state.isolated = False
            state.isolated_at = None
            state.failure_times.clear()
            released.append(state.path)
            logger.info(
                f"Isolation of {state.kind} {state.path} has been reset.",
                extra={"crawler": self._crawler},
            )
        return released
