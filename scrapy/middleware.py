from __future__ import annotations

import logging
import pprint
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, Self, TypeVar, cast

from scrapy.exceptions import NotConfigured, ScrapyDeprecationWarning
from scrapy.utils.conf import build_component_list
from scrapy.utils.defer import ensure_awaitable
from scrapy.utils.deprecate import argument_is_required
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.python import global_object_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from twisted.internet.defer import Deferred

    from scrapy import Spider
    from scrapy._component_failures import _ComponentFailureRegistry
    from scrapy.crawler import Crawler
    from scrapy.settings import Settings


logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_P = ParamSpec("_P")


class _IsolatedHook:
    """Returned instead of calling the hook of an isolated component."""


_isolated_hook = _IsolatedHook()


class MiddlewareManager(ABC):
    """Base class for implementing middleware managers"""

    component_name: str
    # Name of the component priority dictionary setting of this manager.
    # When set, a component list that cannot be normalized because one of
    # its paths fails to import is rebuilt without the normalization, so the
    # offending path is handled per component instead of killing the crawl.
    _component_setting_name: str | None = None
    _compat_spider: Spider | None = None

    def __init__(self, *middlewares: object, crawler: Crawler | None = None) -> None:
        self.crawler: Crawler | None = crawler
        self._component_failures: _ComponentFailureRegistry | None = getattr(
            crawler, "_component_failures", None
        )
        if crawler is None:
            warnings.warn(
                f"MiddlewareManager.__init__() was called without the crawler argument"
                f" when creating {global_object_name(self.__class__)}."
                f" This is deprecated and the argument will be required in future Scrapy versions.",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )
        self.middlewares: tuple[object, ...] = middlewares
        # Only process_spider_output and process_spider_exception can be None.
        self.methods: dict[str, deque[Callable[..., Any] | None]] = defaultdict(deque)
        self._mw_methods_requiring_spider: set[Callable[..., Any]] = set()
        for mw in middlewares:
            self._add_middleware(mw)

    @property
    def _spider(self) -> Spider:
        if self.crawler is not None:
            if self.crawler.spider is None:
                raise ValueError(
                    f"{type(self).__name__} needs to access self.crawler.spider but it is None."
                )
            return self.crawler.spider
        if self._compat_spider is not None:
            return self._compat_spider
        raise ValueError(f"{type(self).__name__} has no known Spider instance.")

    def _set_compat_spider(self, spider: Spider | None) -> None:
        if spider is None or self.crawler is not None:
            return
        # printing a deprecation warning is the caller's responsibility
        if self._compat_spider is None:
            self._compat_spider = spider
        elif self._compat_spider is not spider:
            raise RuntimeError(
                f"Different instances of Spider were passed to {type(self).__name__}:"
                f" {self._compat_spider!r} and {spider!r}"
            )

    @classmethod
    @abstractmethod
    def _get_mwlist_from_settings(cls, settings: Settings) -> list[Any]:
        raise NotImplementedError

    @classmethod
    def _get_mwlist(cls, settings: Settings) -> tuple[list[Any], Exception | None]:
        """Return the component list and the failure of its normalization.

        Normalizing the list imports every configured component path, so a
        single bad path aborts the whole list. When that happens, fall back
        to the unnormalized list: the bad path then fails on its own and can
        be handled according to the configured failure policy.
        """
        try:
            return cls._get_mwlist_from_settings(settings), None
        except ValueError as normalization_failure:
            setting_name = cls._component_setting_name
            if setting_name is None:
                raise
            return (
                build_component_list(settings.getwithbase(setting_name)),
                normalization_failure,
            )

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        failures: _ComponentFailureRegistry | None = getattr(
            crawler, "_component_failures", None
        )
        mwlist, normalization_failure = cls._get_mwlist(crawler.settings)
        middlewares = []
        enabled = []
        for clspath in mwlist:
            try:
                mwcls = load_object(clspath)
                mw = build_from_crawler(mwcls, crawler)
            except NotConfigured as e:
                if e.args:
                    logger.warning(
                        "Disabled %(clspath)s: %(eargs)s",
                        {"clspath": clspath, "eargs": e.args[0]},
                        extra={"crawler": crawler},
                    )
            except Exception as error:
                path = (
                    clspath if isinstance(clspath, str) else global_object_name(clspath)
                )
                if failures is not None and failures.record_assembly_failure(
                    path, cls.component_name, error
                ):
                    continue
                if normalization_failure is not None:
                    raise normalization_failure from error
                raise
            else:
                middlewares.append(mw)
                enabled.append(clspath)

        logger.info(
            "Enabled %(componentname)ss:\n%(enabledlist)s",
            {
                "componentname": cls.component_name,
                "enabledlist": pprint.pformat(enabled),
            },
            extra={"crawler": crawler},
        )
        manager = cls(*middlewares, crawler=crawler)
        if failures is not None:
            for mw in middlewares:
                failures.register(
                    mw, global_object_name(mw.__class__), cls.component_name
                )
        return manager

    def _hook_is_isolated(self, method: Callable[..., Any]) -> bool:
        """Return whether the component of the bound *method* is isolated.

        Record the skipped call when it is.
        """
        failures = self._component_failures
        if failures is None:
            return False
        component = getattr(method, "__self__", None)
        if not failures.is_isolated(component):
            return False
        failures.record_skipped_hook(component)
        return True

    def _record_hook_failure(
        self, method: Callable[..., Any], error: Exception
    ) -> None:
        if self._component_failures is not None:
            self._component_failures.record_runtime_failure(
                getattr(method, "__self__", None), error
            )

    async def _call_hook(
        self,
        method: Callable[..., Any],
        *args: Any,
        _warn: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Call the bound component *method*.

        Skip the call while the component is isolated and return the
        _isolated_hook sentinel. Otherwise record any exception it raises
        before re-raising it, so the failure handling of the caller stays
        unchanged.
        """
        if self._hook_is_isolated(method):
            return _isolated_hook
        try:
            return await ensure_awaitable(method(*args, **kwargs), _warn=_warn)
        except Exception as error:
            self._record_hook_failure(method, error)
            raise

    def _add_middleware(self, mw: Any) -> None:  # noqa: B027
        pass

    def _check_mw_method_spider_arg(self, method: Callable[..., Any]) -> None:
        if argument_is_required(method, "spider"):
            warnings.warn(
                f"{method.__qualname__}() requires a spider argument,"
                f" this is deprecated and the argument will not be passed in future Scrapy versions."
                f" If you need to access the spider instance you can save the crawler instance"
                f" passed to from_crawler() and use its spider attribute.",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )
            self._mw_methods_requiring_spider.add(method)

    async def _process_chain(
        self,
        methodname: str,
        obj: _T,
        *args: Any,
        add_spider: bool = False,
        always_add_spider: bool = False,
        warn_deferred: bool = False,
    ) -> _T:
        methods = cast(
            "Iterable[Callable[Concatenate[_T, _P], _T]]", self.methods[methodname]
        )
        for method in methods:
            warn = global_object_name(method) if warn_deferred else None
            result: Any
            if always_add_spider or (
                add_spider and method in self._mw_methods_requiring_spider
            ):
                result = await self._call_hook(
                    method, obj, *(*args, self._spider), _warn=warn
                )
            else:
                result = await self._call_hook(method, obj, *args, _warn=warn)
            if result is not _isolated_hook:
                obj = result
        return obj

    def open_spider(self, spider: Spider) -> Deferred[list[None]]:  # pragma: no cover
        raise NotImplementedError(
            "MiddlewareManager.open_spider() is no longer implemented"
            " and will be removed in a future Scrapy version."
        )

    def close_spider(self, spider: Spider) -> Deferred[list[None]]:  # pragma: no cover
        raise NotImplementedError(
            "MiddlewareManager.close_spider() is no longer implemented"
            " and will be removed in a future Scrapy version."
        )
