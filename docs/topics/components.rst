.. _topics-components:

==========
Components
==========

A Scrapy component is any class whose objects are built using
:func:`~scrapy.utils.misc.build_from_crawler`.

That includes the classes that you may assign to the following settings:

- :setting:`ADDONS`
- :setting:`DOWNLOAD_HANDLERS`
- :setting:`DOWNLOADER_MIDDLEWARES`
- :setting:`DUPEFILTER_CLASS`
- :setting:`EXTENSIONS`
- :setting:`FEED_EXPORTERS`
- :setting:`FEED_STORAGES`
- :setting:`ITEM_PIPELINES`
- :setting:`SCHEDULER`
- :setting:`SCHEDULER_DISK_QUEUE`
- :setting:`SCHEDULER_MEMORY_QUEUE`
- :setting:`SCHEDULER_PRIORITY_QUEUE`
- :setting:`SCHEDULER_START_DISK_QUEUE`
- :setting:`SCHEDULER_START_MEMORY_QUEUE`
- :setting:`SPIDER_MIDDLEWARES`
- :setting:`TWISTED_DNS_RESOLVER`

Third-party Scrapy components may also let you define additional Scrapy
components, usually configurable through :ref:`settings <topics-settings>`, to
modify their behavior.

.. _from-crawler:

Initializing from the crawler
=============================

Any Scrapy component may optionally define the following class method:

.. classmethod:: from_crawler(cls, crawler: scrapy.crawler.Crawler, *args, **kwargs)

    Return an instance of the component based on *crawler*.

    *args* and *kwargs* are component-specific arguments that some components
    receive. However, most components do not get any arguments, and instead
    :ref:`use settings <component-settings>`.

    If a component class defines this method, this class method is called to
    create any instance of the component.

    The *crawler* object provides access to all Scrapy core components like
    :ref:`settings <topics-settings>` and :ref:`signals <topics-signals>`,
    allowing the component to access them and hook its functionality into
    Scrapy.

.. _component-settings:

Settings
========

Components can be configured through :ref:`settings <topics-settings>`.

Components can read any setting from the
:attr:`~scrapy.crawler.Crawler.settings` attribute of the
:class:`~scrapy.crawler.Crawler` object they can :ref:`get for initialization
<from-crawler>`. That includes both built-in and custom settings.

For example:

.. code-block:: python

    class MyExtension:
        @classmethod
        def from_crawler(cls, crawler):
            settings = crawler.settings
            return cls(settings.getbool("LOG_ENABLED"))

        def __init__(self, log_is_enabled=False):
            if log_is_enabled:
                print("log is enabled!")

Components do not need to declare their custom settings programmatically.
However, they should document them so that users know they exist and how to use
them.

It is a good practice to prefix custom settings with the name of the component,
to avoid collisions with custom settings of other existing (or future)
components. For example, an extension called ``WarcCaching`` could prefix its
custom settings with ``WARC_CACHING_``.

Another good practice, mainly for components meant for :ref:`component priority
dictionaries <component-priority-dictionaries>`, is to provide a boolean setting
called ``<PREFIX>_ENABLED`` (e.g. ``WARC_CACHING_ENABLED``) to allow toggling
that component on and off without changing the component priority dictionary
setting. You can usually check the value of such a setting during
initialization, and if ``False``, raise
:exc:`~scrapy.exceptions.NotConfigured`.

When choosing a name for a custom setting, it is also a good idea to have a
look at the names of :ref:`built-in settings <topics-settings-ref>` to maintain
consistency with them.

.. _enforce-component-requirements:

Enforcing requirements
======================

Sometimes, your components may only be intended to work under certain
conditions. For example, they may require a minimum version of Scrapy to work as
intended, or they may require certain settings to have specific values.

In addition to describing those conditions in the documentation of your
component, it is a good practice to raise an exception from the ``__init__``
method of your component if those conditions are not met at run time.

In the case of :ref:`downloader middlewares <topics-downloader-middleware>`,
:ref:`extensions <topics-extensions>`, :ref:`item pipelines
<topics-item-pipeline>`, and :ref:`spider middlewares
<topics-spider-middleware>`, you should raise
:exc:`~scrapy.exceptions.NotConfigured`, passing a description of the issue as
a parameter to the exception so that it is printed in the logs for the user to
see. For other components, feel free to raise whatever other exception feels
right to you; for example, :exc:`RuntimeError` would make sense for a Scrapy
version mismatch, while :exc:`ValueError` may be better if the issue is the
value of a setting.

If your requirement is a minimum Scrapy version, you may use
:attr:`scrapy.__version__` to enforce your requirement. For example:

.. code-block:: python

    from packaging.version import parse as parse_version

    import scrapy


    class MyComponent:
        def __init__(self):
            if parse_version(scrapy.__version__) < parse_version("2.7"):
                raise RuntimeError(
                    f"{MyComponent.__qualname__} requires Scrapy 2.7 or "
                    f"later, which allows defining the process_spider_output "
                    f"method of spider middlewares as an asynchronous "
                    f"generator."
                )

.. _component-failures:

Failure handling
================

.. versionadded:: VERSION

Failures of :ref:`extensions <topics-extensions>`, :ref:`downloader
middlewares <topics-downloader-middleware>`, :ref:`spider middlewares
<topics-spider-middleware>`, and :ref:`item pipelines <topics-item-pipeline>`
can be isolated from the rest of the crawl.

Startup failures
----------------

A component can opt out of the crawl by raising
:exc:`~scrapy.exceptions.NotConfigured` when it is initialized. This is meant
for components that are disabled through their settings.

Any other failure while loading the component class or initializing the
component aborts the crawl by default. Assign the import path of the
component the ``'skip'`` policy in the
:setting:`COMPONENT_FAILURE_POLICIES` setting to instead log the error and
run the crawl without the component:

.. code-block:: python

    COMPONENT_FAILURE_POLICIES = {
        "myproject.extensions.MonitoringExtension": "skip",
    }

The components that were skipped, together with their error and traceback,
are returned by :meth:`~scrapy.crawler.Crawler.get_component_failures`.

Run-time failures
-----------------

When a component raises an exception from one of its hooks, the exception is
still handled exactly as before, but it is also accounted against the
component. Once the same component fails
:setting:`COMPONENT_FAILURE_MAX_FAILURES` times within
:setting:`COMPONENT_FAILURE_WINDOW` seconds, it is isolated: its hooks are
no longer called, as if the component did not define them, until the crawl
ends or the isolation is lifted.

Exceptions that are part of the regular flow of requests and items are not
accounted: :exc:`~scrapy.exceptions.IgnoreRequest`,
:exc:`~scrapy.exceptions.DropItem`, :exc:`~scrapy.exceptions.CloseSpider`,
:exc:`~scrapy.exceptions.DontCloseSpider`,
:exc:`~scrapy.exceptions.StopDownload`, and
:exc:`~scrapy.exceptions.NotConfigured`.

Signal handlers keep their catch-and-log behavior and are never isolated.
Isolation therefore does not apply to extensions, which connect to
:ref:`signals <topics-signals>` instead of defining hooks.

The isolated components are returned by
:meth:`~scrapy.crawler.Crawler.get_isolated_components`, which reports the
number of failures, the last error, and the number of hooks that were
skipped. The isolation can be lifted with
:meth:`~scrapy.crawler.Crawler.reset_component_isolation`, passing a
component class, an import path, or a component instance:

.. code-block:: python

    crawler.reset_component_isolation(MonitoringMiddleware)

Failures, isolations, and skipped hooks are also counted in the
:ref:`stats <topics-stats>` under the ``component_failures/`` prefix.

API reference
=============

The following function can be used to create an instance of a component class:

.. autofunction:: scrapy.utils.misc.build_from_crawler

The following function can also be useful when implementing a component to
report the import path of the component class, e.g. when reporting problems:

.. autofunction:: scrapy.utils.python.global_object_name
