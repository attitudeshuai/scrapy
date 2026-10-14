from __future__ import annotations

import hashlib
import logging
from io import BytesIO
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any
from unittest import mock
from urllib.parse import urljoin

import pytest
from w3lib.url import file_uri_to_path

import scrapy
from scrapy import Spider, signals
from scrapy.exporters import JsonItemExporter
from scrapy.extensions.feedexport import (
    BlockingFeedStorage,
    FeedDelivery,
    FeedExporter,
    FileFeedStorage,
    FTPFeedStorage,
)
from scrapy.utils._ftp import ftp_store_file
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.mockserver.ftp import MockFTPServer
from tests.utils.decorators import coroutine_test
from tests.utils.feedexport import (
    MyItem,
    crawl_items,
    path_to_url,
    printf_escape,
    unique_path,
)

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from tests.mockserver.http import MockServer


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _batch_url(path: Path) -> str:
    # Batch URIs keep their %(batch_id)d placeholder, so unlike other paths
    # they are not printf-escaped.
    return urljoin("file:", f"/{path.as_posix()}")


def _staging_files(directory: Path) -> list[Path]:
    return [
        path
        for path in directory.iterdir()
        if ".staging" in path.name or path.name.startswith(".")
    ]


class FailingFinishJsonExporter(JsonItemExporter):
    """JsonItemExporter that fails when finalizing the feed."""

    def finish_exporting(self) -> None:
        raise RuntimeError("finish boom")


class AtomicBlockingStorage(BlockingFeedStorage):
    """Blocking storage, like the S3 or GCS ones, that writes to a local
    path and supports atomic delivery."""

    supported_modes = frozenset({"append", "create", "overwrite"})
    supports_atomic_delivery = True

    def __init__(self, uri: str, *, feed_options: dict[str, Any] | None = None):
        self.path = Path(file_uri_to_path(uri))
        super().__init__(feed_options=feed_options)

    def _store_in_thread(self, file: Any) -> None:
        file.seek(0)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(file.read())
        finally:
            file.close()


class FailingAtomicBlockingStorage(AtomicBlockingStorage):
    def _store_in_thread(self, file: Any) -> None:
        try:
            raise OSError("upload failed")
        finally:
            file.close()


class LegacyBlockingStorage(BlockingFeedStorage):
    """Blocking storage that predates the BlockingFeedStorage.__init__()
    method and hence does not call it."""

    def __init__(  # pylint: disable=super-init-not-called
        self, uri: str, *, feed_options: dict[str, Any] | None = None
    ):
        # Emulate storages written before BlockingFeedStorage.__init__()
        # existed, which do not call it.
        self.path = Path(file_uri_to_path(uri))

    def _store_in_thread(self, file: Any) -> None:
        file.seek(0)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(file.read())
        finally:
            file.close()


def _file_url(path: Path) -> str:
    return printf_escape(path_to_url(path))


async def _crawl_with_manifest(
    mockserver: MockServer,
    items: list[Any],
    settings: dict[str, Any],
) -> tuple[Crawler, tuple[FeedDelivery, ...], list[Any]]:
    class TestSpider(Spider):
        name = "testspider"
        start_urls = [mockserver.url("/")]

        def parse(self, response: Any) -> Any:
            yield from items

    crawler = get_crawler(TestSpider, settings)
    manifests: list[tuple[FeedDelivery, ...]] = []

    def receive_manifest(manifest: tuple[FeedDelivery, ...], **_kwargs: Any) -> None:
        manifests.append(manifest)

    crawler.signals.connect(
        receive_manifest,
        signal=signals.feed_exporter_closed,
    )
    closed_slots: list[Any] = []

    def receive_slot(slot: Any, **_kwargs: Any) -> None:
        closed_slots.append(slot)

    crawler.signals.connect(
        receive_slot,
        signal=signals.feed_slot_closed,
    )
    await crawler.crawl_async()
    return crawler, manifests[0] if manifests else (), closed_slots


class TestAtomicFileStorageUnit:
    """Direct tests of the local filesystem atomic staging and commit."""

    @staticmethod
    def _open(path: Path) -> tuple[FileFeedStorage, Any]:
        storage = FileFeedStorage(
            str(path), feed_options={"atomic_delivery": True, "mode": "overwrite"}
        )
        spider = scrapy.Spider("default")
        return storage, storage.open(spider)

    def test_target_not_written_before_store(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        _, file = self._open(path)
        file.write(b"content")
        assert not path.exists()
        file.close()

    def test_staging_is_hidden_in_target_directory(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        _, file = self._open(path)
        staging = Path(file.name)
        try:
            assert staging.parent == path.parent
            assert staging.name.startswith(".file.txt.")
            assert staging.name.endswith(".staging")
        finally:
            file.close()

    def test_overwrite_commit(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        storage, file = self._open(path)
        file.write(b"content")
        storage.store(file)
        assert path.read_bytes() == b"content"
        assert _staging_files(tmp_path) == []

    def test_create_conflict_leaves_target_and_staging(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.write_bytes(b"old")
        storage = FileFeedStorage(
            str(path), feed_options={"atomic_delivery": True, "mode": "create"}
        )
        file = storage.open(scrapy.Spider("default"))
        file.write(b"new")
        with pytest.raises(FileExistsError):
            storage.store(file)
        assert path.read_bytes() == b"old"
        assert _staging_files(tmp_path) != []

    def test_append_commit(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.write_bytes(b"old")
        storage = FileFeedStorage(
            str(path), feed_options={"atomic_delivery": True, "mode": "append"}
        )
        file = storage.open(scrapy.Spider("default"))
        file.write(b"new")
        storage.store(file)
        assert path.read_bytes() == b"oldnew"
        assert _staging_files(tmp_path) == []

    def test_append_without_target_commit(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        storage = FileFeedStorage(
            str(path), feed_options={"atomic_delivery": True, "mode": "append"}
        )
        file = storage.open(scrapy.Spider("default"))
        file.write(b"new")
        storage.store(file)
        assert path.read_bytes() == b"new"

    def test_append_failure_leaves_target_untouched(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.write_bytes(b"old")
        storage = FileFeedStorage(
            str(path), feed_options={"atomic_delivery": True, "mode": "append"}
        )
        file = storage.open(scrapy.Spider("default"))
        file.write(b"new")
        with (
            mock.patch(
                "scrapy.extensions.feedexport.Path.replace",
                side_effect=OSError("disk error"),
            ),
            pytest.raises(OSError, match="disk error"),
        ):
            storage.store(file)
        assert path.read_bytes() == b"old"
        # The staging with the new content stays for the caller to inspect.
        leftovers = _staging_files(tmp_path)
        assert len(leftovers) == 1
        assert leftovers[0].read_bytes() == b"new"

    def test_overwrite_failure_leaves_staging(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        storage, file = self._open(path)
        file.write(b"content")
        with (
            mock.patch(
                "scrapy.extensions.feedexport.Path.replace",
                side_effect=OSError("disk error"),
            ),
            pytest.raises(OSError, match="disk error"),
        ):
            storage.store(file)
        assert not path.exists()
        assert len(_staging_files(tmp_path)) == 1

    def test_non_atomic_opens_target_directly(self, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        storage = FileFeedStorage(str(path))
        file = storage.open(scrapy.Spider("default"))
        file.write(b"content")
        # Without atomic delivery the target is visible while being written.
        assert path.exists()
        storage.store(file)
        assert path.read_bytes() == b"content"


class TestAtomicLocalDelivery:
    @pytest.mark.parametrize(
        ("mode", "preexisting"),
        [
            ("overwrite", None),
            ("create", None),
            ("append", b'{"old": true}\n'),
        ],
    )
    @coroutine_test
    async def test_modes_deliver_atomically(
        self,
        mockserver: MockServer,
        tmp_path: Path,
        mode: str,
        preexisting: bytes | None,
    ) -> None:
        path = unique_path(tmp_path)
        if preexisting is not None:
            path.write_bytes(preexisting)
        settings = {
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "mode": mode,
                    "atomic_delivery": True,
                }
            }
        }
        crawler, manifest, slots = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        payload = b'{"foo": "bar"}\n'
        expected = (preexisting or b"") + payload
        assert path.read_bytes() == expected
        assert _staging_files(tmp_path) == []
        assert len(manifest) == 1
        record = manifest[0]
        assert record.status == "delivered"
        assert record.item_count == 1
        # The checksum covers the staged payload; in append mode that is the
        # appended portion, not the pre-existing target content.
        assert record.size == len(payload)
        assert record.checksum_algorithm == "sha256"
        assert record.checksum == _sha256(payload)
        assert slots[0].delivery is record
        assert crawler.stats.get_value("feedexport/success_count/FileFeedStorage") == 1

    @coroutine_test
    async def test_create_conflict(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        path.write_bytes(b"old")
        settings = {
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "mode": "create",
                    "atomic_delivery": True,
                }
            }
        }
        crawler, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert path.read_bytes() == b"old"
        assert _staging_files(tmp_path) == []
        assert len(manifest) == 1
        assert manifest[0].status == "skipped"
        assert manifest[0].item_count == 1
        assert manifest[0].error is not None
        assert manifest[0].error.startswith("FileExistsError")
        assert crawler.stats.get_value("feedexport/conflicts/FileFeedStorage") == 1

    @coroutine_test
    async def test_finish_failure_removes_staging(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        settings = {
            "FEEDS": {
                _file_url(path): {
                    "format": "json",
                    "atomic_delivery": True,
                }
            },
            "FEED_EXPORTERS": {"json": FailingFinishJsonExporter},
        }
        crawler, manifest, slots = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert not path.exists()
        assert _staging_files(tmp_path) == []
        record = slots[0].delivery
        assert record is not None
        assert record.status == "failed"
        assert record.error is not None
        assert "RuntimeError" in record.error
        assert record.staging_path is None
        assert manifest == (record,)
        assert crawler.stats.get_value("feedexport/failed_count/FileFeedStorage") == 1

    @coroutine_test
    async def test_finish_failure_keeps_staging(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        settings = {
            "FEEDS": {
                _file_url(path): {
                    "format": "json",
                    "atomic_delivery": True,
                    "keep_failed_staging": True,
                }
            },
            "FEED_EXPORTERS": {"json": FailingFinishJsonExporter},
        }
        _, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert not path.exists()
        record = manifest[0]
        assert record.status == "failed"
        assert record.staging_path is not None
        staging = Path(record.staging_path)
        assert staging.parent == tmp_path
        assert staging.exists()
        assert b"bar" in staging.read_bytes()

    @coroutine_test
    async def test_commit_failure_removes_staging(
        self,
        mockserver: MockServer,
        tmp_path: Path,
    ) -> None:
        path = unique_path(tmp_path)
        settings = {
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            }
        }
        with mock.patch(
            "scrapy.extensions.feedexport.Path.replace",
            side_effect=OSError("disk error"),
        ):
            _, manifest, _ = await _crawl_with_manifest(
                mockserver, [{"foo": "bar"}], settings
            )
        assert not path.exists()
        assert _staging_files(tmp_path) == []
        assert manifest[0].status == "failed"
        assert manifest[0].staging_path is None

    @coroutine_test
    async def test_empty_feed_delivered(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        settings = {
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            }
        }
        _, manifest, _ = await _crawl_with_manifest(mockserver, [], settings)
        assert path.read_bytes() == b""
        assert len(manifest) == 1
        assert manifest[0].item_count == 0
        assert manifest[0].size == 0
        assert manifest[0].checksum == _sha256(b"")

    @coroutine_test
    async def test_empty_feed_not_stored(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        settings = {
            "FEED_STORE_EMPTY": False,
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            },
        }
        _, manifest, slots = await _crawl_with_manifest(mockserver, [], settings)
        assert not path.exists()
        assert manifest == ()
        assert slots == []

    @coroutine_test
    async def test_non_atomic_manifest_has_no_checksum(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        settings = {"FEEDS": {_file_url(path): {"format": "jsonlines"}}}
        _, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert manifest[0].status == "delivered"
        assert manifest[0].size is None
        assert manifest[0].checksum is None

    @coroutine_test
    async def test_setting_enables_atomic_delivery(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path)
        settings = {
            "FEED_ATOMIC_DELIVERY": True,
            "FEEDS": {_file_url(path): {"format": "jsonlines"}},
        }
        _, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert manifest[0].checksum == _sha256(b'{"foo": "bar"}\n')

    @coroutine_test
    async def test_batch_manifest(self, mockserver: MockServer, tmp_path: Path) -> None:
        batch_dir = unique_path(tmp_path)
        settings = {
            "FEEDS": {
                _batch_url(batch_dir / "%(batch_id)d.jl"): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        items = [
            MyItem({"foo": "bar1"}),
            MyItem({"foo": "bar2"}),
            MyItem({"foo": "bar3"}),
        ]
        _, manifest, slots = await _crawl_with_manifest(mockserver, items, settings)
        assert len(manifest) == 2
        assert [record.batch_id for record in manifest] == [1, 2]
        assert [record.item_count for record in manifest] == [2, 1]
        for record, slot in zip(manifest, slots, strict=True):
            assert slot.delivery is record
            batch_file = Path(batch_dir, f"{record.batch_id}.jl")
            data = batch_file.read_bytes()
            assert record.checksum == _sha256(data)
            assert record.size == len(data)

    @coroutine_test
    async def test_stdout_atomic_option_ignored(
        self,
        caplog: pytest.LogCaptureFixture,
        mockserver: MockServer,
    ) -> None:
        settings = {
            "FEED_ATOMIC_DELIVERY": True,
            "FEEDS": {"stdout:": {"format": "jsonlines"}},
        }
        with caplog.at_level(logging.WARNING):
            _, manifest, _ = await _crawl_with_manifest(
                mockserver, [{"foo": "bar"}], settings
            )
        assert (
            "Atomic feed delivery is not supported by StdoutFeedStorage" in caplog.text
        )
        assert manifest[0].status == "delivered"
        assert manifest[0].checksum is None


class TestAtomicBlockingStorage:
    @coroutine_test
    async def test_success_removes_staging(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        temp_dir = tmp_path / "temp"
        temp_dir.mkdir()
        path = unique_path(tmp_path)
        settings = {
            "FEED_TEMPDIR": str(temp_dir),
            "FEED_STORAGES": {"file": AtomicBlockingStorage},
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            },
        }
        _, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert path.read_bytes() == b'{"foo": "bar"}\n'
        assert manifest[0].status == "delivered"
        assert manifest[0].checksum == _sha256(b'{"foo": "bar"}\n')
        assert list(temp_dir.iterdir()) == []

    @coroutine_test
    async def test_failure_removes_staging_by_default(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        temp_dir = tmp_path / "temp"
        temp_dir.mkdir()
        path = unique_path(tmp_path)
        settings = {
            "FEED_TEMPDIR": str(temp_dir),
            "FEED_STORAGES": {"file": FailingAtomicBlockingStorage},
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            },
        }
        _, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        assert not path.exists()
        assert list(temp_dir.iterdir()) == []
        assert manifest[0].status == "failed"
        assert manifest[0].staging_path is None

    @coroutine_test
    async def test_failure_keeps_staging(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        temp_dir = tmp_path / "temp"
        temp_dir.mkdir()
        path = unique_path(tmp_path)
        settings = {
            "FEED_TEMPDIR": str(temp_dir),
            "FEED_STORAGES": {"file": FailingAtomicBlockingStorage},
            "FEEDS": {
                _file_url(path): {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                    "keep_failed_staging": True,
                }
            },
        }
        _, manifest, _ = await _crawl_with_manifest(
            mockserver, [{"foo": "bar"}], settings
        )
        record = manifest[0]
        assert record.status == "failed"
        assert record.staging_path is not None
        staging = Path(record.staging_path)
        assert staging.parent == temp_dir
        assert staging.read_bytes() == b'{"foo": "bar"}\n'


class TestAtomicConfiguration:
    def test_unsupported_storage_raises(self) -> None:
        class CustomStorage:
            supported_modes = frozenset({"append", "create", "overwrite"})

            def __init__(self, uri: str, *, feed_options: dict[str, Any] | None = None):
                pass

            def open(self, spider: Spider) -> IO[bytes]:
                return BytesIO()

            def store(self, file: IO[bytes]) -> None:
                pass

        settings = {
            "FEEDS": {
                "custom://host/path": {
                    "format": "jsonlines",
                    "atomic_delivery": True,
                }
            },
            "FEED_STORAGES": {"custom": CustomStorage},
        }
        with pytest.raises(ValueError, match="does not support atomic"):
            FeedExporter(get_crawler(settings_dict=settings))

    def test_ftp_append_atomic_raises(self) -> None:
        settings = {
            "FEEDS": {
                "ftp://example.com/dir/file.jl": {
                    "format": "jsonlines",
                    "mode": "append",
                    "atomic_delivery": True,
                }
            }
        }
        with pytest.raises(ValueError, match="'append'"):
            FeedExporter(get_crawler(settings_dict=settings))

    @coroutine_test
    async def test_legacy_blocking_storage_still_works(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        # A BlockingFeedStorage subclass that does not call __init__() must
        # keep the non-atomic behavior when atomic delivery stays disabled.
        path = unique_path(tmp_path)
        settings = {
            "FEED_STORAGES": {"file": LegacyBlockingStorage},
            "FEEDS": {_file_url(path): {"format": "jsonlines"}},
        }
        await crawl_items(mockserver, [{"foo": "bar"}], settings)
        assert path.read_bytes() == b'{"foo": "bar"}\n'


class TestFtpAtomicUpload:
    @staticmethod
    def _store(ftp: mock.MagicMock, *, atomic: bool) -> None:
        with mock.patch("scrapy.utils._ftp.FTP", return_value=ftp):
            ftp_store_file(
                path="/dir/file",
                file=BytesIO(b"abc"),
                host="host",
                port=21,
                username="user",
                password="pass",
                atomic=atomic,
            )

    def test_uploads_to_staging_and_renames(self) -> None:
        ftp = mock.MagicMock()
        self._store(ftp, atomic=True)
        store_command = ftp.storbinary.call_args.args[0]
        assert store_command.startswith("STOR .file.staging-")
        staging_name, target_name = ftp.rename.call_args.args
        assert staging_name == store_command.removeprefix("STOR ")
        assert target_name == "file"
        ftp.delete.assert_not_called()

    def test_failed_upload_deletes_remote_staging(self) -> None:
        ftp = mock.MagicMock()
        ftp.storbinary.side_effect = OSError("connection lost")
        with pytest.raises(OSError, match="connection lost"):
            self._store(ftp, atomic=True)
        ftp.rename.assert_not_called()
        assert ftp.delete.call_args.args[0].startswith(".file.staging-")

    def test_non_atomic_stores_directly(self) -> None:
        ftp = mock.MagicMock()
        self._store(ftp, atomic=False)
        assert ftp.storbinary.call_args.args[0] == "STOR file"
        ftp.rename.assert_not_called()
        ftp.delete.assert_not_called()


@coroutine_test
async def test_atomic_ftp_end_to_end() -> None:
    with MockFTPServer() as ftp_server:
        filename = "file"
        crawler = get_crawler(settings_dict={})
        storage = build_from_crawler(
            FTPFeedStorage,
            crawler,
            ftp_server.url(filename),
            feed_options={"atomic_delivery": True},
        )
        spider = scrapy.Spider.from_crawler(crawler, "default")
        file = storage.open(spider)
        file.write(b"content")
        await maybe_deferred_to_future(storage.store(file))
        target = ftp_server.path / filename
        assert target.read_bytes() == b"content"
        assert list(ftp_server.path.iterdir()) == [target]
