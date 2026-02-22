"""Tests for Download Manager — source CRUD, decompression, hooks."""

import pytest
from pathlib import Path
from unittest.mock import patch

from tusk.core.downloads import (
    DownloadSource,
    StorageBackend,
    add_source,
    get_source,
    get_sources,
    update_source,
    delete_source,
    get_backends,
    _decompress,
    _sources,
    _backends,
    _runs,
    DOWNLOADS_DIR,
)
from tusk.core.download_hooks import register_hook, get_hooks, run_post_hook


@pytest.fixture(autouse=True)
def _clear_download_state():
    """Reset download state between tests."""
    _sources.clear()
    _backends.clear()
    _runs.clear()
    yield
    _sources.clear()
    _backends.clear()
    _runs.clear()


class TestDownloadSourceCRUD:
    """Test source creation, update, deletion."""

    def test_add_source(self, tmp_path):
        with patch("tusk.core.downloads.SOURCES_FILE", tmp_path / "sources.json"):
            src = DownloadSource(id="test1", name="Test", url="https://example.com/data.csv")
            result = add_source(src)
            assert result.id == "test1"

    def test_get_source(self, tmp_path):
        with patch("tusk.core.downloads.SOURCES_FILE", tmp_path / "sources.json"):
            add_source(DownloadSource(id="s1", name="S1", url="https://example.com/a.csv"))
            result = get_source("s1")
            assert result is not None
            assert result.name == "S1"

    def test_get_source_not_found(self):
        assert get_source("nonexistent") is None

    def test_get_sources_empty(self):
        assert get_sources() == []

    def test_update_source(self, tmp_path):
        with patch("tusk.core.downloads.SOURCES_FILE", tmp_path / "sources.json"):
            add_source(DownloadSource(id="s1", name="Old", url="https://example.com/a.csv"))
            updated = update_source("s1", name="New")
            assert updated is not None
            assert updated.name == "New"
            assert updated.url == "https://example.com/a.csv"

    def test_update_nonexistent(self):
        assert update_source("nope", name="New") is None

    def test_delete_source(self, tmp_path):
        with patch("tusk.core.downloads.SOURCES_FILE", tmp_path / "sources.json"):
            add_source(DownloadSource(id="s1", name="Del", url="https://example.com/a.csv"))
            assert delete_source("s1") is True
            assert get_source("s1") is None

    def test_delete_nonexistent(self):
        assert delete_source("nope") is False

    def test_source_fields(self):
        src = DownloadSource(
            id="full", name="Full", url="https://example.com/data.zip",
            category="geo", schedule="0 3 * * 0",
            format="csv", convert_to_parquet=True,
            enabled=True,
        )
        assert src.category == "geo"
        assert src.schedule == "0 3 * * 0"
        assert src.convert_to_parquet is True


class TestStorageBackends:
    """Test storage backend management."""

    def test_default_local_backend(self, tmp_path):
        with patch("tusk.core.downloads.SOURCES_FILE", tmp_path / "sources.json"):
            backends = get_backends()
            assert any(b.id == "local" for b in backends)

    def test_cannot_delete_local(self):
        assert delete_source("local") is False


class TestDownloadHooks:
    """Test post-download hook system."""

    def test_register_hook(self):
        async def my_hook(p):
            return p
        register_hook("test:my_hook", my_hook)
        hooks = get_hooks()
        assert "test:my_hook" in hooks

    def test_run_hook(self, tmp_path):
        import asyncio

        test_file = tmp_path / "data.csv"
        test_file.write_text("a,b\n1,2\n")
        output = tmp_path / "data_processed.csv"

        async def my_hook(p):
            output.write_text("processed")
            return output

        register_hook("test:process", my_hook)
        result = asyncio.run(run_post_hook("test:process", test_file))
        assert result == output

    def test_run_missing_hook(self, tmp_path):
        import asyncio

        test_file = tmp_path / "data.csv"
        test_file.write_text("a,b\n1,2\n")
        result = asyncio.run(run_post_hook("nonexistent", test_file))
        # Should return original path when hook is missing
        assert result == test_file


class TestDecompression:
    """Test file decompression utilities."""

    def test_decompress_gz(self, tmp_path):
        import gzip
        original = tmp_path / "data.csv"
        original.write_text("a,b\n1,2\n")
        gz_path = tmp_path / "data.csv.gz"
        with gzip.open(gz_path, "wt") as f:
            f.write("a,b\n1,2\n")
        original.unlink()

        result = _decompress(gz_path)
        assert result.suffix == ".csv"
        assert result.read_text() == "a,b\n1,2\n"
        assert not gz_path.exists()  # Original compressed file removed

    def test_decompress_zip(self, tmp_path):
        import zipfile
        zip_path = tmp_path / "data.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2\n")

        result = _decompress(zip_path)
        assert result.name == "data.csv"
        assert not zip_path.exists()

    def test_decompress_plain_file(self, tmp_path):
        plain = tmp_path / "data.csv"
        plain.write_text("a,b\n1,2\n")
        result = _decompress(plain)
        assert result == plain  # No decompression needed
