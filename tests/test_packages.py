"""
Port of cacheR package version tracking tests to pytest.

Tests that the cache hash changes when imported package versions change,
and that package metadata is stored alongside cached results.
"""
import pickle
import sys
import types
from pathlib import Path

import pytest

from cachepy import cache_file
from conftest import count_cache_entries


# Module-level function for AST analysis
def _module_level_pkg_user(x):
    import digest_lib
    return f"hashed_{x}"


# =========================================================================
# Helpers
# =========================================================================

def create_mock_module(name, version):
    """Creates a fake module in sys.modules with a specific version."""
    mod = types.ModuleType(name)
    mod.__version__ = version
    sys.modules[name] = mod
    return mod


def remove_mock_module(name):
    """Cleans up the fake module."""
    if name in sys.modules:
        del sys.modules[name]


@pytest.fixture
def mock_pkg_cleanup():
    """Fixture to ensure we don't leave fake modules polluting other tests."""
    created = []
    def _create(name, version):
        create_mock_module(name, version)
        created.append(name)
    yield _create
    for name in created:
        remove_mock_module(name)


# =========================================================================
# Tests
# =========================================================================

def test_stores_pkgs_metadata_for_imported_modules(tmp_path, mock_pkg_cleanup):
    """R: cacheFile stores package version metadata alongside cached data"""
    cache_dir = tmp_path / "cache"

    mock_pkg_cleanup("digest_lib", "0.6.29")

    cached = cache_file(cache_dir=cache_dir, backend="pickle")(_module_level_pkg_user)
    cached(1)

    files = list(cache_dir.glob("*.pkl"))
    assert len(files) == 1

    with open(files[0], "rb") as f:
        data = pickle.load(f)

    meta = data.get("meta", {})
    assert "pkgs" in meta
    assert "digest_lib" in meta["pkgs"]
    assert meta["pkgs"]["digest_lib"] == "0.6.29"


def test_hash_changes_when_pkg_version_changes(tmp_path, mock_pkg_cleanup):
    """R: cacheFile hash changes when package version changes"""
    cache_dir = tmp_path / "cache"

    mock_pkg_cleanup("fakepkg", "1.0.0")

    @cache_file(cache_dir=cache_dir, backend="pickle")
    def process_data(x):
        import fakepkg
        return x + 1

    process_data(10)
    assert count_cache_entries(cache_dir) == 1

    sys.modules["fakepkg"].__version__ = "1.0.1"

    process_data(10)
    assert count_cache_entries(cache_dir) == 2


def test_pkgs_metadata_is_empty_for_builtins(tmp_path):
    """R: cacheFile pkgs metadata is empty for builtin-only functions"""
    cache_dir = tmp_path / "cache"

    @cache_file(cache_dir=cache_dir, backend="pickle")
    def simple_math(x):
        return sum(range(x))

    simple_math(5)

    files = list(cache_dir.glob("*.pkl"))
    assert len(files) == 1

    with open(files[0], "rb") as f:
        data = pickle.load(f)

    meta = data.get("meta", {})
    pkgs = meta.get("pkgs", {})
    assert not pkgs


def test_captures_multiple_packages(tmp_path, mock_pkg_cleanup):
    """R: cacheFile captures versions for all imported packages"""
    cache_dir = tmp_path / "cache"

    mock_pkg_cleanup("pkgA", "1.0")
    mock_pkg_cleanup("pkgB", "2.0")

    @cache_file(cache_dir=cache_dir, backend="pickle")
    def multi_dep_fun(x):
        import pkgA
        import pkgB
        return x

    multi_dep_fun(100)

    files = list(cache_dir.glob("*.pkl"))
    assert len(files) == 1

    with open(files[0], "rb") as f:
        meta = pickle.load(f)["meta"]

    assert "pkgA" in meta["pkgs"]
    assert "pkgB" in meta["pkgs"]


def test_detects_global_imports(tmp_path, mock_pkg_cleanup):
    """R: cacheFile detects globally imported modules"""
    cache_dir = tmp_path / "cache"

    mock_pkg_cleanup("global_lib", "9.9.9")

    def my_fun(x):
        return global_lib.__version__

    my_fun.__globals__["global_lib"] = sys.modules["global_lib"]

    cached_fun = cache_file(cache_dir=cache_dir, backend="pickle")(my_fun)
    cached_fun(1)

    files = list(cache_dir.glob("*.pkl"))
    assert len(files) == 1

    with open(files[0], "rb") as f:
        meta = pickle.load(f)["meta"]

    assert "global_lib" in meta["pkgs"]
