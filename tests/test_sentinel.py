"""Tests for sentinel waiting logic (concurrency coordination)."""
import os
import pickle
import threading
import time
from pathlib import Path

import pytest

from cachepy import cache_file
from cachepy.cache_file import _wait_for_sentinel


# ============================================================================
# Unit tests for _wait_for_sentinel helper
# ============================================================================

def _make_cache_file(path: Path, value: object) -> None:
    """Write a cache entry in the same format _atomic_save uses."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump({"dat": value, "meta": {}}, f)


def _simple_load(path: Path):
    with path.open("rb") as f:
        obj = pickle.load(f)
    if isinstance(obj, dict) and "dat" in obj:
        return obj["dat"]
    return obj


class TestWaitForSentinel:
    """Direct tests for the _wait_for_sentinel function."""

    def test_no_sentinel_returns_none(self, tmp_path):
        sentinel = tmp_path / "f.pkl.computing"
        outfile = tmp_path / "f.pkl"
        result = _wait_for_sentinel(sentinel, outfile, _simple_load, "f")
        assert result is None

    def test_stale_sentinel_returns_none(self, tmp_path):
        sentinel = tmp_path / "f.pkl.computing"
        outfile = tmp_path / "f.pkl"
        sentinel.touch()
        # backdate mtime by 2 hours
        old_time = time.time() - 7200
        os.utime(sentinel, (old_time, old_time))
        result = _wait_for_sentinel(sentinel, outfile, _simple_load, "f",
                                    stale=3600)
        assert result is None

    def test_fresh_sentinel_waits_and_loads(self, tmp_path):
        sentinel = tmp_path / "f.pkl.computing"
        outfile = tmp_path / "f.pkl"
        sentinel.touch()

        # simulate another process writing the cache after a short delay
        def writer():
            time.sleep(0.5)
            _make_cache_file(outfile, 42)

        t = threading.Thread(target=writer)
        t.start()
        result = _wait_for_sentinel(sentinel, outfile, _simple_load, "f",
                                    poll=0.2, timeout=5)
        t.join()
        assert result == 42

    def test_timeout_returns_none(self, tmp_path):
        sentinel = tmp_path / "f.pkl.computing"
        outfile = tmp_path / "f.pkl"
        sentinel.touch()
        # never write cache file -> should timeout
        result = _wait_for_sentinel(sentinel, outfile, _simple_load, "f",
                                    poll=0.1, timeout=0.3)
        assert result is None

    def test_sentinel_removed_stops_waiting(self, tmp_path):
        """If sentinel disappears but no cache file, stop waiting early."""
        sentinel = tmp_path / "f.pkl.computing"
        outfile = tmp_path / "f.pkl"
        sentinel.touch()

        def remover():
            time.sleep(0.3)
            sentinel.unlink()

        t = threading.Thread(target=remover)
        t.start()
        start = time.time()
        result = _wait_for_sentinel(sentinel, outfile, _simple_load, "f",
                                    poll=0.1, timeout=10)
        elapsed = time.time() - start
        t.join()
        assert result is None
        # should have stopped well before the 10s timeout
        assert elapsed < 3


# ============================================================================
# Integration tests with cache_file decorator
# ============================================================================

class TestSentinelIntegration:
    """Test that cache_file's wrapper properly checks sentinels."""

    def test_parallel_worker_result_loaded(self, tmp_path):
        """Process B should wait on sentinel and pick up Process A's result."""
        cache_dir = tmp_path / "cache"

        @cache_file(cache_dir, backend="pickle")
        def slow_fn(x):
            slow_fn.run_count += 1
            return x * 10

        slow_fn.run_count = 0

        # Call once to establish the cache and learn the outfile path
        result_a = slow_fn(5)
        assert result_a == 50
        assert slow_fn.run_count == 1

        # Find the cache file that was created
        cache_files = list(cache_dir.glob("slow_fn.*.pkl"))
        assert len(cache_files) == 1
        outfile = cache_files[0]

        # Delete the cache and create a sentinel (simulate process A computing)
        outfile.unlink()
        sentinel_path = outfile.with_suffix(outfile.suffix + ".computing")
        sentinel_path.touch()

        # Write cache back after a short delay (simulate process A finishing)
        def writer():
            time.sleep(0.5)
            _make_cache_file(outfile, 50)
            sentinel_path.unlink(missing_ok=True)

        t = threading.Thread(target=writer)
        t.start()

        # Process B calls the function — should wait for sentinel and load
        result_b = slow_fn(5)
        t.join()
        assert result_b == 50
        # function body should NOT have been called again
        assert slow_fn.run_count == 1

    def test_force_ignores_sentinel(self, tmp_path):
        """_force=True should bypass sentinel waiting."""
        cache_dir = tmp_path / "cache"

        @cache_file(cache_dir, backend="pickle")
        def fn(x):
            return x + 1

        # First call to set up cache dir
        fn(1)

        # Find the outfile
        cache_files = list(cache_dir.glob("fn.*.pkl"))
        assert len(cache_files) == 1
        outfile = cache_files[0]

        # Delete cache, create a sentinel
        outfile.unlink()
        sentinel = outfile.with_suffix(outfile.suffix + ".computing")
        sentinel.touch()

        # _force=True should compute immediately, not wait
        start = time.time()
        result = fn(1, _force=True)
        elapsed = time.time() - start
        assert result == 2
        # should not have waited long
        assert elapsed < 2

    def test_stale_sentinel_ignored_in_wrapper(self, tmp_path):
        """A stale sentinel (> threshold) should not block computation."""
        cache_dir = tmp_path / "cache"
        call_count = {"n": 0}

        @cache_file(cache_dir, backend="pickle")
        def fn(x):
            call_count["n"] += 1
            return x * 2

        # First call
        fn(3)
        assert call_count["n"] == 1

        # Find and remove cache file, create stale sentinel
        cache_files = list(cache_dir.glob("fn.*.pkl"))
        outfile = cache_files[0]
        outfile.unlink()
        sentinel = outfile.with_suffix(outfile.suffix + ".computing")
        sentinel.touch()
        old_time = time.time() - 7200
        os.utime(sentinel, (old_time, old_time))

        # Should compute immediately (stale sentinel ignored)
        start = time.time()
        result = fn(3)
        elapsed = time.time() - start
        assert result == 6
        assert call_count["n"] == 2
        assert elapsed < 2
