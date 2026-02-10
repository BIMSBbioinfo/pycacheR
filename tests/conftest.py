"""Shared fixtures and helpers for cachepy tests."""
import os
import re
from pathlib import Path

import pytest

from cachepy import cache_tree_reset
from cachepy.cache_file import _file_state_cache


def count_cache_entries(cache_dir, pattern=r"\.(rds|qs)$"):
    """Count cache files in cache_dir, excluding graph files."""
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        return 0
    files = [f.name for f in cache_dir.iterdir() if f.is_file()]
    return sum(
        1
        for f in files
        if re.search(pattern, f) and not f.startswith("graph.")
    )


@pytest.fixture(autouse=True)
def _reset_cache_state():
    """Reset cache tree and file state cache before each test."""
    cache_tree_reset()
    _file_state_cache.clear()
    yield
    cache_tree_reset()
    _file_state_cache.clear()


