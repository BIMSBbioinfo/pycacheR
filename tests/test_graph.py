"""
Port of cacheR test-cacheTree.R to pytest.

Tests the cache tree (dependency graph) operations: node registration,
parent-child linking, file tracking, save/load, pruning.
"""
import os
import time
from pathlib import Path

import pytest

from cachepy import (
    cache_file,
    cache_tree_nodes,
    cache_tree_reset,
    cache_tree_save,
    cache_tree_load,
    track_file,
)
from cachepy.cache_file import (
    _cache_tree_call_stack,
    _cache_tree_graph,
    _cache_tree_register_node,
    _file_state_cache,
    cache_default_dir,
    cache_prune,
    fast_file_hash,
    probabilistic_file_hash,
)


# ============================================================================
# Section 1: Basic Tree Operations
# ============================================================================

def test_cache_tree_reset_clears_environment():
    """R: cacheTree_reset clears the environment"""
    # Add some data
    _cache_tree_graph["test_node"] = {
        "id": "test_node",
        "fname": "test",
        "hash": "abc",
        "outfile": None,
        "parents": [],
        "children": [],
        "files": [],
        "file_hashes": {},
        "created": time.time(),
    }
    _cache_tree_call_stack.append("test_node")

    assert len(_cache_tree_graph) > 0
    assert len(_cache_tree_call_stack) > 0

    cache_tree_reset()

    assert len(_cache_tree_graph) == 0
    assert len(_cache_tree_call_stack) == 0


def test_node_registration_and_edge_creation():
    """R: Node registration works and handles parent-child linking"""
    # Register node A
    _cache_tree_register_node("A", "func_a", "hash_a", Path("/tmp/a.rds"))

    # Push A onto call stack to make it the parent
    _cache_tree_call_stack.append("A")

    # Register node B (child of A)
    _cache_tree_register_node("B", "func_b", "hash_b", Path("/tmp/b.rds"))

    _cache_tree_call_stack.pop()

    nodes = cache_tree_nodes()
    assert "A" in nodes
    assert "B" in nodes
    assert "B" in nodes["A"]["children"]
    assert "A" in nodes["B"]["parents"]


def test_save_load_preserves_structure(tmp_path):
    """R: cacheTree_save and cacheTree_load preserve structure"""
    # Register a node
    _cache_tree_register_node("N1", "f1", "h1", Path("/tmp/n1.rds"))

    nodes_before = cache_tree_nodes()
    assert "N1" in nodes_before
    assert nodes_before["N1"]["fname"] == "f1"

    graph_path = tmp_path / "graph.pkl"
    cache_tree_save(graph_path)
    assert graph_path.exists()

    cache_tree_reset()
    assert len(cache_tree_nodes()) == 0

    cache_tree_load(graph_path)
    nodes_after = cache_tree_nodes()
    assert "N1" in nodes_after
    assert nodes_after["N1"]["fname"] == "f1"


# ============================================================================
# Section 2: File Hashing
# ============================================================================

def test_probabilistic_hash_is_deterministic(tmp_path):
    """R: probabilistic_file_hash is deterministic"""
    f = tmp_path / "test.bin"
    f.write_bytes(bytes(range(256)) * 100)  # 25.6KB

    h1 = probabilistic_file_hash(f)
    h2 = probabilistic_file_hash(f)

    assert h1 == h2
    assert isinstance(h1, str) and len(h1) > 0


def test_fast_hash_memoization_detects_size_change(tmp_path):
    """R: fast_file_hash uses memoization but detects size changes"""
    f = tmp_path / "data.txt"
    f.write_text("hello")

    h1 = fast_file_hash(f)
    assert str(f) in _file_state_cache

    # Same content => same hash (from memo)
    h1b = fast_file_hash(f)
    assert h1 == h1b

    # Modify content (different size)
    f.write_text("hello world extended content")
    _file_state_cache.clear()

    h2 = fast_file_hash(f)
    assert h1 != h2


# ============================================================================
# Section 3: File Tracking
# ============================================================================

def test_track_file_associates_with_node(tmp_path):
    """R: track_file associates file with current node"""
    tf = tmp_path / "tracked.txt"
    tf.write_text("data")

    cache_dir = tmp_path / "cache"

    @cache_file(cache_dir, backend="rds")
    def f(x):
        track_file(tf)
        return x

    f(1)

    nodes = cache_tree_nodes()
    assert len(nodes) == 1
    node = next(iter(nodes.values()))

    np = tf.resolve()
    assert np in node["files"]
    assert str(np) in node["file_hashes"]


def test_cache_tree_for_file(tmp_path):
    """R: cacheTree_for_file finds relevant nodes"""
    from cachepy import cache_tree_reset
    from cachepy.cache_file import cache_tree_for_file

    tf = tmp_path / "tracked.txt"
    tf.write_text("data")

    cache_dir = tmp_path / "cache"

    @cache_file(cache_dir, backend="rds")
    def f(x):
        track_file(tf)
        return x

    f(1)

    # Should find the node
    matching = cache_tree_for_file(tf)
    assert len(matching) == 1

    # Non-existent file should return empty
    no_match = cache_tree_for_file(tmp_path / "nonexistent.txt")
    assert len(no_match) == 0


# ============================================================================
# Section 4: Configuration
# ============================================================================

def test_default_dir(tmp_path, monkeypatch):
    """R: cacheR_default_dir returns configured directory"""
    monkeypatch.setenv("CACHER_DIR", str(tmp_path / "custom_cache"))

    d = cache_default_dir()
    assert str(d) == str(tmp_path / "custom_cache")
    assert d.exists()


# ============================================================================
# Section 5: Pruning
# ============================================================================

def test_prune_deletes_old_files(tmp_path):
    """R: cachePrune deletes old files"""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    old_file = cache_dir / "old_func.abc.rds"
    old_file.touch()
    # Backdate to 60 days ago
    old_time = time.time() - 60 * 24 * 3600
    os.utime(old_file, (old_time, old_time))

    new_file = cache_dir / "new_func.def.rds"
    new_file.touch()

    cache_prune(cache_dir, days_old=30)

    assert not old_file.exists()
    assert new_file.exists()
