"""
Port of cacheR test-files.R to pytest.

Tests file path detection, content hashing, symlinks, directory state,
and path normalization for the cachepy decorator.
"""
import os
import sys
import time
from pathlib import Path

import pytest

from cachepy import cache_file, cache_tree_reset
from cachepy.cache_file import (
    _file_state_cache,
    _find_path_specs,
    fast_file_hash,
    probabilistic_file_hash,
)
from cachepy.tests.conftest import count_cache_entries


# --- HELPER FUNCTIONS DEFINED AT MODULE LEVEL ---
# inspect.getsource works best on module-level functions

def _dummy_ast_simple(my_dir):
    import os
    a = os.listdir("data")
    b = os.scandir(my_dir)


def _dummy_ast_complex(path_sym, other_arg):
    import os
    if other_arg:
        print("Processing...")
    results = [
        os.listdir(path_sym),
        os.scandir("literal_dir_in_list"),
        os.walk(other_arg),
    ]
    return results


def _dummy_clean_func(x):
    y = x + 1
    return y * 3


# =========================================================================
# Group 1: Automatic File Detection (from args)
# =========================================================================

def test_invalidates_when_arg_path_files_change(tmp_path):
    """R: Automatic Detection: invalidates when number of files in arg path changes"""
    cache_dir = tmp_path / "cache"
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "file1.txt").touch()

    @cache_file(cache_dir=cache_dir, file_args=["path"], backend="rds")
    def count_files(path):
        return len(list(Path(path).iterdir()))

    n1 = count_files(str(input_dir))
    assert n1 == 1

    (input_dir / "file2.txt").touch()
    n2 = count_files(str(input_dir))
    assert n2 == 2


def test_stable_when_file_counts_same(tmp_path):
    """R: Automatic Detection: does not invalidate when file counts stay the same"""
    cache_dir = tmp_path / "cache"
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "a.txt").touch()
    (input_dir / "b.txt").touch()

    @cache_file(cache_dir=cache_dir, backend="rds")
    def list_str(path):
        files = sorted(os.listdir(path))
        return ",".join(files)

    res1 = list_str(str(input_dir))
    time.sleep(0.1)
    res2 = list_str(str(input_dir))

    assert res1 == res2
    assert count_cache_entries(cache_dir) == 1


def test_invalidates_with_imported_alias(tmp_path):
    """R: Automatic Detection: works with base::list.files and explicit namespaces"""
    cache_dir = tmp_path / "cache"
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "file1.txt").touch()

    @cache_file(cache_dir=cache_dir, backend="rds")
    def count_alias(path):
        from os import listdir
        return len(listdir(path))

    n1 = count_alias(str(input_dir))
    assert n1 == 1

    (input_dir / "file2.txt").touch()
    n2 = count_alias(str(input_dir))
    assert n2 == 2


def test_invalidates_when_path_in_kwargs(tmp_path):
    """R: Automatic Detection: works when path is passed via ... (dots/kwargs)"""
    cache_dir = tmp_path / "cache"
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "file1.txt").touch()

    @cache_file(cache_dir=cache_dir, file_args=["path"], backend="rds")
    def fun(path):
        return len(os.listdir(path))

    n1 = fun(path=str(input_dir))
    assert n1 == 1

    (input_dir / "file2.txt").touch()
    n2 = fun(path=str(input_dir))
    assert n2 == 2


# =========================================================================
# Group 2: File Tracking (Literals & Globals)
# =========================================================================

@pytest.mark.xfail(reason="AST detection fails for exec'd functions (inspect.getsource)")
def test_invalidates_when_literal_path_changes(tmp_path):
    """R: Static path detection via AST for hardcoded literal paths"""
    cache_dir = tmp_path / "cache"
    static_dir = tmp_path / "static_dir"
    static_dir.mkdir()
    (static_dir / "file1.txt").touch()

    func_code = f"""
import os
def count_static():
    return len(os.listdir('{str(static_dir)}'))
"""
    namespace = {}
    exec(func_code, namespace)
    raw_fun = namespace["count_static"]

    cached_fun = cache_file(cache_dir=cache_dir, backend="rds")(raw_fun)

    n1 = cached_fun()
    assert n1 == 1

    (static_dir / "file2.txt").touch()
    n2 = cached_fun()
    assert n2 == 2


def test_invalidates_when_global_variable_path_changes(tmp_path):
    """R: Static path detection via symbol lookup in globals"""
    cache_dir = tmp_path / "cache"
    global_dir = tmp_path / "global_input"
    global_dir.mkdir()

    def raw_fun():
        import os
        return len(os.listdir(GLOBAL_DIR))

    raw_fun.__globals__["GLOBAL_DIR"] = str(global_dir)
    cached_fun = cache_file(cache_dir=cache_dir, backend="rds")(raw_fun)

    (global_dir / "file1.txt").touch()
    n1 = cached_fun()
    assert n1 == 1

    (global_dir / "file2.txt").touch()
    n2 = cached_fun()
    assert n2 == 2


# =========================================================================
# Group 3: Multiple Arguments
# =========================================================================

def test_scans_all_args_implicitly(tmp_path):
    """R: Multiple Arguments: Scans all arguments for files"""
    cache_dir = tmp_path / "cache"
    extra_dir = tmp_path / "extra"
    extra_dir.mkdir()
    (extra_dir / "initial.txt").touch()

    @cache_file(cache_dir=cache_dir, backend="rds")
    def fun(primary, secondary):
        return f"{primary}-{secondary}"

    fun("val", str(extra_dir))
    (extra_dir / "change.txt").touch()
    fun("val", str(extra_dir))

    assert count_cache_entries(cache_dir) == 2


# =========================================================================
# Group 4: Safety
# =========================================================================

def test_non_file_strings_ignored(tmp_path):
    """R: Safety: Non-file strings are ignored (no crash)"""
    cache_dir = tmp_path / "cache"

    @cache_file(cache_dir=cache_dir, backend="rds")
    def f(mode, label):
        return f"{mode}-{label}"

    # Non-path strings should not cause errors
    result = f("fast", "run1")
    assert result == "fast-run1"


# =========================================================================
# Group 5: Relative Paths
# =========================================================================

def test_normalizes_relative_paths(tmp_path):
    """R: Normalization: Relative and Absolute paths hit same logic"""
    base = tmp_path / "base"
    base.mkdir()
    subdir = base / "subdir"
    subdir.mkdir()
    cache_dir = base / "cache"
    data_dir = base / "data"
    data_dir.mkdir()
    (data_dir / "file.txt").touch()

    @cache_file(cache_dir=cache_dir, backend="rds")
    def count_rel(path):
        return len(os.listdir(path))

    cwd_orig = os.getcwd()
    os.chdir(base)
    try:
        count_rel("data")
        assert count_cache_entries(cache_dir) == 1

        os.chdir(subdir)
        count_rel("../data")
        # Different arg string => different cache key (matches R behavior)
        assert count_cache_entries(cache_dir) == 2
    finally:
        os.chdir(cwd_orig)


# =========================================================================
# Group 6: AST Unit Tests (_find_path_specs)
# =========================================================================

def test_find_path_specs_empty():
    specs = _find_path_specs(_dummy_clean_func)
    assert specs["literals"] == []
    assert specs["symbols"] == []


def test_find_path_specs_simple():
    specs = _find_path_specs(_dummy_ast_simple)
    assert "data" in specs["literals"]
    assert "my_dir" in specs["symbols"]


def test_find_path_specs_complex():
    specs = _find_path_specs(_dummy_ast_complex)
    assert "literal_dir_in_list" in specs["literals"]


def test_find_path_specs_complex_nested():
    specs = _find_path_specs(_dummy_ast_complex)
    assert "literal_dir_in_list" in specs["literals"]
    assert "path_sym" in specs["symbols"]
    assert "other_arg" in specs["symbols"]


def test_symbol_path_skips_non_directories(tmp_path):
    """Non-directory/non-string symbols don't crash the decorator."""
    cache_dir = tmp_path / "cache"

    def func_with_ignored_symbol():
        import os
        os.listdir(not_a_dir)

    func_with_ignored_symbol.__globals__["not_a_dir"] = "I am just a string"
    cached = cache_file(cache_dir=cache_dir, backend="rds")(func_with_ignored_symbol)

    try:
        cached()
    except OSError:
        pass  # Runtime error expected, decorator hashing must succeed


def test_handles_non_character_symbols_in_specs(tmp_path):
    """Function symbols like os.path.join don't crash hashing."""
    cache_dir = tmp_path / "cache"

    def path_join_user(base):
        from os.path import join
        return join(base, "subdir")

    path_join_user.__globals__["join"] = os.path.join
    cached_fun = cache_file(cache_dir=cache_dir, backend="rds")(path_join_user)

    res = cached_fun("root")
    assert res == os.path.join("root", "subdir")

    res2 = cached_fun("root")
    assert res2 == res


def test_deseq2_like_no_false_positives():
    """Complex data-science function body has no false path dependencies."""
    def dds_like(counts_tbl, sample_table):
        import pandas as pd
        mat = counts_tbl[sample_table["sample_id"]]
        design = f"~ {sample_table['condition']}"
        return {"counts": mat, "design": design}

    specs = _find_path_specs(dds_like)
    assert len(specs["literals"]) == 0
    assert len(specs["symbols"]) == 0


def test_deseq2_like_execution(tmp_path):
    """Caching works for functions returning complex dicts."""
    cache_dir = tmp_path / "cache"

    @cache_file(cache_dir=cache_dir, backend="rds")
    def dds_analysis(counts, samples):
        return {"matrix": [c * 2 for c in counts], "meta": samples}

    data = [1, 2, 3]
    samples = {"id": "A"}

    res1 = dds_analysis(data, samples)
    res2 = dds_analysis(data, samples)

    assert res1 == res2
    assert count_cache_entries(cache_dir) == 1


# =========================================================================
# Group 7: Content vs mtime (from test-files.R)
# =========================================================================

def test_touch_no_invalidation(tmp_path):
    """R: Touching a file (mtime change) does NOT invalidate cache if content identical"""
    cache_dir = tmp_path / "cache"
    data_file = tmp_path / "input.txt"
    data_file.write_text("hello world")

    @cache_file(cache_dir, file_args=["fpath"], backend="rds")
    def f(fpath):
        return {"content": Path(fpath).read_text(), "run_id": time.time()}

    res1 = f(str(data_file))

    time.sleep(1.1)
    # Touch without changing content
    os.utime(data_file, None)
    _file_state_cache.clear()

    res2 = f(str(data_file))
    assert res1["run_id"] == res2["run_id"]  # Should be cache hit


def test_content_change_same_size_invalidates(tmp_path):
    """R: Changing content (even with same size) invalidates cache"""
    cache_dir = tmp_path / "cache"
    data_file = tmp_path / "input.txt"
    data_file.write_text("AAAA")

    @cache_file(cache_dir, file_args=["fpath"], backend="rds")
    def f(fpath):
        return {"content": Path(fpath).read_text(), "run_id": time.time()}

    res1 = f(str(data_file))

    time.sleep(1.1)
    data_file.write_text("BBBB")  # Same size, different content
    _file_state_cache.clear()

    res2 = f(str(data_file))
    assert res1["run_id"] != res2["run_id"]


# =========================================================================
# Group 8: Large File Sampling
# =========================================================================

def test_large_file_header_sampling(tmp_path):
    """R: Sampling: Header changes are detected in large files (>64KB)"""
    cache_dir = tmp_path / "cache"
    large_file = tmp_path / "large.bin"

    # 200KB file of zeros
    data = bytearray(200 * 1024)
    large_file.write_bytes(bytes(data))

    @cache_file(cache_dir, file_args=["fpath"], backend="rds")
    def f(fpath):
        return {"size": Path(fpath).stat().st_size, "run_id": time.time()}

    res1 = f(str(large_file))

    time.sleep(1.1)
    # Change first byte
    data[0] = 0xFF
    large_file.write_bytes(bytes(data))
    _file_state_cache.clear()

    res2 = f(str(large_file))
    assert res1["run_id"] != res2["run_id"]


# =========================================================================
# Group 9: Hash Determinism
# =========================================================================

def test_hash_determinism_across_clears(tmp_path):
    """R: The same file always produces the same hash"""
    f = tmp_path / "stable.txt"
    f.write_text("deterministic content")

    h1 = fast_file_hash(f)
    _file_state_cache.clear()
    h2 = fast_file_hash(f)

    assert h1 == h2


# =========================================================================
# Group 10: Directory Hashing
# =========================================================================

def test_dir_rename_detection(tmp_path):
    """R: Directory hashing detects file renaming (structure changes)"""
    cache_dir = tmp_path / "cache"
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "original.txt").write_text("content")

    @cache_file(cache_dir, file_args=["d"], backend="rds")
    def f(d):
        return {"files": sorted(os.listdir(d)), "run_id": time.time()}

    res1 = f(str(data_dir))

    # Rename file (same content, different name)
    (data_dir / "original.txt").rename(data_dir / "renamed.txt")

    res2 = f(str(data_dir))
    assert res1["run_id"] != res2["run_id"]


# =========================================================================
# Group 11: Nested & Vector File Paths
# =========================================================================

def test_nested_list_file_paths(tmp_path):
    """R: file paths nested inside argument lists invalidate cache when file changes"""
    cache_dir = tmp_path / "cache"
    f1 = tmp_path / "file1.txt"
    f1.write_text("A")

    @cache_file(cache_dir, backend="rds")
    def f(paths):
        return Path(paths[0]).read_text()

    res1 = f([str(f1)])
    assert res1 == "A"

    f1.write_text("B")
    _file_state_cache.clear()

    res2 = f([str(f1)])
    assert res2 == "B"
    assert count_cache_entries(cache_dir) == 2


def test_vector_file_paths(tmp_path):
    """R: Vector of file paths invalidates cache when any file changes"""
    cache_dir = tmp_path / "cache"
    f1 = tmp_path / "file1.txt"
    f2 = tmp_path / "file2.txt"
    f1.write_text("A")
    f2.write_text("B")

    @cache_file(cache_dir, backend="rds")
    def f(path1, path2):
        return Path(path1).read_text() + Path(path2).read_text()

    f(str(f1), str(f2))
    assert count_cache_entries(cache_dir) == 1

    f2.write_text("C")
    _file_state_cache.clear()

    f(str(f1), str(f2))
    assert count_cache_entries(cache_dir) == 2


def test_vector_dir_paths(tmp_path):
    """R: Vector of directory paths invalidates when dir content changes"""
    cache_dir = tmp_path / "cache"
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    (dir1 / "a.txt").touch()
    (dir2 / "b.txt").touch()

    @cache_file(cache_dir, file_args=["d1", "d2"], backend="rds")
    def f(d1, d2):
        return len(os.listdir(d1)) + len(os.listdir(d2))

    f(str(dir1), str(dir2))
    assert count_cache_entries(cache_dir) == 1

    (dir2 / "c.txt").touch()
    f(str(dir1), str(dir2))
    assert count_cache_entries(cache_dir) == 2


# =========================================================================
# Group 12: Symlinks
# =========================================================================

def test_symlinks_resolved(tmp_path):
    """R: symlinks are resolved to their target files for caching"""
    if sys.platform == "win32":
        pytest.skip("Symlinks not reliable on Windows")

    cache_dir = tmp_path / "cache"
    target = tmp_path / "target.txt"
    target.write_text("original")
    link = tmp_path / "link.txt"
    link.symlink_to(target)

    @cache_file(cache_dir, file_args=["fpath"], backend="rds")
    def f(fpath):
        return {"content": Path(fpath).read_text(), "run_id": time.time()}

    # Via symlink
    res1 = f(str(link))
    # Via target directly — should be same cache entry
    res2 = f(str(target))

    assert res1["run_id"] == res2["run_id"]

    # Modify target
    target.write_text("modified")
    _file_state_cache.clear()

    res3 = f(str(link))
    assert res3["run_id"] != res1["run_id"]


# =========================================================================
# Group 13: hash_file_paths Control
# =========================================================================

def test_hash_file_paths_control(tmp_path):
    """R: hash_file_paths controls location sensitivity"""
    cache_dir = tmp_path / "cache"
    dir1 = tmp_path / "loc1"
    dir2 = tmp_path / "loc2"
    dir1.mkdir()
    dir2.mkdir()

    # Same content, different locations
    (dir1 / "data.txt").write_text("same content")
    (dir2 / "data.txt").write_text("same content")

    # With hash_file_paths=True (default): location matters
    @cache_file(cache_dir, file_args=["d"], backend="rds", hash_file_paths=True)
    def f_strict(d):
        return Path(d).read_text()

    f_strict(str(dir1 / "data.txt"))
    f_strict(str(dir2 / "data.txt"))
    assert count_cache_entries(cache_dir) == 2

    # With hash_file_paths=False: only content matters
    cache_dir2 = tmp_path / "cache2"

    @cache_file(cache_dir2, file_args=["d"], backend="rds", hash_file_paths=False)
    def f_portable(d):
        return Path(d).read_text()

    f_portable(str(dir1 / "data.txt"))
    f_portable(str(dir2 / "data.txt"))
    assert count_cache_entries(cache_dir2) == 1


# =========================================================================
# Group 14: File Modification Warning
# =========================================================================

def test_file_modified_during_execution_warning(tmp_path):
    """R: cache saves WITH WARNING if argument file is modified during execution"""
    cache_dir = tmp_path / "cache"
    data_file = tmp_path / "input.txt"
    data_file.write_text("original")

    @cache_file(cache_dir, file_args=["fpath"], backend="rds")
    def f(fpath):
        # Modify the input file during execution
        Path(fpath).write_text("modified")
        return "done"

    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = f(str(data_file))

    assert result == "done"
    # Should warn about file modification
    warning_msgs = [str(warning.message) for warning in w]
    assert any("modif" in msg.lower() for msg in warning_msgs)
