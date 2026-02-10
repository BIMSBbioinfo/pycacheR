"""
Python port of cacheTree.R + cacheFile.R

- cache_file: disk-backed caching decorator (like R's cacheFile)
- cache tree helpers: track parent/child calls and file deps
"""

from __future__ import annotations

import os
import pickle
import random
import re
import sys
import time
import types
import inspect
import hashlib
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import ast
import inspect
import textwrap


try:
    import importlib.metadata as importlib_metadata
except ImportError:  # Python < 3.8
    import importlib_metadata  # type: ignore

logger = logging.getLogger(__name__)

# ============================================================================
# Cache tree global state (R: .cacheTree_env)
# ============================================================================

_cache_tree_call_stack: List[str] = []
_cache_tree_graph: Dict[str, Dict[str, Any]] = {}

# file-state cache for fast_file_hash (R: .file_state_cache)
_file_state_cache: Dict[str, Dict[str, Any]] = {}


# ============================================================================
# Helpers to manage the graph (R: .cacheTree_current_node, .cacheTree_register_node)
# ============================================================================

def _cache_tree_current_node() -> Optional[str]:
    if not _cache_tree_call_stack:
        return None
    return _cache_tree_call_stack[-1]


def _cache_tree_register_node(
    node_id: str,
    fname: str,
    args_hash: str,
    outfile: Path,
) -> None:
    """Register a node and link to current parent (if any)."""

    parent = _cache_tree_current_node()

    node = _cache_tree_graph.get(node_id)
    if node is None:
        node = {
            "id": node_id,
            "fname": fname,
            "hash": args_hash,
            "outfile": Path(outfile),
            "parents": [],
            "children": [],
            "files": [],        # list[Path]
            "file_hashes": {},  # dict[path_str -> hash or None]
            "created": time.time(),
        }

    # link parent/child
    if parent is not None:
        if parent not in node["parents"]:
            node["parents"].append(parent)

        parent_node = _cache_tree_graph.get(parent)
        if parent_node is None:
            parent_node = {
                "id": parent,
                "fname": None,
                "hash": None,
                "outfile": None,
                "parents": [],
                "children": [],
                "files": [],
                "file_hashes": {},
                "created": time.time(),
            }
        if node_id not in parent_node["children"]:
            parent_node["children"].append(node_id)
        _cache_tree_graph[parent] = parent_node

    _cache_tree_graph[node_id] = node


# ============================================================================
# Public helpers (R: cacheTree_nodes, cacheTree_for_file, cacheTree_reset, ...)
# ============================================================================

def cache_tree_nodes() -> Dict[str, Dict[str, Any]]:
    """Return a copy of all nodes currently recorded in the cache tree."""
    # Shallow copy is enough to avoid accidental mutation of dict itself
    return dict(_cache_tree_graph)


def cache_tree_for_file(path: os.PathLike | str) -> Dict[str, Dict[str, Any]]:
    """Return nodes that depend on the given file."""

    np = Path(path).resolve()
    out: Dict[str, Dict[str, Any]] = {}

    for node_id, node in _cache_tree_graph.items():
        files: List[Path] = node.get("files", [])
        if np in files:
            out[node_id] = node
    return out


def cache_tree_reset() -> None:
    """Reset the in-memory cache tree graph + call stack."""
    _cache_tree_call_stack.clear()
    _cache_tree_graph.clear()


def cache_tree_save(path: os.PathLike | str) -> Path:
    """Save a serializable representation (named dict of nodes) to disk."""
    path = Path(path)
    with path.open("wb") as f:
        pickle.dump(cache_tree_nodes(), f)
    return path


def cache_tree_load(path: os.PathLike | str) -> None:
    """Load a cache tree representation saved by cache_tree_save."""
    path = Path(path)
    with path.open("rb") as f:
        graph_dict = pickle.load(f)

    _cache_tree_graph.clear()
    _cache_tree_graph.update(graph_dict)
    _cache_tree_call_stack.clear()


# ============================================================================
# File fingerprinting + probabilistic hashing + metadata cache
# (R: probabilistic_file_hash, fast_file_hash)
# ============================================================================

def _digest_bytes(data: bytes, algo: str = "xxhash64") -> str:
    """
    Small helper: we don't require xxhash, but approximate with blake2b/sha256.
    """
    algo = algo.lower()
    if algo in {"sha256", "sha-256"}:
        h = hashlib.sha256()
    else:
        # treat xxhash64 (and everything else) as blake2b with 16-byte digest
        h = hashlib.blake2b(digest_size=16)
    h.update(data)
    return h.hexdigest()


def _digest_obj(obj: Any, algo: str = "xxhash64") -> str:
    """Hash an arbitrary Python object via pickle, falling back to repr on failure."""
    try:
        data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        data = repr(obj).encode("utf-8", errors="replace")
    return _digest_bytes(data, algo=algo)


def probabilistic_file_hash(
    path: os.PathLike | str,
    block_size: int = 64 * 1024,
    n_blocks: int = 5,
    algo: str = "xxhash64",
) -> str:
    """
    Probabilistic file hash:
      - always read first block
      - optionally sample n_blocks random blocks
      - optionally read last block
    Stable w.r.t file path + size.
    """
    path = Path(path)
    if not path.exists() or not path.is_file():
        return ""

    size = path.stat().st_size
    blocks: List[bytes] = []

    with path.open("rb") as f:
        # first block
        blocks.append(f.read(block_size))

        if size > block_size:
            max_offset = max(size - block_size, 1)

            # deterministic sampling based on path + size
            seed_val = f"{path}:{size}".encode("utf-8")
            random.seed(_digest_bytes(seed_val, algo="sha256"))

            for _ in range(n_blocks):
                offset = random.randint(0, max_offset)
                f.seek(offset)
                blocks.append(f.read(block_size))

        # last block
        if size > block_size:
            f.seek(max(size - block_size, 0))
            blocks.append(f.read(block_size))

    data = b"".join(blocks)
    return _digest_bytes(data, algo=algo)


def fast_file_hash(
    path: os.PathLike | str,
    block_size: int = 64 * 1024,
    n_blocks: int = 5,
    algo: str = "xxhash64",
) -> str:
    """
    Fast file hash using size+mtime as a fingerprint to decide whether
    we need to recompute probabilistic_file_hash.
    """
    path = Path(path)
    try:
        st = path.stat()
    except FileNotFoundError:
        return ""

    fp = f"{st.st_size}|{st.st_mtime}"
    prev = _file_state_cache.get(str(path))
    if prev is not None and prev.get("fp") == fp:
        return prev["hash"]

    h = probabilistic_file_hash(path, block_size=block_size, n_blocks=n_blocks, algo=algo)
    _file_state_cache[str(path)] = {"fp": fp, "hash": h, "atime": time.time()}

    # Bounded eviction: remove oldest entries when cache exceeds limit
    if len(_file_state_cache) > _FILE_STATE_CACHE_LIMIT:
        entries = sorted(_file_state_cache.items(), key=lambda kv: kv[1].get("atime", 0))
        # Keep only the most recent half
        keep = _FILE_STATE_CACHE_LIMIT // 2
        to_remove = [k for k, _ in entries[:-keep]]
        for k in to_remove:
            _file_state_cache.pop(k, None)

    return h


# ============================================================================
# track_file + changed_files (R: track_file, cacheTree_changed_files)
# ============================================================================

def track_file(path: os.PathLike | str) -> Path:
    """
    Record that the current cached node depends on this file path.
    Returns a normalized Path to allow convenient usage inside user code.
    """
    node_id = _cache_tree_current_node()
    if node_id is None:
        return Path(path)

    node = _cache_tree_graph.get(node_id)
    if node is None:
        return Path(path)

    np = Path(path).resolve()

    files: List[Path] = node.get("files", [])
    if np not in files:
        files.append(np)
        node["files"] = files

    fh: Dict[str, Optional[str]] = node.get("file_hashes", {})
    if np.exists():
        fh[str(np)] = fast_file_hash(np)
    else:
        fh[str(np)] = None

    node["file_hashes"] = fh
    _cache_tree_graph[node_id] = node
    return np


def cache_tree_changed_files() -> Dict[str, Dict[str, Any]]:
    """
    For each node, check whether any tracked files changed or disappeared.
    Returns dict[node_id] = {"node": node, "changed_files": [Path, ...]}
    """
    out: Dict[str, Dict[str, Any]] = {}

    for node_id, node in _cache_tree_graph.items():
        fh: Dict[str, Optional[str]] = node.get("file_hashes", {})
        if not fh:
            continue

        changed_paths: List[Path] = []

        for p_str, old_hash in fh.items():
            p = Path(p_str)
            if not p.exists():
                changed_paths.append(p)
            else:
                new_hash = fast_file_hash(p)
                if old_hash != new_hash:
                    changed_paths.append(p)

        if changed_paths:
            out[node_id] = {
                "node": node,
                "changed_files": changed_paths,
            }

    return out


# ============================================================================
# cache_default_dir + pruning (R: cacheR_default_dir, cachePrune)
# ============================================================================

def cache_default_dir() -> Path:
    """
    Default cache directory.
    - honour env var CACHER_DIR if present
    - else use ./ .cacheR
    """
    d = os.getenv("CACHER_DIR", os.path.join(os.getcwd(), ".cacheR"))
    path = Path(d)
    path.mkdir(parents=True, exist_ok=True)
    return path


def cache_prune(cache_dir: os.PathLike | str, days_old: int = 30) -> None:
    """
    Delete cache files older than days_old, based on mtime.
    Also cleans up .lock, .tmp.*, and .computing files unconditionally.
    """
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        return

    cutoff_sec = time.time() - days_old * 24 * 3600
    to_delete: List[Path] = []

    for p in cache_dir.glob("*.rds"):
        if p.stat().st_mtime < cutoff_sec:
            to_delete.append(p)
    for p in cache_dir.glob("*.qs"):
        if p.stat().st_mtime < cutoff_sec:
            to_delete.append(p)

    # Always clean up stale auxiliary files
    for p in cache_dir.glob("*.lock"):
        to_delete.append(p)
    for p in cache_dir.glob("*.tmp.*"):
        to_delete.append(p)
    for p in cache_dir.glob("*.computing"):
        to_delete.append(p)

    if to_delete:
        logger.info("Deleting %d old/stale cache files...", len(to_delete))
        for p in to_delete:
            try:
                p.unlink()
            except OSError:
                logger.warning("Failed to delete %s", p)


# ============================================================================
# cacheInfo + cacheList equivalents (optional)
# ============================================================================

_FILE_STATE_CACHE_LIMIT = 500


def cache_stats(cache_dir: os.PathLike | str) -> Dict[str, Any]:
    """
    Return aggregate statistics for a cache directory.
    Excludes graph.rds from counts.
    """
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        raise FileNotFoundError(f"Cache directory not found: {cache_dir}")

    files = [
        p for p in cache_dir.iterdir()
        if p.is_file()
        and re.search(r"\.(rds|qs)$", p.name)
        and not p.name.startswith("graph.")
    ]

    if not files:
        return {
            "n_entries": 0,
            "total_size_mb": 0.0,
            "oldest": None,
            "newest": None,
        }

    sizes = [p.stat().st_size for p in files]
    mtimes = [p.stat().st_mtime for p in files]
    return {
        "n_entries": len(files),
        "total_size_mb": sum(sizes) / (1024 * 1024),
        "oldest": min(mtimes),
        "newest": max(mtimes),
    }


def load_config(
    path: os.PathLike | str,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Load cachepy configuration from a YAML file.
    If *existing* is provided, keys already in existing are NOT overridden.
    """
    import yaml

    path = Path(path)
    with path.open() as f:
        data = yaml.safe_load(f) or {}

    if existing is not None:
        merged = dict(data)
        merged.update(existing)  # existing wins
        return merged

    return data


def _norm_path(path: os.PathLike | str) -> str:
    return str(Path(path).resolve())


def cache_info(path: os.PathLike | str) -> Dict[str, Any]:
    """
    Read a cache file and return {"value": value, "meta": meta_dict}.
    New format: {"dat": value, "meta": {...}}
    Legacy: anything else -> wrapped with minimal meta.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with path.open("rb") as f:
        obj = pickle.load(f)

    # new format
    if isinstance(obj, dict) and {"dat", "meta"} <= set(obj.keys()) and isinstance(
        obj["meta"], dict
    ):
        return {"value": obj["dat"], "meta": obj["meta"]}

    # legacy
    st = path.stat()
    return {
        "value": obj,
        "meta": {
            "fname": None,
            "args": {},
            "args_hash": None,
            "cache_file": _norm_path(path),
            "cache_dir": _norm_path(path.parent),
            "created": st.st_mtime,
            "legacy": True,
        },
    }


def cache_list(cache_dir: os.PathLike | str):
    """
    List contents of a cache directory as a list of rows (like a tiny data.frame).
    Each row: {"file", "fname", "created", "size_bytes"}
    """
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        return []

    files = list(cache_dir.glob("*.rds"))
    if not files:
        return []

    rows = []
    for fpath in files:
        try:
            info = cache_info(fpath)
        except Exception:
            info = None
        st = fpath.stat()
        if info is None:
            fname = None
        else:
            fname = info["meta"].get("fname")
        rows.append(
            {
                "file": fpath.name,
                "fname": fname,
                "created": st.st_mtime,
                "size_bytes": st.st_size,
            }
        )
    return rows


# ============================================================================
# Recursive closure hasher (R: .get_recursive_closure_hash)
# ============================================================================

def get_recursive_closure_hash(
    obj: Any,
    visited: Optional[Dict[int, str]] = None,
    algo: str = "xxhash64",
) -> str:
    """
    Rough Python analogue of R's .get_recursive_closure_hash():

    - non-callables: hash value directly
    - functions in non-__main__ modules: hash (package, version, qualname)
    - otherwise: hash function source + constants + globals used by code +
      closure vars, recursing into dependent functions / values.

    `visited` is used to avoid infinite recursion on cycles.
    """
    if visited is None:
        visited = {}

    # Non-function: hash directly
    if not callable(obj):
        return _digest_obj(obj, algo=algo)

    obj_id = id(obj)
    if obj_id in visited:
        # recursion cycle
        return visited[obj_id]

    # Mark as cycle sentinel first; overwritten with final hash later
    visited[obj_id] = "RECURSION_CYCLE"

    # If function belongs to a normal package, hash package+version
    try:
        module = inspect.getmodule(obj)
    except Exception:
        module = None

    if module is not None:
        mod_name = getattr(module, "__name__", None)
        if mod_name not in {None, "__main__", "builtins"}:
            try:
                ver = importlib_metadata.version(mod_name)
                h = _digest_obj(
                    {
                        "package": mod_name,
                        "version": ver,
                        "qualname": getattr(obj, "__qualname__", repr(obj)),
                    },
                    algo=algo,
                )
                visited[obj_id] = h
                return h
            except importlib_metadata.PackageNotFoundError:
                # fall back to full code hashing
                pass
            except Exception:
                # be conservative and fall back as well
                pass

    # ---------- Hash body (source + constants) ----------
    try:
        src = inspect.getsource(obj)
    except OSError:
        src = repr(obj)

    code = getattr(obj, "__code__", None)
    if code is not None:
        consts = getattr(code, "co_consts", ())
        names = getattr(code, "co_names", ())
        freevars = getattr(code, "co_freevars", ())
    else:
        consts = ()
        names = ()
        freevars = ()

    # Represent constants as strings so we don't try to pickle code objects, etc.
    consts_repr = tuple(repr(c) for c in consts)

    globs = getattr(obj, "__globals__", {})

    dep_hashes: Dict[str, str] = {}

    # ---------- Globals referenced by the function ----------
    for name in names:
        if name in globs:
            val = globs[name]

            # Skip modules (they’re covered by package version logic above)
            if isinstance(val, types.ModuleType):
                continue

            try:
                dep_hashes[name] = get_recursive_closure_hash(val, visited=visited, algo=algo)
            except Exception:
                dep_hashes[name] = _digest_obj(repr(val), algo=algo)

    # ---------- Closure variables ----------
    closure_val_hashes: Dict[str, str] = {}
    closure = getattr(obj, "__closure__", None)
    if closure and freevars:
        for var, cell in zip(freevars, closure):
            try:
                v = cell.cell_contents
            except ValueError:
                # uninitialized cell
                continue
            if callable(v):
                closure_val_hashes[var] = get_recursive_closure_hash(v, visited=visited, algo=algo)
            else:
                closure_val_hashes[var] = _digest_obj(v, algo=algo)

    body_hash = _digest_obj({"src": src, "consts": consts_repr}, algo=algo)

    final_hash = _digest_obj(
        {
            "body": body_hash,
            "deps": dep_hashes,
            "closure_vals": closure_val_hashes,
        },
        algo=algo,
    )

    visited[obj_id] = final_hash
    return final_hash


# ============================================================================
# Path dependency detection (R: .find_path_specs) – simplified stub
# ============================================================================

import ast
import inspect
import textwrap
from typing import Callable, Dict, List, Set


def _find_path_specs(func: Callable) -> Dict[str, List[str]]:
    """
    Python analogue of R's .find_path_specs(body(f)).

    It inspects the AST of `func` and looks for calls to "path-like"
    functions (os.listdir, glob.glob, Path.iterdir, etc.). From those
    calls it collects:

      - literals: string constants that look like paths
      - symbols:  variable names used as paths

    This is purely static: it never executes the function.
    """
    # Try to get source; if we can't, just return empty specs
    try:
        src = inspect.getsource(func)
    except OSError:
        return {"literals": [], "symbols": []}

    # Dedent in case the function is nested
    src = textwrap.dedent(src)

    try:
        tree = ast.parse(src)
    except SyntaxError:
        return {"literals": [], "symbols": []}

    # Names (or attributes) we treat as "path functions"
    #   e.g. os.listdir, glob.glob, Path.iterdir, Path.rglob, os.walk, etc.
    target_func_names: Set[str] = {
        "listdir",
        "scandir",
        "walk",
        "glob",
        "iglob",
        "rglob",
        "iterdir",
        "listdirs",   # just in case someone wraps
    }

    # Keyword names we treat as "path-like" parameters
    path_kw_names: Set[str] = {"path", "paths", "dir", "directory", "root"}

    literals: Set[str] = set()
    symbols: Set[str] = set()

    def collect(expr: ast.AST) -> None:
        """Recursively collect string literals and variable symbols from expr."""
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            # string literal
            literals.add(expr.value)
        elif isinstance(expr, ast.Name):
            # variable name
            symbols.add(expr.id)
        elif isinstance(expr, (ast.List, ast.Tuple, ast.Set)):
            for elt in expr.elts:
                collect(elt)
        elif isinstance(expr, ast.Dict):
            for k in expr.keys:
                if k is not None:
                    collect(k)
            for v in expr.values:
                collect(v)
        elif isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
            # "base" + "/subdir" style
            collect(expr.left)
            collect(expr.right)
        elif isinstance(expr, ast.JoinedStr):
            # f-strings: f"{base}/subdir"
            for v in expr.values:
                if isinstance(v, ast.FormattedValue):
                    collect(v.value)
                elif isinstance(v, ast.Constant) and isinstance(v.value, str):
                    literals.add(v.value)
        elif isinstance(expr, ast.Call):
            # file.path(base_dir, "subdir", "nested") → collect all args
            for a in expr.args:
                collect(a)
            for kw in expr.keywords:
                collect(kw.value)
        # everything else we ignore (Num, NameConstant, etc.)

    def is_target_call(func_node: ast.AST) -> bool:
        """Return True if call target looks like one of our path functions."""
        # bare name: listdir(...)
        if isinstance(func_node, ast.Name):
            return func_node.id in target_func_names

        # attribute: os.listdir(...), Path().iterdir(), base.glob(...)
        if isinstance(func_node, ast.Attribute):
            return func_node.attr in target_func_names

        return False

    class PathCallVisitor(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:
            if is_target_call(node.func):
                # Decide which argument(s) are "path-like"
                path_exprs: List[ast.AST] = []

                # 1. keyword args with names like path/dir/root
                for kw in node.keywords:
                    if kw.arg in path_kw_names:
                        path_exprs.append(kw.value)

                # 2. if we found none via keywords, fall back to first positional arg
                if not path_exprs and node.args:
                    path_exprs.append(node.args[0])

                # 3. collect from those expressions
                for expr in path_exprs:
                    collect(expr)

            # Continue traversing nested calls/expressions
            self.generic_visit(node)

    PathCallVisitor().visit(tree)

    return {
        "literals": sorted(literals),
        "symbols": sorted(symbols),
    }


def _detect_import_names(func: Callable) -> Set[str]:
    """Parse function AST to find import statements; return set of top-level module names."""
    try:
        src = inspect.getsource(func)
    except OSError:
        return set()
    src = textwrap.dedent(src)
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return set()

    names: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module.split(".")[0])
    return names


def _get_package_versions(import_names: Set[str], func: Callable) -> Dict[str, str]:
    """Look up versions for AST-detected imports + module-type globals referenced by func."""
    all_names = set(import_names)

    # Check referenced globals (co_names) for module types
    code = getattr(func, "__code__", None)
    globs = getattr(func, "__globals__", {})
    if code:
        for name in code.co_names:
            val = globs.get(name)
            if isinstance(val, types.ModuleType):
                mod_name = getattr(val, "__name__", name).split(".")[0]
                all_names.add(mod_name)

    pkgs: Dict[str, str] = {}
    for name in sorted(all_names):
        mod = sys.modules.get(name)
        if mod is None:
            continue
        ver = getattr(mod, "__version__", None)
        if ver is not None:
            pkgs[name] = str(ver)

    return pkgs



# ============================================================================
# cacheFile -> cache_file decorator
# ============================================================================

def cache_file(
    cache_dir: Optional[os.PathLike | str] = None,
    backend: str = "rds",
    file_args: Optional[List[str]] = None,
    ignore_args: Optional[List[str]] = None,
    file_pattern: Optional[str] = None,
    env_vars: Optional[List[str]] = None,
    algo: str = "xxhash64",
    version: Optional[str] = None,
    depends_on_files: Optional[List[str]] = None,
    depends_on_vars: Optional[Dict[str, Any]] = None,
    verbose: bool = False,
    hash_file_paths: bool = True,
) -> Callable[[Callable], Callable]:
    """
    Disk-backed caching decorator (Python analogue of R's cacheFile).
    Usage:

        @cache_file("/tmp/cache")
        def f(x, y=1):
            ...

    or

        f_cached = cache_file("/tmp/cache")(f)
    """
    if cache_dir is None:
        cache_dir_path = cache_default_dir()
    else:
        cache_dir_path = Path(cache_dir)

    # attempt to create directory (race-safe)
    try:
        cache_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.warning("cache_file: could not create cache directory %s", cache_dir_path)

    cache_dir_path = cache_dir_path.resolve()
    backend = backend.lower()
    if backend not in {"rds", "qs"}:
        raise ValueError("backend must be 'rds' or 'qs'")

    # static path specs from function body (stubbed for now)
    path_specs = _find_path_specs  # function; we will call inside decorator

    def decorator(f: Callable) -> Callable:
        sig = inspect.signature(f)
        ps = path_specs(f)
        static_dirs_lit: List[str] = ps.get("literals", [])
        static_dirs_sym: List[str] = ps.get("symbols", [])
        # Detect import names at decoration time (AST is static)
        _import_names = _detect_import_names(f)

        def _get_path_hash(path: os.PathLike | str) -> str:
            p = Path(path).resolve()
            if p.is_dir():
                # list files recursively, optional regex filter
                files = []
                for sub in sorted(p.rglob("*")):
                    if sub.is_file():
                        if file_pattern is not None:
                            if not re.search(file_pattern, sub.name):
                                continue
                        files.append(sub)
                if not files:
                    return "empty_dir"
                # hash (relative name, content hash) for structure + content
                file_entries = []
                for sub in files:
                    rel = str(sub.relative_to(p))
                    file_entries.append((rel, fast_file_hash(sub, algo=algo)))
                return _digest_obj(file_entries, algo=algo)
            elif p.is_file():
                return fast_file_hash(p, algo=algo)
            else:
                return ""

        def _atomic_save(obj: Any, path: Path) -> None:
            """
            Atomic write:
              - write to temp file in same dir
              - os.replace() to target
            """
            path = Path(path)
            tmp_name = f"{path.name}.tmp.{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))}"
            tmp_path = path.with_name(tmp_name)

            try:
                with tmp_path.open("wb") as f2:
                    pickle.dump(obj, f2, protocol=pickle.HIGHEST_PROTOCOL)

                try:
                    os.replace(tmp_path, path)
                except OSError:
                    # fallback: copy+unlink
                    import shutil

                    shutil.copy2(tmp_path, path)
                    tmp_path.unlink(missing_ok=True)

                # best-effort permissions (like 0664)
                if os.name == "posix":
                    try:
                        os.chmod(path, 0o664)
                    except OSError:
                        pass
            except Exception as e:
                logger.warning("cache_file: failed to save cache file %s: %s", path, e)
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass

        def _safe_load(path: Path) -> Any:
            with path.open("rb") as f2:
                obj = pickle.load(f2)
            # expect {"dat": value, "meta": {...}}
            if isinstance(obj, dict) and "dat" in obj:
                return obj["dat"]
            return obj

        def wrapper(*args, _load: bool = True, _force: bool = False, _skip_save: bool = False, **kwargs):
            invoke_env_globals = f.__globals__

            # -------- function name for filename label --------
            fname = getattr(f, "__name__", "anon")

            # -------- normalize arguments (include defaults) --------
            bound = sig.bind_partial(*args, **kwargs)
            bound.apply_defaults()

            args_for_hash: Dict[str, Any] = dict(bound.arguments)
            # remove control params from hashing
            args_for_hash.pop("_load", None)
            args_for_hash.pop("_force", None)
            args_for_hash.pop("_skip_save", None)

            if ignore_args:
                for nm in ignore_args:
                    args_for_hash.pop(nm, None)

            # order by argument name for stability
            args_for_hash = dict(sorted(args_for_hash.items(), key=lambda kv: kv[0]))

            # Sort **kwargs dict values for order-independent hashing
            for param_name, param in sig.parameters.items():
                if param.kind == inspect.Parameter.VAR_KEYWORD and param_name in args_for_hash:
                    val = args_for_hash[param_name]
                    if isinstance(val, dict):
                        args_for_hash[param_name] = dict(sorted(val.items(), key=lambda kv: kv[0]))

            # -------- resolve symlinks and normalize file_args --------
            if file_args:
                for nm in file_args:
                    if nm in args_for_hash:
                        val = args_for_hash[nm]
                        if isinstance(val, (str, Path)):
                            resolved = str(Path(val).resolve())
                            if not hash_file_paths and Path(resolved).exists():
                                args_for_hash[nm] = _get_path_hash(resolved)
                            else:
                                args_for_hash[nm] = resolved

            # -------- dynamic path scanning over arguments --------
            def _collect_paths(val: Any) -> List[Path]:
                out: List[Path] = []
                if isinstance(val, (str, Path)):
                    out.append(Path(val))
                elif isinstance(val, (list, tuple, set)):
                    for v in val:
                        out.extend(_collect_paths(v))
                return out

            dir_hashes_args: Dict[str, str] = {}

            if args_for_hash:
                if file_args:
                    scan_items = {k: v for k, v in args_for_hash.items() if k in file_args}
                else:
                    scan_items = args_for_hash

                for nm, expr_val in scan_items.items():
                    paths = _collect_paths(expr_val)
                    if not paths:
                        continue
                    for p in paths:
                        if p.exists():
                            h = _get_path_hash(p)
                            dir_hashes_args[str(p.resolve())] = h

            # -------- static path scanning (currently just stubbed lists) --------
            # literals
            static_hashes_lit: Dict[str, str] = {}
            for lit in static_dirs_lit:
                h = _get_path_hash(lit)
                static_hashes_lit[lit] = h

            # symbols: look up in globals and hash underlying paths
            static_hashes_sym: Dict[str, str] = {}
            for sym in static_dirs_sym:
                val = invoke_env_globals.get(sym)
                if isinstance(val, (str, Path, list, tuple)):
                    paths = _collect_paths(val)
                    sub_hashes = {str(Path(p).resolve()): _get_path_hash(p) for p in paths}
                    static_hashes_sym[f"sym:{sym}"] = _digest_obj(sub_hashes, algo=algo)

            # -------- environment variables --------
            current_envs: Optional[Dict[str, Optional[str]]] = None
            if env_vars:
                vars_sorted = sorted(env_vars)
                current_envs = {name: os.getenv(name) for name in vars_sorted}

            # -------- recursive closure hash --------
            deep_hash = get_recursive_closure_hash(f, algo=algo)

            # -------- package version detection --------
            pkg_versions = _get_package_versions(_import_names, f)

            # -------- build master hash --------
            dir_states: Dict[str, str] = {}
            dir_states.update(dir_hashes_args)
            dir_states.update(static_hashes_lit)
            dir_states.update(static_hashes_sym)

            # -------- depends_on_files hashing --------
            dep_file_hashes = None
            if depends_on_files:
                dep_file_hashes = {p: _get_path_hash(p) for p in sorted(depends_on_files)}

            hashlist = {
                "call": args_for_hash,
                "closure": deep_hash,
                "dir_states": dict(sorted(dir_states.items(), key=lambda kv: kv[0])),
                "envs": current_envs,
                "version": version,
                "depends_on_files": dep_file_hashes,
                "depends_on_vars": depends_on_vars,
                "pkgs": pkg_versions,
            }

            args_hash = _digest_obj(hashlist, algo=algo)
            outfile = cache_dir_path / f"{fname}.{args_hash}.{backend}"

            # -------- register node in cache tree --------
            node_id = f"{fname}:{args_hash}"
            _cache_tree_register_node(node_id, fname, args_hash, outfile)

            _cache_tree_call_stack.append(node_id)
            try:
                # 1. optimistic load
                if _load and not _force and outfile.exists():
                    try:
                        result = _safe_load(outfile)
                        if verbose:
                            logger.info("[%s] cache hit", fname)
                        return result
                    except Exception:
                        # partial/corrupt -> ignore and recompute
                        pass

                # verbose: report why we're computing
                if verbose:
                    if _force:
                        logger.info("[%s] forced re-execution", fname)
                    else:
                        # Check if any cache files exist for this function
                        existing = list(cache_dir_path.glob(f"{fname}.*.{backend}"))
                        if not existing:
                            logger.info("[%s] first execution", fname)
                        else:
                            logger.info("[%s] cache miss (argument or dependency changed)", fname)

                # 2. record pre-execution file hashes for modification warning
                pre_file_hashes: Dict[str, str] = {}
                if file_args and dir_hashes_args:
                    pre_file_hashes = dict(dir_hashes_args)

                # 3. compute with sentinel
                sentinel_path = outfile.with_suffix(outfile.suffix + ".computing")
                try:
                    sentinel_path.touch()
                except OSError:
                    pass

                try:
                    dat = f(*args, **kwargs)
                except Exception:
                    # Remove graph node on error
                    _cache_tree_graph.pop(node_id, None)
                    raise
                finally:
                    # Always clean up sentinel
                    try:
                        sentinel_path.unlink(missing_ok=True)
                    except OSError:
                        pass

                # 4. check for file modification during execution
                if pre_file_hashes:
                    import warnings as _warnings
                    _file_state_cache.clear()  # force re-hash
                    for pstr, old_h in pre_file_hashes.items():
                        p = Path(pstr)
                        if p.exists():
                            new_h = _get_path_hash(p)
                            if new_h != old_h:
                                _warnings.warn(
                                    f"File modified during execution: {pstr}",
                                    stacklevel=2,
                                )

                if not _skip_save:
                    save_data = {"dat": dat, "meta": hashlist}

                    # 3. try file locking if available
                    lock = None
                    lock_path = outfile.with_suffix(outfile.suffix + ".lock")
                    try:
                        from filelock import FileLock  # type: ignore

                        lock = FileLock(str(lock_path))
                        lock.acquire(timeout=5)
                    except Exception:
                        lock = None

                    try:
                        # double-check: maybe someone else wrote it while we computed
                        if _load and not _force and outfile.exists():
                            try:
                                return _safe_load(outfile)
                            except Exception:
                                pass
                        _atomic_save(save_data, outfile)
                    finally:
                        if lock is not None:
                            try:
                                lock.release()
                            except Exception:
                                pass

                return dat
            finally:
                _cache_tree_call_stack.pop()

        # preserve metadata
        wrapper.__name__ = getattr(f, "__name__", "cached_fn")
        wrapper.__doc__ = f.__doc__
        wrapper.__wrapped__ = f  # for inspect
        return wrapper

    return decorator


# R name alias if you want to keep it
cacheFile = cache_file
