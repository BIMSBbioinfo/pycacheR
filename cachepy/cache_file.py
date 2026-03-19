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

# Sentinel waiting parameters (mirrors cacheR concurrency coordination)
_SENTINEL_POLL_INTERVAL = 2       # seconds between polls
_SENTINEL_WAIT_TIMEOUT = 600      # max seconds to wait
_SENTINEL_STALE_THRESHOLD = 3600  # sentinels older than this (seconds) are ignored


# ============================================================================
# Sentinel waiting (concurrency coordination, mirrors cacheR)
# ============================================================================

def _wait_for_sentinel(
    sentinel_path: Path,
    outfile: Path,
    load_fn: Callable[[Path], Any],
    fname: str,
    poll: float = _SENTINEL_POLL_INTERVAL,
    timeout: float = _SENTINEL_WAIT_TIMEOUT,
    stale: float = _SENTINEL_STALE_THRESHOLD,
) -> Optional[Any]:
    """If another process is computing (sentinel exists and is fresh),
    wait for it to finish and return the cached result. Returns None on timeout."""
    if not sentinel_path.exists():
        return None
    try:
        age = time.time() - sentinel_path.stat().st_mtime
    except OSError:
        return None
    if age >= stale:
        return None  # stale sentinel, ignore

    waited = 0.0
    while waited < timeout:
        time.sleep(poll)
        waited += poll
        if outfile.exists():
            try:
                result = load_fn(outfile)
                if logger.isEnabledFor(logging.INFO):
                    logger.info("[%s] loaded from parallel worker after %.0fs wait", fname, waited)
                return result
            except Exception:
                pass
        # sentinel removed but no cache file → other process failed, stop waiting
        if not sentinel_path.exists():
            break
    if logger.isEnabledFor(logging.INFO):
        logger.info("[%s] wait timed out after %ds; executing", fname, int(timeout))
    return None


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

    # persist to disk (best-effort)
    if outfile is not None:
        try:
            _append_graph_to_disk(Path(outfile).parent, node_id)
        except Exception:
            pass


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


def cache_tree_files() -> List[str]:
    """Return a sorted list of all tracked file paths across all graph nodes."""
    all_files: set = set()
    for node in _cache_tree_graph.values():
        for fp in node.get("files", []):
            all_files.add(str(fp))
        for fp in node.get("file_hashes", {}):
            all_files.add(str(fp))
    return sorted(all_files)


def cache_tree_summary() -> str:
    """Return a human-readable summary of all graph nodes.

    For each node prints: function name, node ID, parents, children,
    and tracked files.
    """
    lines: List[str] = []
    nodes = dict(_cache_tree_graph)
    lines.append(f"Cache tree: {len(nodes)} node(s)")
    lines.append("")
    for nid, node in nodes.items():
        fname = node.get("fname", "?")
        parents = node.get("parents", [])
        children = node.get("children", [])
        files = [str(f) for f in node.get("files", [])]
        fh = node.get("file_hashes", {})
        # merge file sources
        all_f = sorted(set(files) | set(fh.keys()))

        lines.append(f"  {fname}")
        lines.append(f"    id:       {nid}")
        if parents:
            lines.append(f"    parents:  {', '.join(parents)}")
        if children:
            lines.append(f"    children: {', '.join(children)}")
        if all_f:
            lines.append(f"    files:    {', '.join(all_f)}")
        lines.append("")
    return "\n".join(lines)


def cache_tree_to_json(path: Optional[os.PathLike | str] = None) -> str:
    """Export the cache tree as JSON.

    Returns the JSON string.  If *path* is given, also writes to file.
    """
    import json

    nodes = dict(_cache_tree_graph)
    export: Dict[str, Any] = {"nodes": [], "edges": []}
    seen_edges: set = set()

    for nid, node in nodes.items():
        files = [str(f) for f in node.get("files", [])]
        fh = {str(k): v for k, v in node.get("file_hashes", {}).items()}
        export["nodes"].append({
            "id": nid,
            "fname": node.get("fname"),
            "outfile": str(node.get("outfile")) if node.get("outfile") else None,
            "parents": list(node.get("parents", [])),
            "children": list(node.get("children", [])),
            "files": files,
            "file_hashes": fh,
        })
        for child in node.get("children", []):
            edge = (nid, child)
            if edge not in seen_edges:
                seen_edges.add(edge)
                export["edges"].append({"from": nid, "to": child})

    text = json.dumps(export, indent=2)
    if path is not None:
        Path(path).write_text(text)
    return text


def cache_tree_to_dot(path: Optional[os.PathLike | str] = None) -> str:
    """Export the cache tree as Graphviz DOT format.

    Returns the DOT string.  If *path* is given, also writes to file.
    """
    nodes = dict(_cache_tree_graph)
    lines = ["digraph cache_tree {", '  rankdir=TB;', '  node [shape=box, style=filled, fillcolor="#1D3557", fontcolor=white, fontname="sans-serif"];']

    for nid, node in nodes.items():
        fname = node.get("fname", "?")
        label = fname.replace('"', '\\"')
        lines.append(f'  "{nid}" [label="{label}"];')

    seen: set = set()
    for nid, node in nodes.items():
        for child in node.get("children", []):
            edge = (nid, child)
            if edge not in seen:
                seen.add(edge)
                lines.append(f'  "{nid}" -> "{child}";')

    lines.append("}")
    text = "\n".join(lines)
    if path is not None:
        Path(path).write_text(text)
    return text


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


def cache_tree_sync(cache_dir: os.PathLike | str) -> None:
    """Load graph from disk and merge into in-memory graph (new nodes only)."""
    graph_path = Path(cache_dir) / "graph.pkl"
    if not graph_path.exists():
        return
    try:
        with graph_path.open("rb") as f:
            disk_graph = pickle.load(f)
        for k, v in disk_graph.items():
            if k not in _cache_tree_graph:
                _cache_tree_graph[k] = v
    except Exception:
        pass


def _append_graph_to_disk(cache_dir: Path, node_id: str) -> None:
    """Append a node to the persistent graph.pkl (with optional file locking)."""
    graph_path = cache_dir / "graph.pkl"
    lock = None
    try:
        from filelock import FileLock  # type: ignore
        lock_path = graph_path.with_suffix(".pkl.lock")
        lock = FileLock(str(lock_path), timeout=5)
        lock.acquire()
    except Exception:
        lock = None

    try:
        # load existing
        existing: Dict[str, Any] = {}
        if graph_path.exists():
            try:
                with graph_path.open("rb") as f:
                    existing = pickle.load(f)
            except Exception:
                existing = {}

        # merge new node
        node = _cache_tree_graph.get(node_id)
        if node is not None:
            existing[node_id] = node

        # save back
        with graph_path.open("wb") as f:
            pickle.dump(existing, f)
    except Exception as e:
        logger.warning("cache_file: failed to persist graph node: %s", e)
    finally:
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass


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


_FULL_HASH_LIMIT = 5 * 1024 * 1024  # 5 MB — files at or below this are hashed in full


def probabilistic_file_hash(
    path: os.PathLike | str,
    block_size: int = 64 * 1024,
    n_blocks: int = 5,
    algo: str = "xxhash64",
    full_hash_limit: int = _FULL_HASH_LIMIT,
) -> str:
    """
    Probabilistic file hash:
      - files <= full_hash_limit: hash entire file (exact)
      - larger files: sample first block, n_blocks random blocks, last block
    Stable w.r.t file path + size.
    """
    path = Path(path)
    if not path.exists() or not path.is_file():
        return ""

    size = path.stat().st_size

    # small files: hash entirely for exact results
    if size <= full_hash_limit:
        with path.open("rb") as f:
            data = f.read()
        return _digest_bytes(data, algo=algo)

    blocks: List[bytes] = []

    with path.open("rb") as f:
        # first block
        blocks.append(f.read(block_size))

        max_offset = max(size - block_size, 1)

        # deterministic sampling based on path + size
        seed_val = f"{path}:{size}".encode("utf-8")
        random.seed(_digest_bytes(seed_val, algo="sha256"))

        for _ in range(n_blocks):
            offset = random.randint(0, max_offset)
            f.seek(offset)
            blocks.append(f.read(block_size))

        # last block
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
# plot_cache_graph — visualise the live dependency DAG
# ============================================================================

def plot_cache_graph(
    output: Optional[str] = None,
    highlight_stale: bool = True,
):
    """
    Plot the current cache dependency graph.

    Requires ``matplotlib`` (optional dependency).
    Nodes are coloured by state:

    - **navy** (#1D3557): cached result present on disk
    - **amber** (#FBBC04): stale — a tracked file changed since caching
    - **gray** (#ADB5BD): cache file missing
    - **blue** (#457B9D): tracked-file node

    Parameters
    ----------
    output : str or None
        If a file path (e.g. ``"graph.png"``), save the figure there.
        If ``None``, call ``plt.show()``.
    highlight_stale : bool
        When True (default), check tracked files for changes and
        colour stale nodes in amber.

    Returns
    -------
    matplotlib.figure.Figure
    """
    try:
        import matplotlib
        matplotlib.use("Agg")          # headless-safe; overridden by show()
        import matplotlib.pyplot as plt
        from matplotlib.patches import FancyBboxPatch, Polygon, FancyArrowPatch
    except ImportError:
        raise ImportError(
            "plot_cache_graph requires matplotlib.  "
            "Install it with:  pip install matplotlib"
        )

    # -- gather data ---------------------------------------------------------
    nodes = cache_tree_nodes()
    if not nodes:
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.text(0.5, 0.5, "(empty cache graph)", ha="center", va="center",
                fontsize=14, color="#999999")
        ax.axis("off")
        if output:
            fig.savefig(output, dpi=120, bbox_inches="tight")
        return fig

    stale_ids: set = set()
    if highlight_stale:
        stale_ids = set(cache_tree_changed_files().keys())

    # -- determine node states -----------------------------------------------
    CACHED_CLR  = {"fill": "#1D3557", "stroke": "#0D1B2A", "text": "#FFFFFF"}
    STALE_CLR   = {"fill": "#FFF8D5", "stroke": "#FBBC04", "text": "#7C6200"}
    MISSING_CLR = {"fill": "#F1F3F5", "stroke": "#ADB5BD", "text": "#6C757D"}
    FILE_CLR    = {"fill": "#E8F0FE", "stroke": "#457B9D", "text": "#1A3A5C"}

    node_colors: Dict[str, dict] = {}
    node_labels: Dict[str, str] = {}
    # collect file-dependency nodes separately
    file_nodes: Dict[str, set] = {}  # file_path -> set of referencing node ids

    for nid, nd in nodes.items():
        label = nd.get("fname", nid.split(":")[0] if ":" in nid else nid)
        node_labels[nid] = label

        if nid in stale_ids:
            node_colors[nid] = STALE_CLR
        elif nd.get("outfile") and Path(nd["outfile"]).exists():
            node_colors[nid] = CACHED_CLR
        else:
            node_colors[nid] = MISSING_CLR

        for fp in nd.get("files", []):
            fp_str = str(fp)
            file_nodes.setdefault(fp_str, set()).add(nid)

    # -- build adjacency for topological sort --------------------------------
    children_map: Dict[str, List[str]] = {nid: [] for nid in nodes}
    parents_map: Dict[str, List[str]] = {nid: [] for nid in nodes}
    edges: List[tuple] = []

    for nid, nd in nodes.items():
        for child_id in nd.get("children", []):
            if child_id in nodes:
                children_map[nid].append(child_id)
                parents_map[child_id].append(nid)
                edges.append((nid, child_id))

    # add file-dependency pseudo-nodes
    all_node_ids = list(nodes.keys())
    for fp_str, referencing in file_nodes.items():
        fnode_id = f"__file__{fp_str}"
        node_labels[fnode_id] = Path(fp_str).name
        if fnode_id in stale_ids or any(r in stale_ids for r in referencing):
            node_colors[fnode_id] = STALE_CLR
        else:
            node_colors[fnode_id] = FILE_CLR
        all_node_ids.append(fnode_id)
        children_map[fnode_id] = list(referencing)
        for r in referencing:
            parents_map.setdefault(r, []).append(fnode_id)
            edges.append((fnode_id, r))

    # -- topological layer assignment ----------------------------------------
    in_degree = {n: 0 for n in all_node_ids}
    adj = {n: [] for n in all_node_ids}
    for src, dst in edges:
        if src in adj:
            adj[src].append(dst)
            in_degree[dst] = in_degree.get(dst, 0) + 1

    # Kahn's algorithm
    queue = [n for n in all_node_ids if in_degree.get(n, 0) == 0]
    layers: Dict[str, int] = {}
    while queue:
        nxt = []
        for n in queue:
            layers[n] = layers.get(n, 0)
            for child in adj.get(n, []):
                layers[child] = max(layers.get(child, 0), layers[n] + 1)
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    nxt.append(child)
        queue = nxt

    # fallback for any unvisited nodes (cycles)
    for n in all_node_ids:
        if n not in layers:
            layers[n] = 0

    # -- compute positions ---------------------------------------------------
    max_layer = max(layers.values()) if layers else 0
    layer_buckets: Dict[int, List[str]] = {}
    for n, lay in layers.items():
        layer_buckets.setdefault(lay, []).append(n)

    H_SPACING = 2.5
    V_SPACING = 1.2
    positions: Dict[str, tuple] = {}
    for lay, members in layer_buckets.items():
        x = lay * H_SPACING
        n_members = len(members)
        for i, nid in enumerate(sorted(members)):
            y = (i - (n_members - 1) / 2) * V_SPACING
            positions[nid] = (x, y)

    # -- draw ----------------------------------------------------------------
    xs = [p[0] for p in positions.values()]
    ys = [p[1] for p in positions.values()]
    x_margin = 1.5
    y_margin = 1.0
    fig_w = max(6, (max(xs) - min(xs)) + 2 * x_margin + 2)
    fig_h = max(3, (max(ys) - min(ys)) + 2 * y_margin + 2)

    fig, ax = plt.subplots(figsize=(min(fig_w, 16), min(fig_h, 10)))
    ax.set_xlim(min(xs) - x_margin, max(xs) + x_margin)
    ax.set_ylim(min(ys) - y_margin - 0.5, max(ys) + y_margin + 0.8)
    ax.set_aspect("equal")
    ax.axis("off")
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#FFFFFF")

    ax.text(0.5, 0.97, "cachepy — Cache Graph",
            transform=ax.transAxes, ha="center", va="top",
            fontsize=14, fontweight="bold", color="#1D3557",
            fontfamily="sans-serif")

    NODE_W, NODE_H = 1.5, 0.6
    FILE_W, FILE_H = 1.1, 0.5

    def _draw_func_node(nid):
        x, y = positions[nid]
        c = node_colors.get(nid, MISSING_CLR)
        box = FancyBboxPatch(
            (x - NODE_W/2, y - NODE_H/2), NODE_W, NODE_H,
            boxstyle="round,pad=0.08",
            facecolor=c["fill"], edgecolor=c["stroke"],
            linewidth=2, zorder=3)
        ax.add_patch(box)
        ax.text(x, y, node_labels.get(nid, nid), ha="center", va="center",
                fontsize=9, color=c["text"], zorder=5, fontfamily="sans-serif")

    def _draw_file_node(nid):
        x, y = positions[nid]
        c = node_colors.get(nid, FILE_CLR)
        fold = 0.15
        verts = [
            (x - FILE_W/2, y - FILE_H/2),
            (x + FILE_W/2 - fold, y - FILE_H/2),
            (x + FILE_W/2, y - FILE_H/2 + fold),
            (x + FILE_W/2, y + FILE_H/2),
            (x - FILE_W/2, y + FILE_H/2),
        ]
        poly = Polygon(verts, closed=True,
                        facecolor=c["fill"], edgecolor=c["stroke"],
                        linewidth=2, zorder=3)
        ax.add_patch(poly)
        ax.text(x, y, node_labels.get(nid, nid), ha="center", va="center",
                fontsize=8, color=c["text"], zorder=5, fontfamily="sans-serif")

    def _draw_edge(src, dst, dashed=False):
        sx, sy = positions[src]
        dx, dy = positions[dst]
        is_file = src.startswith("__file__")
        arrow = FancyArrowPatch(
            (sx, sy), (dx, dy),
            arrowstyle="-|>", mutation_scale=14,
            color="#888888", linewidth=1.5,
            linestyle="--" if is_file or dashed else "-",
            shrinkA=25, shrinkB=25, zorder=2,
            connectionstyle="arc3,rad=0.0")
        ax.add_patch(arrow)

    # edges first (below nodes)
    for src, dst in edges:
        if src in positions and dst in positions:
            _draw_edge(src, dst)

    # then nodes
    for nid in all_node_ids:
        if nid not in positions:
            continue
        if nid.startswith("__file__"):
            _draw_file_node(nid)
        else:
            _draw_func_node(nid)

    # legend
    legend_items = [
        (CACHED_CLR,  "Cached"),
        (STALE_CLR,   "Stale"),
        (MISSING_CLR, "Missing"),
        (FILE_CLR,    "File dep"),
    ]
    for i, (c, label) in enumerate(legend_items):
        lx = min(xs) - x_margin + 0.2 + i * 1.8
        ly = min(ys) - y_margin
        box = FancyBboxPatch(
            (lx, ly), 0.3, 0.25, boxstyle="round,pad=0.03",
            facecolor=c["fill"], edgecolor=c["stroke"],
            linewidth=1.3, zorder=3)
        ax.add_patch(box)
        ax.text(lx + 0.45, ly + 0.125, label, ha="left", va="center",
                fontsize=8, color="#333", fontfamily="sans-serif")

    plt.tight_layout()

    if output:
        fig.savefig(output, dpi=120, bbox_inches="tight",
                    facecolor="#FFFFFF", edgecolor="none")
    return fig


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


def cache_file_state_info() -> Dict[str, Any]:
    """Return status of the in-memory file hash cache."""
    return {
        "n_entries": len(_file_state_cache),
        "paths": list(_file_state_cache.keys()),
    }


def cache_file_state_clear() -> int:
    """Clear the in-memory file hash cache. Returns the number of entries removed."""
    n = len(_file_state_cache)
    _file_state_cache.clear()
    return n


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

    for p in cache_dir.glob("*.pkl"):
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
    Excludes graph.pkl from counts.
    """
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        raise FileNotFoundError(f"Cache directory not found: {cache_dir}")

    files = [
        p for p in cache_dir.iterdir()
        if p.is_file()
        and p.name.endswith(".pkl")
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

    # per-function breakdown: extract fname from filename pattern "fname.hash.ext"
    by_func: Dict[str, Dict[str, Any]] = {}
    for p, sz in zip(files, sizes):
        parts = p.stem.rsplit(".", 1)  # "fname.hash" -> ["fname", "hash"]
        fn = parts[0] if len(parts) == 2 else p.stem
        if fn not in by_func:
            by_func[fn] = {"fname": fn, "n_files": 0, "total_size_mb": 0.0}
        by_func[fn]["n_files"] += 1
        by_func[fn]["total_size_mb"] += sz / (1024 * 1024)

    return {
        "n_entries": len(files),
        "total_size_mb": sum(sizes) / (1024 * 1024),
        "oldest": min(mtimes),
        "newest": max(mtimes),
        "by_function": sorted(by_func.values(), key=lambda d: d["fname"]),
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

    files = list(cache_dir.glob("*.pkl"))
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
        # fallback: extract fname from filename pattern "fname.hash.ext"
        if fname is None:
            parts = fpath.stem.rsplit(".", 1)
            if len(parts) == 2:
                fname = parts[0]
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

# Mutable framework globals that should NOT affect the closure hash.
# Including these causes hash instability (e.g. _cache_tree_graph grows
# during recursive calls, making every subcall produce a different hash).
_HASH_EXCLUDED_GLOBALS: Set[str] = {
    "_cache_tree_graph",
    "_cache_tree_call_stack",
    "_file_state_cache",
}


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
        if name in _HASH_EXCLUDED_GLOBALS:
            continue
        if name in globs:
            val = globs[name]

            # Skip modules (they're covered by package version logic above)
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
    # Try to get source; if we can't, fall back to co_consts inspection
    try:
        src = inspect.getsource(func)
    except OSError:
        # Fallback: inspect bytecode constants for path-like strings
        code = getattr(func, "__code__", None)
        if code is None:
            return {"literals": [], "symbols": []}
        path_literals = []
        for c in code.co_consts:
            if isinstance(c, str) and ("/" in c or "\\" in c) and len(c) > 1:
                path_literals.append(c)
        return {"literals": sorted(set(path_literals)), "symbols": []}

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
# CacheDecorator — reusable decorator object
# ============================================================================

class CacheDecorator:
    """Reusable caching decorator bound to a directory and default options.

    Supports three usage patterns::

        cf = cache_file("/tmp/cache")

        # 1. Direct decoration
        @cf
        def step1(x): ...

        # 2. Per-function overrides
        @cf(verbose=True, version="2.0")
        def step2(x): ...

        # 3. Programmatic wrapping
        step3_cached = cf(step3)
    """

    # Names of keyword arguments accepted by cache_file().
    _OPTION_KEYS = frozenset({
        "cache_dir", "backend", "file_args", "ignore_args", "file_pattern",
        "env_vars", "algo", "version", "depends_on_files", "depends_on_vars",
        "verbose", "hash_file_paths",
    })

    def __init__(self, _decorator_fn, **defaults):
        # _decorator_fn: the inner ``decorator`` closure returned by
        # ``_cache_file_impl``.  ``defaults`` are the keyword arguments
        # originally passed to ``cache_file`` so we can merge overrides.
        self._decorator_fn = _decorator_fn
        self._defaults = defaults

    def __call__(self, *args, **kwargs):
        # Case 1: @cf  — called with a single callable, no kwargs
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return self._decorator_fn(args[0])

        # Case 2: @cf(verbose=True, ...) — called with option overrides
        if args:
            raise TypeError(
                "CacheDecorator accepts either a single callable (direct "
                "decoration) or keyword-only option overrides, not "
                "positional arguments mixed with keywords."
            )
        unknown = set(kwargs) - self._OPTION_KEYS
        if unknown:
            raise TypeError(f"Unknown cache_file options: {unknown}")
        merged = {**self._defaults, **kwargs}
        return cache_file(**merged)

    def __repr__(self):
        opts = ", ".join(f"{k}={v!r}" for k, v in self._defaults.items()
                         if v is not None)
        return f"CacheDecorator({opts})"


# ============================================================================
# cacheFile -> cache_file decorator
# ============================================================================

def cache_file(
    cache_dir: Optional[os.PathLike | str] = None,
    backend: str = "pickle",
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
) -> CacheDecorator:
    """
    Disk-backed caching decorator (Python analogue of R's cacheFile).

    Returns a :class:`CacheDecorator` that can be reused across functions::

        cf = cache_file("/tmp/cache")

        @cf
        def step1(x): ...

        @cf(verbose=True)
        def step2(x): ...

    Or used as a one-shot decorator::

        @cache_file("/tmp/cache")
        def f(x, y=1): ...
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
    if backend not in {"pickle"}:
        raise ValueError("backend must be 'pickle'")
    ext = "pkl"

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

        def _safe_load_full(path: Path) -> Optional[Dict[str, Any]]:
            """Load a cache file and return the raw dict (with 'dat' and 'meta')."""
            try:
                with path.open("rb") as f2:
                    obj = pickle.load(f2)
                if isinstance(obj, dict) and "meta" in obj:
                    return obj
            except Exception:
                pass
            return None

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
                """Recursively extract file/directory paths from any value."""
                out: List[Path] = []
                if isinstance(val, Path):
                    out.append(val)
                elif isinstance(val, str):
                    # only treat as path if it looks like one
                    if os.sep in val or val.startswith(".") or val.startswith("~"):
                        out.append(Path(val))
                elif isinstance(val, dict):
                    for v in val.values():
                        out.extend(_collect_paths(v))
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
            outfile = cache_dir_path / f"{fname}.{args_hash}.{ext}"

            # -------- register node in cache tree --------
            node_id = f"{fname}:{args_hash}"
            _cache_tree_register_node(node_id, fname, args_hash, outfile)

            _cache_tree_call_stack.append(node_id)
            try:
                # 1. optimistic load
                sentinel_path = outfile.with_suffix(outfile.suffix + ".computing")
                if _load and not _force and outfile.exists():
                    try:
                        result = _safe_load(outfile)
                        if verbose:
                            logger.info("[%s] cache hit", fname)
                        return result
                    except Exception:
                        # partial/corrupt -> ignore and recompute
                        pass

                # 1b. check if another process is already computing
                if not _force:
                    waited_result = _wait_for_sentinel(
                        sentinel_path, outfile, _safe_load, fname
                    )
                    if waited_result is not None:
                        return waited_result

                # verbose: report why we're computing
                if verbose:
                    if _force:
                        logger.info("[%s] forced re-execution", fname)
                    else:
                        existing = sorted(
                            cache_dir_path.glob(f"{fname}.*.{ext}"),
                            key=lambda p: p.stat().st_mtime,
                        )
                        if not existing:
                            logger.info("[%s] first execution", fname)
                        else:
                            stored = _safe_load_full(existing[-1])
                            if stored is not None:
                                sm = stored["meta"]
                                _MISS_LABELS = {
                                    "call": "arguments",
                                    "closure": "function body/closure",
                                    "dir_states": "file/directory contents",
                                    "envs": "environment variables",
                                    "version": "version",
                                    "depends_on_files": "explicit file dependencies",
                                    "depends_on_vars": "explicit variable dependencies",
                                    "pkgs": "package versions",
                                }
                                changes = [
                                    label for key, label in _MISS_LABELS.items()
                                    if sm.get(key) != hashlist.get(key)
                                ]
                                if not changes:
                                    changes = ["unknown (possibly new argument combination)"]
                                logger.info("[%s] cache miss -- changed: %s", fname, ", ".join(changes))
                            else:
                                logger.info("[%s] cache miss (previous entry unreadable)", fname)

                # 2. record pre-execution file hashes for modification warning
                pre_file_hashes: Dict[str, str] = {}
                if file_args and dir_hashes_args:
                    pre_file_hashes = dict(dir_hashes_args)

                # 3. compute with sentinel
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

    defaults = dict(
        cache_dir=cache_dir, backend=backend, file_args=file_args,
        ignore_args=ignore_args, file_pattern=file_pattern,
        env_vars=env_vars, algo=algo, version=version,
        depends_on_files=depends_on_files, depends_on_vars=depends_on_vars,
        verbose=verbose, hash_file_paths=hash_file_paths,
    )
    return CacheDecorator(decorator, **defaults)


# R name alias if you want to keep it
cacheFile = cache_file
