
# pycacheR <img src="https://img.shields.io/badge/status-stable-green" align="right"/> <a href="https://BIMSBbioinfo.github.io/pycacheR/"><img src="https://img.shields.io/badge/docs-mkdocs-blue" align="right"/></a>

**cachepy** tracks your data and code so you don't have to

Python port of [cacheR](https://github.com/BIMSBbioinfo/cacheR).

## What does cachepy do?

It automatically checks for changes in code and input data and re-runs the
function only if necessary. Results are cached to disk as pickle files.

It's like snakemake/nextflow, but on the fly.

<p align="center">
  <img src="docs/assets/cache_graph_animation.gif" alt="cachepy cache graph animation" width="700"/>
</p>

## What is it useful for?

- Keeping analysis results up to date
- Saving time on expensive computations
- Not using obsolete results
- Reusing heavy computations safely and transparently

---

### Installation

```bash
pip install pycacheR
```

The PyPI package is called `pycacheR`, but the import name is `cachepy`:

```python
from cachepy import cache_file
```

### Basic usage

The package provides:

- `cache_file()` — a caching decorator
- `cache_tree_nodes()` / `cache_tree_reset()` — functions for inspecting and managing the cache tree
- `cache_stats()` / `cache_prune()` / `cache_list()` — cache inspection and cleanup

```python
from cachepy import cache_file

cache_dir = "/tmp/my_cache"

@cache_file(cache_dir)
def inner(x):
    return x + 1

@cache_file(cache_dir)
def outer(x):
    return inner(x) * 2

outer(3)
#> 8
```

### How does cachepy decide to recompute?

A cached call is reused **only if** all of the following are unchanged:

- The **function body** (source code hash, including inline changes)
- The **arguments** (normalized and hashed — positional, named, and default-filled forms are equivalent)
- The **tracked files / directories**, where relevant (`file_args`, `depends_on_files`)
- The **package versions** of imported modules used by the function
- The **environment variables** specified via `env_vars`
- The **version string**, if provided
- Any **external variables** specified via `depends_on_vars`

If any of these change, cachepy invalidates the old entry and recomputes.

### Features

| Feature | How |
|---------|-----|
| Argument normalization | `f(1, 2)`, `f(a=1, b=2)`, `f(b=2, a=1)` all hit the same cache entry |
| File dependency tracking | `file_args=["path"]` hashes file content, not just the path |
| Body change detection | Redefining a function invalidates its cache |
| Version parameter | `version="2.0"` for manual cache-busting |
| Force / skip save | `f(x, _force=True)` re-executes; `f(x, _skip_save=True)` runs without writing |
| External dependencies | `depends_on_files=[...]` and `depends_on_vars={...}` |
| Environment variables | `env_vars=["GENOME_BUILD"]` invalidates when env changes |
| Verbose logging | `verbose=True` logs cache hits, misses, and first executions |
| Dependency graph | Nested cached calls are tracked as a parent/child graph |
| Recursive memoization | Recursive cached functions automatically memoize sub-problems |
| Cache statistics | `cache_stats(dir)` reports entry count and disk usage |
| Cache pruning | `cache_prune(dir, days_old=30)` removes stale entries |
| Sentinel / lock files | Concurrent-safe via `.computing` sentinels and `.lock` files |
| YAML configuration | Load project-level defaults from a YAML file |

### Limitations & caveats

- **Package boundaries:**
  cachepy stops tracking when it hits a function imported from an installed
  package. Instead, it records the package name and version. It does not
  inspect the internals of those functions.

- **Native code / C extensions:**
  C/C++ extensions and external tools (e.g. `subprocess.run(["bwa", ...])`)
  are not tracked. If they change, cachepy will not notice unless their
  inputs or outputs change in a tracked place.

- **Side effects:**
  Functions with side effects (writing to global variables, random seeds,
  databases, network calls, etc.) are not fully safe to cache. Prefer pure,
  data-in / data-out functions.

- **Pickle limitations:**
  Results must be picklable. Objects that cannot be pickled (open file handles,
  database connections, running threads, lambda closures in some cases) will
  raise an error at save time. Large results incur serialization overhead on
  every cache hit.

- **Argument hashing:**
  Arguments are hashed via `pickle + hashlib`. Objects with non-deterministic
  pickling (e.g. sets, dicts with hash-randomized key order in older Python,
  custom `__reduce__`) may produce unstable hashes. NumPy arrays, pandas
  DataFrames, and PyTorch tensors are handled correctly.

- **No distributed execution:**
  cachepy is a single-machine, single-process cache. It does not coordinate
  across machines or provide cluster scheduling.

#### When you probably *shouldn't* use cachepy

- Highly stateful / interactive code where caching would confuse you more
  than it helps
- Situations where you need full workflow orchestration, scheduling, and
  cluster execution (use snakemake / nextflow / targets / etc. instead)
- Functions that return non-picklable objects (wrap them to return
  serializable data instead)
