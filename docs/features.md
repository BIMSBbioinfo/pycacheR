# Features

## Feature overview

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

## Body change detection

Redefining a function (e.g. fixing a bug) changes its hash, automatically invalidating the cache:

```python
@cache_file(cache_dir)
def transform(x):
    return x * 2

transform(5)  # returns 10

# Fix the function
@cache_file(cache_dir)
def transform(x):
    return x * 3  # body changed — cache miss

transform(5)  # returns 15 (re-executed)
```

## Version parameter

Manually bump `version` to invalidate without touching the function body:

```python
@cache_file(cache_dir, version="1.0")
def predict(x):
    return x * 42

predict(10)  # executes

@cache_file(cache_dir, version="2.0")
def predict(x):
    return x * 42

predict(10)  # cache miss — different version
```

## Force and skip save

Control caching per-call without changing the decorator:

```python
@cache_file(cache_dir)
def fetch(query):
    return {"query": query, "ts": time.time()}

fetch("TP53")                       # normal execution + cache
fetch("TP53", _force=True)          # forced re-execution
fetch("BRCA1", _skip_save=True)     # execute but don't write to disk
```

## External dependencies

Declare files or variables outside the function signature that should invalidate the cache:

```python
# File dependency
@cache_file(cache_dir, depends_on_files=["config.yml"])
def analyze(x):
    return x ** 2

# Variable dependency
@cache_file(cache_dir, depends_on_vars={"schema": "v3"})
def process(x):
    return x + 1
```

## Environment variables

Track environment variables — the cache invalidates when they change:

```python
@cache_file(cache_dir, env_vars=["GENOME_BUILD"])
def align(reads):
    build = os.environ.get("GENOME_BUILD", "unknown")
    return f"aligned_{reads}_to_{build}"
```

## Verbose logging

Set `verbose=True` to log cache decisions:

```python
import logging
logging.basicConfig(level=logging.INFO)

@cache_file(cache_dir, verbose=True)
def compute(x):
    return x * 2

compute(1)   # logs: first execution
compute(1)   # logs: cache hit
```

## Dependency graph

When cached functions call other cached functions, cachepy tracks the call graph:

```python
@cache_file(cache_dir)
def load_data(path):
    return [1, 2, 3, 4, 5]

@cache_file(cache_dir)
def normalize(data):
    mean = sum(data) / len(data)
    return [x - mean for x in data]

@cache_file(cache_dir)
def pipeline(path):
    return normalize(load_data(path))

pipeline("input.csv")

# Inspect the graph
from cachepy import cache_tree_nodes
for nid, node in cache_tree_nodes().items():
    print(f"{node['fname']}  parents={len(node.get('parents', []))}")
```

## Cache statistics and pruning

```python
from cachepy.cache_file import cache_stats, cache_prune

stats = cache_stats(cache_dir)
print(f"Entries: {stats['n_entries']}, Size: {stats['total_size_mb']:.2f} MB")

# Remove entries older than 30 days
cache_prune(cache_dir, days_old=30)
```

## YAML configuration

Load project-level defaults from a YAML file:

```yaml
# cachepy.yml
cache_dir: /tmp/my_project_cache
backend: pickle
verbose: true
env_vars:
  - HOME
  - GENOME_BUILD
```

```python
from cachepy.cache_file import load_config
cfg = load_config("cachepy.yml")
```
