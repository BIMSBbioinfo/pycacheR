# Feature Showcase

This tutorial demonstrates all major cachepy features. It corresponds to the
[`01_cachepy_showcase.ipynb`](https://github.com/BIMSBbioinfo/cachepy/blob/main/notebooks/01_cachepy_showcase.ipynb) notebook.

## Setup

```python
import os, sys, time, shutil
from pathlib import Path
from cachepy import cache_file, cache_tree_nodes, cache_tree_reset
from cachepy.cache_file import (
    cache_prune, cache_stats, fast_file_hash,
    _file_state_cache, load_config,
)

DEMO_CACHE = Path("demo_cache")

def fresh_cache():
    if DEMO_CACHE.exists():
        shutil.rmtree(DEMO_CACHE)
    cache_tree_reset()
    _file_state_cache.clear()
    return DEMO_CACHE
```

## 1. Basic Caching

Wrap any function with `@cache_file(cache_dir)`. The first call executes normally;
subsequent calls with the same arguments return instantly from disk.

```python
cache_dir = fresh_cache()

@cache_file(cache_dir)
def slow_computation(n):
    """Simulate an expensive computation."""
    time.sleep(1)
    return sum(i**2 for i in range(n))

result1 = slow_computation(10_000)  # ~1s
result2 = slow_computation(10_000)  # instant (cache hit)
result3 = slow_computation(5_000)   # ~1s (different args)
```

> **Takeaway:** Same args = cache hit. Different args = new computation.

## 2. Argument Normalization

cachepy normalizes how arguments are passed — positional, named, or with explicit
defaults all resolve to the same cache key.

```python
cache_dir = fresh_cache()

@cache_file(cache_dir)
def add(a, b, c=0):
    print(f"  -> executing add({a}, {b}, {c})")
    return a + b + c

add(1, 2)          # executes
add(a=1, b=2)      # cache hit
add(b=2, a=1)      # cache hit
add(1, 2, c=0)     # cache hit
add(1, 2, c=10)    # executes (different args)
```

## 3. kwargs Order Independence

For `**kwargs` functions, keyword argument order is ignored (sorted internally).

```python
cache_dir = fresh_cache()

@cache_file(cache_dir)
def config_hash(**kwargs):
    return str(sorted(kwargs.items()))

config_hash(alpha=0.1, beta=0.9)  # executes
config_hash(beta=0.9, alpha=0.1)  # cache hit
```

## 4. File Dependency Tracking

When arguments point to files, cachepy hashes **file content** (not just the path).

```python
cache_dir = fresh_cache()
data_file = Path("demo_data.csv")
data_file.write_text("gene,expr\nTP53,10.5\nBRCA1,8.2\n")

@cache_file(cache_dir, file_args=["fpath"])
def parse_csv(fpath):
    lines = Path(fpath).read_text().strip().split("\n")
    header = lines[0].split(",")
    return [dict(zip(header, l.split(","))) for l in lines[1:]]

parse_csv(str(data_file))  # executes
parse_csv(str(data_file))  # cache hit (same content)

data_file.write_text("gene,expr\nTP53,10.5\nBRCA1,8.2\nEGFR,15.3\n")
_file_state_cache.clear()
parse_csv(str(data_file))  # cache miss (file changed)
```

## 5. Body Change Detection

Redefining a function changes its hash, automatically invalidating the cache.

```python
cache_dir = fresh_cache()

@cache_file(cache_dir)
def transform(x):
    return x * 2

transform(5)  # returns 10

@cache_file(cache_dir)
def transform(x):
    return x * 3  # body changed

transform(5)  # returns 15 (new cache entry)
```

## 6. Version Parameter

Manually bump `version` to invalidate without touching the function body.

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

## 7. Force & Skip Save

Control caching per-call:

- `_force=True` — always re-execute (ignores cache)
- `_skip_save=True` — execute but don't write to disk

```python
@cache_file(cache_dir)
def fetch(query):
    return {"query": query, "ts": time.time()}

r1 = fetch("TP53")                    # executes
r2 = fetch("TP53")                    # cache hit
r3 = fetch("TP53", _force=True)       # forced re-run
fetch("BRCA1", _skip_save=True)       # dry run (no file written)
```

## 8. External Dependencies

Declare files or variables that should invalidate the cache.

```python
# depends_on_files
config = Path("demo_config.yml")
config.write_text("threshold: 0.05\n")

@cache_file(cache_dir, depends_on_files=[str(config)])
def analyze(x):
    return x ** 2

analyze(5)  # executes
config.write_text("threshold: 0.01\n")
_file_state_cache.clear()
analyze(5)  # cache miss — config changed

# depends_on_vars
@cache_file(cache_dir, depends_on_vars={"schema": "v3"})
def process(x):
    return x + 1
```

## 9. Environment Variables

Track environment variables — the cache invalidates when they change.

```python
os.environ["GENOME_BUILD"] = "hg38"

@cache_file(cache_dir, env_vars=["GENOME_BUILD"])
def align(reads):
    build = os.environ.get("GENOME_BUILD", "unknown")
    return f"aligned_{reads}_to_{build}"

align("sample1")  # executes
os.environ["GENOME_BUILD"] = "hg19"
align("sample1")  # cache miss
```

## 10. Verbose Mode

```python
import logging
logging.basicConfig(level=logging.INFO, format="[cachepy] %(message)s")

@cache_file(cache_dir, verbose=True)
def compute(x):
    return x * 2

compute(1)   # logs: first execution
compute(1)   # logs: cache hit
```

## 11. Cache Statistics & Pruning

```python
stats = cache_stats(cache_dir)
print(f"Entries: {stats['n_entries']}  |  Size: {stats['total_size_mb']:.2f} MB")

cache_prune(cache_dir, days_old=0)
```

## 12. Dependency Graph

When cached functions call other cached functions, cachepy tracks the call graph.

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

pipeline('input.csv')

for nid, node in cache_tree_nodes().items():
    print(f"  {node['fname']}  parents={len(node.get('parents', []))}")
```

## 13. Recursive Functions

Recursive cached functions automatically memoize sub-problems.

```python
@cache_file(cache_dir)
def fib(n):
    if n <= 1: return n
    return fib(n-1) + fib(n-2)

fib(10)  # calls fib 11 times
fib(10)  # 0 calls (all cached)
```

## 14. Error Handling

Failed functions don't leave stale cache entries or broken graph nodes.

```python
@cache_file(cache_dir)
def risky(x):
    if x < 0: raise ValueError("negative")
    return x ** 0.5

risky(4)     # succeeds, cached
risky(-1)    # raises ValueError, nothing cached
```

## 15. YAML Configuration

```python
from cachepy.cache_file import load_config

config_path = Path("demo_cachepy.yml")
config_path.write_text(
    "cache_dir: /tmp/my_project_cache\n"
    "backend: pickle\n"
    "verbose: true\n"
)

cfg = load_config(config_path)
```

## 16. Speed Benchmark

Cache overhead is constant (~1-3 ms) regardless of original computation time.
See the [notebook](https://github.com/BIMSBbioinfo/cachepy/blob/main/notebooks/01_cachepy_showcase.ipynb) for the full benchmark with plots.
