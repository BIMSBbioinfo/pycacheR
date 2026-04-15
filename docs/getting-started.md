# Getting Started

## Installation

Install from PyPI:

```bash
pip install cachepy
```

Or install from source for development:

```bash
git clone https://github.com/BIMSBbioinfo/pycacheR.git
cd cachepy
pip install -e ".[dev]"
```

## Basic usage

The package provides:

- `cache_file()` — a caching decorator
- `cache_tree_nodes()` / `cache_tree_reset()` — functions for inspecting and managing the cache tree
- `cache_stats()` / `cache_prune()` / `cache_list()` — cache inspection and cleanup

```python
from cachepy import cache_file

cache_dir = "/tmp/my_cache"

@cache_file(cache_dir)
def slow_computation(n):
    """Simulate an expensive computation."""
    import time
    time.sleep(2)
    return sum(i**2 for i in range(n))

# First call — takes ~2 seconds
result = slow_computation(10_000)

# Second call — returns instantly from disk
result = slow_computation(10_000)
```

## Argument normalization

cachepy normalizes how arguments are passed. All of the following resolve to the same cache key:

```python
@cache_file(cache_dir)
def add(a, b, c=0):
    return a + b + c

add(1, 2)           # cache miss (first call)
add(a=1, b=2)       # cache hit
add(b=2, a=1)       # cache hit
add(1, 2, c=0)      # cache hit
```

## File dependency tracking

When arguments point to files, cachepy hashes **file content**, not just the path:

```python
@cache_file(cache_dir, file_args=["fpath"])
def parse_csv(fpath):
    lines = Path(fpath).read_text().strip().split("\n")
    return [line.split(",") for line in lines]
```

Changing the file content triggers a cache miss. Touching the file without changing content is still a cache hit.

## Next steps

- See [Features](features.md) for the full feature set
- See [API Reference](api.md) for detailed function documentation
- Check out the [Tutorials](tutorials/showcase.md) for hands-on examples
