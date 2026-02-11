# cachepy

**cachepy** tracks your data and code so you don't have to.

Python port of [cacheR](https://github.com/BIMSBbioinfo/cacheR).

## What does cachepy do?

It automatically checks for changes in code and input data and re-runs the
function only if necessary. Results are cached to disk as pickle files.

It's like snakemake/nextflow, but on the fly.

## What is it useful for?

- Keeping analysis results up to date
- Saving time on expensive computations
- Not using obsolete results
- Reusing heavy computations safely and transparently

## Quick example

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

## How does cachepy decide to recompute?

A cached call is reused **only if** all of the following are unchanged:

- The **function body** (source code hash, including inline changes)
- The **arguments** (normalized and hashed — positional, named, and default-filled forms are equivalent)
- The **tracked files / directories**, where relevant (`file_args`, `depends_on_files`)
- The **package versions** of imported modules used by the function
- The **environment variables** specified via `env_vars`
- The **version string**, if provided
- Any **external variables** specified via `depends_on_vars`

If any of these change, cachepy invalidates the old entry and recomputes.

## Limitations & caveats

- **Package boundaries:** cachepy stops tracking when it hits a function imported from an installed package. Instead, it records the package name and version.
- **Native code / C extensions:** C/C++ extensions and external tools are not tracked.
- **Side effects:** Functions with side effects are not fully safe to cache. Prefer pure, data-in / data-out functions.
- **Pickle limitations:** Results must be picklable.
- **Argument hashing:** Objects with non-deterministic pickling may produce unstable hashes. NumPy arrays, pandas DataFrames, and PyTorch tensors are handled correctly.
- **No distributed execution:** cachepy is a single-machine, single-process cache.
