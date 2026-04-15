# Cache Dependency Graph — Deep Dive

cachepy automatically tracks **which cached function called which** and **which files each depends on**. This tutorial covers the full graph API.

!!! tip "Notebook version"
    This tutorial is also available as a Jupyter notebook: [`notebooks/04_cache_graph.ipynb`](https://github.com/BIMSBbioinfo/pycacheR/blob/main/notebooks/04_cache_graph.ipynb)

## Building a Pipeline

When cached functions call other cached functions, parent → child edges are recorded automatically.

```python
from cachepy import cache_file, cache_tree_nodes, cache_tree_reset

cache_dir = "/tmp/graph_demo"

@cache_file(cache_dir)
def load_counts(path):
    return {"genes": ["TP53", "BRCA1", "EGFR"], "counts": [100, 250, 80]}

@cache_file(cache_dir)
def normalize(counts):
    total = sum(counts["counts"])
    return {g: c / total for g, c in zip(counts["genes"], counts["counts"])}

@cache_file(cache_dir)
def pipeline(path):
    return normalize(load_counts(path))

result = pipeline("samples/counts.csv")
```

## Inspecting Nodes

```python
for nid, node in cache_tree_nodes().items():
    print(f"{node['fname']:15s}  parents={node.get('parents', [])}  "
          f"children={node.get('children', [])}")
```

Each node contains: `fname`, `hash`, `parents`, `children`, `files`, `file_hashes`, `outfile`.

## File Tracking

`track_file(path)` registers a file dependency and stores its content hash:

```python
from cachepy import track_file

@cache_file(cache_dir)
def read_data(path):
    p = track_file(path)  # registers dependency
    return p.read_text()
```

## Staleness Detection

```python
from cachepy import cache_tree_changed_files

stale = cache_tree_changed_files()
for nid, info in stale.items():
    print(f"{info['node']['fname']}: {info['changed_files']}")
```

## Visualisation

```python
from cachepy import plot_cache_graph

fig = plot_cache_graph(highlight_stale=True)
```

Node colours:

| Colour | Meaning |
|--------|---------|
| **Navy** (#1D3557) | Cached and up-to-date |
| **Amber** (#FBBC04) | Stale — tracked file changed |
| **Gray** (#F1F3F5) | Cache file missing |
| **Light blue** (#E8F0FE) | Tracked file node |

Save to file:

```python
plot_cache_graph(output="graph.png")
```

## Graph Persistence

```python
from cachepy import cache_tree_save, cache_tree_load, cache_tree_sync

# Save / load
cache_tree_save("my_graph.pkl")
cache_tree_load("my_graph.pkl")

# Sync merges disk graph into memory (non-destructive)
cache_tree_sync(cache_dir)  # reads graph.pkl
```

The graph is also auto-persisted to `graph.pkl` in the cache directory on every function execution.

## Querying by File

```python
from cachepy import cache_tree_for_file

dependents = cache_tree_for_file("/path/to/data.tsv")
for nid, node in dependents.items():
    print(f"  {node['fname']}")
```

## Complex DAG

Diamond-shaped dependencies (two branches merging) are tracked correctly:

```python
@cache_file(cache_dir)
def integrate(sample):
    expr = branch_expression(sample)   # branch A
    muts = branch_mutations(sample)    # branch B
    return {g: expr[g] for g in muts if g in expr}
```

See the [notebook](https://github.com/BIMSBbioinfo/pycacheR/blob/main/notebooks/04_cache_graph.ipynb) for the full diamond DAG example with visualisation.
