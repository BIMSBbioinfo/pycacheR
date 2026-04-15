# Single-Cell RNA-seq Pipeline

This tutorial demonstrates how cachepy accelerates a typical scRNA-seq analysis
using the PBMC 3k dataset from 10x Genomics. It corresponds to the
[`02_scrnaseq_cachepy.ipynb`](https://github.com/BIMSBbioinfo/pycacheR/blob/main/notebooks/02_scrnaseq_cachepy.ipynb) notebook.

Each analysis step is wrapped with `@cache_file`, so re-running the notebook
skips expensive computations. This is especially useful when:

- Iterating on downstream parameters (e.g. clustering resolution)
- Sharing pipelines across sessions
- Resuming after a crash

## Setup

```python
import sys, time, shutil, warnings
from pathlib import Path
import numpy as np
import scanpy as sc

from cachepy import cache_file, cache_tree_nodes, cache_tree_reset
from cachepy.cache_file import cache_stats, _file_state_cache

CACHE_DIR = Path("scrnaseq_cache")
```

## Pipeline Steps

### 1. Load Data

```python
@cache_file(CACHE_DIR, verbose=True)
def load_pbmc3k():
    adata = sc.datasets.pbmc3k()
    adata.var_names_make_unique()
    return adata

adata_raw = load_pbmc3k()
```

### 2. Quality Control

```python
@cache_file(CACHE_DIR, verbose=True)
def run_qc(adata, min_genes=200, min_cells=3, max_mito_pct=5):
    adata = adata.copy()
    adata.var["mt"] = adata.var_names.str.startswith("MT-")
    sc.pp.calculate_qc_metrics(adata, qc_vars=["mt"], percent_top=None, log1p=False, inplace=True)
    sc.pp.filter_cells(adata, min_genes=min_genes)
    sc.pp.filter_genes(adata, min_cells=min_cells)
    adata = adata[adata.obs.pct_counts_mt < max_mito_pct, :].copy()
    return adata

adata_qc = run_qc(adata_raw, min_genes=200, min_cells=3, max_mito_pct=5)
```

### 3. Normalization & HVG Selection

```python
@cache_file(CACHE_DIR, verbose=True)
def normalize_and_hvg(adata, n_top_genes=2000, target_sum=1e4):
    adata = adata.copy()
    sc.pp.normalize_total(adata, target_sum=target_sum)
    sc.pp.log1p(adata)
    adata.raw = adata
    sc.pp.highly_variable_genes(adata, n_top_genes=n_top_genes, flavor="seurat")
    adata = adata[:, adata.var.highly_variable].copy()
    sc.pp.regress_out(adata, ["total_counts", "pct_counts_mt"])
    sc.pp.scale(adata, max_value=10)
    return adata

adata_norm = normalize_and_hvg(adata_qc, n_top_genes=2000)
```

### 4. Dimensionality Reduction

```python
@cache_file(CACHE_DIR, verbose=True)
def run_dimred(adata, n_pcs=40, n_neighbors=10):
    adata = adata.copy()
    sc.tl.pca(adata, n_comps=n_pcs, svd_solver="arpack")
    sc.pp.neighbors(adata, n_neighbors=n_neighbors, n_pcs=n_pcs)
    sc.tl.umap(adata)
    return adata

adata_dimred = run_dimred(adata_norm, n_pcs=40, n_neighbors=10)
```

### 5. Clustering

This is where caching really shines — you can iterate on the resolution
parameter without waiting for PCA/UMAP to recompute.

```python
@cache_file(CACHE_DIR, verbose=True)
def run_clustering(adata, resolution=1.0):
    adata = adata.copy()
    sc.tl.leiden(adata, resolution=resolution, flavor="igraph", n_iterations=2)
    return adata

# Try different resolutions — only the new ones compute
for res in [0.5, 1.0, 1.5]:
    adata_clust = run_clustering(adata_dimred, resolution=res)
```

### 6. Marker Gene Detection

```python
@cache_file(CACHE_DIR, verbose=True)
def find_markers(adata, n_genes=25, method="wilcoxon"):
    adata = adata.copy()
    sc.tl.rank_genes_groups(adata, groupby="leiden", method=method, n_genes=n_genes)
    return adata

adata_markers = find_markers(adata_final, n_genes=25)
```

### 7. Cell Type Annotation

```python
@cache_file(CACHE_DIR, verbose=True)
def annotate_celltypes(adata):
    adata = adata.copy()
    marker_map = {
        "CD4 T":    ["IL7R", "CD4"],
        "CD8 T":    ["CD8A", "CD8B"],
        "B":        ["MS4A1", "CD79A"],
        "NK":       ["GNLY", "NKG7"],
        "Mono":     ["CD14", "LYZ"],
        "DC":       ["FCER1A", "CST3"],
        "Platelet": ["PPBP"],
    }
    # ... scoring logic ...
    return adata

adata_annotated = annotate_celltypes(adata_markers)
```

## Parameter Iteration

The key benefit: change a downstream parameter and only that step re-runs.
Everything upstream is cached.

```python
# Steps 1-4 are all cache hits (instant)
raw = load_pbmc3k()
qc = run_qc(raw)
norm = normalize_and_hvg(qc)
dimred = run_dimred(norm)

# Only step 5 re-runs with new resolution
clust_new = run_clustering(dimred, resolution=0.3)
```

## Inspecting the Cache

```python
stats = cache_stats(CACHE_DIR)
print(f"Entries: {stats['n_entries']}, Size: {stats['total_size_mb']:.1f} MB")

nodes = cache_tree_nodes()
for nid, node in nodes.items():
    print(f"  {node['fname']}  parents={len(node.get('parents', []))}")
```

## Speed Benchmark

Cache overhead is constant (~1-10 ms) regardless of computation time.
See the [full notebook](https://github.com/BIMSBbioinfo/pycacheR/blob/main/notebooks/02_scrnaseq_cachepy.ipynb) for benchmark plots.
