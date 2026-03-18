# Changelog

## 1.0.0

### Breaking changes

- None (first stable release).

### New features

- **Cross-ported features from cacheR (A1–A8)**:
  - Export public inspection APIs: `cache_info`, `cache_list`, `cache_stats`,
    `cache_prune`, `cache_default_dir`, `cache_tree_for_file`,
    `cache_tree_sync`.
  - `cache_file_state_info()` and `cache_file_state_clear()` for managing the
    in-memory file hash cache.
  - Per-function breakdown in `cache_stats()`.
  - Detailed verbose miss reasons comparing 8 hash components (arguments,
    closure, file contents, env vars, version, depends_on_files,
    depends_on_vars, package versions).
  - Enhanced file path auto-detection with dict recursion and path heuristics.
  - Automatic graph disk persistence with optional filelock (`graph.pkl`).
  - `cache_tree_sync()` to merge graph from disk into memory.
  - Full-hash threshold: files <= 5 MB are hashed in full for exact results.

- **Sentinel waiting logic**: Concurrent processes detect `.computing` sentinel
  files and poll/wait instead of redundantly computing the same result.

- **`cache_list()` fname fallback**: Extracts function name from filename
  pattern when not stored in metadata.

### Tests

- Added 33 new tests (116 → 149 total):
  - `test_recursive_deps.py`: closure hash, cycle detection, scope isolation.
  - `test_cache_env.py`: dict/list mutation, file tracking, env var tracking.
  - `test_metadata.py`: cache format, `cache_info`, `cache_list`,
    `cache_stats`, file state API.
- Fixed test file counts to exclude `graph.pkl`.

## 0.1.0

- Initial release: Python port of cacheR.
- `@cache_file` decorator with pickle backend.
- Recursive closure hashing with cycle detection.
- Automatic file path detection in arguments.
- `plot_cache_graph()` for DAG visualisation.
- Cache tree tracking (`cache_tree_nodes`, `cache_tree_save`, `cache_tree_load`,
  `cache_tree_changed_files`).
- `track_file()` for explicit file dependency registration.
- `_find_path_specs()` AST scanning for file-reading calls.
- Package version tracking.
- `.cacheR.yml` / YAML config loading.
- MkDocs documentation site with Material theme.
