# Changelog

## 0.1.0 (2026-10-06)

First release.

- `@step` decorator for sync and async functions, with optional `name=` and `key=`.
- `run(run_id, store=...)` context, `current_run()`, `Run.history()` and `Run.reset()`.
- `SQLiteStore` (WAL, per-thread connections, first result wins) and `MemoryStore`.
