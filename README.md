# durastep

[![CI](https://github.com/farfieldai/durastep/actions/workflows/ci.yml/badge.svg)](https://github.com/farfieldai/durastep/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/durastep)](https://pypi.org/project/durastep/)

Durable, idempotent steps for LLM agents.

Agents retry, workers crash, and users click "run again". Without protection, a
retried run sends the email twice, charges the card twice, and pays again for
LLM calls it already made. `durastep` is one decorator that stops that.

```bash
pip install durastep
```

Zero dependencies. Python 3.10+. Sync and async.

## Quickstart

```python
from durastep import run, step

@step
def send_email(to: str, body: str) -> dict:
    ...  # the real side effect
    return {"sent_to": to}

with run("ticket-4821") as r:
    send_email("ops@acme.com", "Refund approved")
    send_email("ops@acme.com", "Refund approved")  # skipped: stored result returned
    print(r.executed, r.cache_hits)  # 1 1
```

Inside `run(run_id)`, the first successful call to a step with a given set of
arguments executes and stores its result. Every later call with the same
arguments in the same run gets the stored result back, including from a new
process after a crash. Use something stable as the run ID: the ticket, order
or job you're working on.

## Async, custom names and keys

```python
@step
async def call_llm(prompt: str) -> str:
    ...

@step(name="charge_order", key=lambda order, **_: order["id"])
def charge(order: dict, note: str = "") -> str:
    ...
```

- **`name`** identifies the step in the store. It defaults to
  `module.qualname`, so **set it on anything you might rename or move**,
  or a renamed step will run again for runs already in flight.
- **`key`** gets the call's arguments and returns what identifies the call.
  By default that's every argument, bound to the signature, so `f(1)`,
  `f(a=1)` and `f(1, b=<default>)` are the same call. Arguments that aren't
  JSON are keyed by `repr()`; for objects whose `repr` changes between
  processes, pass `key=`.

## How it behaves

- **Only successes are stored.** A step that raises runs again on retry.
- **Results must be JSON-serializable.** Anything else raises a `TypeError`
  naming the step. The first call returns the same decoded JSON a replay
  would, so the first run and a resumed run see identical data.
- **Outside `run()`, steps are plain functions**, so you can test them
  directly.
- **The run follows your code across `await`.** It's a `contextvars` context:
  asyncio tasks inherit it. A thread you start yourself needs
  `contextvars.copy_context().run(...)` to see it.

## Stores

```python
from durastep import SQLiteStore, MemoryStore

with run("job-7", store=SQLiteStore("/var/lib/agent/steps.db")):
    ...
```

- `SQLiteStore(path="durastep.db")` is the default: WAL mode, one connection
  per thread, safe across processes sharing the file. Writes are
  `INSERT OR IGNORE` on `(run_id, step_key)`, so the first completed result
  wins and a concurrent duplicate can never overwrite it.
- `MemoryStore()` keeps results in this process only. Use it in tests.
- Anything implementing the four-method `Store` protocol
  (`get`, `put`, `list_run`, `clear_run`) works.

`r.history()` lists a run's stored steps and `r.reset()` forgets them.

## Limits

These are real, and worth knowing before you rely on it:

- **At most once after success, not exactly once.** If the process dies after
  the side effect happens but before the result is stored, the step runs
  again. For payments and similar, also pass an idempotency key to the
  external API.
- **Two workers racing on the same new step can both execute it.** Only the
  first result is kept, and both callers get that result back, but the side
  effect happened twice. Don't run the same run ID in parallel.

## License

MIT
