"""The @step decorator and the run() context.

Inside ``with run(run_id):``, the first successful call to a step with a given
set of arguments executes and stores its result. Every later call with the
same arguments in the same run returns the stored result instead, including
from a new process after a crash.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar, overload

from .store import SQLiteStore, Store, StoredResult

P = ParamSpec("P")
R = TypeVar("R")

_current: ContextVar[Run | None] = ContextVar("durastep_run", default=None)


@dataclass(frozen=True)
class StepRecord:
    """A completed step, as ``Run.history()`` reports it."""

    step_name: str
    step_key: str
    result: Any
    created_at: float


class Run:
    """The active run, yielded by ``with run(run_id) as r``."""

    def __init__(self, run_id: str, store: Store) -> None:
        self.run_id = run_id
        self.store = store
        self.executed = 0
        """Steps that actually ran during this ``with`` block."""
        self.cache_hits = 0
        """Steps answered from the store instead of running."""
        self._lock = threading.Lock()

    def history(self) -> list[StepRecord]:
        """Every stored step in this run, oldest first, including ones stored
        by earlier attempts."""
        return [
            StepRecord(r.step_name, r.step_key, json.loads(r.value), r.created_at)
            for r in self.store.list_run(self.run_id)
        ]

    def reset(self) -> int:
        """Forget this run's stored steps, so they execute again. Returns how
        many were removed."""
        return self.store.clear_run(self.run_id)

    def _lookup(self, step_key: str) -> tuple[bool, Any]:
        stored = self.store.get(self.run_id, step_key)
        if stored is None:
            return False, None
        with self._lock:
            self.cache_hits += 1
        return True, json.loads(stored.value)

    def _record(self, step_name: str, step_key: str, result: Any) -> Any:
        try:
            value = json.dumps(result)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"durastep: step {step_name!r} returned a {type(result).__name__}, which is not "
                "JSON serializable. Return plain data (dict, list, str, number, bool, None)."
            ) from exc
        with self._lock:
            self.executed += 1
        self.store.put(StoredResult(self.run_id, step_key, step_name, value, time.time()))
        # Return what the store kept. If another worker finished first, its
        # result won, and every caller should see the same value. Decoding
        # also makes a first run and a replay return identical data.
        stored = self.store.get(self.run_id, step_key)
        return json.loads(stored.value if stored is not None else value)


@contextmanager
def run(run_id: str, store: Store | None = None) -> Iterator[Run]:
    """Make every @step call inside the block durable under ``run_id``.

    Use the same ``run_id`` when retrying or resuming the same piece of work,
    e.g. a ticket, order or job ID. The default store is
    ``SQLiteStore("durastep.db")`` in the working directory.
    """
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("durastep: run_id must be a non-empty string")
    active = Run(run_id, store if store is not None else SQLiteStore())
    token = _current.set(active)
    try:
        yield active
    finally:
        _current.reset(token)


def current_run() -> Run | None:
    """The run active in this context, or None outside ``run()``."""
    return _current.get()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=repr, separators=(",", ":"))


def _make_key(
    name: str,
    sig: inspect.Signature,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    key_fn: Callable[..., Any] | None,
) -> str:
    if key_fn is not None:
        material = _canonical(key_fn(*args, **kwargs))
    else:
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()  # positional and keyword calls produce the same key
        material = _canonical(bound.arguments)
    digest = hashlib.sha256(f"{name}|{material}".encode()).hexdigest()[:24]
    return f"{name}:{digest}"


@overload
def step(fn: Callable[P, R], /) -> Callable[P, R]: ...


@overload
def step(
    *, name: str | None = None, key: Callable[..., Any] | None = None
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def step(
    fn: Callable[P, R] | None = None,
    /,
    *,
    name: str | None = None,
    key: Callable[..., Any] | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Make a function a durable step. Works on sync and async functions.

    ``name`` identifies the step in the store; it defaults to
    ``module.qualname``, so set it on anything you might rename or move.
    ``key`` receives the call's arguments and returns what identifies the
    call (default: all arguments). Outside ``run()`` the function behaves
    exactly as if undecorated.
    """

    def decorate(f: Callable[P, R]) -> Callable[P, R]:
        step_name = name or f"{f.__module__}.{f.__qualname__}"
        sig = inspect.signature(f)

        if inspect.iscoroutinefunction(f):
            async_f: Callable[P, Awaitable[Any]] = f

            @functools.wraps(f)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                active = _current.get()
                if active is None:
                    return await async_f(*args, **kwargs)
                step_key = _make_key(step_name, sig, args, kwargs, key)
                found, value = active._lookup(step_key)
                if found:
                    return value
                return active._record(step_name, step_key, await async_f(*args, **kwargs))

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            active = _current.get()
            if active is None:
                return f(*args, **kwargs)
            step_key = _make_key(step_name, sig, args, kwargs, key)
            found, value = active._lookup(step_key)
            if found:
                return value  # type: ignore[no-any-return]
            return active._record(step_name, step_key, f(*args, **kwargs))  # type: ignore[no-any-return]

        return wrapper

    return decorate(fn) if fn is not None else decorate
