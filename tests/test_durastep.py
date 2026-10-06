import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

import pytest

from durastep import MemoryStore, SQLiteStore, current_run, run, step

calls: list[str] = []


@pytest.fixture(autouse=True)
def _reset_calls():
    calls.clear()


@step
def send(to: str, body: str = "hi") -> dict:
    calls.append(f"send:{to}:{body}")
    return {"sent_to": to}


def test_same_args_execute_once():
    with run("r1", store=MemoryStore()) as r:
        first = send("ops@acme.com", "refund")
        second = send("ops@acme.com", "refund")
    assert first == second == {"sent_to": "ops@acme.com"}
    assert calls == ["send:ops@acme.com:refund"]
    assert (r.executed, r.cache_hits) == (1, 1)


def test_different_args_execute_separately():
    with run("r1", store=MemoryStore()) as r:
        send("a@x.com")
        send("b@x.com")
    assert len(calls) == 2
    assert r.executed == 2


def test_positional_and_keyword_calls_share_a_key():
    with run("r1", store=MemoryStore()) as r:
        send("a@x.com", "hi")
        send(to="a@x.com", body="hi")
        send("a@x.com")  # default fills in body="hi"
    assert len(calls) == 1
    assert r.cache_hits == 2


def test_runs_are_isolated():
    store = MemoryStore()
    with run("r1", store=store):
        send("a@x.com")
    with run("r2", store=store):
        send("a@x.com")
    assert len(calls) == 2


def test_resume_after_restart(tmp_path):
    db = tmp_path / "steps.db"
    with run("job-7", store=SQLiteStore(db)):
        send("a@x.com")
    # A new store on the same file stands in for a new process after a crash.
    with run("job-7", store=SQLiteStore(db)) as r:
        assert send("a@x.com") == {"sent_to": "a@x.com"}
    assert len(calls) == 1
    assert r.cache_hits == 1


def test_exceptions_are_not_cached():
    attempts = []

    @step
    def flaky() -> str:
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("upstream timeout")
        return "ok"

    store = MemoryStore()
    with run("r1", store=store), pytest.raises(ConnectionError):
        flaky()
    with run("r1", store=store):
        assert flaky() == "ok"
    assert len(attempts) == 2


def test_custom_key():
    charges = []

    @step(name="charge_order", key=lambda order, **_: order["id"])
    def charge(order: dict, note: str = "") -> str:
        charges.append(order["id"])
        return f"ch_{order['id']}"

    with run("r1", store=MemoryStore()):
        charge({"id": 42, "amount": 10}, note="first")
        # Same order id, different note and amount: still the same step.
        assert charge({"id": 42, "amount": 99}, note="retry") == "ch_42"
    assert charges == [42]


def test_passthrough_outside_run():
    assert current_run() is None
    send("a@x.com")
    send("a@x.com")
    assert len(calls) == 2


def test_non_json_result_raises_type_error():
    @step(name="returns_object")
    def returns_object() -> object:
        return object()

    with run("r1", store=MemoryStore()), pytest.raises(TypeError, match="returns_object"):
        returns_object()


def test_history_and_reset():
    with run("r1", store=MemoryStore()) as r:
        send("a@x.com")
        send("b@x.com")
        history = r.history()
        assert [h.result for h in history] == [{"sent_to": "a@x.com"}, {"sent_to": "b@x.com"}]
        assert all(h.step_name.endswith(".send") for h in history)
        assert r.reset() == 2
        assert r.history() == []
        send("a@x.com")
    assert len(calls) == 3


def test_async_steps():
    seen = []

    @step
    async def call_llm(prompt: str) -> str:
        seen.append(prompt)
        await asyncio.sleep(0)
        return prompt.upper()

    async def agent() -> list[str]:
        with run("r1", store=MemoryStore()):
            return [await call_llm("hello"), await call_llm("hello")]

    assert asyncio.run(agent()) == ["HELLO", "HELLO"]
    assert seen == ["hello"]


def test_empty_run_id_rejected():
    for bad in ["", "   "]:
        with pytest.raises(ValueError), run(bad, store=MemoryStore()):
            pass


@step(name="stamp")
def stamp() -> int:
    return os.getpid()


def _race(db: str) -> int:
    with run("race", store=SQLiteStore(db)):
        return stamp()


def test_concurrent_processes_store_one_row(tmp_path):
    db = str(tmp_path / "race.db")
    with ProcessPoolExecutor(max_workers=4, mp_context=get_context("spawn")) as pool:
        results = list(pool.map(_race, [db] * 4))
    rows = SQLiteStore(db).list_run("race")
    assert len(rows) == 1
    # Racing processes may each run the step, but all see the one kept result.
    assert len(set(results)) == 1
