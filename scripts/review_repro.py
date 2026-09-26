"""Repros for docs/review-2026-09-26.md. Run: .venv/bin/python scripts/review_repro.py [r1 r2 ...]"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from keziah.adapters.mock import MockAdapter  # noqa: E402
from keziah.config import ModelGroupSettings, ModelSettings, SchedulerSettings  # noqa: E402
from keziah.scheduler import PickerState, select  # noqa: E402
from keziah.service import Keziah  # noqa: E402
from keziah.types import Candidate, ModelHealth  # noqa: E402
from tests.conftest import QUESTIONS, make_settings  # noqa: E402


def tmp() -> Path:
    return Path(tempfile.mkdtemp(prefix="kzr-"))


def check(name):
    def deco(fn):
        def run():
            print(f"\n=== {name}")
            try:
                fn()
            except Exception as exc:  # pragma: no cover
                print(f"  ERROR {type(exc).__name__}: {exc}")
        run.__name__ = fn.__name__
        return run
    return deco


@check("R1 scheduler: aged normal backlog vs fresh interactive (defaults)")
def r1():
    s = SchedulerSettings()
    state = PickerState()
    counts = {"interactive": 0, "normal": 0}
    now = 1000.0
    for _ in range(1000):
        c, state = select(state, "m", [
            Candidate("i", "interactive", 0, now, "a", "m", "h"),   # fresh
            Candidate("n", "normal", 0, now - 120, "b", "m", "h"),  # 2 min old
        ], now, s)
        counts[c.scheduling_class] += 1
    print("  shares over 1000 picks:", counts)


@check("R2 one failed health probe dead-letters every job for the TTL")
def r2():
    class Flaky(MockAdapter):
        def __init__(self):
            super().__init__("remote")
            self.health_ok = False
        def health_sync(self):
            return ModelHealth(ok=self.health_ok, permanent=False, detail="probe 5xx (cached)")
    ad = Flaky()
    st = make_settings(tmp(), extra_models={"remote": ModelSettings(adapter="mock", max_concurrency=4)})
    st.scheduler.retry_base_s = 0.05  # production default
    with Keziah(settings=st, adapters={"remote": ad}) as k:
        t0 = time.monotonic()
        ids = [k.submit(model="remote", state=f"s{i}", questions=QUESTIONS) for i in range(5)]
        res = [k.wait(i, timeout=5) for i in ids]
        print(f"  after {time.monotonic()-t0:.2f}s:", sorted({r.status for r in res}),
              "| adapter infer calls:", ad.calls, "| error:", res[0].error)


@check("R3 a job whose lease was lost once can never succeed in this process")
def r3():
    st = make_settings(tmp(), lease_seconds=0.3, max_attempts=3)
    with Keziah(settings=st) as k:
        jid = k.submit(model="mock", state="x", questions=QUESTIONS)
        k._lost.add(jid)  # simulate one heartbeat that found the lease gone (e.g. loop stalled)
        r = k.wait(jid, timeout=10)
        print("  status:", r.status, "| attempts:", r.attempts, "| error:", r.error)


@check("R4 maintenance-finished jobs never wake wait() or fire callbacks")
def r4():
    gate = threading.Event()
    ad = MockAdapter("mock", gate=gate)
    st = make_settings(tmp(), concurrency=1)
    with Keziah(settings=st, adapters={"mock": ad}) as k:
        blocker = k.submit(model="mock", state="block", questions=QUESTIONS)
        time.sleep(0.2)
        fired = []
        deadline = (datetime.now(timezone.utc) + timedelta(milliseconds=300)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        jid = k.submit(model="mock", state="late", questions=QUESTIONS, deadline_at=deadline, callback=fired.append)
        t0 = time.monotonic()
        r = k.wait(jid, timeout=3)
        print(f"  wait returned after {time.monotonic()-t0:.2f}s (deadline was 0.3s) status={r.status}; callback fired: {bool(fired)}")
        gate.set()
        k.wait(blocker, timeout=5)


@check("R5 deadline_at is compared as a raw string")
def r5():
    st = make_settings(tmp())
    with Keziah(settings=st) as k:
        future = datetime.now(timezone(timedelta(hours=-5))) + timedelta(minutes=30)
        jid = k.submit(model="mock", state="x", questions=QUESTIONS, deadline_at=future.isoformat())
        r = k.wait(jid, timeout=5)
        print(f"  deadline 30 min in the future ({future.isoformat()}): status={r.status} error={r.error}")
        jid2 = k.submit(model="mock", state="y", questions=QUESTIONS, deadline_at="not a date")
        print("  deadline 'not a date' accepted; status:", k.wait(jid2, timeout=5).status)


@check("R6 HTTP: malformed batch bodies")
def r6():
    from fastapi.testclient import TestClient
    from keziah.api.app import create_app
    st = make_settings(tmp())
    k = Keziah(settings=st)
    with TestClient(create_app(service=k), raise_server_exceptions=False) as c:
        r1 = c.post("/v1/batches", json={"jobs": [{"state": "x"}]})
        r2 = c.post("/v1/batches", content=b"{not json", headers={"content-type": "application/json"})
        r3 = c.post("/v1/batches?model=mock", content=b'{"state":"x","questions":{"q":{"type":"noul","instructions":"i"}},"priority":"high"}\n',
                    headers={"content-type": "application/x-ndjson"})
        print("  missing questions ->", r1.status_code, "| invalid JSON ->", r2.status_code, "| ndjson priority='high' ->", r3.status_code)
    k.shutdown()


@check("R7 Laya native batch drops per-job parameters (checkpoint/lang)")
def r7():
    from keziah.adapters.laya import LayaAdapter
    seen = {}
    class FakeRouter:
        def predict(self, state, questions, **kw):
            seen.setdefault("predict", []).append(kw)
            return {"answers": {"q": {"type": "noul", "noul": 0.5}}}
        def predict_batch(self, reqs):
            seen.setdefault("predict_batch", []).append([{k: v for k, v in r.items() if k not in ("state", "questions")} for r in reqs])
            return [{"answers": {"q": {"type": "noul", "noul": 0.5}}} for _ in reqs]
    ad = LayaAdapter("laya")
    ad._router = FakeRouter()
    ad.health_sync = lambda: ModelHealth(ok=True, version='0.3.20')
    gate = threading.Event()
    blocker = MockAdapter("blk", gate=gate)
    st = make_settings(tmp(), extra_models={
        "laya": ModelSettings(adapter="laya", execution="thread", max_concurrency=1,
                              supports_native_batch=True, max_native_batch_size=8)})
    q = {"q": {"type": "noul", "instructions": "i"}}
    with Keziah(settings=st, adapters={"laya": ad}, start_workers=False) as k:
        ids = [k.submit(model="laya", state="a", questions=q, parameters={"checkpoint": "multilingual", "lang": "fr"}),
               k.submit(model="laya", state="b", questions=q, parameters={"checkpoint": "english", "lang": "en"})]
        k.start()
        for i in ids:
            print('  job', k.wait(i, timeout=5).status)
    print("  submitted params: [{checkpoint: multilingual, lang: fr}, {checkpoint: english, lang: en}]")
    print("  what Laya received:", seen)
    del blocker


@check("R8 graceful shutdown charges the attempt and dead-letters last-attempt work")
def r8():
    d = tmp()
    gate = threading.Event()
    st = make_settings(d, mode="disk", max_attempts=1)
    k = Keziah(settings=st, adapters={"mock": MockAdapter("mock", gate=gate)})
    jid = k.submit(model="mock", state="x", questions=QUESTIONS)
    time.sleep(0.3)
    k.shutdown(grace_s=0.2)
    gate.set()
    k2 = Keziah(settings=make_settings(d, mode="disk", max_attempts=1), start_workers=False)
    print("  after a clean SIGTERM-style shutdown, job is:", k2.get(jid).status, k2.get(jid).error)
    k2.shutdown()


@check("R9 queue_ms includes inference time; total double-counts it")
def r9():
    st = make_settings(tmp())
    with Keziah(settings=st, adapters={"mock": MockAdapter("mock", latency_s=0.5)}) as k:
        r = k.wait(k.submit(model="mock", state="x", questions=QUESTIONS), timeout=5)
        print("  mock latency 500ms, idle queue ->", {x: r.timings[x] for x in ("queue_ms", "inference_ms", "total_ms")})


@check("R10 /metrics repeats # TYPE for a histogram family")
def r10():
    st = make_settings(tmp(), extra_models={"mock2": ModelSettings(adapter="mock", max_concurrency=2)})
    with Keziah(settings=st) as k:
        for m in ("mock", "mock2"):
            k.wait(k.submit(model=m, state="x", questions=QUESTIONS), timeout=5)
        text = k.render_metrics()
    types = [ln for ln in text.splitlines() if ln.startswith("# TYPE keziah_queue_latency_seconds")]
    print(f"  '# TYPE keziah_queue_latency_seconds' lines: {len(types)}")


@check("R11 group fallback at execution ignores the fallback model's concurrency cap")
def r11():
    class Toggle(MockAdapter):
        ok = True
        def health_sync(self):
            return ModelHealth(ok=Toggle.ok, permanent=False)
    gate = threading.Event()
    peak = {"now": 0, "max": 0}
    lock = threading.Lock()
    class Counting(MockAdapter):
        async def infer(self, request):
            import asyncio
            with lock:
                peak["now"] += 1
                peak["max"] = max(peak["max"], peak["now"])
            await asyncio.sleep(0.3)
            with lock:
                peak["now"] -= 1
            return await super().infer(request)
    st = make_settings(tmp(), extra_models={
        "prim": ModelSettings(adapter="mock", max_concurrency=6),
        "fb": ModelSettings(adapter="mock", max_concurrency=1)},
        groups={"grp": ModelGroupSettings(primary="prim", fallbacks=["fb"])})
    with Keziah(settings=st, adapters={"prim": Toggle("prim"), "fb": Counting("fb")}, start_workers=False) as k:
        ids = [k.submit(model="grp", state=f"s{i}", questions=QUESTIONS) for i in range(6)]
        Toggle.ok = False  # primary goes down after submit, before execution
        k.start()
        res = [k.wait(i, timeout=10) for i in ids]
    print("  fb max_concurrency=1; peak concurrent calls on fb:", peak["max"],
          "| resolved:", sorted({r.resolved_model for r in res}), sorted({r.status for r in res}))
    del gate


@check("R12 memory backend: per-submit cost grows with retained jobs")
def r12():
    from keziah.clock import Clock
    from keziah.queue.backend import Limits
    from keziah.queue.memory import MemoryBackend
    st = make_settings(tmp())
    st.scheduler.max_queued_jobs = 10**7
    k = Keziah(settings=st, start_workers=False)
    k.backend = MemoryBackend(Clock(), Limits(max_queued_jobs=10**7, max_active_jobs_per_client=10**7))
    def timed(n):
        t0 = time.perf_counter()
        for i in range(n):
            k.submit(model="mock", state=f"{i}", questions=QUESTIONS)
        return (time.perf_counter() - t0) / n * 1e6
    first = timed(1000)
    timed(19000)
    last = timed(1000)
    print(f"  mean submit: first 1k = {first:.0f} us, after 20k queued = {last:.0f} us")


@check("R13 SQLite: per-wake maintenance queries walk the whole queued set")
def r13():
    import sqlite3
    d = tmp()
    k = Keziah(settings=make_settings(d, mode="disk"), start_workers=False)
    k.shutdown()
    conn = sqlite3.connect(str(d / "keziah.db"))
    for label, sql in [
        ("cancel sweep", "SELECT job_id FROM jobs WHERE cancel_requested=1 AND state IN ('queued','retry_wait','staged')"),
        ("deadline sweep", "SELECT job_id FROM jobs WHERE state='queued' AND deadline_at IS NOT NULL AND deadline_at<='x'"),
        ("capacity per client", "SELECT COUNT(*) FROM jobs WHERE client_id='c' AND state IN ('leased','queued','retry_wait','running','staged')"),
    ]:
        plan = " / ".join(r[3] for r in conn.execute("EXPLAIN QUERY PLAN " + sql))
        print(f"  {label}: {plan}")


if __name__ == "__main__":
    only = set(sys.argv[1:])
    for fn in [r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13]:
        if not only or fn.__name__ in only:
            fn()
