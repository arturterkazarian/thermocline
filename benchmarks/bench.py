"""Thermocline benchmark harness: every adapter, with and without the cache.

Usage:
    uv run --with asyncpg --with psutil --with matplotlib --with prometheus-client
        python benchmarks/bench.py [--requests 5000] [--concurrency 50]
        [--adapters ...] [--modes ...] [--seconds 15] [--metrics-port 8099]

Modes per adapter:
    nocache       -- every get() goes straight to the source
    strict_probe  -- max_staleness=0, source HAS get_hash: cheap probe per read
    strict_reload -- max_staleness=0, source has NO probe: full reload + hash compare
    ttl           -- max_staleness=60: no checks within the run
    sync          -- max_staleness=inf + delta sync: memory only after bootstrap

Memory: RSS is sampled every 20 ms for the whole run; the table reports the
peak delta per case and a timeline graph is written to
benchmarks/results/memory.png. RSS shows spikes well but drops lazily —
CPython rarely returns freed pages to the OS immediately.

The workload is a zipf-like key distribution: a small set of hot keys takes
most of the traffic — the shape this cache is built for.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import math
import pathlib
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiosql
import asyncpg
import httpx
import psutil
from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from tortoise import Tortoise, fields
from tortoise.models import Model

from thermocline import JsonSerializer, Thermocline
from thermocline.adapters.aiosql import AiosqlSource
from thermocline.adapters.httpx import HttpxSource
from thermocline.adapters.sqlalchemy import SQLAlchemySource
from thermocline.adapters.tortoise import TortoiseSource
from thermocline.source import CacheSource

PG_SQLALCHEMY = "postgresql+asyncpg://bench:bench@localhost:5433/bench"
PG_TORTOISE = "postgres://bench:bench@localhost:5433/bench"
PG_ASYNCPG = "postgresql://bench:bench@localhost:5433/bench"
API = "http://localhost:8077"
RESULTS = pathlib.Path(__file__).parent / "results"


# -- prometheus metrics (optional) ------------------------------------------
class Metrics:
    """Prometheus exporters: bench progress plus CacheStats deltas."""

    def __init__(self, port: int) -> None:
        from prometheus_client import Counter, Gauge, Histogram, start_http_server

        start_http_server(port)
        labels = ["adapter", "mode"]
        self.requests = Counter("bench_requests_total", "requests completed", labels)
        self.latency = Histogram(
            "bench_request_seconds",
            "request latency",
            labels,
            buckets=(1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
        )
        self.reads = Counter("thermocline_reads_total", "reads by outcome", [*labels, "outcome"])
        self.probes = Counter("thermocline_probes_total", "probes sent", labels)
        self.stale = Counter("thermocline_stale_served_total", "stale serves", labels)
        self.coalesced = Counter("thermocline_coalesced_total", "coalesced reads", labels)
        self.hot = Gauge("thermocline_hot_size", "hot tier size", labels)
        self.cold = Gauge("thermocline_cold_size", "cold tier size", labels)
        self.tombs = Gauge("thermocline_tombstones", "tombstones", labels)
        self.memory = Gauge("thermocline_memory_bytes", "cold tier bytes", labels)
        self.sync_age = Gauge("thermocline_last_sync_age_seconds", "sync age", labels)


class StatsPublisher:
    """Polls cache.stats() and republishes deltas as Prometheus series."""

    def __init__(self, metrics: Metrics, cache: Any, adapter: str, mode: str) -> None:
        self._metrics = metrics
        self._cache = cache
        self._labels = (adapter, mode)
        self._prev = cache.stats()
        self._task: asyncio.Task[None] | None = None

    def _publish(self) -> None:
        m, (adapter, mode) = self._metrics, self._labels
        stats, prev = self._cache.stats(), self._prev
        for outcome, now_v, prev_v in (
            ("hot", stats.hot_hits, prev.hot_hits),
            ("cold", stats.cold_hits, prev.cold_hits),
            ("absent", stats.absent_served, prev.absent_served),
            ("source", stats.source_loads, prev.source_loads),
            ("coalesced", stats.coalesced, prev.coalesced),
        ):
            m.reads.labels(adapter, mode, outcome).inc(now_v - prev_v)
        m.probes.labels(adapter, mode).inc(stats.probes - prev.probes)
        m.stale.labels(adapter, mode).inc(stats.stale_served - prev.stale_served)
        m.coalesced.labels(adapter, mode).inc(stats.coalesced - prev.coalesced)
        m.hot.labels(adapter, mode).set(stats.hot_size)
        m.cold.labels(adapter, mode).set(stats.cold_size)
        m.tombs.labels(adapter, mode).set(stats.tombstones)
        m.memory.labels(adapter, mode).set(stats.memory_bytes)
        if stats.last_sync_age is not None:
            m.sync_age.labels(adapter, mode).set(stats.last_sync_age)
        self._prev = stats

    async def _loop(self) -> None:
        while True:
            self._publish()
            await asyncio.sleep(0.2)

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._publish()


# -- sqlalchemy mapping ----------------------------------------------------
class Base(DeclarativeBase):
    pass


class SARow(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(Text)
    price: Mapped[int] = mapped_column(Integer)
    content_hash: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# -- tortoise mapping ------------------------------------------------------
class TRow(Model):
    id = fields.IntField(primary_key=True)
    title = fields.TextField()
    price = fields.IntField()
    content_hash = fields.TextField()
    updated_at = fields.DatetimeField()
    deleted_at = fields.DatetimeField(null=True)

    class Meta:
        table = "products"


Q_GET_ONE = """
-- name: get_one(key)^
SELECT id, title, price, content_hash, updated_at, deleted_at FROM products
WHERE id = :key AND deleted_at IS NULL;
"""

Q_GET_HASH = """
-- name: get_hash(key)$
SELECT content_hash FROM products WHERE id = :key AND deleted_at IS NULL;
"""

Q_CHANGED = """
-- name: load_changed(updated_at, key, limit, offset)
SELECT id, title, price, content_hash, updated_at, deleted_at FROM products
WHERE CAST(:updated_at AS timestamptz) IS NULL
   OR (updated_at, id) > (CAST(:updated_at AS timestamptz), CAST(:key AS integer))
ORDER BY updated_at, id LIMIT :limit OFFSET :offset;
"""


def sa_wire(row: SARow) -> dict[str, Any]:
    return {
        "id": row.id,
        "title": row.title,
        "price": row.price,
        "content_hash": row.content_hash,
        "updated_at": row.updated_at.isoformat(),
    }


def sa_unwire(w: dict[str, Any]) -> SARow:
    return SARow(
        id=w["id"],
        title=w["title"],
        price=w["price"],
        content_hash=w["content_hash"],
        updated_at=datetime.fromisoformat(w["updated_at"]),
    )


async def make_sqlalchemy(probe: bool) -> tuple[CacheSource[int, Any], Any, Any]:
    engine = create_async_engine(PG_SQLALCHEMY, pool_size=50, max_overflow=10)
    hash_cfg: dict[str, Any] = (
        {"hash": SARow.content_hash} if probe else {"hash_fn": lambda r: r.content_hash}
    )
    source: SQLAlchemySource[int, SARow] = SQLAlchemySource(
        async_sessionmaker(engine, expire_on_commit=False),
        model=SARow,
        updated_at=SARow.updated_at,
        deleted_at=SARow.deleted_at,
        **hash_cfg,
    )
    serializer = JsonSerializer(to_wire=sa_wire, from_wire=sa_unwire)
    return source, serializer, engine.dispose


async def make_tortoise(probe: bool) -> tuple[CacheSource[int, Any], Any, Any]:
    await Tortoise.init(db_url=PG_TORTOISE, modules={"models": ["__main__"]})
    hash_cfg: dict[str, Any] = (
        {"hash_field": "content_hash"} if probe else {"hash_fn": lambda r: r.content_hash}
    )
    source: TortoiseSource[int, TRow] = TortoiseSource(
        TRow,
        updated_at_field="updated_at",
        deleted_at_field="deleted_at",
        **hash_cfg,
    )
    serializer = JsonSerializer(
        to_wire=lambda r: {
            "id": r.id,
            "title": r.title,
            "price": r.price,
            "content_hash": r.content_hash,
            "updated_at": r.updated_at.isoformat(),
        },
        from_wire=lambda w: TRow(
            id=w["id"],
            title=w["title"],
            price=w["price"],
            content_hash=w["content_hash"],
            updated_at=datetime.fromisoformat(w["updated_at"]),
        ),
    )
    return source, serializer, Tortoise.close_connections


async def make_aiosql(probe: bool) -> tuple[CacheSource[int, Any], Any, Any]:
    pool = await asyncpg.create_pool(PG_ASYNCPG, min_size=10, max_size=50)
    sql = Q_GET_ONE + (Q_GET_HASH if probe else "") + Q_CHANGED
    queries = aiosql.from_str(sql, "asyncpg")
    source: AiosqlSource[int, dict[str, Any]] = AiosqlSource(
        queries,
        pool,
        hash_field="content_hash",
        from_row=lambda row: dict(row) | {"updated_at": row["updated_at"].isoformat()},
    )
    serializer: JsonSerializer[dict[str, Any]] = JsonSerializer()
    return source, serializer, pool.close


async def make_httpx(probe: bool) -> tuple[CacheSource[int, Any], Any, Any]:
    client = httpx.AsyncClient(base_url=API, limits=httpx.Limits(max_connections=100), timeout=10.0)
    source: HttpxSource[int, dict[str, Any]] = HttpxSource(
        client,
        "/products/{key}",
        hash_url="/products/{key}/hash" if probe else None,
        changed_url="/products/changed",
        hash_field="content_hash",
        items_field="items",
    )
    serializer: JsonSerializer[dict[str, Any]] = JsonSerializer()
    return source, serializer, client.aclose


BACKENDS = {
    "sqlalchemy": make_sqlalchemy,
    "tortoise": make_tortoise,
    "aiosql": make_aiosql,
    "httpx": make_httpx,
}

MODE_CACHE_CONFIG: dict[str, dict[str, Any]] = {
    "strict_probe": {"sync": None, "max_staleness": 0},
    "strict_reload": {"sync": None, "max_staleness": 0},
    "ttl": {"sync": None, "max_staleness": 60},
    "sync": {"sync": "delta", "max_staleness": math.inf},
}


def make_workload(n_keys: int, n_requests: int) -> list[int]:
    rng = random.Random(7)
    weights = [1 / (rank**1.1) for rank in range(1, n_keys + 1)]
    keys = list(range(1, n_keys + 1))
    return rng.choices(keys, weights=weights, k=n_requests)


# -- memory sampling -------------------------------------------------------
class MemorySampler:
    """Samples process RSS on the event loop; marks case boundaries."""

    def __init__(self, interval: float = 0.02) -> None:
        self.interval = interval
        self.samples: list[tuple[float, int]] = []
        self.marks: list[tuple[float, str]] = []
        self._task: asyncio.Task[None] | None = None
        self._process = psutil.Process()

    async def _loop(self) -> None:
        while True:
            self.samples.append((time.perf_counter(), self._process.memory_info().rss))
            await asyncio.sleep(self.interval)

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def mark(self, label: str) -> float:
        now = time.perf_counter()
        self.marks.append((now, label))
        return now

    def peak_delta_mb(self, t0: float, t1: float) -> float:
        window = [rss for t, rss in self.samples if t0 <= t <= t1]
        if not window:
            return 0.0
        return (max(window) - window[0]) / 1_000_000


def plot(sampler: MemorySampler, path: pathlib.Path) -> bool:
    try:
        import matplotlib
    except ModuleNotFoundError:
        return False
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    t0 = sampler.samples[0][0]
    xs = [t - t0 for t, _ in sampler.samples]
    ys = [rss / 1_000_000 for _, rss in sampler.samples]
    fig, ax = plt.subplots(figsize=(16, 6))
    ax.plot(xs, ys, linewidth=1.2, color="#37729E")
    floor = min(ys)
    ax.fill_between(xs, ys, floor, color="#37729E", alpha=0.15)
    ax.set_ylim(floor - (max(ys) - floor) * 0.05, max(ys) * 1.02)
    top = max(ys)
    for t, label in sampler.marks:
        x = t - t0
        ax.axvline(x, color="#13303A", alpha=0.25, linewidth=0.8)
        ax.text(x, top, " " + label, rotation=90, va="top", ha="left", fontsize=7, color="#13303A")
    ax.set_xlabel("seconds")
    ax.set_ylabel("RSS, MB")
    ax.set_title("thermocline benchmark: process memory over time")
    ax.margins(x=0.01)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    return True


@dataclass
class Result:
    ops: float
    p50: float
    p95: float
    p99: float
    mem_mb: float


async def measure(
    target: Any,
    workload: list[int],
    concurrency: int,
    seconds: float | None = None,
    bench_counter: Any = None,
    bench_latency: Any = None,
) -> tuple[float, ...]:
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def one(key: int) -> None:
        async with semaphore:
            t0 = time.perf_counter()
            await target(key)
            dt = time.perf_counter() - t0
            latencies.append(dt)
            if bench_counter is not None:
                bench_counter.inc()
                bench_latency.observe(dt)

    t0 = time.perf_counter()
    await asyncio.gather(*(one(k) for k in workload))
    while seconds is not None and time.perf_counter() - t0 < seconds:
        await asyncio.gather(*(one(k) for k in workload))
    wall = time.perf_counter() - t0
    latencies.sort()

    def pct(p: float) -> float:
        return latencies[min(len(latencies) - 1, int(p * len(latencies)))] * 1000

    return (len(latencies) / wall, pct(0.50), pct(0.95), pct(0.99))


async def run_case(
    name: str,
    mode: str,
    workload: list[int],
    concurrency: int,
    sampler: MemorySampler,
    seconds: float | None = None,
    metrics: Metrics | None = None,
) -> Result:
    probe = mode != "strict_reload"
    source, serializer, aclose = await BACKENDS[name](probe)
    bench_counter = metrics.requests.labels(name, mode) if metrics else None
    bench_latency = metrics.latency.labels(name, mode) if metrics else None
    publisher: StatsPublisher | None = None
    try:
        case_start = sampler.mark(f"{name}/{mode}")
        if mode == "nocache":
            stats = await measure(
                source.get, workload, concurrency, seconds, bench_counter, bench_latency
            )
        else:
            cache = Thermocline(source, serializer, hot_capacity=2_000, **MODE_CACHE_CONFIG[mode])
            if metrics is not None:
                publisher = StatsPublisher(metrics, cache, name, mode)
                publisher.start()
            async with cache:
                if mode != "sync":  # lazy modes: warm up so we measure steady state
                    for key in set(workload):
                        await cache.get(key)
                stats = await measure(
                    cache.get, workload, concurrency, seconds, bench_counter, bench_latency
                )
        case_end = time.perf_counter()
    finally:
        if publisher is not None:
            await publisher.stop()
        await aclose()
    gc.collect()
    await asyncio.sleep(0.1)  # let RSS settle before the next case
    return Result(*stats, mem_mb=sampler.peak_delta_mb(case_start, case_end))


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=5_000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--keys", type=int, default=10_000)
    parser.add_argument("--adapters", default="sqlalchemy,tortoise,aiosql,httpx")
    parser.add_argument("--modes", default="nocache,strict_probe,strict_reload,ttl,sync")
    parser.add_argument(
        "--seconds",
        type=float,
        default=None,
        help="hold each case under load for N seconds (for watching live metrics)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=0,
        help="expose Prometheus metrics on this port (0 = off)",
    )
    args = parser.parse_args()

    metrics = Metrics(args.metrics_port) if args.metrics_port else None
    workload = make_workload(args.keys, args.requests)
    sampler = MemorySampler()
    sampler.start()

    all_results: dict[tuple[str, str], Result] = {}
    for name in args.adapters.split(","):
        print(f"[{name}]")
        for mode in args.modes.split(","):
            result = await run_case(
                name, mode, workload, args.concurrency, sampler, args.seconds, metrics
            )
            all_results[(name, mode)] = result
            print(
                f"  {mode}: {result.ops:,.0f} ops/s  p50={result.p50:.2f}ms "
                f"p99={result.p99:.2f}ms  peak +{result.mem_mb:.1f}MB"
            )

    await sampler.stop()

    print("\n| adapter | mode | ops/s | p50 ms | p95 ms | p99 ms | peak ΔRSS MB |")
    print("|---|---|---|---|---|---|---|")
    for (name, mode), r in all_results.items():
        print(
            f"| {name} | {mode} | {r.ops:,.0f} | {r.p50:.2f} | {r.p95:.2f} "
            f"| {r.p99:.2f} | {r.mem_mb:.1f} |"
        )

    if plot(sampler, RESULTS / "memory.png"):
        print(f"\nmemory timeline: {RESULTS / 'memory.png'}")
    else:
        print("\nmatplotlib not available: run with --with matplotlib for the graph")


if __name__ == "__main__":
    asyncio.run(main())
