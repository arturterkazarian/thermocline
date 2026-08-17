"""Thermocline benchmark harness: every adapter, with and without the cache.

Usage:
    uv run --with asyncpg python benchmarks/bench.py [--requests 5000]
        [--concurrency 50] [--keys 10000] [--adapters sqlalchemy,tortoise,aiosql,httpx]

Modes per adapter:
    nocache  -- every get() goes straight to the source
    strict   -- Thermocline, max_staleness=0 (hash probe on every read)
    ttl      -- Thermocline, max_staleness=60 (no checks within the run)
    sync     -- Thermocline, max_staleness=inf + delta sync (bootstrap, then memory only)

The workload is a zipf-like key distribution: a small set of hot keys takes
most of the traffic — the shape this cache is built for.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiosql
import asyncpg
import httpx
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


AIOSQL_SQL = """
-- name: get_one(key)^
SELECT id, title, price, content_hash, updated_at, deleted_at FROM products
WHERE id = :key AND deleted_at IS NULL;

-- name: get_hash(key)$
SELECT content_hash FROM products WHERE id = :key AND deleted_at IS NULL;

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


async def make_sqlalchemy() -> tuple[CacheSource[int, Any], Any, Any]:
    engine = create_async_engine(PG_SQLALCHEMY, pool_size=50, max_overflow=10)
    source: SQLAlchemySource[int, SARow] = SQLAlchemySource(
        async_sessionmaker(engine, expire_on_commit=False),
        model=SARow,
        hash=SARow.content_hash,
        updated_at=SARow.updated_at,
        deleted_at=SARow.deleted_at,
    )
    serializer = JsonSerializer(to_wire=sa_wire, from_wire=sa_unwire)
    return source, serializer, engine.dispose


async def make_tortoise() -> tuple[CacheSource[int, Any], Any, Any]:
    await Tortoise.init(db_url=PG_TORTOISE, modules={"models": ["__main__"]})
    source: TortoiseSource[int, TRow] = TortoiseSource(
        TRow,
        hash_field="content_hash",
        updated_at_field="updated_at",
        deleted_at_field="deleted_at",
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


async def make_aiosql() -> tuple[CacheSource[int, Any], Any, Any]:
    pool = await asyncpg.create_pool(PG_ASYNCPG, min_size=10, max_size=50)
    queries = aiosql.from_str(AIOSQL_SQL, "asyncpg")
    source: AiosqlSource[int, dict[str, Any]] = AiosqlSource(
        queries,
        pool,
        hash_field="content_hash",
        from_row=lambda row: dict(row) | {"updated_at": row["updated_at"].isoformat()},
    )
    serializer: JsonSerializer[dict[str, Any]] = JsonSerializer()
    return source, serializer, pool.close


async def make_httpx() -> tuple[CacheSource[int, Any], Any, Any]:
    client = httpx.AsyncClient(base_url=API, limits=httpx.Limits(max_connections=100), timeout=10.0)
    source: HttpxSource[int, dict[str, Any]] = HttpxSource(
        client,
        "/products/{key}",
        hash_url="/products/{key}/hash",
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


def make_workload(n_keys: int, n_requests: int) -> list[int]:
    rng = random.Random(7)
    weights = [1 / (rank**1.1) for rank in range(1, n_keys + 1)]
    keys = list(range(1, n_keys + 1))
    return rng.choices(keys, weights=weights, k=n_requests)


@dataclass
class Result:
    ops: float
    p50: float
    p95: float
    p99: float


async def measure(target: Any, workload: list[int], concurrency: int) -> Result:
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def one(key: int) -> None:
        async with semaphore:
            t0 = time.perf_counter()
            await target(key)
            latencies.append(time.perf_counter() - t0)

    t0 = time.perf_counter()
    await asyncio.gather(*(one(k) for k in workload))
    wall = time.perf_counter() - t0
    latencies.sort()

    def pct(p: float) -> float:
        return latencies[min(len(latencies) - 1, int(p * len(latencies)))] * 1000

    return Result(ops=len(workload) / wall, p50=pct(0.50), p95=pct(0.95), p99=pct(0.99))


async def run_backend(
    name: str, workload: list[int], concurrency: int, modes: list[str]
) -> dict[str, Result]:
    results: dict[str, Result] = {}
    for mode in modes:
        source, serializer, aclose = await BACKENDS[name]()
        try:
            if mode == "nocache":
                results[mode] = await measure(source.get, workload, concurrency)
            else:
                config: dict[str, Any] = {
                    "strict": {"sync": None, "max_staleness": 0},
                    "ttl": {"sync": None, "max_staleness": 60},
                    "sync": {"sync": "delta", "max_staleness": math.inf},
                }[mode]
                cache = Thermocline(source, serializer, hot_capacity=2_000, **config)
                async with cache:
                    if mode != "sync":  # lazy modes: warm up so we measure steady state
                        for key in set(workload):
                            await cache.get(key)
                    results[mode] = await measure(cache.get, workload, concurrency)
        finally:
            await aclose()
        print(
            f"  {name}/{mode}: {results[mode].ops:,.0f} ops/s  "
            f"p50={results[mode].p50:.2f}ms p99={results[mode].p99:.2f}ms"
        )
    return results


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=5_000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--keys", type=int, default=10_000)
    parser.add_argument("--adapters", default="sqlalchemy,tortoise,aiosql,httpx")
    parser.add_argument("--modes", default="nocache,strict,ttl,sync")
    args = parser.parse_args()

    workload = make_workload(args.keys, args.requests)
    modes = args.modes.split(",")
    all_results: dict[str, dict[str, Result]] = {}
    for name in args.adapters.split(","):
        print(f"[{name}]")
        all_results[name] = await run_backend(name, workload, args.concurrency, modes)

    print("\n| adapter | mode | ops/s | p50 ms | p95 ms | p99 ms |")
    print("|---|---|---|---|---|---|")
    for name, per_mode in all_results.items():
        for mode, r in per_mode.items():
            print(f"| {name} | {mode} | {r.ops:,.0f} | {r.p50:.2f} | {r.p95:.2f} | {r.p99:.2f} |")


if __name__ == "__main__":
    asyncio.run(main())
