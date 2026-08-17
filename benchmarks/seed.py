"""Seed the benchmark Postgres and dump the dataset for the Go server.

Usage: uv run --with asyncpg python benchmarks/seed.py [--rows 10000]
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import pathlib
import random
from datetime import datetime, timedelta, timezone

import asyncpg

DSN = "postgresql://bench:bench@localhost:5433/bench"
T0 = datetime(2026, 8, 1, tzinfo=timezone.utc)

SCHEMA = """
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    price INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);
CREATE INDEX products_updated_at_id ON products (updated_at, id);
"""


def make_rows(count: int) -> list[tuple]:
    rng = random.Random(42)
    rows = []
    for i in range(1, count + 1):
        title = f"product-{i}-{rng.randbytes(8).hex()}"
        price = rng.randint(100, 100_000)
        content_hash = hashlib.sha1(f"{i}:{title}:{price}".encode()).hexdigest()
        updated_at = T0 + timedelta(seconds=i)
        rows.append((i, title, price, content_hash, updated_at, None))
    return rows


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10_000)
    args = parser.parse_args()

    rows = make_rows(args.rows)
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(SCHEMA)
        await conn.copy_records_to_table(
            "products",
            records=rows,
            columns=["id", "title", "price", "content_hash", "updated_at", "deleted_at"],
        )
    finally:
        await conn.close()

    dump = [
        {
            "id": r[0],
            "title": r[1],
            "price": r[2],
            "content_hash": r[3],
            "updated_at": r[4].isoformat(),
            "deleted_at": None,
        }
        for r in rows
    ]
    out = pathlib.Path(__file__).parent / "server" / "data" / "products.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(dump))
    print(f"seeded {len(rows)} rows into postgres and {out}")


if __name__ == "__main__":
    asyncio.run(main())
