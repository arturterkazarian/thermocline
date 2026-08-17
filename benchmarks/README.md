# Benchmark stand

Measures every adapter with and without the cache, against real backends:
Postgres in Docker for the DB adapters and an in-memory Go server for the
HTTP one (so the backend is never the bottleneck).

## Run

```bash
# 1. start postgres
docker compose -f benchmarks/docker-compose.yml up -d db

# 2. seed 10k rows and dump the dataset for the Go server
uv run --with asyncpg python benchmarks/seed.py --rows 10000

# 3. build and start the API server
docker compose -f benchmarks/docker-compose.yml up -d --build api

# 4. run the benchmark
uv run --with asyncpg python benchmarks/bench.py --requests 5000 --concurrency 50
```

## Modes

| Mode | Meaning |
|---|---|
| `nocache` | every `get()` goes straight to the source |
| `strict` | Thermocline, `max_staleness=0` — hash probe on every read |
| `ttl` | Thermocline, `max_staleness=60` — no checks within the run |
| `sync` | Thermocline, `max_staleness=inf` + delta sync — memory only after bootstrap |

The workload is zipf-like (hot keys take most of the traffic) over 10k keys,
`hot_capacity=2000`.

## Sample results

MacBook (8 CPU), Docker via OrbStack, 5000 requests, concurrency 50:

| adapter | mode | ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| sqlalchemy | nocache | 2,255 | 13.01 | 26.28 | 656.79 |
| sqlalchemy | strict | 2,348 | 12.67 | 26.19 | 383.00 |
| sqlalchemy | ttl | 151,664 | 0.00 | 0.00 | 0.00 |
| sqlalchemy | sync | 143,040 | 0.00 | 0.01 | 0.01 |
| tortoise | nocache | 4,511 | 9.97 | 19.00 | 72.02 |
| tortoise | strict | 4,616 | 9.90 | 18.81 | 36.84 |
| tortoise | ttl | 149,482 | 0.00 | 0.00 | 0.00 |
| tortoise | sync | 147,777 | 0.00 | 0.01 | 0.01 |
| aiosql | nocache | 5,261 | 3.56 | 5.00 | 281.04 |
| aiosql | strict | 5,387 | 3.47 | 4.37 | 284.52 |
| aiosql | ttl | 231,757 | 0.00 | 0.00 | 0.00 |
| aiosql | sync | 203,487 | 0.00 | 0.00 | 0.00 |
| httpx | nocache | 345 | 75.54 | 515.37 | 919.68 |
| httpx | strict | 483 | 66.97 | 305.54 | 484.91 |
| httpx | ttl | 111,923 | 0.00 | 0.00 | 0.00 |
| httpx | sync | 203,108 | 0.00 | 0.00 | 0.00 |

Reading the numbers:

* `ttl`/`sync` serve from memory: 30–600× over the direct source; latency
  is microseconds and the throughput ceiling is the Python event loop, not
  the backend — this is the number to optimize.
* `strict` tracks `nocache` closely: the probe still pays a full network
  round trip per read; it buys freshness, not speed.
* `nocache` differences between adapters are the backends' own client
  overheads (ORM hydration for SQLAlchemy, httpx's per-request cost).
