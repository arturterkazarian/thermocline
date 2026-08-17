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
uv run --with asyncpg --with psutil --with matplotlib \
    python benchmarks/bench.py --requests 5000 --concurrency 50
```

## Modes

| Mode | Meaning |
|---|---|
| `nocache` | every `get()` goes straight to the source |
| `strict_probe` | `max_staleness=0`, source **has** `get_hash` — cheap probe on every read |
| `strict_reload` | `max_staleness=0`, source has **no** probe — full reload + hash compare |
| `ttl` | `max_staleness=60` — no checks within the run |
| `sync` | `max_staleness=inf` + delta sync — memory only after bootstrap |

The workload is zipf-like (hot keys take most of the traffic) over 10k keys,
`hot_capacity=2000`.

## Sample results

MacBook (8 CPU), Docker via OrbStack, 5000 requests, concurrency 50:

| adapter | mode | ops/s | p50 ms | p95 ms | p99 ms | peak ΔRSS MB |
|---|---|---|---|---|---|---|
| sqlalchemy | nocache | 2,143 | 13.31 | 26.29 | 730.53 | 15.9 |
| sqlalchemy | strict_probe | 2,230 | 12.69 | 28.59 | 358.83 | 4.2 |
| sqlalchemy | strict_reload | 2,249 | 13.99 | 16.95 | 608.84 | 1.6 |
| sqlalchemy | ttl | 240,190 | 0.00 | 0.00 | 0.00 | 0.0 |
| sqlalchemy | sync | 143,750 | 0.00 | 0.01 | 0.01 | 0.8 |
| tortoise | nocache | 4,492 | 9.91 | 19.12 | 69.06 | 0.1 |
| tortoise | strict_probe | 4,692 | 9.84 | 18.81 | 62.32 | 0.0 |
| tortoise | strict_reload | 4,614 | 10.03 | 19.14 | 61.69 | 0.0 |
| tortoise | ttl | 232,576 | 0.00 | 0.00 | 0.00 | 0.0 |
| tortoise | sync | 145,782 | 0.00 | 0.01 | 0.01 | 0.1 |
| aiosql | nocache | 5,326 | 3.48 | 4.35 | 482.35 | 0.0 |
| aiosql | strict_probe | 5,447 | 3.31 | 4.21 | 236.17 | 1.0 |
| aiosql | strict_reload | 5,243 | 3.67 | 4.73 | 305.05 | 0.8 |
| aiosql | ttl | 231,292 | 0.00 | 0.00 | 0.00 | 0.2 |
| aiosql | sync | 204,092 | 0.00 | 0.00 | 0.00 | 0.6 |
| httpx | nocache | 349 | 75.27 | 475.45 | 910.81 | 0.5 |
| httpx | strict_probe | 476 | 68.45 | 296.14 | 492.97 | 2.2 |
| httpx | strict_reload | 484 | 64.00 | 316.22 | 534.57 | 0.1 |
| httpx | ttl | 110,566 | 0.00 | 0.00 | 0.00 | 0.0 |
| httpx | sync | 185,864 | 0.00 | 0.00 | 0.00 | 1.8 |

Reading the numbers:

* `ttl`/`sync` serve from memory: 20–600x over the direct source; latency is
  microseconds and the throughput ceiling is the Python event loop, not the
  backend — this is the number to optimize.
* both strict modes track `nocache` closely — a revalidation still pays a
  full network round trip. `strict_probe` vs `strict_reload` isolates what
  the cheap hash query buys over refetching the object: visible in p95/p99
  and in transfer volume, not in ops/s on this small payload.
* `nocache` differences between adapters are the backends' own client
  overheads (ORM hydration for SQLAlchemy, httpx's per-request cost).

## Memory timeline

Process RSS is sampled every 20 ms; each case boundary is marked. The graph
lands in `benchmarks/results/memory.png` (generated, gitignored). RSS shows
spikes well but drops lazily — CPython rarely returns freed pages to the OS
right away, so read the spikes, not the plateaus.
