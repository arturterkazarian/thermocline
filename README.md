# thermocline

[![PyPI](https://img.shields.io/pypi/v/thermocline)](https://pypi.org/project/thermocline/)
[![CI](https://github.com/arturterkazarian/thermocline/actions/workflows/ci.yml/badge.svg)](https://github.com/arturterkazarian/thermocline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%E2%80%933.14-blue)](https://github.com/arturterkazarian/thermocline/blob/main/pyproject.toml)
[![License](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)

A memory-efficient, tiered read-through cache for read-heavy Python services — hot objects over a compact cold tier, backed by any source.

Named after the ocean layer that separates warm surface water from the cold depths — the same boundary this library draws between hot deserialized objects and their compact cold copies.

## Why

Read-heavy services often hold reference data — product catalogs, pricing plans, feature flags — that is read constantly, changes rarely, and is edited by *another* application. Keeping it all as live Python objects can cost gigabytes; hitting the database on every read costs milliseconds. Thermocline keeps the full dataset in memory in a compact binary form (the **cold tier**) and only the working set as live objects (the **hot tier**), with the source of record as the last resort:

<img src="docs/assets/01-read-path.gif" alt="The read path: hot, then cold, then the database" width="400">

- **Hot tier** — a small LRU-bounded set of live, ready-to-return objects.
- **Cold tier** — *every* known object as a compact envelope `(key, hash, payload)`.
- **Source** — your database or API, reached only on misses and invalidations.

The tiers are inclusive: the hot tier is a subset of the cold one, so eviction is just a `del`, and a hot miss never needs the database.

## Quickstart

```python
from dataclasses import dataclass

from thermocline import CacheSource, JsonSerializer, Thermocline


@dataclass(frozen=True)
class Product:
    id: int
    price: int


class ProductSource(CacheSource[int, Product]):
    async def get(self, key: int) -> Product | None: ...  # load from your store
    def key_of(self, obj: Product) -> int: ...  # return obj.id
    def hash_of(self, obj: Product) -> str: ...  # cheap version hash


serializer = JsonSerializer(
    to_wire=lambda p: {"id": p.id, "price": p.price},
    from_wire=lambda w: Product(id=w["id"], price=w["price"]),
)

cache = Thermocline(ProductSource(), serializer, hot_capacity=10_000, sync=None)
async with cache:
    product = await cache.get(42)  # None if it does not exist
```

The core contract is three methods; everything else is an optional capability that unlocks more efficient behavior.

## Source capabilities

| Capability | Method | Unlocks |
|---|---|---|
| *(core, required)* | `get`, `key_of`, `hash_of` | Read-through caching over anything that can look up by key |
| `SupportsHashProbe` | `get_hash(key)` | Cheap freshness checks without shipping the object |
| `SupportsSnapshot` | `load_all()` | Timer-driven full reloads of the cold tier |
| `SupportsDelta` | `load_changed(cursor)` | Incremental sync: fetch only what changed |

Capabilities are structural — implement the method and the cache detects it. The requested mode is validated against the source's capabilities at construction: nothing degrades silently.

## Adapters

Ready-made sources for the popular backends, each behind its own extra. All follow one rule: **what you declared is what you get** — every declared column, field, query, or URL unlocks the matching capability, and nothing is guessed from naming conventions.

| Adapter | Extra | Declare capabilities with |
|---|---|---|
| `SQLAlchemySource` | `thermocline[sqlalchemy]` | Mapped columns (`hash=Model.content_hash`) |
| `TortoiseSource` | `thermocline[tortoise]` | Field names, validated at startup |
| `AiosqlSource` | `thermocline[aiosql]` | Named queries in your `.sql` file |
| `HttpxSource` | `thermocline[httpx]` | Endpoint URLs |
| `PydanticSerializer` | `thermocline[pydantic]` | — (cold-tier codec, pairs with any source) |

### SQLAlchemy

Columns are declared as columns, not strings — a typo is caught by your IDE, not in production. The key defaults to the mapped primary key.

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from thermocline.adapters.sqlalchemy import SQLAlchemySource

engine = create_async_engine("postgresql+asyncpg://app@db/catalog")
source = SQLAlchemySource(
    async_sessionmaker(engine, expire_on_commit=False),
    model=ProductRow,
    hash=ProductRow.content_hash,  # unlocks cheap freshness probes
    updated_at=ProductRow.updated_at,  # unlocks incremental delta sync
    deleted_at=ProductRow.deleted_at,  # soft deletes flow into the delta
)
```

### Tortoise ORM

Same pattern with field names; every name is validated against the model at construction. The key defaults to the model's primary key.

```python
from thermocline.adapters.tortoise import TortoiseSource

source = TortoiseSource(
    Product,
    hash_field="content_hash",
    updated_at_field="updated_at",
    deleted_at_field="deleted_at",
)
```

### aiosql — raw SQL

The queries you wrote are the capabilities you get; the SQL — including pagination and soft-delete filtering — belongs entirely to you. `get_one` is required, the rest are optional.

```sql
-- name: get_one(key)^
SELECT id, title, content_hash FROM products WHERE id = :key AND deleted_at IS NULL;

-- name: get_hash(key)$
SELECT content_hash FROM products WHERE id = :key AND deleted_at IS NULL;

-- name: load_changed(updated_at, key, limit, offset)
SELECT id, title, content_hash, updated_at, deleted_at FROM products
WHERE :updated_at IS NULL OR (updated_at, id) > (:updated_at, :key)
ORDER BY updated_at, id LIMIT :limit OFFSET :offset;
```

```python
import aiosql
from thermocline.adapters.aiosql import AiosqlSource

queries = aiosql.from_path("products.sql", "asyncpg")
source = AiosqlSource(queries, pool, hash_field="content_hash")
```

### httpx — any REST API

The URLs you passed are the capabilities you get. Your `httpx.AsyncClient` owns auth, base URL, and retries; `404` means "does not exist", any other error propagates untouched.

```python
import httpx
from thermocline.adapters.httpx import HttpxSource

client = httpx.AsyncClient(base_url="https://catalog.internal", auth=token_auth)
source = HttpxSource(
    client,
    get_url="/products/{key}",
    hash_url="/products/{key}/hash",
    changed_url="/products/changed",
    hash_field="content_hash",
    items_field="items",
)
```

### Putting it together

The adapters compose: SQLAlchemy rows on the bottom, Pydantic models for your code, MessagePack in the cold tier.

```python
from thermocline import Thermocline
from thermocline.adapters.pydantic import PydanticSerializer

source = SQLAlchemySource(
    sessions,
    model=ProductRow,
    hash=ProductRow.content_hash,
    updated_at=ProductRow.updated_at,
    deleted_at=ProductRow.deleted_at,
    to_obj=lambda row: Product.model_validate(row, from_attributes=True),
)
cache = Thermocline(source, PydanticSerializer(Product), hot_capacity=10_000)

async with cache:  # bootstrap: the cold tier fills once
    product = await cache.get(42)  # then reads live above the thermocline
```

## Freshness: one dial

`max_staleness` is how long a copy may be served without checking, in seconds. The checking mechanism is picked automatically: a cheap hash probe when the source supports it, a full reload otherwise.

| Setting | Behavior | Use for |
|---|---|---|
| `0` | Verify on **every** read | Prices, permissions — anything where staleness costs money |
| `N` seconds | TTL gate: trust for `N` seconds, then verify once | Most reference data |
| `math.inf` | Never verify (explicit opt-in only) | Insert-only / immutable data |

The default is `"auto"`: `0` with a hash probe available, `300` without one. `auto` never picks `inf` — turning verification off is a decision you make explicitly.

Strict mode in action — an update and a deletion, both caught by a probe:

<img src="docs/assets/04-freshness-probe.gif" alt="Hash probe catches an update and a deletion" width="400">

The TTL gate — reads inside the window pay nothing, staleness stays bounded:

<img src="docs/assets/05-freshness-ttl.gif" alt="TTL gate: trust for five seconds, then verify once" width="400">

Freshness is a property of the read, not only of the object — any call may tighten it:

```python
await cache.get(key)  # cache-wide default
await cache.get(key, max_staleness=0)  # this read must be exact
```

### When the source is down

`stale_grace` is the extra staleness budget allowed while the source is unreachable. Default `0`: the failure propagates. Set a grace window to trade freshness for availability during outages — explicitly, like everything else.

## Background sync

| Mode | Needs | Behavior |
|---|---|---|
| `"delta"` | `SupportsDelta` | Periodically fetch only changes, driven by an opaque source-owned cursor |
| `"snapshot"` | `SupportsSnapshot` | Periodically re-stream the full dataset, atomically swap the cold tier |
| `None` | — | Lazy only: the cache fills as keys are read |

`sync="auto"` prefers delta, falls back to snapshot, and refuses a source that supports neither — pass `None` explicitly for a lazy-only cache. `start()` performs the initial load, so after it returns the cold tier holds the full dataset — which makes it **authoritative for absence**: a miss answers `None` straight from memory, as fresh as the last sync, without touching the source.

<img src="docs/assets/06-background-sync.gif" alt="Background delta sync: a task pulls only the changed rows every interval" width="400">

## Eviction and pinning

The hot tier is bounded by `hot_capacity` — a count of live objects, the main performance dial. LRU by default; the policy is pluggable (`EvictionPolicy`). Eviction only drops the live object — the compact copy stays in the cold tier:

<img src="docs/assets/02-eviction-lru.gif" alt="LRU eviction under a tight hot limit" width="400">

Pinned keys are never evicted, but they consume the hot budget:

<img src="docs/assets/03-pinned.gif" alt="Pinned keys survive eviction pressure" width="400">

```python
cache.pin(hot_key)  # e.g. today's featured product
cache.unpin(hot_key)
```

### Bounding memory in lazy mode

Without background sync the cold tier fills on demand, so it gets its own dials:

```python
cache = Thermocline(
    source,
    serializer,
    sync=None,
    hot_capacity=10_000,  # live objects, counted
    memory_limit=256_000_000,  # cold-tier bytes, measured exactly
    negative_capacity=10_000,  # confirmed absences to remember (default 0: off)
)
```

`memory_limit` bounds what is honestly measurable — the compact payloads. On overflow the cache evicts tombstones first, then the least recently used envelopes; an envelope with a hot copy goes last and takes it along. `negative_capacity` opts into negative caching: absences obey the same freshness rules as objects, and a bounded budget means a scan of random keys cannot grow memory without limit. Both dials are lazy-mode only — a synced cold tier holds the full dataset by design and needs neither.

## Observability

`cache.stats()` returns a point-in-time snapshot — cumulative counters plus size gauges, O(1), no dependencies. Feed it to whatever metrics system you use:

```python
stats = cache.stats()
stats.hit_rate  # share of reads answered from memory
stats.hot_hits, stats.cold_hits, stats.source_loads, stats.absent_served
stats.coalesced  # reads that joined another read's in-flight source call
stats.probes  # freshness probes sent
stats.stale_served  # reads served beyond max_staleness while the source was down
stats.sync_runs, stats.sync_failures, stats.last_sync_age
stats.hot_size, stats.cold_size, stats.tombstones, stats.pinned, stats.memory_bytes
```

Every read that returns counts toward exactly one outcome (`hot_hits` / `cold_hits` / `absent_served` / `source_loads`); `probes` and `stale_served` count on top. A growing `stale_served` is the visible trace of degradation under `stale_grace`; `last_sync_age` answers "how far behind am I" in the sync modes.

## Design principles

- **The cache never amplifies load.** Concurrent misses and revalidations of one key coalesce into a single source operation (single-flight, always on): a cold start under traffic sends one query per key, not one per reader.
- **No silent fallbacks.** Misconfiguration fails at construction with `MisconfiguredCacheError`; `auto` resolutions are visible in `repr(cache)` and properties.
- **Your errors stay yours.** A failing source raises its own exception through the cache, unwrapped.
- **Deletions can't be ignored.** Sync delivers them as keys, probes report them as `None` — a deleted object never masquerades as a live one.
- **Model deactivation as state, deletion as hygiene.** If business logic depends on an object disappearing, flip a status field — the hash catches it instantly; deletion is for data that is truly gone.
- **asyncio-only (v0.1).** A cache instance belongs to one event loop; no locks on the read path.

## Installation

```bash
pip install thermocline
```

Optional extras: `thermocline[msgpack]` for the compact cold-tier codec; `thermocline[sqlalchemy]`, `thermocline[tortoise]`, `thermocline[aiosql]`, `thermocline[httpx]`, `thermocline[pydantic]` for the adapters.

## Status

Beta. The source contract, serializers, the two-tier cache facade, five adapters (SQLAlchemy, Tortoise ORM, aiosql, httpx, Pydantic), single-flight stampede protection, memory bounding, and `stats()` observability are implemented and tested across Python 3.10–3.14, with a reproducible benchmark stand in [`benchmarks/`](benchmarks/). The public API may still change before 1.0. On the roadmap: batch reads (`get_many`), a raw-payload fast path for bulk sync, push-based invalidation, and thread-safety beyond asyncio.

## License

Apache-2.0 — see [LICENSE](LICENSE).
