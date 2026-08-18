# Changelog

## 0.1.0b1 — 2026-08-18

First public beta.

### Core
- `CacheSource` contract: a three-method core (`get` / `key_of` / `hash_of`)
  plus structural capabilities — `SupportsHashProbe` (cheap freshness
  probes), `SupportsSnapshot` (streaming full reloads), `SupportsDelta`
  (paged incremental sync over an opaque source-owned cursor).
- Two-tier inclusive cache (`Thermocline`): hot deserialized objects over a
  compact cold tier of `(key, hash, payload)` envelopes.
- Freshness as one dial: `max_staleness` (0 / N seconds / inf), auto-resolved
  to the safest mechanism the source supports, overridable per call.
- Availability dial: `stale_grace` — bounded staleness while the source is
  unreachable; failures propagate by default.
- Background sync: delta or snapshot modes, blocking bootstrap, a
  crash-surviving sync loop; a synced cold tier is authoritative for absence.
- Single-flight: concurrent misses and revalidations of one key coalesce
  into one source operation; a cancelled leader does not abort the flight.
- Memory bounding in lazy mode: `memory_limit` (exact cold-tier byte budget)
  and `negative_capacity` (opt-in bounded negative cache, default off).
- Hot-tier eviction: pluggable `EvictionPolicy`, LRU built in, pinning.
- Observability: `cache.stats()` snapshot — outcome counters with a strict
  one-outcome-per-read invariant, probe/stale/coalesced counters, sync
  health, size gauges.

### Adapters
- SQLAlchemy 2.x async (columns declare capabilities), Tortoise ORM
  (validated field names), aiosql (queries declare capabilities), httpx
  (URLs declare capabilities), Pydantic v2 serializer. Zero mandatory
  dependencies: every adapter lives behind its own extra.

### Tooling
- CI across Python 3.10–3.14 (uv, ruff, mypy strict, pytest).
- Benchmark stand: Postgres + in-memory Go API, five modes per adapter,
  RSS tracking, optional Prometheus/Grafana live metrics.
