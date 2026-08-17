"""Behavioral tests for the Thermocline facade."""

import asyncio
import logging
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest

from thermocline import (
    JsonSerializer,
    MisconfiguredCacheError,
    Serializer,
    SyncBatch,
    Thermocline,
)
from thermocline.source import CacheSource


@dataclass(frozen=True)
class Item:
    id: int
    payload: str


def make_serializer() -> Serializer[Item]:
    return JsonSerializer(
        to_wire=lambda obj: {"id": obj.id, "payload": obj.payload},
        from_wire=lambda wire: Item(id=wire["id"], payload=wire["payload"]),
    )


class BareSource(CacheSource[int, Item]):
    """Core-only source; every optional capability is absent."""

    def __init__(self, items: dict[int, Item] | None = None) -> None:
        self.items = items or {}
        self.get_calls = 0

    async def get(self, key: int) -> Item | None:
        self.get_calls += 1
        return self.items.get(key)

    def key_of(self, obj: Item) -> int:
        return obj.id

    def hash_of(self, obj: Item) -> str:
        return f"{obj.id}:{obj.payload}"


class ProbeSource(BareSource):
    """Bare source plus the hash-probe capability."""

    def __init__(self, items: dict[int, Item] | None = None) -> None:
        super().__init__(items)
        self.probe_calls = 0
        self.probe_error: Exception | None = None

    async def get_hash(self, key: int) -> str | None:
        self.probe_calls += 1
        if self.probe_error is not None:
            raise self.probe_error
        obj = self.items.get(key)
        return None if obj is None else self.hash_of(obj)


class DeltaSource(ProbeSource):
    """Probe source plus scripted delta batches."""

    def __init__(self, items: dict[int, Item] | None = None) -> None:
        super().__init__(items)
        self.batches: list[SyncBatch[int, Item]] = []
        self.cursors_seen: list[str | None] = []
        self.sync_error: Exception | None = None

    async def load_changed(self, cursor: str | None) -> AsyncIterator[SyncBatch[int, Item]]:
        self.cursors_seen.append(cursor)
        if self.sync_error is not None:
            raise self.sync_error
        for batch in self.batches:
            yield batch
        self.batches = []


class SnapshotSource(ProbeSource):
    """Probe source plus full-snapshot streaming."""

    async def load_all(self) -> AsyncIterator[Item]:
        for item in self.items.values():
            yield item


def lazy_cache(source: CacheSource[int, Item], **kwargs: Any) -> Thermocline[int, Item]:
    kwargs.setdefault("hot_capacity", 100)
    return Thermocline(source, make_serializer(), sync=None, **kwargs)


class TestConfigurationValidation:
    def test_auto_sync_rejects_bare_source(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="sync=None"):
            Thermocline(BareSource(), make_serializer(), hot_capacity=10)

    def test_explicit_delta_requires_capability(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="delta"):
            Thermocline(BareSource(), make_serializer(), hot_capacity=10, sync="delta")

    def test_explicit_snapshot_requires_capability(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="snapshot"):
            Thermocline(BareSource(), make_serializer(), hot_capacity=10, sync="snapshot")

    def test_negative_staleness_is_rejected(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="max_staleness"):
            lazy_cache(BareSource(), max_staleness=-1)

    def test_negative_capacity_is_rejected(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="hot_capacity"):
            lazy_cache(BareSource(), hot_capacity=-1)

    def test_auto_staleness_with_probe_resolves_to_zero(self) -> None:
        assert lazy_cache(ProbeSource()).max_staleness == 0.0

    def test_auto_staleness_without_probe_resolves_to_ttl(self) -> None:
        assert lazy_cache(BareSource()).max_staleness == 300.0

    def test_auto_sync_prefers_delta(self) -> None:
        source = DeltaSource()
        cache = Thermocline(source, make_serializer(), hot_capacity=10)
        assert cache.sync_mode == "delta"

    def test_resolution_is_visible_in_repr(self) -> None:
        cache = lazy_cache(BareSource())
        assert "max_staleness=300.0 (auto)" in repr(cache)


class TestLazyReads:
    async def test_miss_loads_from_source_and_caches(self) -> None:
        source = BareSource({1: Item(1, "a")})
        cache = lazy_cache(source, max_staleness=math.inf)
        assert await cache.get(1) == Item(1, "a")
        assert await cache.get(1) == Item(1, "a")
        assert source.get_calls == 1

    async def test_absence_not_cached_by_default(self) -> None:
        source = BareSource()
        cache = lazy_cache(source, max_staleness=math.inf)
        assert await cache.get(42) is None
        assert await cache.get(42) is None
        assert source.get_calls == 2  # negative caching is opt-in

    async def test_absence_cached_when_enabled(self) -> None:
        source = BareSource()
        cache = lazy_cache(source, max_staleness=math.inf, negative_capacity=10)
        assert await cache.get(42) is None
        assert await cache.get(42) is None
        assert source.get_calls == 1

    async def test_hot_hit_preserves_object_identity(self) -> None:
        source = BareSource({1: Item(1, "a")})
        cache = lazy_cache(source, max_staleness=math.inf)
        first = await cache.get(1)
        assert await cache.get(1) is first

    async def test_strict_freshness_probes_every_read(self) -> None:
        source = ProbeSource({1: Item(1, "a")})
        cache = lazy_cache(source)  # auto resolves to max_staleness=0
        first = await cache.get(1)
        second = await cache.get(1)
        assert second is first  # unchanged hash keeps the hot identity
        assert source.get_calls == 1
        assert source.probe_calls >= 1

    async def test_probe_mismatch_reloads_object(self) -> None:
        source = ProbeSource({1: Item(1, "a")})
        cache = lazy_cache(source)
        assert await cache.get(1) == Item(1, "a")
        source.items[1] = Item(1, "b")
        assert await cache.get(1) == Item(1, "b")
        assert source.get_calls == 2

    async def test_probe_detects_deletion(self) -> None:
        source = ProbeSource({1: Item(1, "a")})
        cache = lazy_cache(source)
        assert await cache.get(1) == Item(1, "a")
        del source.items[1]
        assert await cache.get(1) is None
        assert source.get_calls == 1  # probe alone was enough

    async def test_reload_without_probe_detects_change(self) -> None:
        source = BareSource({1: Item(1, "a")})
        cache = lazy_cache(source, max_staleness=0)
        assert await cache.get(1) == Item(1, "a")
        source.items[1] = Item(1, "b")
        assert await cache.get(1) == Item(1, "b")

    async def test_per_call_override_tightens_freshness(self) -> None:
        source = ProbeSource({1: Item(1, "a")})
        cache = lazy_cache(source, max_staleness=math.inf)
        await cache.get(1)
        source.items[1] = Item(1, "b")
        assert await cache.get(1) == Item(1, "a")  # default: trusts forever
        assert await cache.get(1, max_staleness=0) == Item(1, "b")


class TestStaleGrace:
    async def test_default_propagates_probe_failure(self) -> None:
        source = ProbeSource({1: Item(1, "a")})
        cache = lazy_cache(source)
        await cache.get(1)
        source.probe_error = ConnectionError("db is down")
        with pytest.raises(ConnectionError):
            await cache.get(1)

    async def test_grace_serves_stale_copy(self) -> None:
        source = ProbeSource({1: Item(1, "a")})
        cache = lazy_cache(source, stale_grace=60)
        first = await cache.get(1)
        source.probe_error = ConnectionError("db is down")
        assert await cache.get(1) is first

    async def test_miss_never_uses_grace(self) -> None:
        source = ProbeSource()
        cache = lazy_cache(source, stale_grace=60)

        async def failing_get(key: int) -> Item | None:
            raise ConnectionError("db is down")

        source.get = failing_get  # type: ignore[method-assign]
        with pytest.raises(ConnectionError):
            await cache.get(1)


class TestEvictionAndPinning:
    async def test_eviction_keeps_cold_copy(self) -> None:
        source = BareSource({1: Item(1, "a"), 2: Item(2, "b")})
        cache = lazy_cache(source, hot_capacity=1, max_staleness=math.inf)
        first = await cache.get(1)
        await cache.get(2)  # evicts 1 from hot
        again = await cache.get(1)  # re-deserialized from cold, not from source
        assert again == first
        assert again is not first
        assert source.get_calls == 2

    async def test_pinned_key_survives_eviction_pressure(self) -> None:
        source = BareSource({1: Item(1, "a"), 2: Item(2, "b"), 3: Item(3, "c")})
        cache = lazy_cache(source, hot_capacity=2, max_staleness=math.inf)
        cache.pin(1)
        first = await cache.get(1)
        await cache.get(2)
        await cache.get(3)  # pressure: must evict 2, never 1
        assert await cache.get(1) is first

    async def test_pin_overflow_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        cache = lazy_cache(BareSource(), hot_capacity=1)
        with caplog.at_level(logging.WARNING, logger="thermocline"):
            cache.pin(1)
        assert any("hot_capacity" in message for message in caplog.messages)


class TestDeltaSync:
    async def test_bootstrap_fills_cold_tier(self) -> None:
        source = DeltaSource()
        source.batches = [
            SyncBatch(changed=[Item(1, "a"), Item(2, "b")], deleted=[], cursor="c1"),
        ]
        cache = Thermocline(source, make_serializer(), hot_capacity=10, max_staleness=math.inf)
        async with cache:
            assert await cache.get(1) == Item(1, "a")
            assert await cache.get(2) == Item(2, "b")
        assert source.get_calls == 0

    async def test_sync_applies_updates_and_deletions(self) -> None:
        source = DeltaSource()
        source.batches = [SyncBatch(changed=[Item(1, "a"), Item(2, "b")], deleted=[], cursor="c1")]
        cache = Thermocline(source, make_serializer(), hot_capacity=10, max_staleness=math.inf)
        async with cache:
            assert await cache.get(1) == Item(1, "a")
            source.batches = [SyncBatch(changed=[Item(1, "a2")], deleted=[2], cursor="c2")]
            await cache.sync_now()
            assert await cache.get(1) == Item(1, "a2")
            assert await cache.get(2) is None
        assert source.get_calls == 0
        assert source.cursors_seen == [None, "c1"]

    async def test_unchanged_object_keeps_hot_identity_across_sync(self) -> None:
        source = DeltaSource()
        source.batches = [SyncBatch(changed=[Item(1, "a")], deleted=[], cursor="c1")]
        cache = Thermocline(source, make_serializer(), hot_capacity=10, max_staleness=math.inf)
        async with cache:
            first = await cache.get(1)
            source.batches = [SyncBatch(changed=[Item(1, "a")], deleted=[], cursor="c2")]
            await cache.sync_now()
            assert await cache.get(1) is first

    async def test_background_loop_survives_sync_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        source = DeltaSource()
        cache = Thermocline(source, make_serializer(), hot_capacity=10, sync_interval=0.01)
        async with cache:
            source.sync_error = ConnectionError("db is down")
            with caplog.at_level(logging.WARNING, logger="thermocline"):
                await asyncio.sleep(0.05)
        assert any("background sync failed" in m for m in caplog.messages)

    async def test_sync_now_requires_sync_mode(self) -> None:
        cache = lazy_cache(BareSource())
        with pytest.raises(MisconfiguredCacheError, match="sync"):
            await cache.sync_now()


class TestSnapshotSync:
    async def test_snapshot_swap_drops_absent_keys(self) -> None:
        source = SnapshotSource({1: Item(1, "a"), 2: Item(2, "b")})
        cache = Thermocline(
            source, make_serializer(), hot_capacity=10, sync="snapshot", max_staleness=math.inf
        )
        async with cache:
            assert await cache.get(2) == Item(2, "b")
            del source.items[2]
            source.items[1] = Item(1, "a2")
            await cache.sync_now()
            assert await cache.get(1) == Item(1, "a2")
            assert await cache.get(2) is None  # complete cold tier is authoritative
        assert source.get_calls == 0

    async def test_lifecycle_is_reentrant_and_idempotent(self) -> None:
        source = SnapshotSource({1: Item(1, "a")})
        cache = Thermocline(source, make_serializer(), hot_capacity=10, sync="snapshot")
        await cache.start()
        await cache.start()  # idempotent
        await cache.close()
        await cache.close()  # idempotent


class TestNegativeCache:
    async def test_tombstones_are_bounded(self) -> None:
        source = BareSource()
        cache = lazy_cache(source, max_staleness=math.inf, negative_capacity=5)
        for key in range(100, 120):
            await cache.get(key)
        assert len(cache._tombstones) == 5

    async def test_stale_absence_revalidated_by_probe(self) -> None:
        source = ProbeSource()
        cache = lazy_cache(source, negative_capacity=10)  # auto staleness = 0
        assert await cache.get(42) is None
        assert await cache.get(42) is None
        assert source.get_calls == 1  # the second check was a probe
        assert source.probe_calls == 1

    async def test_object_coming_into_existence_is_loaded(self) -> None:
        source = ProbeSource()
        cache = lazy_cache(source, negative_capacity=10)
        assert await cache.get(1) is None
        source.items[1] = Item(1, "born")
        assert await cache.get(1) == Item(1, "born")

    async def test_absence_grace_serves_none_on_probe_failure(self) -> None:
        source = ProbeSource()
        cache = lazy_cache(source, negative_capacity=10, stale_grace=60)
        assert await cache.get(42) is None
        source.probe_error = ConnectionError("db is down")
        assert await cache.get(42) is None  # stale absence within grace


class TestMemoryLimit:
    async def test_memory_stays_under_limit(self) -> None:
        items = {i: Item(i, "x" * 50) for i in range(1, 21)}
        source = BareSource(items)
        cache = lazy_cache(source, max_staleness=math.inf, memory_limit=1500)
        for key in items:
            await cache.get(key)
        assert cache.memory_bytes <= 1500
        assert len(cache._cold) < len(items)

    async def test_lru_envelope_evicted_and_refetched(self) -> None:
        items = {i: Item(i, "x" * 50) for i in range(1, 11)}
        source = BareSource(items)
        cache = lazy_cache(source, max_staleness=math.inf, memory_limit=800, hot_capacity=0)
        for key in items:
            await cache.get(key)
        calls = source.get_calls
        assert await cache.get(1) == items[1]  # oldest key: evicted, refetched
        assert source.get_calls == calls + 1

    async def test_tombstones_evicted_before_envelopes(self) -> None:
        items = {i: Item(i, "x" * 50) for i in range(1, 4)}
        source = BareSource(items)
        cache = lazy_cache(source, max_staleness=math.inf, memory_limit=900, negative_capacity=10)
        await cache.get(404)
        await cache.get(405)
        assert len(cache._tombstones) == 2
        for key in items:
            await cache.get(key)
        assert len(cache._tombstones) == 0  # tombstones paid for the envelopes
        assert len(cache._cold) == 3

    async def test_hot_copy_falls_with_its_envelope(self) -> None:
        items = {i: Item(i, "x" * 50) for i in range(1, 4)}
        source = BareSource(items)
        cache = lazy_cache(source, max_staleness=math.inf, memory_limit=300)
        for key in items:
            await cache.get(key)  # every entry is hot: rung 3 must fire
        assert cache.memory_bytes <= 300
        assert set(cache._hot) == set(cache._cold)  # inclusivity survived


class TestAuthoritativeAbsence:
    async def test_synced_cache_answers_none_from_memory(self) -> None:
        source = DeltaSource()
        source.batches = [SyncBatch(changed=[Item(1, "a")], deleted=[], cursor="c1")]
        cache = Thermocline(source, make_serializer(), hot_capacity=10, max_staleness=math.inf)
        async with cache:
            assert await cache.get(404) is None
        assert source.get_calls == 0

    async def test_strict_freshness_still_asks_the_source(self) -> None:
        source = DeltaSource()
        source.batches = [SyncBatch(changed=[Item(1, "a")], deleted=[], cursor="c1")]
        cache = Thermocline(source, make_serializer(), hot_capacity=10, max_staleness=0)
        async with cache:
            assert await cache.get(404) is None
        assert source.get_calls == 1

    def test_memory_limit_rejected_with_sync(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="memory_limit"):
            Thermocline(DeltaSource(), make_serializer(), hot_capacity=10, memory_limit=1000)

    def test_negative_capacity_rejected_with_sync(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="negative_capacity"):
            Thermocline(DeltaSource(), make_serializer(), hot_capacity=10, negative_capacity=5)
