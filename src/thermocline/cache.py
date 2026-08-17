"""Two-tier read-through cache facade.

The cache keeps one dataset in two forms. The cold tier holds every known
object as a compact envelope ``(hash, payload)``; the hot tier holds a small
subset of them as live deserialized objects. Tiers are inclusive: the hot
tier is a subset of the cold one, so evicting a hot object is a plain
``del`` and a hot miss is always served from the cold tier without
coordination.

Freshness is one dial, ``max_staleness``: how long a copy may be trusted
since we last *knew* it was current (loaded it, probed it, or a background
sync confirmed it). ``0`` checks on every read, a finite value is a TTL
gate, ``inf`` — explicit opt-in only — never checks. The checking mechanism
is chosen automatically from the source's capabilities: a cheap hash probe
when available, a full reload otherwise. ``auto`` resolves to the safest
mode available and its resolution is visible via properties and ``repr``.

Availability is a second, separate dial: ``stale_grace`` is the extra
staleness budget allowed when the source is unreachable. The default of
``0`` propagates the failure.

A cache instance belongs to one event loop; no method is thread-safe.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Generic, Literal, TypeVar, cast

from thermocline.errors import MisconfiguredCacheError
from thermocline.eviction import EvictionPolicy, LruEviction
from thermocline.serializer import Serializer
from thermocline.source import (
    CacheSource,
    SupportsDelta,
    SupportsHashProbe,
    SupportsSnapshot,
    SyncBatch,
)

K = TypeVar("K")
T = TypeVar("T")

logger = logging.getLogger("thermocline")

_AUTO_STALENESS_WITHOUT_PROBE = 300.0
_ENTRY_OVERHEAD = 200  # rough per-envelope bookkeeping cost, bytes
_TOMBSTONE_OVERHEAD = 100  # rough per-tombstone bookkeeping cost, bytes


def _as_seconds(value: float | timedelta, name: str) -> float:
    seconds = value.total_seconds() if isinstance(value, timedelta) else float(value)
    if math.isnan(seconds) or seconds < 0:
        raise MisconfiguredCacheError(f"{name} must be a non-negative number of seconds")
    return seconds


@dataclass(slots=True)
class _Envelope:
    """One live cold-tier entry; absences are tracked separately."""

    hash: str
    payload: bytes
    checked_at: float


class Thermocline(Generic[K, T]):
    """Tiered read-through cache over a :class:`CacheSource`.

    The constructor only validates configuration; nothing touches the event
    loop until :meth:`start`. Use it as an async context manager::

        cache = Thermocline(source, serializer, hot_capacity=10_000)
        async with cache:
            obj = await cache.get(key)

    Args:
        source: The system of record. Its capabilities determine which sync
            modes and freshness mechanisms are available.
        serializer: Codec between live objects and cold-tier payloads.
        hot_capacity: Maximum number of live objects kept deserialized,
            pinned ones included. The main performance dial.
        max_staleness: Seconds a copy may be served without checking.
            ``"auto"`` (default) resolves to ``0`` when the source supports
            hash probes and to 300 otherwise; ``math.inf`` (never check) is
            accepted only explicitly.
        stale_grace: Extra staleness budget, in seconds, allowed when a
            freshness check fails because the source is unreachable. ``0``
            (default) propagates the failure to the caller.
        sync: Background synchronization mode. ``"auto"`` picks ``"delta"``
            when the source supports it, else ``"snapshot"``, and refuses a
            source that supports neither — pass ``None`` explicitly to run
            without background sync.
        sync_interval: Seconds between background sync cycles.
        eviction: Hot-tier eviction policy; LRU by default.
        memory_limit: Byte budget for the cold tier — payload bytes plus a
            small per-entry overhead; the measurable part of the cache. On
            overflow the cache evicts tombstones first, then the least
            recently used envelopes; an envelope with a hot copy goes last
            and takes the hot copy with it. ``None`` (default) means
            unbounded. Lazy mode only: with background sync the cold tier
            holds the full dataset by design.
        negative_capacity: How many confirmed absences (tombstones) to
            remember. ``0`` (default) disables negative caching: every miss
            goes to the source. Lazy mode only — a synced cold tier is
            authoritative for absence and needs no tombstones.

    Raises:
        MisconfiguredCacheError: If a requested mode needs a capability the
            source lacks, or a value is out of its domain.
    """

    def __init__(
        self,
        source: CacheSource[K, T],
        serializer: Serializer[T],
        *,
        hot_capacity: int,
        max_staleness: float | timedelta | Literal["auto"] = "auto",
        stale_grace: float | timedelta = 0.0,
        sync: Literal["auto", "delta", "snapshot"] | None = "auto",
        sync_interval: float | timedelta = 60.0,
        eviction: EvictionPolicy[K] | None = None,
        memory_limit: int | None = None,
        negative_capacity: int = 0,
    ) -> None:
        if hot_capacity < 0:
            raise MisconfiguredCacheError("hot_capacity must be non-negative")
        self._source = source
        self._serializer = serializer
        self._hot_capacity = hot_capacity

        self._probe: Callable[[K], Awaitable[str | None]] | None = (
            source.get_hash if isinstance(source, SupportsHashProbe) else None
        )

        self._staleness_auto = max_staleness == "auto"
        if isinstance(max_staleness, str):
            if max_staleness != "auto":
                raise MisconfiguredCacheError(f"unknown max_staleness: {max_staleness!r}")
            self._max_staleness = 0.0 if self._probe is not None else _AUTO_STALENESS_WITHOUT_PROBE
        else:
            self._max_staleness = _as_seconds(max_staleness, "max_staleness")
        self._stale_grace = _as_seconds(stale_grace, "stale_grace")

        self._sync_auto = sync == "auto"
        self._sync_mode: str | None
        if sync == "auto":
            if isinstance(source, SupportsDelta):
                self._sync_mode = "delta"
            elif isinstance(source, SupportsSnapshot):
                self._sync_mode = "snapshot"
            else:
                raise MisconfiguredCacheError(
                    f"{type(source).__name__} supports neither delta nor snapshot sync; "
                    "implement load_changed() or load_all(), "
                    "or opt out explicitly with sync=None"
                )
        elif sync == "delta":
            if not isinstance(source, SupportsDelta):
                raise MisconfiguredCacheError(
                    f"{type(source).__name__} does not support delta sync: "
                    "implement load_changed() or switch to snapshot mode"
                )
            self._sync_mode = "delta"
        elif sync == "snapshot":
            if not isinstance(source, SupportsSnapshot):
                raise MisconfiguredCacheError(
                    f"{type(source).__name__} does not support snapshot sync: "
                    "implement load_all() or switch to delta mode"
                )
            self._sync_mode = "snapshot"
        elif sync is None:
            self._sync_mode = None
        else:
            raise MisconfiguredCacheError(f"unknown sync mode: {sync!r}")
        self._sync_interval = _as_seconds(sync_interval, "sync_interval")
        if self._sync_mode is not None and self._sync_interval <= 0:
            raise MisconfiguredCacheError("sync_interval must be positive")

        if negative_capacity < 0:
            raise MisconfiguredCacheError("negative_capacity must be non-negative")
        if memory_limit is not None and memory_limit <= 0:
            raise MisconfiguredCacheError("memory_limit must be positive")
        if self._sync_mode is not None:
            if memory_limit is not None:
                raise MisconfiguredCacheError(
                    "memory_limit applies to the lazy mode only: with background sync "
                    "the cold tier holds the full dataset by design"
                )
            if negative_capacity:
                raise MisconfiguredCacheError(
                    "negative_capacity applies to the lazy mode only: a synced cold "
                    "tier is authoritative for absence and needs no tombstones"
                )
        self._memory_limit = memory_limit
        self._negative_capacity = negative_capacity

        self._evictor: EvictionPolicy[K] = eviction if eviction is not None else LruEviction()
        self._cold: OrderedDict[K, _Envelope] = OrderedDict()
        self._tombstones: OrderedDict[K, float] = OrderedDict()
        self._cold_bytes = 0
        self._last_sync: float | None = None
        self._hot: dict[K, T] = {}
        self._pinned: set[K] = set()
        self._cursor: str | None = None
        self._task: asyncio.Task[None] | None = None
        logger.debug("configured %r", self)

    def __repr__(self) -> str:
        """Show the resolved configuration and tier sizes."""
        sync = f"{self._sync_mode!r} (auto)" if self._sync_auto else repr(self._sync_mode)
        staleness = (
            f"{self._max_staleness} (auto)" if self._staleness_auto else str(self._max_staleness)
        )
        return (
            f"Thermocline(sync={sync}, max_staleness={staleness}, "
            f"stale_grace={self._stale_grace}, "
            f"hot={len(self._hot)}/{self._hot_capacity}, cold={len(self._cold)})"
        )

    @property
    def sync_mode(self) -> str | None:
        """Resolved background sync mode: ``"delta"``, ``"snapshot"`` or ``None``."""
        return self._sync_mode

    @property
    def max_staleness(self) -> float:
        """Resolved default freshness limit, in seconds."""
        return self._max_staleness

    @property
    def stale_grace(self) -> float:
        """Extra staleness budget allowed while the source is unreachable."""
        return self._stale_grace

    @property
    def memory_bytes(self) -> int:
        """Estimated cold-tier footprint: payload bytes plus bookkeeping overhead."""
        return self._cold_bytes

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Bootstrap the cold tier and launch the background sync task.

        Returns only when the initial load is complete: after ``start()``
        the cold tier holds the full dataset (in a sync mode). A no-op when
        ``sync=None`` or when already started.
        """
        if self._sync_mode is None or self._task is not None:
            return
        await self.sync_now()
        self._task = asyncio.create_task(self._sync_loop())

    async def close(self) -> None:
        """Stop the background sync task. Idempotent."""
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def __aenter__(self) -> Thermocline[K, T]:
        """Start the cache and return it."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop the background sync task."""
        await self.close()

    # -- reading -----------------------------------------------------------

    async def get(self, key: K, *, max_staleness: float | timedelta | None = None) -> T | None:
        """Return the object for ``key``, or ``None`` if it does not exist.

        Served from the hot tier when possible, else deserialized from the
        cold tier, else loaded from the source. The copy is revalidated
        first when it is older than the freshness limit.

        Args:
            key: The object's key.
            max_staleness: Per-call override of the cache-wide freshness
                limit — freshness is a property of the read, not only of
                the object.
        """
        limit = (
            self._max_staleness
            if max_staleness is None
            else _as_seconds(max_staleness, "max_staleness")
        )
        envelope = self._cold.get(key)
        if envelope is None:
            return await self._absent(key, limit)
        if time.monotonic() - envelope.checked_at <= limit:
            return self._serve(key, envelope)
        return await self._revalidate(key, envelope, limit)

    def pin(self, key: K) -> None:
        """Exempt a key from eviction; pins consume ``hot_capacity``."""
        self._pinned.add(key)
        self._evictor.forget(key)
        if len(self._pinned) >= self._hot_capacity:
            logger.warning(
                "pinned keys (%d) reach hot_capacity (%d); no room left for unpinned entries",
                len(self._pinned),
                self._hot_capacity,
            )

    def unpin(self, key: K) -> None:
        """Return a key to normal eviction rules."""
        self._pinned.discard(key)
        if key in self._hot:
            self._evictor.on_admit(key)

    # -- synchronization ---------------------------------------------------

    async def sync_now(self) -> None:
        """Run one synchronization cycle immediately.

        Raises:
            MisconfiguredCacheError: If the cache was configured with
                ``sync=None``.
        """
        if self._sync_mode is None:
            raise MisconfiguredCacheError("background sync is disabled (sync=None)")
        if self._sync_mode == "delta":
            await self._sync_delta()
        else:
            await self._sync_snapshot()
        self._last_sync = time.monotonic()

    async def _sync_loop(self) -> None:
        while True:
            await asyncio.sleep(self._sync_interval)
            try:
                await self.sync_now()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("background sync failed; keeping last good state", exc_info=True)

    async def _sync_delta(self) -> None:
        source = cast(SupportsDelta[K, T], self._source)
        async for batch in source.load_changed(self._cursor):
            self._apply_batch(batch)

    def _apply_batch(self, batch: SyncBatch[K, T]) -> None:
        for obj in batch.changed:
            self._upsert(obj)
        for key in batch.deleted:
            self._delete_entry(key)
        self._cursor = batch.cursor

    async def _sync_snapshot(self) -> None:
        source = cast(SupportsSnapshot[T], self._source)
        old = self._cold
        fresh: OrderedDict[K, _Envelope] = OrderedDict()
        fresh_bytes = 0
        async for obj in source.load_all():
            key = self._source.key_of(obj)
            new_hash = self._source.hash_of(obj)
            previous = old.get(key)
            if previous is not None and previous.hash == new_hash:
                previous.checked_at = time.monotonic()
                fresh[key] = previous
            else:
                fresh[key] = _Envelope(new_hash, self._serializer.encode(obj), time.monotonic())
            fresh_bytes += len(fresh[key].payload) + _ENTRY_OVERHEAD
        self._cold = fresh  # atomic swap: readers never see a half-built tier
        self._cold_bytes = fresh_bytes
        for key in list(self._hot):
            if fresh.get(key) is not old.get(key):
                self._drop_hot(key)

    def _upsert(self, obj: T) -> None:
        key = self._source.key_of(obj)
        new_hash = self._source.hash_of(obj)
        envelope = self._cold.get(key)
        if envelope is not None and envelope.hash == new_hash:
            envelope.checked_at = time.monotonic()
            return
        if envelope is not None:
            self._cold_bytes -= len(envelope.payload) + _ENTRY_OVERHEAD
        payload = self._serializer.encode(obj)
        self._cold[key] = _Envelope(new_hash, payload, time.monotonic())
        self._cold_bytes += len(payload) + _ENTRY_OVERHEAD
        self._drop_hot(key)

    # -- internals ---------------------------------------------------------

    def _serve(self, key: K, envelope: _Envelope) -> T | None:
        self._cold.move_to_end(key)
        obj = self._hot.get(key)
        if obj is not None:
            if key not in self._pinned:
                self._evictor.on_access(key)
            return obj
        obj = self._serializer.decode(envelope.payload)
        self._admit(key, obj)
        return obj

    def _admit(self, key: K, obj: T) -> None:
        if key in self._hot:
            self._hot[key] = obj
            if key not in self._pinned:
                self._evictor.on_access(key)
            return
        if self._hot_capacity == 0:
            return
        evictable = len(self._hot) - len(self._pinned & self._hot.keys())
        while len(self._hot) >= self._hot_capacity and evictable > 0:
            victim = self._evictor.pick_victim()
            self._evictor.forget(victim)
            self._hot.pop(victim, None)
            evictable -= 1
        if len(self._hot) >= self._hot_capacity:
            return  # every resident is pinned; nothing to evict
        self._hot[key] = obj
        if key not in self._pinned:
            self._evictor.on_admit(key)

    def _drop_hot(self, key: K) -> None:
        self._hot.pop(key, None)
        self._evictor.forget(key)

    def _delete_entry(self, key: K) -> None:
        self._drop_hot(key)
        self._evict_envelope(key)
        self._remember_absent(key)

    def _evict_envelope(self, key: K) -> None:
        envelope = self._cold.pop(key, None)
        if envelope is not None:
            self._cold_bytes -= len(envelope.payload) + _ENTRY_OVERHEAD

    def _remember_absent(self, key: K) -> None:
        if self._sync_mode is not None or self._negative_capacity == 0:
            return
        if key not in self._tombstones:
            self._cold_bytes += _TOMBSTONE_OVERHEAD
        self._tombstones[key] = time.monotonic()
        self._tombstones.move_to_end(key)
        while len(self._tombstones) > self._negative_capacity:
            self._tombstones.popitem(last=False)
            self._cold_bytes -= _TOMBSTONE_OVERHEAD
        self._shrink()

    def _drop_tombstone(self, key: K) -> None:
        if self._tombstones.pop(key, None) is not None:
            self._cold_bytes -= _TOMBSTONE_OVERHEAD

    def _shrink(self) -> None:
        """Enforce memory_limit: tombstones first, then LRU envelopes."""
        if self._memory_limit is None:
            return
        while self._cold_bytes > self._memory_limit:
            if self._tombstones:
                self._tombstones.popitem(last=False)
                self._cold_bytes -= _TOMBSTONE_OVERHEAD
                continue
            victim = next((k for k in self._cold if k not in self._hot), None)
            if victim is None:
                victim = next(iter(self._cold), None)
                if victim is None:
                    return
                self._drop_hot(victim)  # the envelope goes: the hot copy cannot stay
            self._evict_envelope(victim)

    async def _absent(self, key: K, limit: float) -> T | None:
        if self._sync_mode is not None:
            # a complete cold tier is authoritative: absence is as fresh as the last sync
            if self._last_sync is not None and time.monotonic() - self._last_sync <= limit:
                return None
            return await self._load_miss(key)
        checked_at = self._tombstones.get(key)
        if checked_at is None:
            return await self._load_miss(key)
        if time.monotonic() - checked_at <= limit:
            self._tombstones.move_to_end(key)
            return None
        return await self._revalidate_absent(key, checked_at, limit)

    async def _revalidate_absent(self, key: K, checked_at: float, limit: float) -> T | None:
        if self._probe is not None:
            try:
                current = await self._probe(key)
            except asyncio.CancelledError:
                raise
            except Exception:
                if time.monotonic() - checked_at <= limit + self._stale_grace:
                    logger.debug("serving stale absence %r: probe failed within grace", key)
                    return None
                raise
            if current is None:
                self._remember_absent(key)
                return None
            return await self._load_miss(key)  # the object came into existence
        try:
            obj = await self._source.get(key)
        except asyncio.CancelledError:
            raise
        except Exception:
            if time.monotonic() - checked_at <= limit + self._stale_grace:
                logger.debug("serving stale absence %r: reload failed within grace", key)
                return None
            raise
        if obj is None:
            self._remember_absent(key)
            return None
        self._store(key, obj)
        return obj

    async def _load_miss(self, key: K) -> T | None:
        obj = await self._source.get(key)  # no cached copy: errors propagate
        if obj is None:
            self._remember_absent(key)
            return None
        self._store(key, obj)
        return obj

    def _store(self, key: K, obj: T) -> None:
        self._drop_tombstone(key)
        self._evict_envelope(key)  # replacing: release the old payload's bytes
        payload = self._serializer.encode(obj)
        self._cold[key] = _Envelope(self._source.hash_of(obj), payload, time.monotonic())
        self._cold_bytes += len(payload) + _ENTRY_OVERHEAD
        self._admit(key, obj)  # admit first: shrink must see the entry as hot
        self._shrink()

    def _within_grace(self, envelope: _Envelope, limit: float) -> bool:
        return time.monotonic() - envelope.checked_at <= limit + self._stale_grace

    async def _revalidate(self, key: K, envelope: _Envelope, limit: float) -> T | None:
        if self._probe is None:
            return await self._reload(key, envelope, limit)
        try:
            current = await self._probe(key)
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._within_grace(envelope, limit):
                logger.debug("serving stale %r: freshness probe failed within grace", key)
                return self._serve(key, envelope)
            raise
        if current is None:
            self._delete_entry(key)
            return None
        if current == envelope.hash:
            envelope.checked_at = time.monotonic()
            return self._serve(key, envelope)
        return await self._reload(key, envelope, limit)

    async def _reload(self, key: K, envelope: _Envelope, limit: float) -> T | None:
        try:
            obj = await self._source.get(key)
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._within_grace(envelope, limit):
                logger.debug("serving stale %r: reload failed within grace", key)
                return self._serve(key, envelope)
            raise
        if obj is None:
            self._delete_entry(key)
            return None
        if self._source.hash_of(obj) == envelope.hash:
            envelope.checked_at = time.monotonic()
            return self._serve(key, envelope)  # unchanged: keep the hot identity
        self._store(key, obj)
        return obj
