"""Source contract: how the cache talks to the system of record.

The cache assumes nothing about the backing store. A source implements the
small mandatory core (:class:`CacheSource`); everything else is an optional
capability that unlocks more efficient cache behavior:

* :class:`SupportsHashProbe` — inexpensive freshness checks instead of full reloads.
* :class:`SupportsSnapshot` — timer-driven full reloads of the cold tier.
* :class:`SupportsDelta` — incremental sync: fetch only what changed.

Capabilities are structural (:class:`typing.Protocol`): implementing the
method on a source is enough, no extra inheritance required. The cache
validates the configured sync mode against the source's capabilities at
construction time and fails fast if something is missing - there are no
silent fallbacks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar, runtime_checkable

K = TypeVar("K")
T = TypeVar("T")
K_contra = TypeVar("K_contra", contravariant=True)
T_co = TypeVar("T_co", covariant=True)

__all__ = [
    "CacheSource",
    "SupportsDelta",
    "SupportsHashProbe",
    "SupportsSnapshot",
    "SyncBatch",
]


@dataclass(frozen=True)
class SyncBatch(Generic[K, T]):
    """One increment of synchronization produced by a source.

    Attributes:
        changed: Objects created or updated since the previous cursor.
            Live objects only — deleted ones never appear here.
        deleted: Keys of objects deleted since the previous cursor. A key
            is all the cache needs to drop an entry from both tiers.
        cursor: Opaque marker of the position right after this batch. Valid
            as a resume point for :meth:`SupportsDelta.load_changed`. Only
            the source that produced it knows what is inside.
    """

    changed: Sequence[T]
    deleted: Sequence[K]
    cursor: str


class CacheSource(ABC, Generic[K, T]):
    """Mandatory core every source must implement.

    Deliberately minimal: any store that can look up an object by key can
    back the cache. No service columns, timestamps, or stored hashes are
    required here — those belong to the optional capability protocols.
    """

    @abstractmethod
    async def get(self, key: K) -> T | None:
        """Load the full object by key, or ``None`` if it does not exist.

        Soft-deleted objects count as nonexistent and must yield ``None``.
        """

    @abstractmethod
    def key_of(self, obj: T) -> K:
        """Return the cache key of a loaded object.

        Must be consistent with :meth:`get`: for any object this source
        returns, ``get(key_of(obj))`` addresses the same logical object.
        """

    @abstractmethod
    def hash_of(self, obj: T) -> str:
        """Return a deterministic hash of the object's state.

        "Return", not necessarily "compute": a source that stores a
        maintained hash next to the data (e.g. a trigger-updated column
        loaded together with the object) just reads it back — that is the
        preferred implementation. Actual computation is the fallback for
        sources that store no hash.

        The same logical state must always produce the same string across
        processes, runs and workers — otherwise peers disagree on freshness
        and trigger spurious invalidations. Watch out for dict key order,
        float formatting and datetime serialization when implementing.
        """


@runtime_checkable
class SupportsHashProbe(Protocol[K_contra]):
    """Capability: answer freshness probes without shipping the object."""

    async def get_hash(self, key: K_contra) -> str | None:
        """Return the current hash of the object, or ``None`` if it is gone.

        Must return ``None`` for soft-deleted objects, exactly as for ones
        that never existed — this is how the lazy freshness path detects
        deletions. Returning a hash of a deleted row would keep its cached
        copy alive forever.

        The returned value must match what :meth:`CacheSource.hash_of`
        yields for the same object state.
        """
        ...


@runtime_checkable
class SupportsSnapshot(Protocol[T_co]):
    """Capability: enumerate the full live dataset."""

    def load_all(self) -> AsyncIterator[T_co]:
        """Stream every live object.

        Deleted objects are simply absent, which is why snapshot mode needs
        no deletion markers at all: whatever is missing from the snapshot no
        longer exists.

        Streaming keeps the peak memory at one object regardless of dataset
        size: the cache compacts each object into the cold tier and releases
        it before pulling the next one. How the stream is produced —
        server-side cursor, keyset pagination, page tokens — is entirely the
        source's business.
        """
        ...


@runtime_checkable
class SupportsDelta(Protocol[K, T]):
    """Capability: fetch only what changed since a known position."""

    def load_changed(self, cursor: str | None) -> AsyncIterator[SyncBatch[K, T]]:
        """Stream everything that changed at or after ``cursor``, in pages.

        ``None`` means "from the beginning": the stream must cover the full
        live dataset. A routine delta is typically a single small batch; a
        bootstrap or a catch-up after long downtime arrives as many pages,
        keeping the peak memory at one page.

        Contract for implementations:

        * The boundary is inclusive. When in doubt, re-send an object: a
          duplicate is a harmless idempotent upsert, a missed change is a
          permanent cache divergence.
        * Every batch carries the cursor of its own last position, and each
          such cursor must be a valid resume point: if the consumer stops
          after applying a batch and later calls ``load_changed`` with that
          batch's cursor, no changes may be lost.
        * A cursor must account for deleted rows as well (e.g., take
          ``max(updated_at)`` over the whole page, deletions included).
          Otherwise, every subsequent delta re-sends the tail after the
          newest deletion.
        * The cache treats cursors as opaque tokens and never inspects
          them — their format is entirely up to the source.
        """
        ...
