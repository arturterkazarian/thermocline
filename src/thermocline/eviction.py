"""Eviction strategies for the hot tier.

A policy sees only keys and access events — never the objects themselves.
Eviction is cheap by construction: the hot tier is a subset of the cold one,
so evicting is a plain ``del``; the compact copy stays where it was.

Pinned keys are handled by the cache itself and are never admitted into a
policy, so implementations do not need to know pinning exists.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Generic, Protocol, TypeVar

K = TypeVar("K")

__all__ = [
    "EvictionPolicy",
    "LruEviction",
]


class EvictionPolicy(Protocol[K]):
    """Strategy deciding which hot entry to evict when the tier is full.

    Contract for implementations:

    * The cache calls :meth:`pick_victim` only when at least one key is
      tracked; returning a key the policy does not track is a bug.
    * Every tracked key was previously reported via :meth:`on_admit` and
      not yet withdrawn via :meth:`forget`.
    * All calls happen on one event loop; implementations need no locking.
    """

    def on_admit(self, key: K) -> None:
        """Track a key that has just entered the hot tier."""
        ...

    def on_access(self, key: K) -> None:
        """Register a read of a tracked key."""
        ...

    def pick_victim(self) -> K:
        """Return the tracked key that should be evicted next."""
        ...

    def forget(self, key: K) -> None:
        """Stop tracking a key (evicted, invalidated, or pinned)."""
        ...


class LruEviction(Generic[K]):
    """Least-recently-used policy: evict what has not been read the longest."""

    def __init__(self) -> None:
        self._order: OrderedDict[K, None] = OrderedDict()

    def on_admit(self, key: K) -> None:
        """Track a key as the most recently used one."""
        self._order[key] = None
        self._order.move_to_end(key)

    def on_access(self, key: K) -> None:
        """Move a tracked key to the most-recently-used position."""
        if key in self._order:
            self._order.move_to_end(key)

    def pick_victim(self) -> K:
        """Return the least recently used tracked key."""
        return next(iter(self._order))

    def forget(self, key: K) -> None:
        """Stop tracking a key; unknown keys are ignored."""
        self._order.pop(key, None)
