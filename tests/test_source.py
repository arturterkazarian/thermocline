"""Contract-level tests for the source module."""

import dataclasses
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from thermocline import (
    CacheSource,
    SupportsDelta,
    SupportsHashProbe,
    SupportsSnapshot,
    SyncBatch,
)


@dataclass(frozen=True)
class Item:
    id: int
    payload: str


class MinimalSource(CacheSource[int, Item]):
    """Bare-core source: implements nothing beyond the mandatory methods."""

    def __init__(self, items: dict[int, Item] | None = None) -> None:
        self._items = items or {}

    async def get(self, key: int) -> Item | None:
        return self._items.get(key)

    def key_of(self, obj: Item) -> int:
        return obj.id

    def hash_of(self, obj: Item) -> str:
        return f"{obj.id}:{obj.payload}"


class FullSource(MinimalSource):
    """Source implementing every optional capability."""

    async def get_hash(self, key: int) -> str | None:
        obj = self._items.get(key)
        return None if obj is None else self.hash_of(obj)

    async def load_all(self) -> AsyncIterator[Item]:
        for item in self._items.values():
            yield item

    async def load_changed(self, cursor: str | None) -> AsyncIterator[SyncBatch[int, Item]]:
        yield SyncBatch(changed=list(self._items.values()), deleted=[], cursor="full")


class TestCore:
    async def test_get_returns_object(self) -> None:
        item = Item(id=1, payload="a")
        assert await MinimalSource({1: item}).get(1) is item

    async def test_get_returns_none_for_missing_key(self) -> None:
        assert await MinimalSource().get(42) is None

    def test_key_of_matches_get_key(self) -> None:
        item = Item(id=7, payload="b")
        assert MinimalSource().key_of(item) == 7

    def test_hash_of_is_deterministic(self) -> None:
        source = MinimalSource()
        a, b = Item(id=1, payload="x"), Item(id=1, payload="x")
        assert source.hash_of(a) == source.hash_of(b)

    def test_incomplete_source_cannot_be_instantiated(self) -> None:
        class Incomplete(CacheSource[int, Item]):
            async def get(self, key: int) -> Item | None:
                return None

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]


class TestCapabilityDetection:
    def test_minimal_source_has_no_capabilities(self) -> None:
        source = MinimalSource()
        assert not isinstance(source, SupportsHashProbe)
        assert not isinstance(source, SupportsSnapshot)
        assert not isinstance(source, SupportsDelta)

    def test_full_source_has_all_capabilities(self) -> None:
        source = FullSource()
        assert isinstance(source, SupportsHashProbe)
        assert isinstance(source, SupportsSnapshot)
        assert isinstance(source, SupportsDelta)

    async def test_hash_probe_returns_none_for_missing_key(self) -> None:
        assert await FullSource().get_hash(42) is None

    async def test_load_all_streams_every_object(self) -> None:
        item = Item(id=1, payload="a")
        source = FullSource({1: item})
        assert [obj async for obj in source.load_all()] == [item]

    async def test_load_changed_streams_batches(self) -> None:
        item = Item(id=1, payload="a")
        source = FullSource({1: item})
        batches = [batch async for batch in source.load_changed(None)]
        assert len(batches) == 1
        assert batches[0].changed == [item]
        assert batches[0].cursor == "full"


class TestSyncBatch:
    def test_is_frozen(self) -> None:
        batch: SyncBatch[int, Item] = SyncBatch(changed=[], deleted=[], cursor="c")
        with pytest.raises(dataclasses.FrozenInstanceError):
            batch.cursor = "x"  # type: ignore[misc]

    def test_holds_fields(self) -> None:
        item = Item(id=1, payload="a")
        batch = SyncBatch(changed=[item], deleted=[2], cursor="c")
        assert batch.changed == [item]
        assert batch.deleted == [2]
        assert batch.cursor == "c"
