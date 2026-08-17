"""Behavioral tests for the Tortoise ORM source adapter, on SQLite."""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import pytest
from tortoise import Tortoise, fields
from tortoise.models import Model

from thermocline import (
    MisconfiguredCacheError,
    SupportsDelta,
    SupportsHashProbe,
    SupportsSnapshot,
)
from thermocline.adapters.tortoise import TortoiseSource

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)


class ProductRow(Model):
    id = fields.IntField(primary_key=True)
    title = fields.CharField(max_length=100)
    content_hash = fields.CharField(max_length=64)
    updated_at = fields.DatetimeField()
    deleted_at = fields.DatetimeField(null=True)

    class Meta:
        table = "products"


async def touch(
    row_id: int,
    *,
    title: str | None = None,
    deleted_at: datetime | None = None,
    at: datetime,
) -> None:
    row = await ProductRow.get(id=row_id)
    if title is not None:
        row.title = title
        row.content_hash = f"{row_id}:{title}"
    row.deleted_at = deleted_at
    row.updated_at = at
    await row.save()


@pytest.fixture
async def db() -> AsyncIterator[None]:
    await Tortoise.init(db_url="sqlite://:memory:", modules={"models": ["test_tortoise_source"]})
    await Tortoise.generate_schemas()
    for i, title in ((1, "anchor"), (2, "buoy"), (3, "compass")):
        await ProductRow.create(
            id=i,
            title=title,
            content_hash=f"{i}:{title}",
            updated_at=T0 + timedelta(seconds=i - 1),
        )
    yield
    await Tortoise.close_connections()


def full_source(**kwargs: object) -> TortoiseSource[int, ProductRow]:
    kwargs.setdefault("hash_field", "content_hash")
    kwargs.setdefault("updated_at_field", "updated_at")
    kwargs.setdefault("deleted_at_field", "deleted_at")
    return TortoiseSource(ProductRow, **kwargs)  # type: ignore[arg-type]


@pytest.mark.usefixtures("db")
class TestConfiguration:
    def test_requires_hash_field_or_hash_fn(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="hash"):
            TortoiseSource(ProductRow)

    def test_unknown_field_name_fails_fast(self) -> None:
        with pytest.raises(MisconfiguredCacheError, match="content_hsah"):
            TortoiseSource(ProductRow, hash_field="content_hsah")

    def test_key_defaults_to_primary_key(self) -> None:
        source = full_source()
        assert source.key_of(ProductRow(id=7, title="x")) == 7

    def test_capabilities_follow_declared_fields(self) -> None:
        bare = TortoiseSource(ProductRow, hash_fn=lambda r: r.content_hash)
        assert isinstance(bare, SupportsSnapshot)
        assert not isinstance(bare, SupportsHashProbe)
        assert not isinstance(bare, SupportsDelta)
        full = full_source()
        assert isinstance(full, SupportsSnapshot)
        assert isinstance(full, SupportsHashProbe)
        assert isinstance(full, SupportsDelta)


@pytest.mark.usefixtures("db")
class TestCore:
    async def test_get_returns_row(self) -> None:
        row = await full_source().get(1)
        assert row is not None
        assert (row.id, row.title) == (1, "anchor")

    async def test_get_missing_returns_none(self) -> None:
        assert await full_source().get(404) is None

    async def test_get_hides_soft_deleted(self) -> None:
        stamp = T0 + timedelta(seconds=9)
        await touch(1, deleted_at=stamp, at=stamp)
        assert await full_source().get(1) is None

    async def test_hash_of_reads_field(self) -> None:
        source = full_source()
        row = await source.get(2)
        assert row is not None
        assert source.hash_of(row) == "2:buoy"

    async def test_to_obj_converts_rows(self) -> None:
        source: TortoiseSource[int, dict[str, object]] = TortoiseSource(
            ProductRow,
            hash_field="content_hash",
            to_obj=lambda r: {"id": r.id, "title": r.title, "content_hash": r.content_hash},
        )
        obj = await source.get(1)
        assert obj == {"id": 1, "title": "anchor", "content_hash": "1:anchor"}


@pytest.mark.usefixtures("db")
class TestHashProbe:
    async def test_returns_hash_for_live_row(self) -> None:
        assert await full_source().get_hash(1) == "1:anchor"

    async def test_returns_none_for_missing_and_deleted(self) -> None:
        source = full_source()
        assert await source.get_hash(404) is None
        stamp = T0 + timedelta(seconds=9)
        await touch(1, deleted_at=stamp, at=stamp)
        assert await source.get_hash(1) is None


@pytest.mark.usefixtures("db")
class TestSnapshot:
    async def test_streams_pages_of_live_rows(self) -> None:
        stamp = T0 + timedelta(seconds=9)
        await touch(2, deleted_at=stamp, at=stamp)
        source = full_source(page_size=1)
        ids = [row.id async for row in source.load_all()]
        assert ids == [1, 3]


@pytest.mark.usefixtures("db")
class TestDelta:
    async def test_initial_load_pages_full_dataset(self) -> None:
        source = full_source(page_size=2)
        batches = [batch async for batch in source.load_changed(None)]
        assert [len(b.changed) for b in batches] == [2, 1]
        assert [row.id for b in batches for row in b.changed] == [1, 2, 3]

    async def test_cursor_resumes_without_replaying(self) -> None:
        source = full_source()
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        assert [b async for b in source.load_changed(cursor)] == []
        await touch(2, title="beacon", at=T0 + timedelta(seconds=10))
        delta = [b async for b in source.load_changed(cursor)]
        assert [row.id for b in delta for row in b.changed] == [2]

    async def test_soft_deletes_travel_as_keys(self) -> None:
        source = full_source()
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        stamp = T0 + timedelta(seconds=11)
        await touch(3, deleted_at=stamp, at=stamp)
        delta = [b async for b in source.load_changed(cursor)]
        assert [key for b in delta for key in b.deleted] == [3]
        assert [row for b in delta for row in b.changed] == []
