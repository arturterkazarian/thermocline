"""Behavioral tests for the SQLAlchemy source adapter, on async SQLite."""

import math
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

import pytest
from sqlalchemy import DateTime, String
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from thermocline import (
    JsonSerializer,
    MisconfiguredCacheError,
    SupportsDelta,
    SupportsHashProbe,
    SupportsSnapshot,
    Thermocline,
)
from thermocline.adapters.sqlalchemy import SQLAlchemySource

T0 = datetime(2026, 8, 10, 12, 0, 0)


class Base(DeclarativeBase):
    pass


class ProductRow(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(100))
    content_hash: Mapped[str] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


def stamp(row: ProductRow) -> ProductRow:
    row.content_hash = f"{row.id}:{row.title}"
    return row


@pytest.fixture
async def sessions() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        session.add_all(
            [
                stamp(ProductRow(id=1, title="anchor", updated_at=T0)),
                stamp(ProductRow(id=2, title="buoy", updated_at=T0 + timedelta(seconds=1))),
                stamp(ProductRow(id=3, title="compass", updated_at=T0 + timedelta(seconds=2))),
            ]
        )
        await session.commit()
    yield factory
    await engine.dispose()


def full_source(
    sessions: async_sessionmaker[AsyncSession],
    **kwargs: object,
) -> SQLAlchemySource[int, ProductRow]:
    kwargs.setdefault("hash", ProductRow.content_hash)
    kwargs.setdefault("updated_at", ProductRow.updated_at)
    kwargs.setdefault("deleted_at", ProductRow.deleted_at)
    return SQLAlchemySource(sessions, ProductRow, **kwargs)  # type: ignore[arg-type]


async def touch(
    sessions: async_sessionmaker[AsyncSession],
    row_id: int,
    *,
    title: str | None = None,
    delete_at: datetime | None = None,
    at: datetime,
) -> None:
    async with sessions() as session:
        row = await session.get(ProductRow, row_id)
        assert row is not None
        if title is not None:
            row.title = title
        row.deleted_at = delete_at
        row.updated_at = at
        stamp(row)
        await session.commit()


class TestConfiguration:
    def test_requires_hash_or_hash_fn(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        with pytest.raises(MisconfiguredCacheError, match="hash"):
            SQLAlchemySource(sessions, ProductRow)

    def test_key_defaults_to_primary_key(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        source = full_source(sessions)
        assert source.key_of(ProductRow(id=7, title="x", updated_at=T0)) == 7

    def test_capabilities_follow_declared_columns(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        bare = SQLAlchemySource(sessions, ProductRow, hash_fn=lambda r: r.content_hash)
        assert isinstance(bare, SupportsSnapshot)
        assert not isinstance(bare, SupportsHashProbe)
        assert not isinstance(bare, SupportsDelta)
        full = full_source(sessions)
        assert isinstance(full, SupportsSnapshot)
        assert isinstance(full, SupportsHashProbe)
        assert isinstance(full, SupportsDelta)


class TestCore:
    async def test_get_returns_row(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        row = await full_source(sessions).get(1)
        assert row is not None
        assert (row.id, row.title) == (1, "anchor")

    async def test_get_missing_returns_none(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        assert await full_source(sessions).get(404) is None

    async def test_get_hides_soft_deleted(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        await touch(sessions, 1, delete_at=T0 + timedelta(seconds=9), at=T0 + timedelta(seconds=9))
        assert await full_source(sessions).get(1) is None

    async def test_hash_of_reads_column(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        source = full_source(sessions)
        row = await source.get(2)
        assert row is not None
        assert source.hash_of(row) == "2:buoy"

    async def test_to_obj_converts_rows(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        source: SQLAlchemySource[int, dict[str, object]] = SQLAlchemySource(
            sessions,
            ProductRow,
            hash=ProductRow.content_hash,
            to_obj=lambda r: {"id": r.id, "title": r.title, "content_hash": r.content_hash},
        )
        obj = await source.get(1)
        assert obj == {"id": 1, "title": "anchor", "content_hash": "1:anchor"}


class TestHashProbe:
    async def test_returns_hash_for_live_row(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        assert await full_source(sessions).get_hash(1) == "1:anchor"

    async def test_returns_none_for_missing_and_deleted(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        source = full_source(sessions)
        assert await source.get_hash(404) is None
        await touch(sessions, 1, delete_at=T0 + timedelta(seconds=9), at=T0 + timedelta(seconds=9))
        assert await source.get_hash(1) is None


class TestSnapshot:
    async def test_streams_live_rows_only(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        await touch(sessions, 2, delete_at=T0 + timedelta(seconds=9), at=T0 + timedelta(seconds=9))
        ids = [row.id async for row in full_source(sessions).load_all()]
        assert ids == [1, 3]


class TestDelta:
    async def test_initial_load_pages_full_dataset(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        source = full_source(sessions, page_size=2)
        batches = [batch async for batch in source.load_changed(None)]
        assert [len(b.changed) for b in batches] == [2, 1]
        assert [row.id for b in batches for row in b.changed] == [1, 2, 3]

    async def test_cursor_resumes_without_replaying_everything(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        source = full_source(sessions)
        batches = [batch async for batch in source.load_changed(None)]
        cursor = batches[-1].cursor
        assert [b async for b in source.load_changed(cursor)] == []
        await touch(sessions, 2, title="beacon", at=T0 + timedelta(seconds=10))
        delta = [b async for b in source.load_changed(cursor)]
        assert [row.id for b in delta for row in b.changed] == [2]

    async def test_soft_deletes_travel_as_keys(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        source = full_source(sessions)
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        await touch(
            sessions, 3, delete_at=T0 + timedelta(seconds=11), at=T0 + timedelta(seconds=11)
        )
        delta = [b async for b in source.load_changed(cursor)]
        assert [key for b in delta for key in b.deleted] == [3]
        assert [row for b in delta for row in b.changed] == []


class TestEndToEnd:
    async def test_thermocline_over_sqlalchemy_delta_sync(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        source = full_source(sessions)
        serializer: JsonSerializer[ProductRow] = JsonSerializer(
            to_wire=lambda r: {"id": r.id, "title": r.title, "content_hash": r.content_hash},
            from_wire=lambda w: stamp(ProductRow(id=w["id"], title=w["title"], updated_at=T0)),
        )
        cache = Thermocline(source, serializer, hot_capacity=10, max_staleness=math.inf)
        async with cache:
            row = await cache.get(1)
            assert row is not None and row.title == "anchor"
            await touch(sessions, 1, title="beacon", at=T0 + timedelta(seconds=10))
            await touch(
                sessions, 3, delete_at=T0 + timedelta(seconds=11), at=T0 + timedelta(seconds=11)
            )
            await cache.sync_now()
            updated = await cache.get(1)
            assert updated is not None and updated.title == "beacon"
            assert await cache.get(3) is None
