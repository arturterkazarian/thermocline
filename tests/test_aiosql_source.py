"""Behavioral tests for the aiosql source adapter, on async SQLite."""

from collections.abc import AsyncIterator
from typing import Any

import aiosql
import aiosqlite
import pytest

from thermocline import (
    MisconfiguredCacheError,
    SupportsDelta,
    SupportsHashProbe,
    SupportsSnapshot,
)
from thermocline.adapters.aiosql import AiosqlSource

SQL = """
-- name: get_one(key)^
SELECT id, title, content_hash, updated_at, deleted_at FROM products
WHERE id = :key AND deleted_at IS NULL;

-- name: get_hash(key)$
SELECT content_hash FROM products WHERE id = :key AND deleted_at IS NULL;

-- name: load_all(limit, offset)
SELECT id, title, content_hash FROM products WHERE deleted_at IS NULL
ORDER BY id LIMIT :limit OFFSET :offset;

-- name: load_changed(updated_at, key, limit, offset)
SELECT id, title, content_hash, updated_at, deleted_at FROM products
WHERE :updated_at IS NULL OR (updated_at, id) > (:updated_at, :key)
ORDER BY updated_at, id LIMIT :limit OFFSET :offset;
"""

MINIMAL_SQL = """
-- name: get_one(key)^
SELECT id, title, content_hash FROM products WHERE id = :key;
"""

SCHEMA = """
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT
)
"""

SEED = [
    (1, "anchor", "1:anchor", "2026-08-10T12:00:00", None),
    (2, "buoy", "2:buoy", "2026-08-10T12:00:01", None),
    (3, "compass", "3:compass", "2026-08-10T12:00:02", None),
]


@pytest.fixture
async def conn() -> AsyncIterator[aiosqlite.Connection]:
    connection = await aiosqlite.connect(":memory:")
    connection.row_factory = aiosqlite.Row
    await connection.execute(SCHEMA)
    await connection.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?)", SEED)
    await connection.commit()
    yield connection
    await connection.close()


def make_source(
    conn: aiosqlite.Connection, sql: str = SQL, **kwargs: Any
) -> AiosqlSource[int, dict[str, Any]]:
    kwargs.setdefault("hash_field", "content_hash")
    queries = aiosql.from_str(sql, "aiosqlite")
    return AiosqlSource(queries, conn, **kwargs)


async def touch(
    conn: aiosqlite.Connection,
    row_id: int,
    *,
    title: str | None = None,
    deleted_at: str | None = None,
    at: str,
) -> None:
    if title is not None:
        await conn.execute(
            "UPDATE products SET title = ?, content_hash = ?, updated_at = ?, deleted_at = ?"
            " WHERE id = ?",
            (title, f"{row_id}:{title}", at, deleted_at, row_id),
        )
    else:
        await conn.execute(
            "UPDATE products SET updated_at = ?, deleted_at = ? WHERE id = ?",
            (at, deleted_at, row_id),
        )
    await conn.commit()


class TestConfiguration:
    def test_requires_hash_field_or_hash_fn(self, conn: aiosqlite.Connection) -> None:
        with pytest.raises(MisconfiguredCacheError, match="hash"):
            AiosqlSource(aiosql.from_str(SQL, "aiosqlite"), conn)

    def test_missing_required_query_fails(self, conn: aiosqlite.Connection) -> None:
        queries = aiosql.from_str("-- name: something_else()\nSELECT 1;", "aiosqlite")
        with pytest.raises(MisconfiguredCacheError, match="get_one"):
            AiosqlSource(queries, conn, hash_field="content_hash")

    def test_explicitly_named_missing_query_fails(self, conn: aiosqlite.Connection) -> None:
        with pytest.raises(MisconfiguredCacheError, match="my_probe"):
            make_source(conn, get_hash="my_probe")

    def test_capabilities_follow_written_queries(self, conn: aiosqlite.Connection) -> None:
        minimal = make_source(conn, sql=MINIMAL_SQL)
        assert not isinstance(minimal, SupportsHashProbe)
        assert not isinstance(minimal, SupportsSnapshot)
        assert not isinstance(minimal, SupportsDelta)
        full = make_source(conn)
        assert isinstance(full, SupportsHashProbe)
        assert isinstance(full, SupportsSnapshot)
        assert isinstance(full, SupportsDelta)

    def test_custom_query_names_resolve(self, conn: aiosqlite.Connection) -> None:
        sql = SQL.replace("get_one", "fetch_product")
        source = make_source(conn, sql=sql, get_one="fetch_product")
        assert source is not None


class TestCore:
    async def test_get_returns_row_dict(self, conn: aiosqlite.Connection) -> None:
        obj = await make_source(conn).get(1)
        assert obj is not None
        assert (obj["id"], obj["title"]) == (1, "anchor")

    async def test_get_missing_returns_none(self, conn: aiosqlite.Connection) -> None:
        assert await make_source(conn).get(404) is None

    async def test_get_hides_soft_deleted(self, conn: aiosqlite.Connection) -> None:
        await touch(conn, 1, deleted_at="2026-08-10T12:00:09", at="2026-08-10T12:00:09")
        assert await make_source(conn).get(1) is None

    async def test_key_and_hash_read_declared_fields(self, conn: aiosqlite.Connection) -> None:
        source = make_source(conn)
        obj = await source.get(2)
        assert obj is not None
        assert source.key_of(obj) == 2
        assert source.hash_of(obj) == "2:buoy"

    async def test_from_row_converts(self, conn: aiosqlite.Connection) -> None:
        source = make_source(conn, from_row=lambda row: (row["id"], row["title"]))
        assert await source.get(1) == (1, "anchor")


class TestHashProbe:
    async def test_returns_hash_for_live_row(self, conn: aiosqlite.Connection) -> None:
        assert await make_source(conn).get_hash(1) == "1:anchor"

    async def test_returns_none_for_missing_and_deleted(self, conn: aiosqlite.Connection) -> None:
        source = make_source(conn)
        assert await source.get_hash(404) is None
        await touch(conn, 1, deleted_at="2026-08-10T12:00:09", at="2026-08-10T12:00:09")
        assert await source.get_hash(1) is None


class TestSnapshot:
    async def test_streams_pages_of_live_rows(self, conn: aiosqlite.Connection) -> None:
        await touch(conn, 2, deleted_at="2026-08-10T12:00:09", at="2026-08-10T12:00:09")
        source = make_source(conn, page_size=1)
        ids = [obj["id"] async for obj in source.load_all()]
        assert ids == [1, 3]


class TestDelta:
    async def test_initial_load_pages_full_dataset(self, conn: aiosqlite.Connection) -> None:
        source = make_source(conn, page_size=2)
        batches = [batch async for batch in source.load_changed(None)]
        assert [len(b.changed) for b in batches] == [2, 1]
        assert [obj["id"] for b in batches for obj in b.changed] == [1, 2, 3]

    async def test_cursor_resumes_without_replaying(self, conn: aiosqlite.Connection) -> None:
        source = make_source(conn)
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        assert [b async for b in source.load_changed(cursor)] == []
        await touch(conn, 2, title="beacon", at="2026-08-10T12:00:10")
        delta = [b async for b in source.load_changed(cursor)]
        assert [obj["id"] for b in delta for obj in b.changed] == [2]

    async def test_soft_deletes_travel_as_keys(self, conn: aiosqlite.Connection) -> None:
        source = make_source(conn)
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        await touch(conn, 3, deleted_at="2026-08-10T12:00:11", at="2026-08-10T12:00:11")
        delta = [b async for b in source.load_changed(cursor)]
        assert [key for b in delta for key in b.deleted] == [3]
        assert [obj for b in delta for obj in b.changed] == []
