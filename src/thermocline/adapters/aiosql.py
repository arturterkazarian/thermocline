"""Source adapter for aiosql query collections.

aiosql is a deliberately low level of abstraction — named SQL queries in
plain ``.sql`` files — and this adapter keeps it that way: the SQL belongs
entirely to the user, including pagination and soft-delete filtering. The
adapter performs no magic on the user's behalf; anything more would turn it
into a half-baked ORM.

The queries you wrote are the capabilities you get. Default query names::

    -- name: get_one(key)^
    SELECT id, title, content_hash FROM products
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

``get_one`` is required; each optional query unlocks the matching
capability. Names are configurable — only the signatures are fixed. A name
you passed explicitly must exist; a missing *default* optional name simply
leaves the capability off.

Signature contract for the queries:

* ``get_one(key)`` — one live row or none (``^`` operator).
* ``get_hash(key)`` — the hash scalar of a live row, or none (``$``).
* ``load_all(limit, offset)`` — a page of live rows; the adapter loops
  pages until an empty one.
* ``load_changed(updated_at, key, limit, offset)`` — a page of rows (live
  *and* soft-deleted) changed strictly after the ``(updated_at, key)``
  position, ordered by it; ``NULL`` position means "from the beginning".
  Rows must carry the ``updated_at`` and (if soft delete exists) the
  ``deleted_at`` fields.

Rows must be mapping-like (configure your driver's row factory — e.g.
``aiosqlite.Row``). ``updated_at`` values travel through the cursor as-is
via JSON (datetimes become ISO strings), so store timestamps in a form your
database can compare with a bound string — ISO text columns work everywhere.

The adapter never imports aiosql itself: the queries object is duck-typed,
so any object exposing the same callables works.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import datetime
from typing import Any, Generic, TypeVar

from thermocline.errors import MisconfiguredCacheError
from thermocline.source import CacheSource, SyncBatch

K = TypeVar("K")
T = TypeVar("T")

__all__ = ["AiosqlSource"]

_DEFAULT_NAMES = {
    "get_one": "get_one",
    "get_hash": "get_hash",
    "load_all": "load_all",
    "load_changed": "load_changed",
}


def _as_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {name: row[name] for name in row.keys()}  # noqa: SIM118 - sqlite3.Row is not a Mapping
    return dict(row)


class AiosqlSource(CacheSource[K, T], Generic[K, T]):
    """Cache source over an aiosql queries object and one connection (or pool).

    Args:
        queries: The aiosql queries object (``aiosql.from_path(...)``).
        connection: Passed as the first argument to every query call — a
            live connection or a pool, whatever your aiosql driver accepts.
        key_field: Row field holding the key.
        hash_field: Row field holding the maintained content hash. One of
            ``hash_field``/``hash_fn`` is required.
        hash_fn: Fallback hash computed from a produced object.
        updated_at_field: Row field used to build delta cursors.
        deleted_at_field: Row field marking soft deletes in delta pages;
            rows where it is non-null travel as deleted keys.
        from_row: Converter from a row mapping to the produced object.
            Defaults to a plain ``dict`` of the row.
        get_one: Override for the required point-lookup query name.
        get_hash: Override for the hash-probe query name.
        load_all: Override for the snapshot query name.
        load_changed: Override for the delta query name.
        page_size: Value passed as ``:limit``.

    Raises:
        MisconfiguredCacheError: If ``get_one`` is missing, an explicitly
            named query does not exist, neither ``hash_field`` nor
            ``hash_fn`` is given, or ``page_size`` is not positive.
    """

    def __init__(
        self,
        queries: Any,
        connection: Any,
        *,
        key_field: str = "id",
        hash_field: str | None = None,
        hash_fn: Callable[[T], str] | None = None,
        updated_at_field: str = "updated_at",
        deleted_at_field: str = "deleted_at",
        from_row: Callable[[Mapping[str, Any]], T] | None = None,
        get_one: str | None = None,
        get_hash: str | None = None,
        load_all: str | None = None,
        load_changed: str | None = None,
        page_size: int = 1000,
    ) -> None:
        if hash_field is None and hash_fn is None:
            raise MisconfiguredCacheError(
                "declare hash_field=... or provide hash_fn=...; "
                "the cache cannot stamp envelopes without a hash"
            )
        if page_size <= 0:
            raise MisconfiguredCacheError("page_size must be positive")
        self._conn = connection
        self._key_field = key_field
        self._hash_field = hash_field
        self._hash_fn = hash_fn
        self._updated_at_field = updated_at_field
        self._deleted_at_field = deleted_at_field
        self._from_row = from_row
        self._page_size = page_size

        self._q_get_one: Any = self._resolve(queries, "get_one", get_one, required=True)
        self._q_get_hash = self._resolve(queries, "get_hash", get_hash)
        self._q_load_all = self._resolve(queries, "load_all", load_all)
        self._q_load_changed = self._resolve(queries, "load_changed", load_changed)
        # the queries you wrote are the capabilities you get
        if self._q_get_hash is not None:
            self.get_hash = self._get_hash_impl
        if self._q_load_all is not None:
            self.load_all = self._load_all_impl
        if self._q_load_changed is not None:
            self.load_changed = self._load_changed_impl

    @staticmethod
    def _resolve(
        queries: Any, role: str, override: str | None, *, required: bool = False
    ) -> Any | None:
        name = override if override is not None else _DEFAULT_NAMES[role]
        query = getattr(queries, name, None)
        if query is None and (required or override is not None):
            raise MisconfiguredCacheError(
                f"query {name!r} (role: {role}) is not defined in the queries object"
            )
        return query

    # -- core --------------------------------------------------------------

    async def get(self, key: K) -> T | None:
        """Run the point-lookup query; ``None`` when absent or soft-deleted."""
        row = await self._q_get_one(self._conn, key=key)
        if row is None:
            return None
        return self._convert(row)

    def key_of(self, obj: T) -> K:
        """Read the key field from a produced object."""
        return self._field(obj, self._key_field)  # type: ignore[no-any-return]

    def hash_of(self, obj: T) -> str:
        """Read the hash field, or fall back to ``hash_fn``."""
        if self._hash_field is not None:
            return self._field(obj, self._hash_field)  # type: ignore[no-any-return]
        assert self._hash_fn is not None  # guaranteed by __init__
        return self._hash_fn(obj)

    # -- capabilities (attached in __init__ for the queries that exist) ----

    async def _get_hash_impl(self, key: K) -> str | None:
        assert self._q_get_hash is not None
        return await self._q_get_hash(self._conn, key=key)  # type: ignore[no-any-return]

    async def _load_all_impl(self) -> AsyncIterator[T]:
        assert self._q_load_all is not None
        offset = 0
        while True:
            rows = await _fetch_rows(
                self._q_load_all(self._conn, limit=self._page_size, offset=offset)
            )
            if not rows:
                return
            for row in rows:
                yield self._convert(row)
            offset += len(rows)

    async def _load_changed_impl(self, cursor: str | None) -> AsyncIterator[SyncBatch[K, T]]:
        assert self._q_load_changed is not None
        position = json.loads(cursor) if cursor is not None else (None, None)
        offset = 0
        while True:
            rows = [
                _as_dict(row)
                for row in await _fetch_rows(
                    self._q_load_changed(
                        self._conn,
                        updated_at=position[0],
                        key=position[1],
                        limit=self._page_size,
                        offset=offset,
                    )
                )
            ]
            if not rows:
                return
            changed: list[T] = []
            deleted: list[K] = []
            for row in rows:
                if row.get(self._deleted_at_field) is not None:
                    deleted.append(row[self._key_field])
                else:
                    changed.append(self._convert(row))
            last = rows[-1]
            page_cursor = json.dumps(
                [last[self._updated_at_field], last[self._key_field]],
                default=_json_datetime,
            )
            yield SyncBatch(changed=changed, deleted=deleted, cursor=page_cursor)
            offset += len(rows)

    # -- helpers -----------------------------------------------------------

    def _convert(self, row: Any) -> T:
        mapping = _as_dict(row)
        if self._from_row is not None:
            return self._from_row(mapping)
        return mapping  # type: ignore[return-value]

    @staticmethod
    def _field(obj: Any, name: str) -> Any:
        try:
            return obj[name]
        except TypeError:
            return getattr(obj, name)


async def _fetch_rows(result: Any) -> list[Any]:
    """Collect a page from either an awaitable of rows or an async generator.

    aiosql returns multi-row selects as awaitable lists in older versions and
    as async generators in newer ones; the adapter accepts both.
    """
    if hasattr(result, "__aiter__"):
        return [row async for row in result]
    return list(await result)


def _json_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"cursor value {value!r} is not JSON-serializable")
