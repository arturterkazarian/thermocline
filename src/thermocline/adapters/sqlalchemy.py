"""Source adapter for SQLAlchemy 2.x async ORM models.

Service columns are declared explicitly — as columns, not strings — and each
declared column unlocks the matching capability, mirroring the library-wide
ladder::

    source = SQLAlchemySource(
        session_factory,
        model=ProductRow,                    # key defaults to the mapped primary key
        hash=ProductRow.content_hash,        # unlocks SupportsHashProbe
        updated_at=ProductRow.updated_at,    # unlocks SupportsDelta
        deleted_at=ProductRow.deleted_at,    # soft deletes flow into delta batches
    )

Snapshot streaming (``load_all``) is always available: any mapped table can
be selected in full. There are no naming conventions — what you did not
declare, the cache will not silently guess.

By default ``T`` is the ORM instance itself, detached from its session:
eager-load everything the consumer needs and do not touch lazy
relationships. Pass ``to_obj`` to hand out a different representation —
e.g. ``to_obj=lambda row: Product.model_validate(row, from_attributes=True)``
pairs this source with :class:`~thermocline.adapters.pydantic.PydanticSerializer`.
With ``to_obj``, the produced objects must still expose the key (and hash,
if declared) under the same attribute names as the mapped columns.

Contract requirements for the mapped table:

* ``updated_at`` must be set on every insert, update **and soft delete** —
  a deletion that does not touch ``updated_at`` never enters the delta.
* ``updated_at`` values must be comparable across rows (use UTC) and not NULL.

Requires the ``sqlalchemy`` extra (``pip install thermocline[sqlalchemy]``).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from typing import Any, Generic, TypeVar

try:
    from sqlalchemy import inspect, select, tuple_
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise ModuleNotFoundError(
        "sqlalchemy is required for SQLAlchemySource; "
        "install it with: pip install thermocline[sqlalchemy]"
    ) from exc

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import InstrumentedAttribute

from thermocline.errors import MisconfiguredCacheError
from thermocline.source import CacheSource, SyncBatch

K = TypeVar("K")
T = TypeVar("T")

__all__ = ["SQLAlchemySource"]


class SQLAlchemySource(CacheSource[K, T], Generic[K, T]):
    """Cache source backed by one mapped SQLAlchemy model.

    Args:
        session_factory: An ``async_sessionmaker``; a fresh session is
            opened per operation.
        model: The mapped ORM class to serve objects from.
        key: Key column. Defaults to the model's single-column primary key;
            required explicitly when the primary key is composite.
        hash: Column holding the maintained content hash. Declaring it
            unlocks cheap freshness probes (``get_hash``).
        hash_fn: Fallback for tables without a hash column: computes the
            hash from a loaded object. One of ``hash``/``hash_fn`` is
            required.
        updated_at: Change-tracking timestamp column. Declaring it unlocks
            incremental sync (``load_changed``).
        deleted_at: Soft-delete timestamp column. When declared, deleted
            rows disappear from reads and travel through delta batches as
            deleted keys.
        to_obj: Optional converter from the ORM instance to the object
            handed out of the source.
        page_size: Rows per page for streaming and delta queries.

    Raises:
        MisconfiguredCacheError: If neither ``hash`` nor ``hash_fn`` is
            given, the primary key is composite and ``key`` is omitted,
            or ``page_size`` is not positive.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        model: type[Any],
        *,
        key: InstrumentedAttribute[Any] | None = None,
        hash: InstrumentedAttribute[Any] | None = None,
        hash_fn: Callable[[T], str] | None = None,
        updated_at: InstrumentedAttribute[Any] | None = None,
        deleted_at: InstrumentedAttribute[Any] | None = None,
        to_obj: Callable[[Any], T] | None = None,
        page_size: int = 1000,
    ) -> None:
        if hash is None and hash_fn is None:
            raise MisconfiguredCacheError(
                f"{model.__name__}: declare a hash column (hash=...) or provide hash_fn=...; "
                "the cache cannot stamp envelopes without a hash"
            )
        if page_size <= 0:
            raise MisconfiguredCacheError("page_size must be positive")
        if key is None:
            primary_key = inspect(model).primary_key
            if len(primary_key) != 1:
                raise MisconfiguredCacheError(
                    f"{model.__name__} has a composite primary key; pass key=<column> explicitly"
                )
            key = getattr(model, inspect(model).get_property_by_column(primary_key[0]).key)
        self._sessions = session_factory
        self._model = model
        self._key = key
        self._key_name: str = key.key
        self._hash = hash
        self._hash_name: str | None = hash.key if hash is not None else None
        self._hash_fn = hash_fn
        self._updated_at = updated_at
        self._deleted_at = deleted_at
        self._to_obj = to_obj
        self._page_size = page_size
        # capabilities appear only for what was declared (structural detection)
        if hash is not None:
            self.get_hash = self._get_hash
        self.load_all = self._load_all
        if updated_at is not None:
            self.load_changed = self._load_changed

    # -- core --------------------------------------------------------------

    async def get(self, key: K) -> T | None:
        """Load one live object by key, or ``None`` when absent or soft-deleted."""
        stmt = select(self._model).where(self._key == key)
        if self._deleted_at is not None:
            stmt = stmt.where(self._deleted_at.is_(None))
        async with self._sessions() as session:
            row = (await session.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None
        return self._convert(row)

    def key_of(self, obj: T) -> K:
        """Read the key from the attribute named after the key column."""
        return getattr(obj, self._key_name)  # type: ignore[no-any-return]

    def hash_of(self, obj: T) -> str:
        """Read the declared hash attribute, or fall back to ``hash_fn``."""
        if self._hash_name is not None:
            return getattr(obj, self._hash_name)  # type: ignore[no-any-return]
        assert self._hash_fn is not None  # guaranteed by __init__
        return self._hash_fn(obj)

    # -- capabilities (attached in __init__ only when declared) ------------

    async def _get_hash(self, key: K) -> str | None:
        assert self._hash is not None
        stmt = select(self._hash).where(self._key == key)
        if self._deleted_at is not None:
            stmt = stmt.where(self._deleted_at.is_(None))
        async with self._sessions() as session:
            return (await session.execute(stmt)).scalar_one_or_none()

    async def _load_all(self) -> AsyncIterator[T]:
        stmt = select(self._model)
        if self._deleted_at is not None:
            stmt = stmt.where(self._deleted_at.is_(None))
        async with self._sessions() as session:
            result = await session.stream_scalars(stmt.execution_options(yield_per=self._page_size))
            async for row in result:
                yield self._convert(row)

    async def _load_changed(self, cursor: str | None) -> AsyncIterator[SyncBatch[K, T]]:
        assert self._updated_at is not None
        position = _decode_cursor(cursor)
        while True:
            stmt = select(self._model).order_by(self._updated_at, self._key).limit(self._page_size)
            if position is not None:
                stmt = stmt.where(tuple_(self._updated_at, self._key) > position)
            async with self._sessions() as session:
                rows = (await session.execute(stmt)).scalars().all()
            if not rows:
                return
            changed: list[T] = []
            deleted: list[K] = []
            for row in rows:
                if self._deleted_at is not None and getattr(row, self._deleted_at.key) is not None:
                    deleted.append(getattr(row, self._key_name))
                else:
                    changed.append(self._convert(row))
            last = rows[-1]
            position = (getattr(last, self._updated_at.key), getattr(last, self._key_name))
            yield SyncBatch(changed=changed, deleted=deleted, cursor=_encode_cursor(position))

    # -- helpers -----------------------------------------------------------

    def _convert(self, row: Any) -> T:
        return self._to_obj(row) if self._to_obj is not None else row


def _encode_cursor(position: tuple[datetime, Any]) -> str:
    updated_at, key = position
    return json.dumps([updated_at.isoformat(), key])


def _decode_cursor(cursor: str | None) -> tuple[datetime, Any] | None:
    if cursor is None:
        return None
    updated_at, key = json.loads(cursor)
    return (datetime.fromisoformat(updated_at), key)
