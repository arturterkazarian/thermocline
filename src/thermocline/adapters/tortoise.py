"""Source adapter for Tortoise ORM models.

Follows the same pattern as the SQLAlchemy adapter: service fields are
declared explicitly and each declared field unlocks the matching
capability. Tortoise addresses fields by name, so the declarations are
strings — every name is validated against the model's field map at
construction, so a typo fails at startup, not in production::

    source = TortoiseSource(
        Product,                             # key defaults to the model's pk
        hash_field="content_hash",           # unlocks SupportsHashProbe
        updated_at_field="updated_at",       # unlocks SupportsDelta
        deleted_at_field="deleted_at",       # soft deletes flow into delta batches
    )

Snapshot streaming (``load_all``) is always available. Tortoise manages
connections globally (``Tortoise.init``), so the adapter needs no session
factory — initialize Tortoise before starting the cache.

Contract requirements for the model:

* ``updated_at`` must be set on every insert, update **and soft delete** —
  a deletion that does not touch it never enters the delta. Note that
  ``auto_now=True`` fields update on ``.save()`` but not on bulk
  ``.update()`` queries; whichever write path you use must bump the field.
* ``updated_at`` values must be comparable across rows and not NULL.

Requires the ``tortoise`` extra (``pip install thermocline[tortoise]``).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from typing import Any, Generic, TypeVar

try:
    from tortoise.expressions import Q
    from tortoise.models import Model
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise ModuleNotFoundError(
        "tortoise-orm is required for TortoiseSource; "
        "install it with: pip install thermocline[tortoise]"
    ) from exc

from thermocline.errors import MisconfiguredCacheError
from thermocline.source import CacheSource, SyncBatch

K = TypeVar("K")
T = TypeVar("T")

__all__ = ["TortoiseSource"]


class TortoiseSource(CacheSource[K, T], Generic[K, T]):
    """Cache source backed by one Tortoise ORM model.

    Args:
        model: The Tortoise model class to serve objects from.
        key_field: Key field name. Defaults to the model's primary key.
        hash_field: Field holding the maintained content hash. Declaring
            it unlocks cheap freshness probes (``get_hash``).
        hash_fn: Fallback for models without a hash field: computes the
            hash from a produced object. One of ``hash_field``/``hash_fn``
            is required.
        updated_at_field: Change-tracking timestamp field. Declaring it
            unlocks incremental sync (``load_changed``).
        deleted_at_field: Soft-delete timestamp field. When declared,
            deleted rows disappear from reads and travel through delta
            batches as deleted keys.
        to_obj: Optional converter from the model instance to the object
            handed out of the source.
        page_size: Rows per page for streaming and delta queries.

    Raises:
        MisconfiguredCacheError: If a declared field does not exist on the
            model, neither ``hash_field`` nor ``hash_fn`` is given, or
            ``page_size`` is not positive.
    """

    def __init__(
        self,
        model: type[Model],
        *,
        key_field: str | None = None,
        hash_field: str | None = None,
        hash_fn: Callable[[T], str] | None = None,
        updated_at_field: str | None = None,
        deleted_at_field: str | None = None,
        to_obj: Callable[[Any], T] | None = None,
        page_size: int = 1000,
    ) -> None:
        if hash_field is None and hash_fn is None:
            raise MisconfiguredCacheError(
                f"{model.__name__}: declare hash_field=... or provide hash_fn=...; "
                "the cache cannot stamp envelopes without a hash"
            )
        if page_size <= 0:
            raise MisconfiguredCacheError("page_size must be positive")
        known = model._meta.fields_map
        for name, value in (
            ("key_field", key_field),
            ("hash_field", hash_field),
            ("updated_at_field", updated_at_field),
            ("deleted_at_field", deleted_at_field),
        ):
            if value is not None and value not in known:
                raise MisconfiguredCacheError(
                    f"{model.__name__} has no field {value!r} (declared as {name})"
                )
        self._model = model
        self._key_field = key_field if key_field is not None else model._meta.pk_attr
        self._hash_field = hash_field
        self._hash_fn = hash_fn
        self._updated_at_field = updated_at_field
        self._deleted_at_field = deleted_at_field
        self._to_obj = to_obj
        self._page_size = page_size
        # the fields you declared are the capabilities you get
        if hash_field is not None:
            self.get_hash = self._get_hash_impl
        self.load_all = self._load_all_impl
        if updated_at_field is not None:
            self.load_changed = self._load_changed_impl

    # -- core --------------------------------------------------------------

    async def get(self, key: K) -> T | None:
        """Load one live object by key, or ``None`` when absent or soft-deleted."""
        row = await self._alive().filter(**{self._key_field: key}).first()
        if row is None:
            return None
        return self._convert(row)

    def key_of(self, obj: T) -> K:
        """Read the key from the attribute named after the key field."""
        return getattr(obj, self._key_field)  # type: ignore[no-any-return]

    def hash_of(self, obj: T) -> str:
        """Read the declared hash attribute, or fall back to ``hash_fn``."""
        if self._hash_field is not None:
            return getattr(obj, self._hash_field)  # type: ignore[no-any-return]
        assert self._hash_fn is not None  # guaranteed by __init__
        return self._hash_fn(obj)

    # -- capabilities (attached in __init__ only when declared) ------------

    async def _get_hash_impl(self, key: K) -> str | None:
        assert self._hash_field is not None
        values = await (
            self._alive()
            .filter(**{self._key_field: key})
            .limit(1)
            .values_list(self._hash_field, flat=True)
        )
        return values[0] if values else None

    async def _load_all_impl(self) -> AsyncIterator[T]:
        offset = 0
        while True:
            rows = await (
                self._alive().order_by(self._key_field).offset(offset).limit(self._page_size)
            )
            if not rows:
                return
            for row in rows:
                yield self._convert(row)
            offset += len(rows)

    async def _load_changed_impl(self, cursor: str | None) -> AsyncIterator[SyncBatch[K, T]]:
        assert self._updated_at_field is not None
        updated, key = self._updated_at_field, self._key_field
        queryset = self._model.all()
        if cursor is not None:
            since_raw, last_key = json.loads(cursor)
            since = datetime.fromisoformat(since_raw)
            after_ts: dict[str, Any] = {f"{updated}__gt": since}
            same_ts: dict[str, Any] = {updated: since}
            after_key: dict[str, Any] = {f"{key}__gt": last_key}
            queryset = queryset.filter(Q(**after_ts) | (Q(**same_ts) & Q(**after_key)))
        offset = 0
        while True:
            rows = await queryset.order_by(updated, key).offset(offset).limit(self._page_size)
            if not rows:
                return
            changed: list[T] = []
            deleted: list[K] = []
            for row in rows:
                if (
                    self._deleted_at_field is not None
                    and getattr(row, self._deleted_at_field) is not None
                ):
                    deleted.append(getattr(row, key))
                else:
                    changed.append(self._convert(row))
            last = rows[-1]
            page_cursor = json.dumps([getattr(last, updated).isoformat(), getattr(last, key)])
            yield SyncBatch(changed=changed, deleted=deleted, cursor=page_cursor)
            offset += len(rows)

    # -- helpers -----------------------------------------------------------

    def _alive(self) -> Any:
        queryset = self._model.all()
        if self._deleted_at_field is not None:
            queryset = queryset.filter(**{f"{self._deleted_at_field}__isnull": True})
        return queryset

    def _convert(self, row: Any) -> T:
        return self._to_obj(row) if self._to_obj is not None else row
