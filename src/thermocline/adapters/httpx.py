"""Source adapter for REST APIs, designed around an httpx.AsyncClient.

REST APIs are the most heterogeneous backend there is, so this adapter
follows the same philosophy as the aiosql one: it provides the plumbing —
URL templates, pagination loops, status-code semantics — and the user's
client describes everything specific (base URL, auth, retries, timeouts).
No magic on the user's behalf.

The URLs you passed are the capabilities you get::

    source = HttpxSource(
        client,                              # your configured httpx.AsyncClient
        get_url="/products/{key}",           # required core
        hash_url="/products/{key}/hash",     # unlocks SupportsHashProbe
        list_url="/products",                # unlocks SupportsSnapshot
        changed_url="/products/changed",     # unlocks SupportsDelta
        hash_field="content_hash",
    )

Endpoint contract:

* ``get_url`` — GET one live object as JSON; ``404`` means "does not
  exist" (soft-deleted objects must also answer 404).
* ``hash_url`` — GET the current hash as a JSON string or plain text;
  404 for missing and soft-deleted objects.
* ``list_url`` — GET a page of live objects; called with ``limit`` and
  ``offset`` query parameters until an empty page comes back.
* ``changed_url`` — GET a page of objects (live *and* soft-deleted)
  changed strictly after the ``(updated_at, key)`` position, ordered by
  it; called with ``limit``/``offset`` and, when resuming, the
  ``updated_at``/``key`` parameters. Items must carry the ``updated_at``
  and (if soft delete exists) ``deleted_at`` fields.

Any non-404 error status raises the client's own exception untouched
(``response.raise_for_status()``): your errors stay yours, and the cache's
``stale_grace`` knows how to live through them.

List responses are either bare JSON arrays or an envelope object; pass
``items_field="items"`` to unwrap ``{"items": [...], "total": ...}``.

The adapter never imports httpx: the client is duck-typed — anything with
an ``async get(url, params=...)`` returning an object with
``status_code``, ``raise_for_status()``, ``json()`` and ``text`` works.
ETag/HEAD-based probing is a possible future capability; today the hash
travels in the body like in every other adapter.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from typing import Any, Generic, TypeVar

from thermocline.errors import MisconfiguredCacheError
from thermocline.source import CacheSource, SyncBatch

K = TypeVar("K")
T = TypeVar("T")

__all__ = ["HttpxSource"]


class HttpxSource(CacheSource[K, T], Generic[K, T]):
    """Cache source over a REST API reached through one async HTTP client.

    Args:
        client: A configured async HTTP client (``httpx.AsyncClient`` or
            anything duck-compatible). Owns auth, base URL and retries.
        get_url: URL template with a ``{key}`` placeholder for the point
            lookup. Required.
        hash_url: URL template with ``{key}`` for the cheap hash probe.
        list_url: URL for the paginated snapshot listing.
        changed_url: URL for the paginated delta listing.
        key_field: Item field holding the key.
        hash_field: Item field holding the maintained content hash. One of
            ``hash_field``/``hash_fn`` is required.
        hash_fn: Fallback hash computed from a produced object.
        updated_at_field: Item field used to build delta cursors.
        deleted_at_field: Item field marking soft deletes in delta pages.
        items_field: Envelope field to unwrap list responses from; by
            default responses are bare arrays.
        from_json: Converter from the decoded JSON item to the produced
            object. Defaults to the item as-is (a dict).
        page_size: Value passed as the ``limit`` query parameter.

    Raises:
        MisconfiguredCacheError: If a ``{key}`` template lacks the
            placeholder, neither ``hash_field`` nor ``hash_fn`` is given,
            or ``page_size`` is not positive.
    """

    def __init__(
        self,
        client: Any,
        get_url: str,
        *,
        hash_url: str | None = None,
        list_url: str | None = None,
        changed_url: str | None = None,
        key_field: str = "id",
        hash_field: str | None = None,
        hash_fn: Callable[[T], str] | None = None,
        updated_at_field: str = "updated_at",
        deleted_at_field: str = "deleted_at",
        items_field: str | None = None,
        from_json: Callable[[Any], T] | None = None,
        page_size: int = 1000,
    ) -> None:
        if hash_field is None and hash_fn is None:
            raise MisconfiguredCacheError(
                "declare hash_field=... or provide hash_fn=...; "
                "the cache cannot stamp envelopes without a hash"
            )
        if page_size <= 0:
            raise MisconfiguredCacheError("page_size must be positive")
        for name, template in (("get_url", get_url), ("hash_url", hash_url)):
            if template is not None and "{key}" not in template:
                raise MisconfiguredCacheError(f"{name} must contain a {{key}} placeholder")
        self._client = client
        self._get_url = get_url
        self._hash_url = hash_url
        self._list_url = list_url
        self._changed_url = changed_url
        self._key_field = key_field
        self._hash_field = hash_field
        self._hash_fn = hash_fn
        self._updated_at_field = updated_at_field
        self._deleted_at_field = deleted_at_field
        self._items_field = items_field
        self._from_json = from_json
        self._page_size = page_size
        # the URLs you passed are the capabilities you get
        if hash_url is not None:
            self.get_hash = self._get_hash_impl
        if list_url is not None:
            self.load_all = self._load_all_impl
        if changed_url is not None:
            self.load_changed = self._load_changed_impl

    # -- core --------------------------------------------------------------

    async def get(self, key: K) -> T | None:
        """GET the object; ``404`` means absent or soft-deleted."""
        response = await self._client.get(self._get_url.format(key=key))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return self._convert(response.json())

    def key_of(self, obj: T) -> K:
        """Read the key field from a produced object."""
        return self._field(obj, self._key_field)  # type: ignore[no-any-return]

    def hash_of(self, obj: T) -> str:
        """Read the hash field, or fall back to ``hash_fn``."""
        if self._hash_field is not None:
            return self._field(obj, self._hash_field)  # type: ignore[no-any-return]
        assert self._hash_fn is not None  # guaranteed by __init__
        return self._hash_fn(obj)

    # -- capabilities (attached in __init__ for the URLs that exist) -------

    async def _get_hash_impl(self, key: K) -> str | None:
        assert self._hash_url is not None
        response = await self._client.get(self._hash_url.format(key=key))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        try:
            value = response.json()
        except ValueError:
            value = response.text.strip()
        if not isinstance(value, str):
            raise TypeError(f"hash endpoint must return a string, got {type(value).__name__}")
        return value

    async def _load_all_impl(self) -> AsyncIterator[T]:
        assert self._list_url is not None
        offset = 0
        while True:
            items = await self._fetch_page(
                self._list_url, {"limit": self._page_size, "offset": offset}
            )
            if not items:
                return
            for item in items:
                yield self._convert(item)
            offset += len(items)

    async def _load_changed_impl(self, cursor: str | None) -> AsyncIterator[SyncBatch[K, T]]:
        assert self._changed_url is not None
        position: tuple[Any, Any] | None = tuple(json.loads(cursor)) if cursor else None
        offset = 0
        while True:
            params: dict[str, Any] = {"limit": self._page_size, "offset": offset}
            if position is not None:
                params["updated_at"] = position[0]
                params["key"] = position[1]
            items = await self._fetch_page(self._changed_url, params)
            if not items:
                return
            changed: list[T] = []
            deleted: list[K] = []
            for item in items:
                if item.get(self._deleted_at_field) is not None:
                    deleted.append(item[self._key_field])
                else:
                    changed.append(self._convert(item))
            last = items[-1]
            page_cursor = json.dumps([last[self._updated_at_field], last[self._key_field]])
            yield SyncBatch(changed=changed, deleted=deleted, cursor=page_cursor)
            offset += len(items)

    # -- helpers -----------------------------------------------------------

    async def _fetch_page(self, url: str, params: dict[str, Any]) -> list[Any]:
        response = await self._client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if self._items_field is not None:
            data = data[self._items_field]
        if not isinstance(data, list):
            raise TypeError(
                f"list endpoint {url!r} must return a JSON array"
                + (f" under {self._items_field!r}" if self._items_field else "")
            )
        return data

    def _convert(self, item: Any) -> T:
        if self._from_json is not None:
            return self._from_json(item)
        return item  # type: ignore[no-any-return]

    @staticmethod
    def _field(obj: Any, name: str) -> Any:
        try:
            return obj[name]
        except TypeError:
            return getattr(obj, name)
