"""Behavioral tests for the httpx source adapter, over a mock REST API."""

import json
import re
from typing import Any

import httpx
import pytest

from thermocline import (
    MisconfiguredCacheError,
    SupportsDelta,
    SupportsHashProbe,
    SupportsSnapshot,
)
from thermocline.adapters.httpx import HttpxSource

SEED = [
    {
        "id": 1,
        "title": "anchor",
        "content_hash": "1:anchor",
        "updated_at": "2026-08-10T12:00:00",
        "deleted_at": None,
    },
    {
        "id": 2,
        "title": "buoy",
        "content_hash": "2:buoy",
        "updated_at": "2026-08-10T12:00:01",
        "deleted_at": None,
    },
    {
        "id": 3,
        "title": "compass",
        "content_hash": "3:compass",
        "updated_at": "2026-08-10T12:00:02",
        "deleted_at": None,
    },
]


class FakeApi:
    """In-memory REST API served through httpx.MockTransport."""

    def __init__(self) -> None:
        self.rows: dict[int, dict[str, Any]] = {row["id"]: dict(row) for row in SEED}

    def touch(
        self,
        row_id: int,
        *,
        title: str | None = None,
        deleted_at: str | None = None,
        at: str,
    ) -> None:
        row = self.rows[row_id]
        if title is not None:
            row["title"] = title
            row["content_hash"] = f"{row_id}:{title}"
        row["deleted_at"] = deleted_at
        row["updated_at"] = at

    def _alive(self, row_id: int) -> dict[str, Any] | None:
        row = self.rows.get(row_id)
        return row if row is not None and row["deleted_at"] is None else None

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = request.url.params
        if match := re.fullmatch(r"/products/(\d+)/hash", path):
            row = self._alive(int(match.group(1)))
            return httpx.Response(200, json=row["content_hash"]) if row else httpx.Response(404)
        if match := re.fullmatch(r"/products/(\d+)", path):
            row = self._alive(int(match.group(1)))
            return httpx.Response(200, json=row) if row else httpx.Response(404)
        if path == "/products":
            alive = sorted(
                (r for r in self.rows.values() if r["deleted_at"] is None),
                key=lambda r: r["id"],
            )
            page = alive[int(params["offset"]) : int(params["offset"]) + int(params["limit"])]
            return httpx.Response(200, json={"items": page})
        if path == "/products/changed":
            rows = sorted(self.rows.values(), key=lambda r: (r["updated_at"], r["id"]))
            if "updated_at" in params:
                position = (params["updated_at"], int(params["key"]))
                rows = [r for r in rows if (r["updated_at"], r["id"]) > position]
            page = rows[int(params["offset"]) : int(params["offset"]) + int(params["limit"])]
            return httpx.Response(200, json={"items": page})
        if path == "/boom":
            return httpx.Response(500)
        return httpx.Response(404)


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


@pytest.fixture
def client(api: FakeApi) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(api.handler), base_url="http://api.test")


def make_source(client: httpx.AsyncClient, **kwargs: Any) -> HttpxSource[int, dict[str, Any]]:
    kwargs.setdefault("hash_field", "content_hash")
    kwargs.setdefault("hash_url", "/products/{key}/hash")
    kwargs.setdefault("list_url", "/products")
    kwargs.setdefault("changed_url", "/products/changed")
    kwargs.setdefault("items_field", "items")
    return HttpxSource(client, "/products/{key}", **kwargs)


class TestConfiguration:
    def test_requires_hash_field_or_hash_fn(self, client: httpx.AsyncClient) -> None:
        with pytest.raises(MisconfiguredCacheError, match="hash"):
            HttpxSource(client, "/products/{key}")

    def test_get_url_requires_key_placeholder(self, client: httpx.AsyncClient) -> None:
        with pytest.raises(MisconfiguredCacheError, match="placeholder"):
            HttpxSource(client, "/products", hash_field="content_hash")

    def test_hash_url_requires_key_placeholder(self, client: httpx.AsyncClient) -> None:
        with pytest.raises(MisconfiguredCacheError, match="placeholder"):
            make_source(client, hash_url="/hash")

    def test_capabilities_follow_passed_urls(self, client: httpx.AsyncClient) -> None:
        bare: HttpxSource[int, dict[str, Any]] = HttpxSource(
            client, "/products/{key}", hash_field="content_hash"
        )
        assert not isinstance(bare, SupportsHashProbe)
        assert not isinstance(bare, SupportsSnapshot)
        assert not isinstance(bare, SupportsDelta)
        full = make_source(client)
        assert isinstance(full, SupportsHashProbe)
        assert isinstance(full, SupportsSnapshot)
        assert isinstance(full, SupportsDelta)


class TestCore:
    async def test_get_returns_item(self, client: httpx.AsyncClient) -> None:
        obj = await make_source(client).get(1)
        assert obj is not None
        assert (obj["id"], obj["title"]) == (1, "anchor")

    async def test_get_404_returns_none(self, client: httpx.AsyncClient) -> None:
        assert await make_source(client).get(404) is None

    async def test_get_hides_soft_deleted(self, api: FakeApi, client: httpx.AsyncClient) -> None:
        api.touch(1, deleted_at="2026-08-10T12:00:09", at="2026-08-10T12:00:09")
        assert await make_source(client).get(1) is None

    async def test_server_error_propagates_untouched(self, client: httpx.AsyncClient) -> None:
        boom: HttpxSource[int, dict[str, Any]] = HttpxSource(
            client, "/boom?key={key}", hash_field="content_hash"
        )
        with pytest.raises(httpx.HTTPStatusError):
            await boom.get(1)

    async def test_key_and_hash_read_declared_fields(self, client: httpx.AsyncClient) -> None:
        source = make_source(client)
        obj = await source.get(2)
        assert obj is not None
        assert source.key_of(obj) == 2
        assert source.hash_of(obj) == "2:buoy"

    async def test_from_json_converts(self, client: httpx.AsyncClient) -> None:
        source = make_source(client, from_json=lambda item: (item["id"], item["title"]))
        assert await source.get(1) == (1, "anchor")


class TestHashProbe:
    async def test_returns_hash_for_live_item(self, client: httpx.AsyncClient) -> None:
        assert await make_source(client).get_hash(1) == "1:anchor"

    async def test_returns_none_for_missing_and_deleted(
        self, api: FakeApi, client: httpx.AsyncClient
    ) -> None:
        source = make_source(client)
        assert await source.get_hash(404) is None
        api.touch(1, deleted_at="2026-08-10T12:00:09", at="2026-08-10T12:00:09")
        assert await source.get_hash(1) is None


class TestSnapshot:
    async def test_streams_pages_of_live_items(
        self, api: FakeApi, client: httpx.AsyncClient
    ) -> None:
        api.touch(2, deleted_at="2026-08-10T12:00:09", at="2026-08-10T12:00:09")
        source = make_source(client, page_size=1)
        ids = [obj["id"] async for obj in source.load_all()]
        assert ids == [1, 3]


class TestDelta:
    async def test_initial_load_pages_full_dataset(self, client: httpx.AsyncClient) -> None:
        source = make_source(client, page_size=2)
        batches = [batch async for batch in source.load_changed(None)]
        assert [len(b.changed) for b in batches] == [2, 1]
        assert [obj["id"] for b in batches for obj in b.changed] == [1, 2, 3]

    async def test_cursor_resumes_without_replaying(
        self, api: FakeApi, client: httpx.AsyncClient
    ) -> None:
        source = make_source(client)
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        assert [b async for b in source.load_changed(cursor)] == []
        api.touch(2, title="beacon", at="2026-08-10T12:00:10")
        delta = [b async for b in source.load_changed(cursor)]
        assert [obj["id"] for b in delta for obj in b.changed] == [2]
        assert json.loads(delta[-1].cursor) == ["2026-08-10T12:00:10", 2]

    async def test_soft_deletes_travel_as_keys(
        self, api: FakeApi, client: httpx.AsyncClient
    ) -> None:
        source = make_source(client)
        cursor = [b async for b in source.load_changed(None)][-1].cursor
        api.touch(3, deleted_at="2026-08-10T12:00:11", at="2026-08-10T12:00:11")
        delta = [b async for b in source.load_changed(cursor)]
        assert [key for b in delta for key in b.deleted] == [3]
        assert [obj for b in delta for obj in b.changed] == []
