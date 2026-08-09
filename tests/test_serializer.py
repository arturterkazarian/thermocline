"""Round-trip and configuration tests for the cold-tier serializers."""

import sys
from dataclasses import dataclass
from typing import Any

import pytest

from thermocline import JsonSerializer, MsgpackSerializer, Serializer


@dataclass(frozen=True)
class Item:
    id: int
    payload: str


def item_to_wire(obj: Item) -> dict[str, Any]:
    return {"id": obj.id, "payload": obj.payload}


def item_from_wire(wire: Any) -> Item:
    return Item(id=wire["id"], payload=wire["payload"])


def make_json_serializer() -> Serializer[Item]:
    return JsonSerializer(to_wire=item_to_wire, from_wire=item_from_wire)


def make_msgpack_serializer() -> Serializer[Item]:
    return MsgpackSerializer(to_wire=item_to_wire, from_wire=item_from_wire)


@pytest.fixture(params=[make_json_serializer, make_msgpack_serializer])
def serializer(request: pytest.FixtureRequest) -> Serializer[Item]:
    factory: Any = request.param
    return factory()  # type: ignore[no-any-return]


class TestRoundTrip:
    def test_restores_equal_object(self, serializer: Serializer[Item]) -> None:
        item = Item(id=7, payload="сорок два")  # non-ascii survives the trip
        assert serializer.decode(serializer.encode(item)) == item

    def test_produces_bytes(self, serializer: Serializer[Item]) -> None:
        assert isinstance(serializer.encode(Item(id=1, payload="a")), bytes)


class TestIdentityMode:
    def test_json_handles_plain_primitives(self) -> None:
        codec: JsonSerializer[dict[str, Any]] = JsonSerializer()
        wire = {"id": 1, "tags": ["a", "b"], "active": True}
        assert codec.decode(codec.encode(wire)) == wire

    def test_msgpack_handles_plain_primitives(self) -> None:
        codec: MsgpackSerializer[dict[str, Any]] = MsgpackSerializer()
        wire = {"id": 1, "tags": ["a", "b"], "active": True}
        assert codec.decode(codec.encode(wire)) == wire


class TestMissingExtra:
    def test_msgpack_serializer_fails_fast_without_msgpack(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for name in [m for m in sys.modules if m == "msgpack" or m.startswith("msgpack.")]:
            monkeypatch.delitem(sys.modules, name)
        monkeypatch.setattr(sys, "path", [])
        with pytest.raises(ModuleNotFoundError, match=r"thermocline\[msgpack\]"):
            MsgpackSerializer()
