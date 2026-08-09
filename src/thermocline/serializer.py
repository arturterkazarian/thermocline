"""Cold-tier codec: how live objects become compact bytes and back.

The serializer is the layer between a concrete object representation (an ORM
model, a Pydantic model, a plain class) and the cache machinery. The cache
moves envelopes of ``(key, hash, payload)`` around without ever looking
inside a payload; everything the payload format knows about the object's
shape is encapsulated here.

There is deliberately no default serializer: the core has zero dependencies
and silently falling back to some format would hide an important decision.
The cache requires an explicit :class:`Serializer` at construction, in the
same spirit as capability validation — misconfiguration fails fast.

Two implementations ship out of the box:

* :class:`JsonSerializer` — stdlib only, works everywhere, verbose.
* :class:`MsgpackSerializer` — compact and fast; requires the ``msgpack``
  extra (``pip install thermocline[msgpack]``).

Both convert an object to a wire value (JSON-compatible primitives) via a
``to_wire`` callable and back via ``from_wire``. Representation-specific
helpers (e.g. a serializer derived from a Pydantic model class) belong to
the corresponding adapters, next to the representation they know about.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Generic, Protocol, TypeVar, cast

T = TypeVar("T")

__all__ = [
    "JsonSerializer",
    "MsgpackSerializer",
    "Serializer",
]


class Serializer(Protocol[T]):
    """Strategy for moving objects across the live/compact boundary.

    Methods are synchronous on purpose: encoding is pure CPU-bound work,
    there is nothing to await. If decoding large objects ever stalls the
    event loop, offloading to an executor is the cache's concern, not the
    serializer's.

    Contract for implementations:

    * ``decode(encode(obj))`` must be equivalent to ``obj`` as far as
      :meth:`CacheSource.key_of` and :meth:`CacheSource.hash_of` are
      concerned.
    * With the raw path, payloads produced by a source must be decodable
      by the serializer configured on the cache — the two are a pair.
    """

    def encode(self, obj: T) -> bytes:
        """Encode a live object into compact bytes."""
        ...

    def decode(self, data: bytes) -> T:
        """Rebuild a live object from bytes produced by :meth:`encode`."""
        ...


class JsonSerializer(Generic[T]):
    """Stdlib JSON codec: zero dependencies, human-readable, verbose.

    A reasonable starting point; switch to :class:`MsgpackSerializer` when
    compactness starts to matter.

    Args:
        to_wire: Convert an object to JSON-compatible primitives. Omit if
            objects already are plain primitives (dicts, lists, scalars).
        from_wire: Rebuild an object from primitives. Omit together with
            ``to_wire``.
    """

    def __init__(
        self,
        to_wire: Callable[[T], Any] | None = None,
        from_wire: Callable[[Any], T] | None = None,
    ) -> None:
        self._to_wire = to_wire
        self._from_wire = from_wire

    def encode(self, obj: T) -> bytes:
        """Encode a live object into compact JSON bytes."""
        wire = self._to_wire(obj) if self._to_wire is not None else obj
        return json.dumps(wire, ensure_ascii=False, separators=(",", ":")).encode()

    def decode(self, data: bytes) -> T:
        """Rebuild a live object from JSON bytes produced by :meth:`encode`."""
        wire = json.loads(data)
        return self._from_wire(wire) if self._from_wire is not None else cast(T, wire)


class MsgpackSerializer(Generic[T]):
    """MessagePack codec: compact binary, the recommended default choice.

    Requires the ``msgpack`` extra: ``pip install thermocline[msgpack]``.
    The dependency is checked at construction time so a missing extra
    surfaces at startup, not on the first cache miss.

    Args:
        to_wire: Convert an object to msgpack-compatible primitives. Omit
            if objects already are plain primitives.
        from_wire: Rebuild an object from primitives. Omit together with
            ``to_wire``.
    """

    def __init__(
        self,
        to_wire: Callable[[T], Any] | None = None,
        from_wire: Callable[[Any], T] | None = None,
    ) -> None:
        try:
            import msgpack
        except ModuleNotFoundError as exc:  # pragma: no cover - import guard
            raise ModuleNotFoundError(
                "msgpack is required for MsgpackSerializer; "
                "install it with: pip install thermocline[msgpack]"
            ) from exc
        self._msgpack = msgpack
        self._to_wire = to_wire
        self._from_wire = from_wire

    def encode(self, obj: T) -> bytes:
        """Encode a live object into compact MessagePack bytes."""
        wire = self._to_wire(obj) if self._to_wire is not None else obj
        return cast(bytes, self._msgpack.packb(wire, use_bin_type=True))

    def decode(self, data: bytes) -> T:
        """Rebuild a live object from bytes produced by :meth:`encode`."""
        wire = self._msgpack.unpackb(data, raw=False)
        return self._from_wire(wire) if self._from_wire is not None else cast(T, wire)
