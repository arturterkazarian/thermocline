"""Serializer derived from a Pydantic model class.

The most common representation gets a zero-boilerplate codec: the model
class already knows how to dump and validate itself, so the user supplies
nothing but the class::

    from thermocline.adapters.pydantic import PydanticSerializer

    serializer = PydanticSerializer(Product)

Requires the ``pydantic`` extra (``pip install thermocline[pydantic]``);
the dependency is checked at construction time so a missing extra surfaces
at startup.

The wire format is Pydantic's own JSON — the fastest path in Pydantic v2
and readable in debugging. When cold-tier compactness matters more, combine
the model with the MessagePack codec instead; the model class already
provides both converters::

    MsgpackSerializer(to_wire=Product.model_dump, from_wire=Product.model_validate)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from pydantic import BaseModel

M = TypeVar("M", bound="BaseModel")

__all__ = ["PydanticSerializer"]


class PydanticSerializer(Generic[M]):
    """Codec between instances of one Pydantic model and JSON bytes.

    Round-trip fidelity follows the model's own serialization contract:
    ``Model.model_validate_json(obj.model_dump_json())`` must reconstruct
    an equivalent object. Custom types need the usual Pydantic
    serializers/validators for that to hold.

    Args:
        model: The model class this codec encodes and decodes.

    Raises:
        ModuleNotFoundError: If pydantic is not installed.
        RuntimeError: If the installed pydantic is the EOL 1.x line.
        TypeError: If ``model`` is not a Pydantic ``BaseModel`` subclass.
    """

    def __init__(self, model: type[M]) -> None:
        try:
            import pydantic
        except ModuleNotFoundError as exc:  # pragma: no cover - import guard
            raise ModuleNotFoundError(
                "pydantic is required for PydanticSerializer; "
                "install it with: pip install thermocline[pydantic]"
            ) from exc
        if pydantic.VERSION.startswith("1."):
            raise RuntimeError(
                f"PydanticSerializer requires pydantic v2, found {pydantic.VERSION}; "
                "pydantic 1.x is end-of-life and not supported"
            )
        if not (isinstance(model, type) and issubclass(model, pydantic.BaseModel)):
            raise TypeError(f"PydanticSerializer expects a pydantic model class, got {model!r}")
        self._model = model

    def encode(self, obj: M) -> bytes:
        """Encode a model instance into its JSON representation."""
        return obj.model_dump_json().encode()

    def decode(self, data: bytes) -> M:
        """Validate JSON bytes back into a model instance."""
        return self._model.model_validate_json(data)
