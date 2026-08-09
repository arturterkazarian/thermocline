"""Tests for the Pydantic-derived serializer."""

import sys
from datetime import datetime, timezone

import pytest
from pydantic import BaseModel

from thermocline import Serializer
from thermocline.adapters.pydantic import PydanticSerializer


class Product(BaseModel):
    id: int
    title: str
    tags: list[str]
    updated_at: datetime


def make_product() -> Product:
    return Product(
        id=7,
        title="северное сияние",  # non-ascii survives the trip
        tags=["featured", "sale"],
        updated_at=datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc),
    )


def make_serializer() -> Serializer[Product]:
    return PydanticSerializer(Product)


class TestRoundTrip:
    def test_restores_equal_model(self) -> None:
        codec = make_serializer()
        product = make_product()
        assert codec.decode(codec.encode(product)) == product

    def test_produces_bytes(self) -> None:
        assert isinstance(make_serializer().encode(make_product()), bytes)


class TestConstruction:
    def test_rejects_non_model_class(self) -> None:
        with pytest.raises(TypeError, match="model class"):
            PydanticSerializer(dict)  # type: ignore[type-var]

    def test_rejects_model_instance(self) -> None:
        with pytest.raises(TypeError, match="model class"):
            PydanticSerializer(make_product())  # type: ignore[arg-type]

    def test_rejects_eol_pydantic_v1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pydantic

        monkeypatch.setattr(pydantic, "VERSION", "1.10.13")
        with pytest.raises(RuntimeError, match="end-of-life"):
            PydanticSerializer(Product)

    def test_fails_fast_without_pydantic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for name in [m for m in sys.modules if m == "pydantic" or m.startswith("pydantic.")]:
            monkeypatch.delitem(sys.modules, name)
        monkeypatch.setattr(sys, "path", [])
        with pytest.raises(ModuleNotFoundError, match=r"thermocline\[pydantic\]"):
            PydanticSerializer(Product)


class TestImportIsolation:
    def test_adapters_package_pulls_no_optional_dependencies(self) -> None:
        import subprocess

        code = (
            "import sys; import thermocline; import thermocline.adapters; "
            "assert 'pydantic' not in sys.modules, 'pydantic leaked into base import'"
        )
        subprocess.run([sys.executable, "-c", code], check=True)
