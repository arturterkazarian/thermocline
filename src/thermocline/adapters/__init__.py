"""Adapters binding thermocline to concrete representations and backends.

Each adapter lives in its own module and requires the matching optional
extra (``thermocline[pydantic]``, ``thermocline[sqlalchemy]``, ...). This
package deliberately imports none of them: ``import thermocline`` must work
with zero optional dependencies installed, so import adapters explicitly::

    from thermocline.adapters.pydantic import PydanticSerializer
"""
