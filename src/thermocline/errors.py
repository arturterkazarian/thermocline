"""Exceptions raised by thermocline itself.

The library raises its own errors only for its own failures — misconfiguration
and misuse. Errors coming from user code (a failing source, a broken
``to_wire`` converter) propagate untouched: their owners already know how to
handle them, and wrapping would hide the real cause.
"""

from __future__ import annotations

__all__ = [
    "MisconfiguredCacheError",
    "ThermoclineError",
]


class ThermoclineError(Exception):
    """Base class for every error raised by thermocline."""


class MisconfiguredCacheError(ThermoclineError):
    """Cache configuration rejected at construction or use.

    Raised when a requested mode needs a capability the source does not
    provide, or a configuration value is out of its domain. Never raised
    for runtime data problems — only for wiring mistakes.
    """
