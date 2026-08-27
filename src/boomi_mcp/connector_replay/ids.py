"""Identifier grammars for the connector-replay evidence registry.

This module OWNS the identifier languages the registry validates and the capture
tooling produces. It is deliberately the lowest layer of ``connector_replay`` and
imports nothing from ``boomi_mcp.compiler`` or ``boomi_mcp.categories`` at module
load, so the registry can be packaged and loaded without dragging the authoring
stack in behind it.

Ownership matters more than convenience here. The Boomi component-id language was
spelled out by hand in two places before this module existed — once with a
lowercase class plus ``re.IGNORECASE`` and once with an explicit ``a-fA-F`` class —
two spellings of exactly the same language, each free to drift from the other. A
third spelling added here would have made it three. Both existing sites now import
:data:`BOOMI_COMPONENT_ID_PATTERN` from this module, which is why the import arrow
points *into* the authoring stack and never back out of it.

The execution-id grammar earns its strictness from two negatives the platform
itself supplies, and both are the kind a hand-written fixture reaches for first:

* ``execution-110b23f4-567a-8d90-1234-56789e0b123d`` is the platform's own
  documented example for ``ExecutionRecord.executionId`` — and it is UNDATED.
  No real execution id looks like this; every one of the 57 captured under this
  issue carries a trailing ``-YYYY.MM.DD``. A fixture pasted from the API
  documentation would therefore be accepted by any grammar loose enough to make
  the date optional, and would prove nothing about the ids the platform emits.
* ``executionrecord-110b23f4-...`` is a DIFFERENT object's id that shares the
  leading characters. A prefix test written as ``startswith("execution")``
  accepts it.

Both are rejected here, and the test module pins both directions.
"""

from __future__ import annotations

import re
from typing import Final

__all__ = [
    "BOOMI_COMPONENT_ID_PATTERN",
    "BOOMI_COMPONENT_ID_RE",
    "EXECUTION_ID_PATTERN",
    "EXECUTION_ID_RE",
    "is_boomi_component_id",
    "is_execution_id",
]


#: The Boomi component-id language: a lowercase-or-uppercase hex UUID in canonical
#: 8-4-4-4-12 form. Anchored at both ends — an id is the whole string, never a
#: substring of one, so a value carrying trailing whitespace or an appended
#: fragment is rejected rather than silently truncated to its leading match.
BOOMI_COMPONENT_ID_PATTERN: Final[str] = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

BOOMI_COMPONENT_ID_RE: Final[re.Pattern[str]] = re.compile(BOOMI_COMPONENT_ID_PATTERN)


#: The Boomi execution-id language: the literal ``execution-``, a hex UUID, then a
#: dotted calendar date. The date is REQUIRED — see the module docstring for why
#: making it optional would admit the platform's own documentation example. The
#: leading segment is matched as ``execution-`` immediately followed by a hex
#: digit, which is what separates it from ``executionrecord-``: that value's next
#: character after the shared prefix is ``r``, not a hex digit.
#:
#: The date is validated as a SHAPE, not as a calendar. A registry that refused
#: 2026.02.30 would be asserting an authority it does not have — the platform
#: mints these strings and this module only recognises them, so a shape that the
#: platform could emit is accepted even if no such day exists.
EXECUTION_ID_PATTERN: Final[str] = (
    r"^execution-"
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"-[0-9]{4}\.[0-9]{2}\.[0-9]{2}$"
)

EXECUTION_ID_RE: Final[re.Pattern[str]] = re.compile(EXECUTION_ID_PATTERN)


def is_boomi_component_id(value: object) -> bool:
    """Return True when ``value`` is a Boomi component id.

    Non-strings are False rather than a TypeError: this is a recogniser used to
    classify values arriving from JSON and from the platform, where a null or a
    number is a thing that happens and is simply not an id.
    """
    return isinstance(value, str) and BOOMI_COMPONENT_ID_RE.match(value) is not None


def is_execution_id(value: object) -> bool:
    """Return True when ``value`` is a Boomi execution id.

    See :data:`EXECUTION_ID_PATTERN` — the trailing date is required, so the
    platform's undated documentation example is rejected here on purpose.
    """
    return isinstance(value, str) and EXECUTION_ID_RE.match(value) is not None
