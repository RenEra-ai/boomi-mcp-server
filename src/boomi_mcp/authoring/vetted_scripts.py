"""The server-owned vetted script-contract registry (#154 M12.16).

WHY A REGISTRY AND NOT A DECLARATION
------------------------------------
A caller can prove WHICH script a declaration is about — recompute the digest of
the resolved source and compare. That is an IDENTITY check, and identity is not
effect truth: a matching digest says "this declaration is about exactly this
script", never "this script's claimed reads and writes are real".

Nothing in the artifact can establish what an arbitrary Groovy body does. So the
only thing that may supply a script's effect CONTENT is a contract the server
itself vouches for, keyed by the recomputed ``(language, digest)``. A script whose
digest matches but has no entry here is INERT: the server knows which script it
is and still has no authority for what it does, so every strict finding stands.

WHY THE PRODUCTION SEED IS EMPTY
--------------------------------
It is empty at this HEAD, and that is a measured fact rather than an oversight:
recipe contribution validation rejects ``script``, ``script_body``,
``custom_scripting`` and ``language`` keys outright (pinned by
``tests/test_recipe_contribution_models.py``), so this repository ships no
recipe-produced script for the registry to vouch for.

It lives under ``authoring/`` rather than ``recipes/`` for that same reason: the
recipe layer neither produces nor consumes a vetted script today, and putting an
unused module inside that package would have moved the recipe layer's served
revision digest for a surface no recipe touches. Its one consumer is
``authoring.process_ir_effects``.

A digest-only production row must NOT be invented to make the acceptance path
demonstrable. A registry entry is an assertion that the server has VETTED a
specific source; fabricating one to light up a code path would make the registry
lie about the only thing it exists to say. Tests use
``vetted_script_registry_for_tests`` instead, which is explicit about being a
fixture.
"""

from __future__ import annotations

import hashlib
from types import MappingProxyType
from typing import Any, Mapping, Optional, Tuple

__all__ = [
    "VettedScriptContractV1",
    "PRODUCTION_VETTED_SCRIPTS",
    "vetted_script_registry_for_tests",
    "lookup_vetted_script",
    "script_digest",
    "vetted_script_revision_rows",
]


def script_digest(source: str) -> str:
    """The registry key half derived from the source. Bare lowercase hex.

    Hashes the EXACT UTF-8 bytes: no newline translation, no strip, no
    normalisation of any kind. A digest that tolerated whitespace differences
    would vouch for a script that is not the one that was vetted.
    """
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


class VettedScriptContractV1:
    """One vetted script: its exact source, and the effect the server vouches for.

    Immutable, and it stores the SOURCE rather than only a digest. Keeping the
    source means the key is DERIVED here (``script_digest(self.source)``) instead
    of being a second hand-written fact that could drift from the thing it names.
    """

    __slots__ = ("language", "source", "reads", "writes", "replay_safe", "rationale")

    def __init__(
        self,
        language: str,
        source: str,
        *,
        reads: Tuple[Tuple[str, str], ...] = (),
        writes: Tuple[Tuple[str, str], ...] = (),
        replay_safe: bool = False,
        rationale: str = "",
    ) -> None:
        object.__setattr__(self, "language", language)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "reads", tuple(sorted(set(reads))))
        object.__setattr__(self, "writes", tuple(sorted(set(writes))))
        object.__setattr__(self, "replay_safe", bool(replay_safe))
        object.__setattr__(self, "rationale", rationale)

    def __setattr__(self, name: str, value: Any) -> None:  # pragma: no cover - guard
        raise AttributeError("VettedScriptContractV1 is immutable")

    @property
    def key(self) -> Tuple[str, str]:
        return (self.language, script_digest(self.source))


def _registry(*contracts: VettedScriptContractV1) -> Mapping[Tuple[str, str], VettedScriptContractV1]:
    table: dict = {}
    for contract in contracts:
        if contract.key in table:
            raise ValueError("duplicate vetted script key")
        table[contract.key] = contract
    return MappingProxyType(table)


#: EMPTY at this HEAD — see the module docstring. Not a placeholder to fill in.
PRODUCTION_VETTED_SCRIPTS: Mapping[Tuple[str, str], VettedScriptContractV1] = _registry()


def vetted_script_registry_for_tests(
    *contracts: VettedScriptContractV1,
) -> Mapping[Tuple[str, str], VettedScriptContractV1]:
    """A registry a test owns, named so it can never be mistaken for the shipped one."""
    return _registry(*contracts)


def lookup_vetted_script(
    language: str,
    source: str,
    registry: Optional[Mapping[Tuple[str, str], VettedScriptContractV1]] = None,
) -> Optional[VettedScriptContractV1]:
    """Look up by RECOMPUTED key. The caller never supplies the key."""
    table = PRODUCTION_VETTED_SCRIPTS if registry is None else registry
    return table.get((language, script_digest(source)))


def vetted_script_revision_rows(
    registry: Optional[Mapping[Tuple[str, str], VettedScriptContractV1]] = None,
) -> Tuple[Tuple[str, ...], ...]:
    """Sorted, sanitized rows for the served capability revision.

    Carries the DIGEST, never the source: a vetted script body is server-owned
    text and has no business in a served payload.
    """
    table = PRODUCTION_VETTED_SCRIPTS if registry is None else registry
    return tuple(
        (
            language,
            digest,
            ";".join("{0}:{1}".format(*pair) for pair in contract.reads),
            ";".join("{0}:{1}".format(*pair) for pair in contract.writes),
            "replay_safe" if contract.replay_safe else "replay_unsafe",
        )
        for (language, digest), contract in sorted(table.items())
    )
