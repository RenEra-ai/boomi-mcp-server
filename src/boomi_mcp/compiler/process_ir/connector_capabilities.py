"""Closed connector-call capability registry (issue #140, M12.5).

The registry answers exactly one question per call: *for this canonical connector
family and action, what may the compiler assume about documents?* It is a closed
ALLOWLIST — a family/action that is not a row here is unsupported, full stop.
That is the fail-closed direction the issue's research gate mandates ("if any
required family/action cannot be safely emitted, gate it explicitly instead of
inventing fields"), and it means no gated-row table is load-bearing: WSS
``LISTEN``, Database V2, an OEM connector subtype and every unverified REST verb
are rejected by *absence*, not by remembering to list them.

Every row is traceable to checkout evidence or official Boomi documentation; the
capture ledger is ``.codex/plans/issue-140-live-captures.md``.

This module is compiler-internal: it is never exported from
``compiler.process_ir.__all__`` and no MCP tool, schema, or builder reaches it.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Dict, Literal, Mapping, Optional, Tuple

from pydantic import Field

from .contracts import _CompilerModel

# The three connector families this checkout can actually build AND emit. They
# are the canonical subtypes ``_canonical_connector_type`` resolves to, so an
# alias (``rest``/``rest_client``/``soap_client``) reaches the registry already
# normalized and a family it does not recognise (an OEM subtype, a future
# connector) passes through verbatim and simply misses every row.
REST_FAMILY = "officialboomi-X3979C-rest-prod"
SOAP_FAMILY = "wssoapclientsdk"
DATABASE_FAMILY = "database"


class ConnectorCapabilityV1(_CompilerModel):
    """What the compiler may assume about one (family, action) pair's documents.

    ``accepts_input``:

    * ``none_or_documents`` — runs as the entry call (no inbound documents) AND
      mid-flow. Checkout-verified: each such action is a ``sync_pipeline`` source
      stage (``read``/``fetch``), and a live process uses a database ``Get``
      mid-flow off a ``catcherrors`` leg (capture FINDING 5).
    * ``documents_required`` — consumes inbound documents, so it can never be the
      entry call.

    ``produces_output`` is the single fact the Send gate turns on. It is FALSE
    only where the platform says so: official Boomi documentation states a
    ``Send`` action "sends data to a given destination but does not return any
    data to the process for further processing", and Database (Legacy) declares
    no response profile at all — only a Write profile, which is an *input*
    ("referenced as the destination profile in a map").
    """

    family: str = Field(..., min_length=1)
    action: str = Field(..., min_length=1)
    accepts_input: Literal["none_or_documents", "documents_required"]
    produces_output: bool
    side_effect: Literal["read", "write"]


def _rows(*specs: ConnectorCapabilityV1) -> Mapping[Tuple[str, str], ConnectorCapabilityV1]:
    table: Dict[Tuple[str, str], ConnectorCapabilityV1] = {}
    for spec in specs:
        key = (spec.family, spec.action.casefold())
        if key in table:  # pragma: no cover - guards a typo at import time
            raise ValueError("duplicate connector capability row")
        table[key] = spec
    return MappingProxyType(table)


#: The closed allowlist. Keyed by ``(canonical family, case-folded action)``;
#: the action is case-folded for LOOKUP only — the spelling that reaches the wire
#: is always the authoritative one carried by the operation symbol (``Get`` and
#: ``Send`` stay mixed-case for the database family, ``EXECUTE`` for SOAP, and
#: the REST family is upper-cased by the shared canonicalizer).
CONNECTOR_CALL_CAPABILITIES_V1 = _rows(
    # REST Client GET — the `rest_fetch` source primitive; the operation declares
    # a Response Profile, so it produces documents.
    ConnectorCapabilityV1(
        family=REST_FAMILY,
        action="GET",
        accepts_input="none_or_documents",
        produces_output=True,
        side_effect="read",
    ),
    # REST Client PATCH — a `rest_send`-class verb. It consumes the outbound
    # document AND declares a Response Profile ("Structure of the response
    # received by the connector"), so unlike a database Send it does produce.
    ConnectorCapabilityV1(
        family=REST_FAMILY,
        action="PATCH",
        accepts_input="documents_required",
        produces_output=True,
        side_effect="write",
    ),
    # SOAP Client EXECUTE — the single action of `wssoapclientsdk` (#126), used
    # by BOTH the `soap_fetch` source and `soap_send` target primitives, and
    # declaring request AND response XML profiles.
    ConnectorCapabilityV1(
        family=SOAP_FAMILY,
        action="EXECUTE",
        accepts_input="none_or_documents",
        produces_output=True,
        side_effect="read",
    ),
    # Database (Legacy) Get — the `db_get` source primitive; its ReadProfile is
    # the output ("in a map, the Read profile is referenced as the source
    # profile").
    ConnectorCapabilityV1(
        family=DATABASE_FAMILY,
        action="Get",
        accepts_input="none_or_documents",
        produces_output=True,
        side_effect="read",
    ),
    # Database (Legacy) Send — the `db_write` target primitive. TERMINAL: only a
    # WriteProfile (an input), no response profile, and the official Get-vs-Send
    # rule. This is the row that makes continuation-after-Send a hard error.
    ConnectorCapabilityV1(
        family=DATABASE_FAMILY,
        action="Send",
        accepts_input="documents_required",
        produces_output=False,
        side_effect="write",
    ),
)


#: Static, value-free remediation for the classes a caller is most likely to hit.
#: Purely explanatory — rejection is driven by absence from the allowlist above,
#: never by membership here, so a class missing from this map still fails closed.
GATED_CONNECTOR_CALL_REASONS: Mapping[str, str] = MappingProxyType(
    {
        "listener": (
            "Listener/WSS entry is not representable as a connector_call: the legacy "
            "path fuses the start and connector into one start_listen shape, which "
            "this compiler does not emit."
        ),
        "database_v2": (
            "Database V2 is a different connector from Database (Legacy) and has no "
            "verified operation/emitter contract here; it is never aliased to 'database'."
        ),
        "unverified_action": (
            "This connector family/action pair has no verified operation, emitter and "
            "document-cardinality evidence; see the #140 capability matrix in "
            "docs/architecture/PROCESS_IR_COMPILER_V1.md."
        ),
    }
)


def canonicalize_connector_metadata(
    connector_type: str, action_type: str
) -> Tuple[str, str]:
    """Normalise connector metadata exactly as the legacy LINEAR builder does.

    Moved here from ``lowering`` in #140 so the capability registry and the
    emitter input are keyed off ONE canonicalization; ``lowering`` keeps a thin
    wrapper. Semantics are unchanged from #139C — family-conditional and
    role-independent:

    * REST family (either role) -> canonical subtype, action UPPER-cased
    * any other family          -> canonical subtype LOWER-cased, action VERBATIM

    The action stays verbatim off the REST path because a DATABASE write target
    emits the mixed-case verb ``Send`` (mirroring the DB source's ``Get``); an
    unconditional ``.upper()`` corrupts it to ``SEND``.

    The legacy helpers are imported lazily so they stay the single source of
    truth (a duplicated alias table would drift) without charging every
    ``import boomi_mcp.compiler`` for the 7k-line builder module.
    """
    from ...categories.components.builders.process_flow_builder import (
        _canonical_connector_type,
        _resolve_rest_connector_type,
    )

    canonical = _canonical_connector_type(connector_type)
    action = str(action_type or "").strip()
    if _resolve_rest_connector_type(connector_type) is not None:
        return canonical, action.upper()
    return canonical.lower(), action


def lookup_capability(family: str, action: str) -> Optional[ConnectorCapabilityV1]:
    """Resolve one CANONICAL (family, action) pair, or ``None`` when unsupported.

    Both arguments must already have been through
    :func:`canonicalize_connector_metadata` — looking up a raw alias would let
    ``rest_client`` miss a row that ``officialboomi-…-rest-…`` hits and silently
    reject a supported call.
    """
    return CONNECTOR_CALL_CAPABILITIES_V1.get((family, str(action or "").casefold()))


__all__ = [
    "CONNECTOR_CALL_CAPABILITIES_V1",
    "GATED_CONNECTOR_CALL_REASONS",
    "ConnectorCapabilityV1",
    "canonicalize_connector_metadata",
    "lookup_capability",
]
