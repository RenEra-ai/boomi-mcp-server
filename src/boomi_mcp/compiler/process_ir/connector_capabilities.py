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

**Charter (amended by #146).** This module stays compiler-internal: it is never
exported from ``compiler.process_ir.__all__``, and no MCP tool, schema, or
builder may reach it — with ONE named exception. ``boomi_mcp.authoring.
process_ir_projection`` may READ :data:`CONNECTOR_CALL_CAPABILITIES_V1` (through
:func:`connector_capability_rows`) to derive the read-only
``process_ir_authoring`` contract that ``get_schema_template`` serves.

The exception is bounded exactly as it is for the body registry: the projection
is output only and can never re-enter the compiler as capability context; this
table stays the sole enforcement authority; and the projection publishes the
SEMANTIC FACTS under a distinct public vocabulary
(:data:`PUBLIC_CAPABILITY_FIELDS`) rather than this model's own field names,
which are compiler-internal identifiers the served surface may not carry.

The classification VALUES (``read_only``, ``idempotent_write``,
``conditionally_idempotent``, ``non_idempotent``, ``unverified``) are published
VERBATIM — renaming a state would be exactly the second-vocabulary drift #146
exists to remove. Only the field names change.

Nothing else crosses: no binding, no resolution machinery, no symbol table.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Literal, Mapping, Optional, Tuple

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

    ``accepts_input`` IS the placement statement — which is why placement is not
    a third key dimension. ``none_or_documents`` says "entry AND downstream are
    both supported", ``documents_required`` says "downstream only". Keying on
    ``(family, action, placement)`` as well would encode the same fact twice, and
    two encodings of one fact is the duplicate authority ADR-001 §6 removes.

    Both placements are separately evidenced for every ``none_or_documents`` row
    (recorded per row below, so the claim is auditable rather than assumed):

    * ``none_or_documents`` — runs as the entry call (no inbound documents) AND
      mid-flow.
    * ``documents_required`` — consumes inbound documents, so it can never be the
      entry call.

    ``produces_output`` is the single fact the Send gate turns on. It is FALSE
    only where the platform says so: official Boomi documentation states a
    ``Send`` action "sends data to a given destination but does not return any
    data to the process for further processing", and Database (Legacy) declares
    no response profile at all — only a Write profile, which is an *input*
    ("referenced as the destination profile in a map").

    ``retry_safety`` (#142) is a SEPARATE fact from ``side_effect`` and must not
    be inferred from it. ``side_effect`` answers "does this change anything?";
    ``retry_safety`` answers "may this be run twice?" — and a read-shaped action
    is not automatically replay-safe (a SOAP EXECUTE can invoke anything). It has
    NO DEFAULT on purpose: a new row cannot forget to classify itself, because
    omitting the field is an import-time error rather than a silent "safe".

    ``unverified`` is a fail-closed SENTINEL, not a fifth safe classification. It
    means "nobody has established this", and it can never authorise a positive
    retry no matter what evidence a caller attaches.
    """

    family: str = Field(..., min_length=1)
    action: str = Field(..., min_length=1)
    accepts_input: Literal["none_or_documents", "documents_required"]
    produces_output: bool
    side_effect: Literal["read", "write"]
    retry_safety: Literal[
        "read_only",
        "idempotent_write",
        "conditionally_idempotent",
        "non_idempotent",
        "unverified",
    ]


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
    # REST Client GET. Produces: the operation declares a Response Profile.
    #   entry      <- `fetch(rest_fetch)` is a verified sync_pipeline SOURCE stage
    #                 (inventory #139C ledger, live-QA'd, byte golden).
    #   downstream <- official "Connector actions: Get versus Send": "Documents
    #                 retrieved by 'Get' connectors used MID-PROCESS or within
    #                 another process step (as a look-up, for example)".
    ConnectorCapabilityV1(
        family=REST_FAMILY,
        action="GET",
        accepts_input="none_or_documents",
        produces_output=True,
        side_effect="read",
        # A GET retrieves; replaying it re-reads. This is the one classification
        # the HTTP method itself establishes.
        retry_safety="read_only",
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
        # UNVERIFIED, deliberately. PATCH is not idempotent in general (a
        # relative/append-style patch compounds on replay), and the knowledge base
        # returns no authoritative retry-safety answer for this connector at all —
        # the one on-point official statement makes the CALLER responsible for
        # ensuring retries are safe rather than promising the connector is
        # (.codex/plans/issue-142-live-captures.md §G4). Fail closed.
        retry_safety="unverified",
    ),
    # SOAP Client EXECUTE — the single action of `wssoapclientsdk` (#126),
    # declaring request AND response XML profiles, so it produces.
    #   entry      <- `fetch(soap_fetch)` is a verified sync_pipeline SOURCE stage
    #                 (byte golden `sync_pipeline_soap_fetch_soap_send.xml`).
    #   downstream <- `send(soap_send)` is the verified TARGET stage of the same
    #                 shipped chain.
    ConnectorCapabilityV1(
        family=SOAP_FAMILY,
        action="EXECUTE",
        accepts_input="none_or_documents",
        produces_output=True,
        side_effect="read",
        # UNVERIFIED even though ``side_effect`` is "read" — and this row is
        # exactly why the two facts are separate. EXECUTE is a single generic
        # action covering every operation a SOAP service exposes, so "fetch-shaped
        # here" says nothing about whether the remote call may be replayed.
        retry_safety="unverified",
    ),
    # Database (Legacy) Get. Produces: its ReadProfile is the output ("in a map,
    # the Read profile is referenced as the source profile").
    #   entry      <- `read(db_read)` is a verified sync_pipeline SOURCE stage.
    #   downstream <- live capture FINDING 5: a real process runs a database
    #                 `Get` mid-flow, reached from a `catcherrors` leg.
    ConnectorCapabilityV1(
        family=DATABASE_FAMILY,
        action="Get",
        accepts_input="none_or_documents",
        produces_output=True,
        side_effect="read",
        # A Get issues a read query; replaying it re-reads.
        retry_safety="read_only",
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
        # UNVERIFIED. A Send carries whatever statement the operation configures —
        # an INSERT duplicates on replay, an idempotent UPSERT does not — and the
        # registry keys on (family, action), which cannot see that configuration.
        # No authoritative source classifies the action itself as replay-safe
        # (capture §G4), so no positive retry is authorised over it.
        retry_safety="unverified",
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
        # #146: these two strings are SERVED, so neither may cite a repository
        # page — a caller cannot fetch `docs/architecture/` through any MCP tool.
        # They name the fetchable contract selector instead.
        "unverified_action": (
            "This connector family/action pair has no verified operation, emitter and "
            "document-cardinality evidence. The callable pairs are published at "
            "get_schema_template(schema_name='process_ir_authoring', "
            "category='connector_action')."
        ),
        "retry_unverified_write": (
            "This connector action has no established retry safety, so it may not sit "
            "inside a retried region; set the retry count to zero or move the call "
            "outside the protected scope. Per-action replay classifications are at "
            "get_schema_template(schema_name='process_ir_authoring', "
            "category='connector_action')."
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


#: Internal capability field -> the PUBLIC name the authoring contract publishes.
#:
#: Three of this model's own field names are compiler-internal identifiers the
#: served surface may not carry (``tests/test_process_ir_compiler_surface.py::
#: FORBIDDEN_NAMES``), so publishing the rows verbatim is not an option. The
#: public names say the same thing in the caller's terms: what a call takes, what
#: it gives back, and whether it may be replayed.
#:
#: ``family``/``action`` pass through unchanged — they are already the caller's
#: vocabulary. ``side_effect`` likewise: "does this change anything?" is a
#: distinct question from replay safety, and flattening the two is the exact
#: mistake the model's own docstring warns against.
#:
#: TOTAL and INJECTIVE over the model's fields, pinned in both directions, so the
#: projection loses no fact and cannot merge two facts into one.
class ConnectorFamilyCapabilityV1(_CompilerModel):
    """What a connector FAMILY offers, independent of which action is called.

    Per-document path binding is a property of the family's connector step, not
    of one verb: every REST Client action exposes the same single dynamic
    operation property, and no database or SOAP action exposes any. Keeping it
    here — one row per family, joined onto the action rows at projection time —
    is what stops it becoming N hand-written copies of one fact as the action
    table grows.

    ``bindable_locations`` is a closed tuple of the dynamic operation properties
    a caller may bind per document. It is deliberately not a boolean: the wire
    name is what the emitted shape carries, and a family that later exposes a
    second location extends this tuple rather than growing a parallel flag.
    """

    family: str
    bindable_locations: Tuple[Literal["Path"], ...] = ()


#: One row per family in the action allowlist. The pairing is checked both ways
#: — every family with an action row has a family row, and no family row is
#: unused — so a new family cannot arrive without declaring what it binds.
#:
#: The tests are `test_every_action_family_has_a_capability_row_and_none_is_unused`
#: and its two non-vacuity cases in `test_process_ir_authoring_contract_parity.py`.
#: They are named because this comment previously asserted they existed when they
#: did not: an unused family row could be added and the ENTIRE suite stayed green.
CONNECTOR_FAMILY_CAPABILITIES_V1: Mapping[str, ConnectorFamilyCapabilityV1] = (
    MappingProxyType(
        {
            # The REST Client exposes exactly ONE dynamic operation property,
            # "Path" — the product's only per-document request location. Query
            # parameters and headers are static operation configuration.
            REST_FAMILY: ConnectorFamilyCapabilityV1(
                family=REST_FAMILY, bindable_locations=("Path",)
            ),
            # SOAP EXECUTE addresses one configured endpoint, and a database
            # action addresses a statement, not a route. Neither has a
            # per-document location to bind, so a binding on either is refused
            # rather than silently ignored.
            SOAP_FAMILY: ConnectorFamilyCapabilityV1(family=SOAP_FAMILY),
            DATABASE_FAMILY: ConnectorFamilyCapabilityV1(family=DATABASE_FAMILY),
        }
    )
)


def lookup_connector_family_capability(
    family: str,
) -> Optional[ConnectorFamilyCapabilityV1]:
    """Resolve one CANONICAL family, or ``None`` when the family is unknown.

    Unknown means unsupported: an unrecognised family has no declared binding
    location, so a binding on it fails closed exactly as an absent action row
    does.
    """
    return CONNECTOR_FAMILY_CAPABILITIES_V1.get(family)


PUBLIC_CAPABILITY_FIELDS: Mapping[str, str] = MappingProxyType(
    {
        "family": "family",
        "action": "action",
        "accepts_input": "input_documents",
        "produces_output": "output_documents",
        "side_effect": "side_effect",
        "retry_safety": "replay_classification",
    }
)

#: The family-level fields joined onto every published action row, under their
#: own public names. Separate from :data:`PUBLIC_CAPABILITY_FIELDS` because the
#: two models are projected separately and the totality of each is pinned on its
#: own; merging them would hide a field that belongs to neither.
#:
#: Total over every field of :class:`ConnectorFamilyCapabilityV1` EXCEPT the join
#: key `family`, which the action row already carries from its own projection —
#: publishing it twice under two names is the thing being avoided. The earlier
#: wording claimed totality over the model's fields outright, which is false as
#: stated and was pinned by nothing; `test_the_public_field_map_is_total_over_the_joined_fields`
#: now pins the claim in the form that is actually true.
PUBLIC_FAMILY_CAPABILITY_FIELDS: Mapping[str, str] = MappingProxyType(
    {"bindable_locations": "per_document_bindable_locations"}
)


def connector_capability_rows() -> Tuple[Mapping[str, Any], ...]:
    """Every callable (family, action) pair as sorted public data.

    Each row carries EVERY field of :class:`ConnectorCapabilityV1`, renamed
    through :data:`PUBLIC_CAPABILITY_FIELDS` — no field is dropped, because a
    partially projected row is one a caller would have to guess the rest of.
    Values are published verbatim.

    As with the body registry, the DENIED pairs are not enumerated: the registry
    is an allowlist, absence is the rule, and the contract publishes that rule
    once as ``unlisted_connector_action_state``.
    """
    rows = []
    for spec in CONNECTOR_CALL_CAPABILITIES_V1.values():
        dumped = spec.model_dump(mode="json")
        row = {PUBLIC_CAPABILITY_FIELDS[key]: value for key, value in dumped.items()}
        # The family fact is JOINED, never restated per action: an action row
        # added later inherits its family's binding locations automatically, so
        # the published answer cannot drift from the enforced one.
        family_spec = CONNECTOR_FAMILY_CAPABILITIES_V1[spec.family]
        family_dumped = family_spec.model_dump(mode="json")
        for key, public in PUBLIC_FAMILY_CAPABILITY_FIELDS.items():
            row[public] = family_dumped[key]
        rows.append(MappingProxyType(row))
    return tuple(sorted(rows, key=lambda row: (row["family"], row["action"])))


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
    "CONNECTOR_FAMILY_CAPABILITIES_V1",
    "GATED_CONNECTOR_CALL_REASONS",
    "PUBLIC_CAPABILITY_FIELDS",
    "PUBLIC_FAMILY_CAPABILITY_FIELDS",
    "ConnectorCapabilityV1",
    "ConnectorFamilyCapabilityV1",
    "canonicalize_connector_metadata",
    "connector_capability_rows",
    "lookup_connector_family_capability",
    "lookup_capability",
]
