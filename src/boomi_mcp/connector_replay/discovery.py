"""Candidate discovery for idempotency contracts (#155 slice D).

Answers one question: for a named operation and connection, does the packaged
registry hold an operation record whose identity matches what the account stores
right now?

Everything here is a CANDIDATE, never an authority. The compiler decides whether
evidence is sufficient; this surface only tells an author which contract
reference is worth naming, and says so in the payload it serves. A caller that
treated the answer as permission would be reading a suggestion as a grant, so the
projection carries `authority` and `candidate_only` rather than leaving the
distinction to prose someone has to remember.

The served field set is CLOSED. Bodies, headers, request paths and credential
material never appear — not because they are filtered on the way out, but because
the projection names the fields it emits and there is nowhere for anything else
to ride along.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional

#: Everything a candidate may carry. Written as data so a test can assert the
#: served keys EQUAL this set: a projection that grows a field silently is how a
#: closed contract stops being closed.
CANDIDATE_FIELDS: tuple = (
    "authority",
    "candidate_only",
    "contract_ref",
    "family",
    "action",
    "semantics_id",
    "semantics_revision",
    "account_scope_hash",
    "operation_component_id",
    "operation_version",
    "connection_component_id",
    "connection_version",
    "record_digest",
    "route_coverage_kind",
)

#: Everything a SUPERSEDED row may carry. Closed for the same reason
#: `CANDIDATE_FIELDS` is closed: a projection that can grow a field silently is
#: not a closed contract. Versions and a reference only — no bodies, no routes, no
#: digests of anything the account holds.
SUPERSEDED_FIELDS: tuple = (
    "contract_ref",
    "operation_version_recorded",
    "operation_version_current",
    "connection_version_recorded",
    "connection_version_current",
    "moved",
)

#: The one value the `authority` field may take. A candidate is never authority,
#: so the field is a constant rather than something a caller can vary.
CANDIDATE_AUTHORITY = "non_authoritative_candidate"


def _candidate(record: Any) -> Dict[str, Any]:
    """Project ONE operation record into the closed candidate shape."""
    coverage = getattr(record, "route_coverage", None)
    return {
        "authority": CANDIDATE_AUTHORITY,
        "candidate_only": True,
        "contract_ref": record.contract_ref,
        "family": record.family,
        "action": record.action,
        "semantics_id": record.semantics_id,
        "semantics_revision": record.semantics_revision,
        "account_scope_hash": record.account_scope_hash,
        "operation_component_id": record.operation_identity.component_id,
        "operation_version": record.operation_identity.version,
        "connection_component_id": record.connection_identity.component_id,
        "connection_version": record.connection_identity.version,
        "record_digest": record.record_digest,
        # The KIND only. A static coverage carries the route it covers, and a
        # route is a path — exactly the shape this projection must not serve.
        #
        # FROM THE MODEL'S OWN `kind`, not from its class name. Both coverage
        # models declare `kind` and pin it with a pattern, and the apply-boundary
        # attestation serves that value under this same key — while this one
        # served the Python class name, so a consumer correlating a candidate with
        # an attestation could never match them. One served name, two disjoint
        # vocabularies; the class name is a hand-model that moves on a rename.
        "route_coverage_kind": getattr(coverage, "kind", None) if coverage is not None else None,
    }


def idempotency_contract_candidates(
    *,
    operation_component_id: str,
    connection_component_id: str,
    live_identity: Callable[[str], Optional[Mapping[str, Any]]],
    registry: Any = None,
) -> Dict[str, Any]:
    """Candidates for the named pair, or an explicit unavailability.

    ``live_identity`` returns ``{"component_id", "version"}`` for a component id,
    or ``None`` when the account cannot be read for it. It is injected rather
    than imported so this module stays free of the transport layer — the same
    reason the rest of this package does.

    Returns a served envelope. An empty ``candidates`` list is a real answer:
    the registry holds no record for that pair, which is the normal state until
    evidence is ingested.
    """
    from ..errors import CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE

    for label, component_id in (
        ("operation", operation_component_id),
        ("connection", connection_component_id),
    ):
        if not isinstance(component_id, str) or not component_id.strip():
            return {
                "_success": False,
                "error_code": CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE,
                "error": f"{label}_component_id is required and must be a non-blank string",
            }

    identities = {}
    for label, component_id in (
        ("operation", operation_component_id),
        ("connection", connection_component_id),
    ):
        identity = live_identity(component_id)
        if identity is None:
            # Fail closed and SAY WHICH side could not be read. "Unavailable"
            # with no subject makes the caller guess which of two components to
            # investigate.
            return {
                "_success": False,
                "error_code": CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE,
                "error": (
                    f"the account's identity for the {label} component "
                    f"{component_id!r} could not be read"
                ),
            }
        identities[label] = identity

    if registry is None:
        from .registry import load_registry

        registry = load_registry()

    #: What a SUPERSEDED record may say about itself. Deliberately narrower than a
    #: candidate: this is not a contract anyone may name, it is an explanation of
    #: why the candidate list is empty, so it carries the versions and the
    #: reference and nothing else.
    candidates: List[Dict[str, Any]] = []
    superseded: List[Dict[str, Any]] = []
    for record in registry.operation_records:
        # Identity is the PAIR of component and version, on both sides. Matching
        # ids alone offered a record minted against version 2 while the envelope
        # reported the account at version 9 — a candidate that does not describe
        # the component the caller was just told about.
        # BOTH IDS FIRST, then both versions. Splitting the pair this way is what
        # lets an empty candidate list explain itself: a record for these very
        # components, at a version the account has moved past, is a completely
        # different situation from no record at all — and the caller was shown the
        # same empty list for both.
        if (
            record.operation_identity.component_id,
            record.connection_identity.component_id,
        ) != (operation_component_id, connection_component_id):
            continue
        operation_moved = (
            record.operation_identity.version != identities["operation"]["version"]
        )
        connection_moved = (
            record.connection_identity.version != identities["connection"]["version"]
        )
        if operation_moved or connection_moved:
            # A Boomi component version advances on ANY update, including one
            # confined to fields the configuration digest deliberately excludes —
            # rotating a credential is the routine case. The evidence is then
            # voided, correctly and fail-closed, and until now the refusal a
            # caller received was indistinguishable from never having captured
            # anything. This row is that distinction.
            #
            # WHETHER THE CONFIGURATION ALSO CHANGED IS NOT SERVED HERE, and that
            # is a design decision rather than an omission: answering it means
            # fetching the component's XML and projecting it, which would put
            # credential-bearing bytes into a surface whose whole construction is
            # that there is nowhere for them to ride along. The digest comparison
            # already exists at the apply boundary, which reads the account by
            # design and refuses on exactly that basis.
            superseded.append({
                "contract_ref": record.contract_ref,
                "operation_version_recorded": record.operation_identity.version,
                "operation_version_current": identities["operation"]["version"],
                "connection_version_recorded": record.connection_identity.version,
                "connection_version_current": identities["connection"]["version"],
                "moved": tuple(
                    side for side, moved in (
                        ("operation", operation_moved),
                        ("connection", connection_moved),
                    ) if moved
                ),
            })
            continue
        candidates.append(_candidate(record))

    candidates.sort(key=lambda item: (item["contract_ref"], item["record_digest"]))
    superseded.sort(key=lambda item: item["contract_ref"])
    return {
        "_success": True,
        "authority": CANDIDATE_AUTHORITY,
        "operation_component_id": operation_component_id,
        "operation_version": identities["operation"]["version"],
        "connection_component_id": connection_component_id,
        "connection_version": identities["connection"]["version"],
        "candidates": candidates,
        # EMPTY IS STILL AN ANSWER, and this is what makes it a legible one.
        "superseded_records": superseded,
    }


__all__ = [
    "CANDIDATE_AUTHORITY",
    "CANDIDATE_FIELDS",
    "SUPERSEDED_FIELDS",
    "idempotency_contract_candidates",
]
