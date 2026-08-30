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
        "route_coverage_kind": type(coverage).__name__ if coverage is not None else None,
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

    candidates: List[Dict[str, Any]] = []
    for record in registry.operation_records:
        if record.operation_identity.component_id != operation_component_id:
            continue
        if record.connection_identity.component_id != connection_component_id:
            continue
        candidates.append(_candidate(record))

    candidates.sort(key=lambda item: (item["contract_ref"], item["record_digest"]))
    return {
        "_success": True,
        "authority": CANDIDATE_AUTHORITY,
        "operation_component_id": operation_component_id,
        "operation_version": identities["operation"]["version"],
        "connection_component_id": connection_component_id,
        "connection_version": identities["connection"]["version"],
        "candidates": candidates,
    }


__all__ = [
    "CANDIDATE_AUTHORITY",
    "CANDIDATE_FIELDS",
    "idempotency_contract_candidates",
]
