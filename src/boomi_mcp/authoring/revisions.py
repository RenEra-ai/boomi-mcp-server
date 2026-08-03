"""Deterministic fingerprints for the authoring surface (issue #146, M12.11).

Five related but NON-INTERCHANGEABLE digests, all spelled ``sha256:<64 hex>``:

``schema_revision``
    Every authoring schema ``get_schema_template`` serves.
``capability_revision``
    The whole public authoring capability manifest, including ``schema_revision``.
``semantic_hash``
    One normalized, secret-free authoring intent.
``plan_hash`` / ``compile_hash``
    That intent bound to its planning and compilation evidence.

**Why hashes and not a server-side plan cache.** A cached plan token is
authoritative only on the instance that minted it and only until it is evicted;
a recomputable hash is authoritative everywhere and survives a restart. Apply
therefore RE-COMPUTES and compares rather than looking anything up.

**What is deliberately excluded from every fingerprint** (asserted by tests):
credentials, tokens, headers, connection properties, environment extensions and
document data; absolute paths; timestamps, pids and build ids; profile names and
raw account ids; live component contents; git worktree state. A fingerprint that
moved when the clock did would make every comparison fail; one that carried a
profile name would leak it into an LLM-facing response.

**Not source hashes, not git SHAs.** Equivalent packaged code must produce the
same capability revision, or a rebuilt-but-identical deployment would report
drift against itself.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Tuple

from ..build_info import implementation_digest

_CANONICAL_JSON = {
    "sort_keys": True,
    "separators": (",", ":"),
    "ensure_ascii": True,
    "allow_nan": False,
}

_DIGEST_PREFIX = "sha256:"


def canonical_json_bytes(value: Any) -> bytes:
    """UTF-8 canonical JSON: recursively sorted keys, compact, ASCII, no NaN.

    Raises ``TypeError`` on a value JSON cannot represent and ``ValueError`` on a
    non-finite float, rather than hashing an implementation-dependent ``repr``.
    A digest computed over ``<object at 0x7f…>`` would differ between runs of the
    same code, which is the one thing a revision fingerprint may never do.
    """
    return json.dumps(value, **_CANONICAL_JSON).encode("utf-8")


def sha256_fingerprint(payload: Any) -> str:
    """``sha256:<hex>`` over the canonical JSON of ``payload``.

    Delegates to :func:`boomi_mcp.build_info.implementation_digest` rather than
    hashing directly: that helper is LENGTH-PREFIXED, is already the basis of the
    recipe registry's ``registry_revision`` and ``descriptor_sha256``, and is
    already pinned by tests. A second hashing primitive in the same codebase is
    two things to keep in agreement.
    """
    return _DIGEST_PREFIX + implementation_digest(
        (canonical_json_bytes(payload).decode("utf-8"),)
    )


def _sorted_registry(entries, key_fields: Tuple[str, ...]):
    """Registry collections hash order-independently; schema arrays do not.

    A registry is a SET the server happens to serialize as a list — two servers
    listing the same archetypes in a different order are the same server. A JSON
    Schema's ``required`` array is not: its order is part of the document.
    """
    def _key(entry):
        if isinstance(entry, Mapping):
            return tuple(str(entry.get(field, "")) for field in key_fields)
        return (str(entry),)

    return sorted(entries, key=_key)


def schema_fingerprint(schema_bundle: Mapping[str, Any]) -> str:
    """Fingerprint of every served authoring schema, keyed by selector."""
    return sha256_fingerprint({"schemas": dict(schema_bundle)})


def capability_fingerprint(manifest: Mapping[str, Any]) -> str:
    """Fingerprint of the whole capability manifest (which embeds the schema one)."""
    return sha256_fingerprint({"manifest": dict(manifest)})


def semantic_fingerprint(normalized_intent: Mapping[str, Any]) -> str:
    """Fingerprint of one normalized authoring intent.

    Covers the canonical ProcessIR / IntegrationSpec / topology / recipe data and
    the caller's explicit decision resolutions. Carries no diagnostic wording (it
    is prose, and prose is edited), no timestamps, and no resolved secret values.
    """
    return sha256_fingerprint({"semantic": dict(normalized_intent)})


def plan_fingerprint(
    *,
    semantic_hash: str,
    revision_binding: Mapping[str, Any],
    resolved_references,
    validation_report: Mapping[str, Any],
    capability_gaps,
    required_decisions,
    integration_spec_preview: Mapping[str, Any],
) -> str:
    """Bind an intent to the planning evidence it was validated against.

    ``resolved_references`` is what makes staleness detectable: it carries each
    reference's version token, so a component edited between plan and apply moves
    this hash and the apply preflight refuses.
    """
    return sha256_fingerprint(
        {
            "semantic_hash": semantic_hash,
            "revision_binding": dict(revision_binding),
            "resolved_references": _sorted_registry(
                resolved_references, ("ref", "component_type")
            ),
            "validation_report": dict(validation_report),
            "capability_gaps": _sorted_registry(
                capability_gaps, ("capability_id", "path")
            ),
            "required_decisions": _sorted_registry(
                required_decisions, ("decision_id", "path")
            ),
            "integration_spec_preview": dict(integration_spec_preview),
        }
    )


def compile_fingerprint(
    *,
    plan_hash: str,
    normalized_intent_digest: str,
    integration_spec: Mapping[str, Any],
    artifact_fingerprints,
    compiler_revision: str,
    capability_revision: str,
) -> str:
    """Bind a plan to the artifacts canonical compilation actually produced."""
    return sha256_fingerprint(
        {
            "plan_hash": plan_hash,
            "normalized_intent_digest": normalized_intent_digest,
            "integration_spec": dict(integration_spec),
            "artifacts": _sorted_registry(
                artifact_fingerprints,
                ("component_key", "component_type", "artifact_kind"),
            ),
            "compiler_revision": compiler_revision,
            "capability_revision": capability_revision,
        }
    )


def artifact_fingerprint(canonical_text: str) -> Tuple[str, int]:
    """``(digest, byte_length)`` for one compiled artifact's canonical form.

    Returns the length alongside the digest because the length is the only part
    a caller may see besides the digest, and deriving it separately is how the
    two end up describing different bytes.
    """
    encoded = canonical_text.encode("utf-8")
    return _DIGEST_PREFIX + implementation_digest((canonical_text,)), len(encoded)


def account_scope_fingerprint(
    profile: str, account_id: Optional[str] = None
) -> str:
    """One-way scope hash. Neither input is recoverable from the result.

    The scope is the **account**, not the profile name. A profile is an alias;
    two profiles can address one account, and one profile can be repointed at a
    different account. Keying on the name got both cases wrong: two profiles for
    the same account produced different bindings, so a valid compile was refused
    with a false "stale plan" diagnosis (issue #146 QA, bug #408) — while the
    shipped remediation text claimed account semantics the hash did not have.

    The profile name is used ONLY when no account id is available, and the
    payload records which of the two was used so the fallback can never collide
    with a real account scope.

    A binding is valid only in the scope it was minted in. Without this, a
    compile produced against a sandbox account would satisfy an apply against
    production: the payloads are identical, and only the credentials differ —
    which are exactly what must never enter a hash.
    """
    if account_id:
        payload = {"scope": "authoring.v1", "keyed_on": "account_id",
                   "account_id": str(account_id)}
    else:
        payload = {"scope": "authoring.v1", "keyed_on": "profile",
                   "profile": profile or ""}
    return sha256_fingerprint(payload)


__all__ = [
    "account_scope_fingerprint",
    "artifact_fingerprint",
    "canonical_json_bytes",
    "capability_fingerprint",
    "compile_fingerprint",
    "plan_fingerprint",
    "schema_fingerprint",
    "semantic_fingerprint",
    "sha256_fingerprint",
]
