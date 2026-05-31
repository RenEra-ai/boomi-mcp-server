"""Issue #47: M7.1 read-only profile-field inference layer.

Turns four kinds of *discovered* artifact into the issue-#43 builder-ready
profile-field contracts:

* ``profile_from_db_metadata`` — caller-supplied DB column metadata summary.
* ``profile_from_sample_json`` — a sample JSON document.
* ``profile_from_xsd``         — a conservative XSD subset.
* ``profile_from_sample_xml``  — a sample XML document.

Each ``infer_profile_*`` function PARSES the artifact and DELEGATES to the
issue-#43 helpers (``profile_from_db_read_fields`` / ``profile_from_json_schema``
/ ``profile_from_xml_schema``) so the emitted builder contract
(``profile_config`` / ``field_index_by_path`` / ``mappable_paths``) stays
byte-identical to what the existing profile/map builders already consume.

Safety contract (this layer is pure):

* No Boomi API calls, no credential lookups, no SDK client construction, no
  network, no direct JDBC. DB metadata is whatever the caller passes in.
* Never echoes sample VALUES (JSON values / XML text) into the output — only
  paths, inferred types, confidence, and ambiguity notes.
* Inference metadata (confidence / ambiguities / confirmation_required) lives in
  a PARALLEL ``fields`` list + top-level ``issues`` list. It is NEVER injected
  into ``field_index_by_path`` nodes or the ``profile_config`` tree, so the
  delegated #43 contract is preserved exactly.
* Field names that look like credentials are withheld from the contract (see
  ``_is_secret_named``) and surfaced as a warning issue, never forwarded into a
  profile/map.

Ambiguity is non-fatal: an ambiguous field is kept with a safe fallback type,
``confidence="ambiguous"``, ``confirmation_required=True`` and forces the
response ``ready_for_builder=False`` — the caller must confirm before applying.
Structural shapes that cannot be represented in the #43 contract at all (scalar
JSON root, empty/heterogeneous arrays, XML attributes, XSD choice, binary DB
columns, namespaces, recursion) raise structured ``PROFILE_INFERENCE_*`` errors.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .connector_builder import BuilderValidationError
from .map_builder import _FORBIDDEN_SECRET_FIELDS


# ---------------------------------------------------------------------------
# Error / issue codes
# ---------------------------------------------------------------------------

PROFILE_INFERENCE_INVALID_INPUT = "PROFILE_INFERENCE_INVALID_INPUT"
PROFILE_INFERENCE_INVALID_SAMPLE = "PROFILE_INFERENCE_INVALID_SAMPLE"
PROFILE_INFERENCE_UNSUPPORTED_SHAPE = "PROFILE_INFERENCE_UNSUPPORTED_SHAPE"
PROFILE_INFERENCE_AMBIGUOUS_SHAPE = "PROFILE_INFERENCE_AMBIGUOUS_SHAPE"
PROFILE_INFERENCE_INPUT_TOO_LARGE = "PROFILE_INFERENCE_INPUT_TOO_LARGE"
PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE = "PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE"
PROFILE_INFERENCE_RECURSIVE_XML = "PROFILE_INFERENCE_RECURSIVE_XML"

# Non-fatal advisory issue code (surfaced in the ``issues`` list, not raised).
PROFILE_INFERENCE_SECRET_FIELD_WITHHELD = "PROFILE_INFERENCE_SECRET_FIELD_WITHHELD"


# ---------------------------------------------------------------------------
# Input limits
# ---------------------------------------------------------------------------

# Defaults the caller can lower freely; they may only be RAISED up to the hard
# caps. ``_resolve_limits`` clamps every requested value to ``[1, hard_cap]``.
_DEFAULT_LIMITS: Dict[str, int] = {
    "max_input_chars": 200_000,
    "max_nodes": 1_000,
    "max_fields": 500,
}
_HARD_CAPS: Dict[str, int] = {
    "max_input_chars": 2_000_000,
    "max_nodes": 10_000,
    "max_fields": 5_000,
}


def _resolve_limits(options: Optional[Dict[str, Any]]) -> Dict[str, int]:
    """Return effective limits: defaults, overridden by ``options`` and clamped
    to ``[1, hard_cap]``. Non-integer / missing overrides keep the default."""
    out = dict(_DEFAULT_LIMITS)
    if not options:
        return out
    for key in out:
        if key in options and options[key] is not None:
            try:
                val = int(options[key])
            except (TypeError, ValueError):
                continue
            out[key] = max(1, min(val, _HARD_CAPS[key]))
    return out


# ---------------------------------------------------------------------------
# Secret-name hygiene
# ---------------------------------------------------------------------------


def _is_secret_named(name: Any) -> bool:
    """True when a field name is a credential-shaped WHOLE name.

    Exact (normalized) match against the canonical forbidden set — NOT a
    substring scan — so legitimate columns such as ``authorization_date``,
    ``token_count``, ``bearer_name`` or ``secret_santa_id`` are not withheld,
    while ``password`` / ``api_key`` / ``client_secret`` / ``access_token`` are.
    Mirrors ``map_builder._scan_forbidden_secret_fields`` exact-key semantics.
    """
    if not isinstance(name, str):
        return False
    norm = name.strip().lower().replace("-", "_").replace(" ", "_")
    return norm in _FORBIDDEN_SECRET_FIELDS
