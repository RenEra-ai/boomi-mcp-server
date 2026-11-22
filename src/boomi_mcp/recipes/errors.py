"""Value-free recipe diagnostics and the public error envelope (issue #145).

Every message and remediation in this module is a STATIC table lookup. Nothing is
interpolated from an exception, an input, a component name, a label, or a
reference. That is not caution for its own sake: the compatibility adapters that
feed the recipe layer accept SQL, hosts, headers, credentials and script bodies as
their existing public inputs, so anything that echoes an input back is a leak
path — and diagnostics are logged, chained, and returned to the caller.

The structural fields carry vetted material: ``path`` is an RFC 6901 pointer
built from a structural walk. ``target`` is a closed literal, an authority name,
or a SEMANTIC KEY from the composed document — a process key, a component key, a
topology object key. Those originate in the caller's own spec, so like
``recipe_ids`` below they are the caller's material returned to the caller, and
naming the contested slot is what makes a conflict actionable. What ``target``
never carries is a value from a compatibility adapter's private catalog, or a
capability SUBJECT: those name dark compiler internals and are reduced to their
authority. An earlier version called ``target`` "a closed slot literal" full
stop, which was false; live QA measured it (issue #145). ``recipe_ids`` normally holds REGISTERED ids, with two
deliberate exceptions — both failures that happen BEFORE resolution, where
naming the request is the only way to make the diagnostic usable:
``RECIPE_NOT_FOUND`` echoes the id the caller asked for, and
``RECIPE_REQUEST_INVALID`` echoes the id attached to a duplicate invocation. That is the
caller's own input returned to the caller — never material from a compatibility
adapter's private catalog, which is the leak this module exists to prevent. It is
NOT bounded by ``RecipeSemanticId``: ``resolve`` takes a plain ``str``, so an
arbitrary string round-trips. Two earlier versions of this note over-claimed
(first that no caller text appeared at all, then that it was pattern-bounded);
live QA falsified both, and this is the measured statement.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, get_args

from pydantic import BaseModel, ConfigDict, Field

from ..errors import (
    DB_TO_API_RETRY_UNAVAILABLE,
    RECIPE_CAPABILITY_GATED,
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_CONTRIBUTION_INVALID,
    RECIPE_INPUT_INVALID,
    RECIPE_NOT_FOUND,
    RECIPE_OUTPUT_NONDETERMINISTIC,
    RECIPE_PATCH_CONFLICT,
    RECIPE_REQUEST_INVALID,
    RECIPE_PATCH_TARGET_NOT_FOUND,
    RECIPE_VERSION_UNAVAILABLE,
)

RecipePhase = Literal[
    "lookup",
    "capability",
    "input",
    "execution",
    "composition",
    "validation",
    "determinism",
]

#: The literal producer name a DIRECT-authored base carries in a conflict, so a
#: two-producer diagnostic can name the non-recipe side without inventing an id.
DIRECT_AUTHORING_PRODUCER = "direct_authoring"

_MESSAGES: Dict[str, str] = {
    RECIPE_NOT_FOUND: "no registered recipe carries the requested id",
    RECIPE_VERSION_UNAVAILABLE: "the recipe id is registered, but not at the requested version",
    RECIPE_CAPABILITY_GATED: "a required capability is absent, gated, guidance-only or unsupported",
    RECIPE_INPUT_INVALID: "the recipe input failed the forbidden-shape scan or its strict model",
    RECIPE_CONTRIBUTION_INVALID: "a returned value is not a declared, strictly-valid contribution",
    RECIPE_PATCH_TARGET_NOT_FOUND: "a closed patch operation names a slot that does not resolve",
    RECIPE_PATCH_CONFLICT: "two writers target one semantic slot with no declared merge rule",
    RECIPE_CONSTRAINT_FAILED: "a canonical validator or declared requirement rejected the result",
    RECIPE_OUTPUT_NONDETERMINISTIC: "two runs over independently rebuilt inputs produced different contributions",
    RECIPE_REQUEST_INVALID: "the recipe request envelope is malformed",
}

_REMEDIATIONS: Dict[str, str] = {
    RECIPE_NOT_FOUND: (
        "List the registry snapshot and use a registered recipe id; advisory "
        "doctrine names recipes but is never itself executable."
    ),
    RECIPE_VERSION_UNAVAILABLE: (
        "Request one of the versions the diagnostic lists, or omit the version "
        "to select the code-declared default."
    ),
    RECIPE_CAPABILITY_GATED: (
        "The construct this recipe needs is not shippable in this build. Check "
        "the published capability manifests; do not work around the gate."
    ),
    RECIPE_INPUT_INVALID: (
        "Recipe inputs carry opaque references and closed enumerations only — no "
        "configuration, credentials, headers, SQL, raw XML, code, or graph edges."
    ),
    RECIPE_CONTRIBUTION_INVALID: (
        "A recipe may return only the contribution types its descriptor declares, "
        "each strictly valid and free of forbidden fields."
    ),
    RECIPE_PATCH_TARGET_NOT_FOUND: (
        "Establish the process root (or the terminal Branch) before contributing "
        "an insert or append against it."
    ),
    RECIPE_PATCH_CONFLICT: (
        "Both recipes named in this diagnostic must declare the merge rule for "
        "this slot, or one of them must stop writing it. There is no precedence "
        "setting and no last-writer-wins."
    ),
    RECIPE_CONSTRAINT_FAILED: (
        "Fix the underlying condition the cause codes name. Recipe output is "
        "validated exactly as direct authoring and cannot be exempted."
    ),
    RECIPE_OUTPUT_NONDETERMINISTIC: (
        "A recipe executor must be a pure function of its validated input — no "
        "clock, randomness, environment, I/O, or mutable module state."
    ),
    RECIPE_REQUEST_INVALID: (
        "Each requested invocation needs its own unique invocation_id; the "
        "composer identifies invocations by it. This is about the request "
        "envelope, not about the recipe input's contents."
    ),
}

#: Per-CAUSE remediations (#159 S2). When a diagnostic carries exactly ONE cause
#: code listed here, its static remediation replaces the code's generic one,
#: because the generic text ("fix the condition the cause codes name") cannot
#: say what the fix is. Static and value-free like every entry above.
_CAUSE_REMEDIATIONS: Dict[str, str] = {
    DB_TO_API_RETRY_UNAVAILABLE: (
        "Set reliability.retry.max_attempts to 1. A retried REST POST send needs "
        "evidence-gated retries for reused operations, which issue #199 tracks; "
        "until then the preset refuses 2 to 6 rather than reducing them."
    ),
}


#: The CLOSED set of codes a recipe diagnostic may carry, derived from the ten
#: constants this module already imports rather than retyped as string literals —
#: a second copy is a second thing to forget. ``RecipeDiagnosticV1.code`` was a
#: bare ``str``, which made the "all ten codes and nothing else" property a
#: convention held up by ``recipe_diagnostic`` being the only constructor. It was
#: not: the model is public and directly constructible, so any string was a legal
#: diagnostic code (issue #145, §6 architect review).
RECIPE_ERROR_CODES: Tuple[str, ...] = (
    RECIPE_NOT_FOUND,
    RECIPE_VERSION_UNAVAILABLE,
    RECIPE_CAPABILITY_GATED,
    RECIPE_INPUT_INVALID,
    RECIPE_CONTRIBUTION_INVALID,
    RECIPE_PATCH_TARGET_NOT_FOUND,
    RECIPE_PATCH_CONFLICT,
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_OUTPUT_NONDETERMINISTIC,
    RECIPE_REQUEST_INVALID,
)

#: ``Literal`` needs its members spelled at type-check time, so the annotation is
#: written out — and ``_assert_error_codes_match`` below pins it to
#: ``RECIPE_ERROR_CODES`` at import, so the two can never silently diverge.
RecipeErrorCode = Literal[
    "RECIPE_NOT_FOUND",
    "RECIPE_VERSION_UNAVAILABLE",
    "RECIPE_CAPABILITY_GATED",
    "RECIPE_INPUT_INVALID",
    "RECIPE_CONTRIBUTION_INVALID",
    "RECIPE_PATCH_TARGET_NOT_FOUND",
    "RECIPE_PATCH_CONFLICT",
    "RECIPE_CONSTRAINT_FAILED",
    "RECIPE_OUTPUT_NONDETERMINISTIC",
    "RECIPE_REQUEST_INVALID",
]

if set(get_args(RecipeErrorCode)) != set(RECIPE_ERROR_CODES):  # pragma: no cover
    raise AssertionError(
        "RecipeErrorCode and RECIPE_ERROR_CODES disagree; a code was added to "
        "one and not the other"
    )


class RecipeDiagnosticV1(BaseModel):
    """One deterministic, value-free recipe diagnostic."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: RecipeErrorCode
    phase: RecipePhase
    path: str = ""
    target: Optional[str] = None
    recipe_ids: Tuple[str, ...] = ()
    recipe_versions: Tuple[str, ...] = ()
    invocation_ids: Tuple[str, ...] = ()
    contribution_indexes: Tuple[int, ...] = ()
    available_versions: Tuple[str, ...] = ()
    cause_codes: Tuple[str, ...] = ()
    message: str = Field(..., min_length=1)
    remediation: str = Field(..., min_length=1)


def cause_remediation(cause_codes: Sequence[str]) -> Optional[str]:
    """The static per-cause remediation when ``cause_codes`` is exactly ONE
    cause that has one, else None (#159 S2). One rule for every surface that
    serves a recipe refusal, so the recipe layer and the authoring channel
    cannot disagree on when it applies."""
    causes = tuple(sorted(set(cause_codes)))
    return _CAUSE_REMEDIATIONS.get(causes[0]) if len(causes) == 1 else None


def recipe_diagnostic(
    code: str,
    *,
    phase: RecipePhase,
    path: str = "",
    target: Optional[str] = None,
    recipe_ids: Tuple[str, ...] = (),
    recipe_versions: Tuple[str, ...] = (),
    invocation_ids: Tuple[str, ...] = (),
    contribution_indexes: Tuple[int, ...] = (),
    available_versions: Tuple[str, ...] = (),
    cause_codes: Tuple[str, ...] = (),
) -> RecipeDiagnosticV1:
    """Build a diagnostic with STATIC message/remediation for ``code``.

    ``available_versions`` and ``cause_codes`` are sorted here rather than at
    each call site, so two runs that discovered the same facts in a different
    order still produce byte-identical diagnostics. A diagnostic whose ONLY
    cause has a per-cause remediation serves that one instead of the code's.
    """
    causes = tuple(sorted(set(cause_codes)))
    remediation = cause_remediation(causes)
    if remediation is None:
        remediation = _REMEDIATIONS.get(code, "Consult specs/TYPED_RECIPE_CONTRIBUTIONS_V1.md.")
    return RecipeDiagnosticV1(
        code=code,
        phase=phase,
        path=path,
        target=target,
        recipe_ids=tuple(recipe_ids),
        recipe_versions=tuple(recipe_versions),
        invocation_ids=tuple(invocation_ids),
        contribution_indexes=tuple(contribution_indexes),
        available_versions=tuple(sorted(available_versions)),
        cause_codes=causes,
        message=_MESSAGES.get(code, "recipe processing failed"),
        remediation=remediation,
    )


class RecipeError(Exception):
    """A recipe-layer failure carrying value-free diagnostics.

    ``__str__`` and ``__repr__`` deliberately render only codes and pointers.
    An exception's text reaches tracebacks and log records, so the usual
    "include the offending value for debuggability" instinct is exactly wrong
    here — the offending value is the thing that must not travel.
    """

    def __init__(self, diagnostics: Tuple[RecipeDiagnosticV1, ...]) -> None:
        self.diagnostics: Tuple[RecipeDiagnosticV1, ...] = tuple(diagnostics)
        super().__init__(self._render())

    def _render(self) -> str:
        return "; ".join(f"{d.code}@{d.path or '/'}" for d in self.diagnostics)

    def __repr__(self) -> str:  # noqa: D105
        return f"RecipeError({self._render()})"

    @property
    def primary_code(self) -> str:
        return self.diagnostics[0].code if self.diagnostics else RECIPE_CONTRIBUTION_INVALID


def recipe_error(code: str, **kwargs: Any) -> RecipeError:
    """One-diagnostic convenience constructor."""
    return RecipeError((recipe_diagnostic(code, **kwargs),))


def recipe_error_envelope(
    exc: RecipeError, *, registry_revision: Optional[str] = None
) -> Dict[str, Any]:
    """The MCP response envelope for a recipe failure.

    ``raw_xml_exposed``/``boomi_mutation`` are the repo's standing assertions on
    every authoring response; a recipe path reads and plans only, so both are
    ``False`` here by construction rather than by inspection.
    """
    return {
        "_success": False,
        "error_code": exc.primary_code,
        "error": "Recipe processing failed",
        "recipe_diagnostics": [d.model_dump(mode="json") for d in exc.diagnostics],
        "recipe_registry_revision": registry_revision,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
    }


def declared_loc_segments(
    model: Any, loc: Sequence[Any], *, request_path: bool = False
) -> Tuple[str, ...]:
    """The REQUEST path a pydantic ``loc`` addresses, walked against ``model``'s schema.

    pydantic inserts the variant tag of a discriminated union (``create``,
    ``reuse``) into ``loc``; the tag is a schema fact, not a request key, so it
    is dropped. A segment that is neither a declared property, an array index
    into a declared array, nor a declared tag was authored by the caller: the
    walk stops BEFORE it, so an unknown key's name is never served.

    ``request_path=True`` walks a path through the caller's RAW request instead
    (the forbidden-shape scan's path, #159 S1 CDX-r1-03). A raw path carries no
    variant tags, so none is dropped: a discriminated union admits a segment
    any of its variants declares, and a raw key that happens to spell a tag is
    judged like any other key.

    SEGMENTS ARE TYPED, end to end (#159 S1, CDX-r2-04): a sequence index is an
    ``int`` and a mapping key a ``str`` — as pydantic's ``loc`` already is, and
    as both raw-path producers (the forbidden-shape scan and the retired-spelling
    walk) now emit them. An ARRAY step is taken ONLY for an ``int``; a ``str``
    is accepted only as a declared property or a discriminator tag. A caller's
    object key ``"904417"`` where an array is declared is a key, not an index,
    and the walk stops before it (stringifying first made it look like one)."""
    try:
        schema = model.model_json_schema()
    except Exception:  # noqa: BLE001 — an empty path truncates the pointer, safely
        return ()
    defs = schema.get("$defs") or {}

    def flatten(node: Any) -> List[Mapping]:
        while isinstance(node, Mapping) and "$ref" in node:
            node = defs.get(str(node["$ref"]).rsplit("/", 1)[-1], {})
        if not isinstance(node, Mapping):
            return []
        if "discriminator" in node and not request_path:
            return [node]
        members = [m for key in ("anyOf", "oneOf", "allOf") for m in (node.get(key) or ())]
        if members:
            return [n for member in members for n in flatten(member)]
        return [node]

    current = flatten(schema)
    kept: List[str] = []
    for part in loc:
        is_index = isinstance(part, int) and not isinstance(part, bool)
        if not is_index and not isinstance(part, str):
            break  # neither an index nor a key: nothing declared can match it
        segment = str(part)
        if not request_path and not is_index and segment.startswith("function-"):
            continue
        following: List[Mapping] = []
        is_tag = False
        for node in current:
            if is_index:
                if node.get("items") is not None:
                    following.extend(flatten(node["items"]))
                continue
            mapping = {} if request_path else ((node.get("discriminator") or {}).get("mapping") or {})
            if segment in mapping:
                is_tag = True
                following.extend(flatten({"$ref": mapping[segment]}))
            elif segment in (node.get("properties") or {}):
                following.extend(flatten(node["properties"][segment]))
        if not following:
            break
        if not is_tag:
            kept.append(segment)
        current = following
    return tuple(kept)


__all__ = [
    "declared_loc_segments",
    "DIRECT_AUTHORING_PRODUCER",
    "RecipeDiagnosticV1",
    "RecipeError",
    "RecipePhase",
    "recipe_diagnostic",
    "recipe_error",
    "recipe_error_envelope",
]
