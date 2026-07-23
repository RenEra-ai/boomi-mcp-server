"""Production ``database_to_api_sync`` ``flow_sequence`` -> ProcessIR adapter.

Issue #139 (M12.4). Normalizes an already-validated composed ``flow_sequence``
config into a :class:`ProcessIRV1` plus one symbol requirement per authored
reference. The forward normalization is NOT re-implemented here: it reuses the
single translator :func:`legacy_flow_sequence_to_ir` (the #136 codec forward
core), fed a config projected to its known keys so the production adapter never
rejects a safe unknown field the legacy build path accepted-and-ignored (no
unknown-field tightening; ADR-001 backward compat) — the stripped keys are
recorded as ``compatibility_noop_paths`` instead.

Since #139B every id/ref occurrence is rewritten to an OCCURRENCE-SCOPED alias
(``$ref:legacy.adapter:<RFC6901-pointer>``) before the codec runs, and each
requirement carries a ``legacy_selector`` (the original id) the caller resolves.
Distinct aliases can therefore resolve to the SAME component id while keeping
their own type + connector metadata, so one id reused across roles (even
incompatible ones, e.g. a ``map_ref`` and a ``document_cache_id``) round-trips
byte-faithfully instead of collapsing into one symbol — this REPLACES the #139A
connector-conflict guard (no collision is possible). The requirements are still
derived from the compiled emission plan (the exact ``(ref, component-type)`` pairs
the emitter validates); connector ``connector_type`` / ``action_type`` rides on
the operation requirement, mirroring the #136 codec's ``_resolve_binding``. A dead
root target is excluded structurally: the codec drops it, so its alias never
reaches the CFG and produces no requirement.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ....errors import LEGACY_ADAPTER_SEMANTIC_LOSS
from ....models._process_ir_compat import (
    _BINDING_KEYS as _CODEC_BINDING_KEYS,
    _FLOW_ROOT_KEYS as _CODEC_FLOW_ROOT_KEYS,
    legacy_flow_sequence_to_ir,
)
from ....models.process_ir import ProcessIRV1
from ..contracts import ComponentSymbolV1, SymbolTableV1
from ..lowering import lower_cfg_to_emission_plan, lower_process_ir_to_cfg
from .contracts import (
    LegacyAdapterResultV1,
    LegacySymbolRequirementV1,
    adapter_diagnostic,
)

# The frozen codec's own known-key sets (imported, not re-declared, so the two
# cannot drift). ``source`` / ``target`` / ``flow_sequence`` feed the IR;
# ``transform`` (passthrough only) / ``reliability`` (no-op only) are inert
# siblings; ``description`` is envelope data the codec also tolerates.
_CODEC_ROOT_KEYS = _CODEC_FLOW_ROOT_KEYS
_BINDING_KEYS = _CODEC_BINDING_KEYS
# Envelope-owned root keys the legacy build path consumes OUTSIDE the codec (the
# component assembler / processOverrides), so they are neither codec input nor a
# safe-ignored no-op. Stripped from the codec input, never recorded as noop.
_ENVELOPE_ROOT_KEYS = frozenset({"process_extensions"})
# Fields the codec actually reads from the lenient inert sibling blocks; any other
# key there is accepted-and-ignored (recorded as a compatibility no-op path).
_TRANSFORM_CONSUMED = frozenset({"mode"})
_RELIABILITY_CONSUMED = frozenset(
    {"retry_count", "dlq", "catch_exception", "catch_notify"}
)
_DLQ_CONSUMED = frozenset({"mode"})

_DP_PROFILE_COMPONENT_TYPE = {"json": "profile.json", "xml": "profile.xml"}

# Component id/ref keys the pre-#139 builder normalized with ``str(x).strip()``
# before writing XML (baseline `process_flow_builder.py:901-1088`). The strict
# ProcessIR reference validator rejects surrounding whitespace, so the adapter
# must reproduce that coercion or a validated whitespace-padded ref raises.
_ID_REF_KEYS = frozenset(
    {"connection_id", "operation_id", "map_ref", "document_cache_id", "profile_id"}
)
# String-typed IR fields the pre-#139 builder wrote with ``str(x or "")`` (labels
# and free-text/operand fields). The strict ProcessIR string validators reject a
# validated-but-non-string value (e.g. a numeric decision-operand default_value),
# so the adapter reproduces the same coercion. NOT ids (those also .strip()) and
# NOT structural discriminators (kind/value_type/operation/mode).
_TEXT_KEYS = frozenset(
    {
        "label",
        "message_text",
        "name",
        "comparison",
        "message_template",
        "title",
        "property_name",
        "default_value",
        "static_value",
        "value",
        "element_id",
        "element_name",
        "property_id",
    }
)
# Reserved internal alias namespace for occurrence-scoped IR references (#139B).
# The alias embeds the RFC 6901 pointer to the legacy field and NO authored value;
# it is a valid ``$ref:`` ComponentRefV1 token (whitespace-free key) that resolves
# to a real component id through the symbol table before emission — it never
# appears in emitted XML.
_ALIAS_PREFIX = "$ref:legacy.adapter:"


def _rfc6901(token: str) -> str:
    """Escape one RFC 6901 reference-token component (``~`` -> ``~0``, ``/`` -> ``~1``)."""
    return token.replace("~", "~0").replace("/", "~1")


def _project_binding(
    binding: Dict[str, Any], path: str, noop: List[str]
) -> Dict[str, Any]:
    """Keep only the binding keys the codec knows; record extras as no-op paths.

    The legacy ``_validate_*_binding`` accepts and ignores unknown binding keys
    (a lenient location, §2.7), so stripping-and-recording them preserves
    acceptance instead of tightening the surface."""
    kept: Dict[str, Any] = {}
    for bkey, bvalue in binding.items():
        if bkey in _BINDING_KEYS:
            kept[bkey] = bvalue
        else:
            noop.append(f"{path}/{bkey}")
    return kept


def _project_steps(steps: Any, path: str, noop: List[str]) -> Any:
    """Project the lenient binding location inside the flow_sequence: a branch
    leg ``target`` (validated by the same lenient ``_validate_target_binding``).
    Step keys themselves are strict on both the validator and the codec, so they
    are left untouched; only leg target bindings and nested decision arms are
    recursed."""
    if not isinstance(steps, list):
        return steps
    out = []
    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            out.append(step)
            continue
        kind = str(step.get("kind") or "").strip()
        new_step = dict(step)
        if kind == "branch" and isinstance(step.get("legs"), list):
            new_legs = []
            for j, leg in enumerate(step["legs"]):
                if isinstance(leg, dict):
                    new_leg = dict(leg)
                    if isinstance(leg.get("target"), dict):
                        new_leg["target"] = _project_binding(
                            leg["target"], f"{path}/{i}/legs/{j}/target", noop
                        )
                    if isinstance(leg.get("steps"), list):
                        new_leg["steps"] = _project_steps(
                            leg["steps"], f"{path}/{i}/legs/{j}/steps", noop
                        )
                    new_legs.append(new_leg)
                else:
                    new_legs.append(leg)
            new_step["legs"] = new_legs
        elif kind == "decision":
            for arm in ("true_steps", "false_steps"):
                if isinstance(step.get(arm), list):
                    new_step[arm] = _project_steps(
                        step[arm], f"{path}/{i}/{arm}", noop
                    )
        out.append(new_step)
    return out


def _coerce_legacy_values(node: Any) -> Any:
    """Reproduce the pre-#139 builder's value coercion so a validated but
    non-canonical value survives strict ProcessIR parsing byte-identically:
    ``str(x or "").strip()`` on component id/ref keys and ``str(x or "")`` on
    labels — the EXACT idiom the old builder used (baseline
    `process_flow_builder.py:901-1088`), so a falsy non-string value (0/False)
    maps to "" identically. A no-op on already-canonical values (so every
    existing golden is unchanged)."""
    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            if (
                k in _ID_REF_KEYS
                and isinstance(v, (str, int, float))
                and not isinstance(v, bool)
            ):
                out[k] = str(v or "").strip()
            elif (
                k in _TEXT_KEYS
                and v is not None
                and not isinstance(v, (dict, list, bool))
            ):
                out[k] = str(v or "")
            else:
                out[k] = _coerce_legacy_values(v)
        return out
    if isinstance(node, list):
        return [_coerce_legacy_values(item) for item in node]
    return node


def _project(config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Project a validated config to the codec's known keys; collect noop paths."""
    noop: List[str] = []
    projected: Dict[str, Any] = {}
    for key, value in config.items():
        if key in _ENVELOPE_ROOT_KEYS:
            # Envelope-owned (handled by the component assembler), never codec
            # input and never a no-op.
            continue
        if key not in _CODEC_ROOT_KEYS:
            noop.append(f"/{key}")
            continue
        if key in ("source", "target") and isinstance(value, dict):
            projected[key] = _project_binding(value, f"/{key}", noop)
        elif key == "flow_sequence" and isinstance(value, list):
            projected[key] = _project_steps(value, "/flow_sequence", noop)
        else:
            projected[key] = value
            # The lenient inert siblings (a passthrough transform, a no-op
            # reliability block) are passed to the codec wholesale but only their
            # consumed fields are read; record the ignored extras so
            # compatibility_noop_paths is complete (the secret/reference walkers
            # already ran over them in validate_config).
            if key == "transform" and isinstance(value, dict):
                noop.extend(f"/transform/{k}" for k in sorted(set(value) - _TRANSFORM_CONSUMED))
            elif key == "reliability" and isinstance(value, dict):
                noop.extend(f"/reliability/{k}" for k in sorted(set(value) - _RELIABILITY_CONSUMED))
                dlq = value.get("dlq")
                if isinstance(dlq, dict):
                    noop.extend(f"/reliability/dlq/{k}" for k in sorted(set(dlq) - _DLQ_CONSUMED))
    # Coerce legacy-accepted non-canonical id/label values AFTER projection so the
    # config handed to the strict codec matches what the pre-#139 builder emitted.
    return _coerce_legacy_values(projected), sorted(set(noop))


def _alias_refs(
    node: Any, pointer: str, facts: Dict[str, Dict[str, Any]]
) -> Any:
    """Rewrite every component id/ref occurrence to an occurrence-scoped alias.

    Walks the projected+coerced payload; for each ``_ID_REF_KEYS`` occurrence it
    substitutes ``$ref:legacy.adapter:<pointer>`` and records an alias fact
    (original ``legacy_selector``, exact source pointer, and — only for an
    ``operation_id`` — the sibling connector metadata). Because each alias is
    unique by path, two bindings that reuse ONE component id become two distinct
    IR references, so lowering never collapses them into one symbol (#139B)."""
    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            child = f"{pointer}/{_rfc6901(str(k))}"
            if k in _ID_REF_KEYS and isinstance(v, str) and v:
                alias = f"{_ALIAS_PREFIX}{child}"
                if alias in facts:  # unreachable: pointers are unique
                    raise adapter_diagnostic(
                        LEGACY_ADAPTER_SEMANTIC_LOSS,
                        child,
                        "duplicate role/path alias generated for one occurrence",
                    )
                fact: Dict[str, Any] = {
                    "legacy_selector": v,
                    "source_pointer": child,
                    "connector_type": None,
                    "action_type": None,
                }
                if k == "operation_id":
                    fact["connector_type"] = node.get("connector_type")
                    fact["action_type"] = node.get("action_type")
                facts[alias] = fact
                out[k] = alias
            else:
                out[k] = _alias_refs(v, child, facts)
        return out
    if isinstance(node, list):
        return [_alias_refs(item, f"{pointer}/{i}", facts) for i, item in enumerate(node)]
    return node


def _requirements_from_ir(
    ir: ProcessIRV1, facts: Dict[str, Dict[str, Any]]
) -> Tuple[LegacySymbolRequirementV1, ...]:
    """Derive the exact symbol requirements the emitter validates, from the
    compiled emission plan of the ALIAS-bearing IR (never a config hand-walk).

    Every reference the emitter sees is an occurrence-scoped alias; its type comes
    from the plan and its ``legacy_selector`` + connector metadata come from the
    recorded alias fact. A live alias with no recorded fact means the codec emitted
    a reference outside ``_ID_REF_KEYS`` (a future vocabulary addition) that would
    silently reintroduce raw-id collapse — fail closed."""
    cfg = lower_process_ir_to_cfg(ir)
    aliases = _cfg_refs(cfg)
    # Fail closed BEFORE lowering: every live CFG reference must have a recorded
    # alias fact. A future codec vocabulary addition producing a ref from a field
    # outside ``_ID_REF_KEYS`` would otherwise reach lowering as a raw id and raise
    # a generic compile error (PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID) —
    # silently reintroducing raw-id collapse. This is the plan's value-free guard.
    missing = [a for a in sorted(aliases) if a not in facts]
    if missing:
        raise adapter_diagnostic(
            LEGACY_ADAPTER_SEMANTIC_LOSS,
            missing[0] if missing[0].startswith(_ALIAS_PREFIX) else "/",
            "a live IR reference has no recorded legacy selector",
        )
    # Sentinel table keyed by ALIAS (component_id == alias); connector metadata for
    # operation aliases so lowering canonicalizes the operation's family.
    sentinel = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=alias,
                component_id=alias,
                component_type="sentinel",
                connector_type=(facts.get(alias) or {}).get("connector_type"),
                action_type=(facts.get(alias) or {}).get("action_type"),
            )
            for alias in sorted(aliases)
        )
    )
    plan = lower_cfg_to_emission_plan(cfg, sentinel)

    id_type: Dict[str, str] = {}
    role: Dict[str, str] = {}
    for node in plan.nodes:
        e = node.emitter_input
        k = e.emitter_kind
        if k in ("connectoraction_source", "connectoraction_target"):
            id_type[e.connection_id] = "connector-settings"
            id_type[e.operation_id] = "connector-action"
            role.setdefault(e.connection_id, f"{k}.connection")
            role.setdefault(e.operation_id, f"{k}.operation")
        elif k == "map":
            id_type[e.map_id] = "transform.map"
            role.setdefault(e.map_id, "map")
        elif k in ("doccacheload", "doccacheretrieve", "doccacheremove"):
            id_type[e.document_cache_id] = "documentcache"
            role.setdefault(e.document_cache_id, k)
        elif k == "processcall":
            id_type[e.process_id] = "process"
            role.setdefault(e.process_id, "processcall")
        elif k == "dataprocess":
            for st in e.steps:
                pid = getattr(st, "profile_id", "")
                if pid:
                    kind = str(getattr(st, "profile_type", "")).strip().lower()
                    id_type[pid] = _DP_PROFILE_COMPONENT_TYPE.get(kind, "profile.json")
                    role.setdefault(pid, "dataprocess.profile")
        elif k == "setproperties_step":
            for src in e.source_values:
                if src.value_type == "profile":
                    id_type[src.profile_id] = src.profile_type
                    role.setdefault(src.profile_id, "setproperties.profile")

    requirements = []
    for alias in sorted(id_type):
        # id_type keys are a subset of `aliases`, all validated present above.
        fact = facts[alias]
        expected = id_type[alias]
        is_operation = expected == "connector-action"
        requirements.append(
            LegacySymbolRequirementV1(
                role=role.get(alias, "ref"),
                ir_ref=alias,
                legacy_selector=fact["legacy_selector"],
                source_pointer=fact["source_pointer"],
                expected_component_type=expected,
                connector_type=fact["connector_type"] if is_operation else None,
                action_type=fact["action_type"] if is_operation else None,
            )
        )
    return tuple(requirements)


def _cfg_refs(cfg: Any) -> set:
    refs = set()
    for node in cfg.nodes:
        s = node.semantic
        for field in (
            "connection_ref",
            "operation_ref",
            "map_ref",
            "cache_ref",
            "process_ref",
        ):
            value = getattr(s, field, None)
            if value:
                refs.add(value)
        for step in getattr(s, "steps", ()) or ():
            if getattr(step, "profile_ref", None):
                refs.add(step.profile_ref)
        for src in getattr(s, "source_values", ()) or ():
            if getattr(src, "profile_ref", None):
                refs.add(src.profile_ref)
    return refs


def adapt_flow_sequence(config: Dict[str, Any]) -> LegacyAdapterResultV1:
    """Normalize a validated database_to_api_sync flow_sequence config."""
    projected, noop = _project(config)
    # Validate the RAW (post-coercion) refs through the codec first: aliasing would
    # otherwise mask a malformed original ref and change #139A error ordering.
    legacy_flow_sequence_to_ir(projected)
    # Rewrite every id/ref occurrence to a role/path-scoped alias, then produce the
    # final IR from the aliased copy through the UNCHANGED codec.
    facts: Dict[str, Dict[str, Any]] = {}
    aliased = _alias_refs(projected, "", facts)
    ir = legacy_flow_sequence_to_ir(aliased)
    requirements = _requirements_from_ir(ir, facts)
    return LegacyAdapterResultV1(
        process_ir=ir,
        symbol_requirements=requirements,
        compatibility_noop_paths=tuple(sorted(set(noop))),
        pipeline_view=None,
        pipeline_view_status="not_representable",
    )


__all__ = ["adapt_flow_sequence"]
