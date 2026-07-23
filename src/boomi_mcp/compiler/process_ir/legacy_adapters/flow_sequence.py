"""Production ``database_to_api_sync`` ``flow_sequence`` -> ProcessIR adapter.

Issue #139 (M12.4). Normalizes an already-validated composed ``flow_sequence``
config into a :class:`ProcessIRV1` plus one symbol requirement per authored
reference. The forward normalization is NOT re-implemented here: it reuses the
single translator :func:`legacy_flow_sequence_to_ir` (the #136 codec forward
core), fed a config projected to its known keys so the production adapter never
rejects a safe unknown field the legacy build path accepted-and-ignored (no
unknown-field tightening; ADR-001 backward compat) — the stripped keys are
recorded as ``compatibility_noop_paths`` instead.

The symbol requirements are derived from the compiled emission plan — the exact
``(component-id, component-type)`` pairs the emitter registry validates — rather
than a hand-walk of the config, so a role/kind can never be missed or mistyped.
Connector ``connector_type`` / ``action_type`` is DERIVED from the config
bindings (source, target, branch/decision leg targets) and rides on the
operation symbol, mirroring the #136 codec's ``_resolve_binding``.
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

_DP_PROFILE_COMPONENT_TYPE = {"json": "profile.json", "xml": "profile.xml"}

# Component id/ref keys the pre-#139 builder normalized with ``str(x).strip()``
# before writing XML (baseline `process_flow_builder.py:901-1088`). The strict
# ProcessIR reference validator rejects surrounding whitespace, so the adapter
# must reproduce that coercion or a validated whitespace-padded ref raises.
_ID_REF_KEYS = frozenset(
    {"connection_id", "operation_id", "map_ref", "document_cache_id", "profile_id"}
)


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
            elif k == "label" and v is not None and not isinstance(v, (dict, list)):
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
    # Coerce legacy-accepted non-canonical id/label values AFTER projection so the
    # config handed to the strict codec matches what the pre-#139 builder emitted.
    return _coerce_legacy_values(projected), noop


def _collect_binding_meta(config: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Map each connector ref (connection_id AND operation_id) to its derived
    ``connector_type`` / ``action_type``, from source, target, and every
    branch/decision leg target binding. Both ids are keyed (mirrors the #136
    parity oracle); lowering reads the metadata from the operation symbol only.

    Only the OPERATION symbol's connector family reaches emitted XML (lowering
    reads connector metadata from the operation symbol; the connection symbol's
    family is never read). So the conflict guard applies to OPERATION ids only,
    and compares the CANONICAL connector family — NOT the raw string and NOT the
    action_type. That way a single operation id bound to two genuinely different
    families (e.g. a database source and a REST target reusing one operation id —
    always semantically invalid, and reachable now that padded/unpadded ids
    normalize together) fails closed with ``LEGACY_ADAPTER_SEMANTIC_LOSS``
    (translated by the builder to ``PROCESS_XML_VALIDATION_FAILED``) rather than
    silently emit one binding with the other's family, while these VALID reuses
    are preserved: one connection hosting operations with different actions
    (action belongs to the operation, not the connection), and the same component
    referenced via equivalent aliases (``rest`` / ``rest_client`` / the canonical
    subtype all normalize to one family)."""
    # Lazy import (mirrors lowering._canonical_connector_metadata) so the adapter
    # does not eagerly pull in the 6k-line builder module.
    from ....categories.components.builders.process_flow_builder import (
        _canonical_connector_type,
    )

    meta: Dict[str, Dict[str, Any]] = {}
    seen_family: Dict[str, str] = {}

    def add(binding: Any, path: str) -> None:
        if not isinstance(binding, dict):
            return
        ct = binding.get("connector_type")
        at = binding.get("action_type")
        pair = {"connector_type": ct, "action_type": at}
        family = _canonical_connector_type(ct)
        for id_key in ("connection_id", "operation_id"):
            ref = binding.get(id_key)
            if isinstance(ref, str) and ref.strip():
                ref = ref.strip()
                if id_key == "operation_id":
                    if ref in seen_family and seen_family[ref] != family:
                        raise adapter_diagnostic(
                            LEGACY_ADAPTER_SEMANTIC_LOSS,
                            f"{path}/{id_key}",
                            "the same operation id is bound with conflicting "
                            "connector families across roles",
                        )
                    seen_family[ref] = family
                meta[ref] = pair

    def walk_steps(steps: Any, path: str) -> None:
        if not isinstance(steps, list):
            return
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            kind = str(step.get("kind") or "").strip()
            if kind == "branch":
                for j, leg in enumerate(step.get("legs") or []):
                    if isinstance(leg, dict):
                        add(leg.get("target"), f"{path}/{i}/legs/{j}/target")
                        walk_steps(leg.get("steps"), f"{path}/{i}/legs/{j}/steps")
            elif kind == "decision":
                walk_steps(step.get("true_steps"), f"{path}/{i}/true_steps")
                walk_steps(step.get("false_steps"), f"{path}/{i}/false_steps")

    add(config.get("source"), "/source")
    add(config.get("target"), "/target")
    walk_steps(config.get("flow_sequence"), "/flow_sequence")
    return meta


def _requirements_from_ir(
    ir: ProcessIRV1, binding_meta: Dict[str, Dict[str, str]]
) -> Tuple[LegacySymbolRequirementV1, ...]:
    """Derive the exact symbol requirements the emitter validates, from the
    compiled emission plan (never a config hand-walk)."""
    cfg = lower_process_ir_to_cfg(ir)
    # Sentinel table: component_id == ref, connector metadata for lowering.
    refs = _cfg_refs(cfg)
    sentinel = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type="sentinel",
                connector_type=(binding_meta.get(ref) or {}).get("connector_type"),
                action_type=(binding_meta.get(ref) or {}).get("action_type"),
            )
            for ref in sorted(refs)
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
    for cid in sorted(id_type):
        meta = binding_meta.get(cid) or {}
        requirements.append(
            LegacySymbolRequirementV1(
                role=role.get(cid, "ref"),
                ir_ref=cid,
                source_pointer="/flow_sequence",
                expected_component_type=id_type[cid],
                connector_type=meta.get("connector_type"),
                action_type=meta.get("action_type"),
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
    ir = legacy_flow_sequence_to_ir(projected)
    # Collect connector metadata from the PROJECTED config (same id coercion the
    # codec saw), so the meta keys match the stripped IR refs — otherwise a
    # whitespace-padded operation id would key the metadata under the padded
    # spelling and lowering would find no connector family on the operation symbol.
    binding_meta = _collect_binding_meta(projected)
    requirements = _requirements_from_ir(ir, binding_meta)
    return LegacyAdapterResultV1(
        process_ir=ir,
        symbol_requirements=requirements,
        compatibility_noop_paths=tuple(sorted(set(noop))),
        pipeline_view=None,
        pipeline_view_status="not_representable",
    )


__all__ = ["adapt_flow_sequence"]
