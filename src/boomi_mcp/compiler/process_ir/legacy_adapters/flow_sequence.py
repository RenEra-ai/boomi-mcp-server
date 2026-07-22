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

from ....models._process_ir_compat import (
    _BINDING_KEYS as _CODEC_BINDING_KEYS,
    _FLOW_ROOT_KEYS as _CODEC_FLOW_ROOT_KEYS,
    legacy_flow_sequence_to_ir,
)
from ....models.process_ir import ProcessIRV1
from ..contracts import ComponentSymbolV1, SymbolTableV1
from ..lowering import lower_cfg_to_emission_plan, lower_process_ir_to_cfg
from .contracts import LegacyAdapterResultV1, LegacySymbolRequirementV1

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
            binding: Dict[str, Any] = {}
            for bkey, bvalue in value.items():
                if bkey in _BINDING_KEYS:
                    binding[bkey] = bvalue
                else:
                    noop.append(f"/{key}/{bkey}")
            projected[key] = binding
        else:
            projected[key] = value
    return projected, noop


def _collect_binding_meta(config: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Map each connector ref (connection_id AND operation_id) to its derived
    ``connector_type`` / ``action_type``, from source, target, and every
    branch/decision leg target binding. Both ids are keyed (mirrors the #136
    parity oracle); lowering reads the metadata from the operation symbol only."""
    meta: Dict[str, Dict[str, str]] = {}

    def add(binding: Any) -> None:
        if not isinstance(binding, dict):
            return
        ct = binding.get("connector_type")
        at = binding.get("action_type")
        pair = {"connector_type": ct, "action_type": at}
        for id_key in ("connection_id", "operation_id"):
            ref = binding.get(id_key)
            if isinstance(ref, str) and ref.strip():
                meta[ref] = pair

    def walk_steps(steps: Any) -> None:
        if not isinstance(steps, list):
            return
        for step in steps:
            if not isinstance(step, dict):
                continue
            kind = str(step.get("kind") or "").strip()
            if kind == "branch":
                for leg in step.get("legs") or []:
                    if isinstance(leg, dict):
                        add(leg.get("target"))
                        walk_steps(leg.get("steps"))
            elif kind == "decision":
                walk_steps(step.get("true_steps"))
                walk_steps(step.get("false_steps"))

    add(config.get("source"))
    add(config.get("target"))
    walk_steps(config.get("flow_sequence"))
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
    binding_meta = _collect_binding_meta(config)
    requirements = _requirements_from_ir(ir, binding_meta)
    return LegacyAdapterResultV1(
        process_ir=ir,
        symbol_requirements=requirements,
        compatibility_noop_paths=tuple(sorted(set(noop))),
        pipeline_view=None,
        pipeline_view_status="not_representable",
    )


__all__ = ["adapt_flow_sequence"]
