"""Production ``sync_pipeline`` -> ProcessIR adapter (issue #139 M12.4, slice C).

Normalizes the **lowered** linear core a ``sync_pipeline`` config produces
(``{process_kind, source, transform, target}`` plus envelope keys) into a
:class:`ProcessIRV1` plus the component-symbol requirements the canonical
compiler/emitter needs.

**It consumes the LOWERED core, not the raw pipeline graph.**
``SyncPipelineBuilder.lower_config`` is the only sync_pipeline-exclusive code in
the repo and stays the single source of truth for the dialect's semantics — the
accepted stage chains, the connector-family agreement guards, and the default
verb per stage kind all live there. Re-deriving any of that here would create
exactly the second semantic compiler ADR-001 exists to prevent. A useful
consequence: this adapter is a *linear single-shape core* adapter, so #140 can
promote it to the ordinary ``database_to_api_sync`` dialect unchanged once that
dialect's four capability gaps (start_listen, dynamic_path, catcherrors, notify)
close.

Envelope data (``description``, ``folder_name``, ``process_extensions``) is NOT
represented here — the component assembler owns it, exactly as the legacy path
does. Per ADR-001 §6 the IR carries no XML, layout, shape ids, CFG edges,
credentials, or raw legacy config, and every diagnostic is value-free.

**Listener chains are NOT handled here (#140).** A WSS listener source is not
representable in ProcessIR v1 at three independent layers: there is no
``start_listen`` emitter key, the compiler fails closed for a listener source,
and ``SourceEndpointV1.connection_ref`` is required while a lowered listener
binding carries no ``connection_id`` at all. The builder routes those chains to
the legacy renderer *before* calling this adapter; the guard below is the second,
independent gate so a future caller cannot route one in past the first.

Refs are **occurrence-scoped aliases** (#139B): every id slot becomes
``$ref:legacy.adapter:<RFC 6901 pointer>`` and the requirement carries the real
id as its ``legacy_selector``. Distinct aliases may resolve to the same component
id, so a config that reuses one id across incompatible roles (say
``map_ref == source.connection_id``) still round-trips byte-faithfully instead of
collapsing into one incompatible symbol.
"""

from __future__ import annotations

from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from ....errors import LEGACY_ADAPTER_SEMANTIC_LOSS, LEGACY_ADAPTER_UNSUPPORTED_KIND
from ....models.process_ir import parse_process_ir_v1
from .contracts import (
    LEGACY_ADAPTER_ALIAS_PREFIX,
    LegacyAdapterResultV1,
    LegacySymbolRequirementV1,
    adapter_diagnostic,
)

# Every root key `lower_config` can emit. Unlike the wrapper adapter (whose legacy
# path accepted-and-ignored unknown root keys, so they are recorded as no-ops),
# this dialect is strictly fail-closed: `lower_config` builds its result from a
# literal dict, so an unknown root key can only reach here through a direct
# `build()` bypass carrying a hand-crafted core — where it would be one of
# `flow_sequence` / `reliability` / `branch` / `decision` / `flow_control` /
# `return_documents`, every one of which CHANGES the emitted XML. Silently
# dropping any of them is the mis-shaping this boundary exists to prevent.
_KNOWN_ROOT_KEYS = frozenset(
    {"process_kind", "source", "transform", "target", "description", "process_extensions"}
)
# Consumed by the component assembler, outside the IR. Stripped, and deliberately
# NOT recorded as no-op paths (the #139A `_ENVELOPE_ROOT_KEYS` precedent): a key
# the envelope honours was never dropped.
_ENVELOPE_ROOT_KEYS = frozenset({"description", "process_extensions"})
_BINDING_KEYS = frozenset(
    {"connector_type", "action_type", "connection_id", "operation_id", "label"}
)
_SUPPORTED_TRANSFORM_MODES = frozenset({"passthrough", "map_ref"})


class _PointerBases(NamedTuple):
    """Where in the CALLER'S config each slot's fields actually live.

    ``source_pointer`` is contractually the EXACT RFC 6901 pointer to the legacy
    field a reference came from, and the alias embeds that same pointer. Both are
    therefore relative to whatever document the caller handed in — which differs by
    entry point: the builder passes the lowered core (``/source/...``), while the
    registry entry passes the raw dialect config, where the very same values live
    under ``/pipeline/stages/<index>/config/...``.

    Injecting the bases (rather than rewriting pointers afterwards) keeps one table
    feeding aliases, requirements AND diagnostics, so the three can never disagree.
    """

    source: str
    transform: str
    target: str
    #: The exact field that identifies an entry as a listener.
    listener: str


_CORE_BASES = _PointerBases(
    source="/source",
    transform="/transform",
    target="/target",
    listener="/source/connector_type",
)

# Which stage kinds fill which slot of the lowered linear core.
_SOURCE_STAGE_KINDS = frozenset({"read", "fetch", "listener"})
_TARGET_STAGE_KINDS = frozenset({"send", "write"})


def _raw_pointer_bases(config: Dict[str, Any]) -> _PointerBases:
    """Locate each slot's originating stage in a RAW sync_pipeline config.

    Stages are matched by KIND, never by position: the ``stages`` list is authored
    in arbitrary order and the flow order comes from ``dependencies``, so using a
    flow index would yield pointers that RESOLVE but name the wrong stage — a
    strictly worse failure than not resolving at all.

    Only ever called on a config ``lower_config`` already accepted, which
    guarantees exactly one source, at most one map and one target; the per-slot
    fallback keeps it total anyway.
    """
    stages = (config.get("pipeline") or {}).get("stages") or []
    index: Dict[str, int] = {}
    for i, stage in enumerate(stages):
        if not isinstance(stage, dict):
            continue
        kind = str(stage.get("kind") or "").strip().lower()
        if kind in _SOURCE_STAGE_KINDS:
            index.setdefault("source", i)
        elif kind == "map":
            index.setdefault("transform", i)
        elif kind in _TARGET_STAGE_KINDS:
            index.setdefault("target", i)

    def base(slot: str, fallback: str) -> str:
        i = index.get(slot)
        return f"/pipeline/stages/{i}/config" if i is not None else fallback

    source = base("source", _CORE_BASES.source)
    return _PointerBases(
        source=source,
        transform=base("transform", _CORE_BASES.transform),
        target=base("target", _CORE_BASES.target),
        # A raw listener stage is identified by its PRIMITIVE (``wss_listen``).
        # `connector_type` is accepted on a listener stage but wholly inert -- it
        # does not select the listener path and every value emits identical XML --
        # so pointing a diagnostic at it would misdirect the reader.
        listener=(
            f"{source}/primitive" if "source" in index else _CORE_BASES.listener
        ),
    )


def _require_dict(value: Any, pointer: str, what: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise adapter_diagnostic(
            LEGACY_ADAPTER_SEMANTIC_LOSS, pointer, f"{what} must be a JSON object"
        )
    return value


def _coerce_id(value: Any) -> str:
    """The legacy id idiom: ``str(x or "").strip()`` (pfb linear params block)."""
    return str(value or "").strip()


def _label_field(block: Dict[str, Any]) -> Dict[str, str]:
    """The legacy label idiom: present only when authored, coerced with ``str(x or "")``.

    Reproduced exactly so a validated non-string label (e.g. ``7``) survives strict
    ProcessIR parsing byte-identically, and a falsy ``0``/``False`` maps to ``""``.
    An absent label yields no key at all, which the emitter renders as ``userlabel=""`` —
    the same bytes as legacy's unconditional ``str(x or "")`` over a missing key.
    """
    return {"label": str(block.get("label") or "")} if block.get("label") is not None else {}


def _check_binding(binding: Dict[str, Any], pointer: str) -> None:
    extra = sorted(set(binding) - _BINDING_KEYS)
    if extra:
        # `dynamic_path` is the one that matters in practice: it inserts a Set
        # Properties shape BEFORE the connector and shifts every downstream index,
        # and ProcessIR v1 cannot express it (#139/#140). Reject rather than emit a
        # silently different flow.
        raise adapter_diagnostic(
            LEGACY_ADAPTER_UNSUPPORTED_KIND,
            f"{pointer}/{extra[0]}",
            "connector binding key is not representable in ProcessIR v1",
        )


def _is_listener(binding: Dict[str, Any]) -> bool:
    """Would the canonical compiler REFUSE this source as a listener entry?

    Deliberately the compiler's own ``LISTENER_CONNECTOR_TYPES``, NOT the builder's
    ``_resolve_wss_connector_type``. The two gates mirror different things on
    purpose: the builder's ROUTING gate must agree with the LEGACY selector (it
    decides which renderer runs, so disagreeing there would mis-shape a real
    listener), while this REFUSAL gate must agree with what the canonical chain can
    represent. The compiler's set is a strict superset — it also holds ``wssserver``
    and ``listener`` — so refusing on it converts a deep
    ``PROCESS_IR_CAPABILITY_UNSUPPORTED`` raised during lowering into a precise
    adapter diagnostic pointing at ``/source/connector_type``.

    No reachable config can tell the two apart: ``_check_source_connector_family``
    admits only the ``wss`` aliases, and the adapter is reached only through
    ``lower_config``. The distinction matters for #140, which promotes this adapter
    to a dialect whose input is not pre-filtered that way.
    """
    from ..contracts import LISTENER_CONNECTOR_TYPES

    connector_type = binding.get("connector_type")
    if not isinstance(connector_type, str):
        return False
    return connector_type.strip().lower() in LISTENER_CONNECTOR_TYPES


def _binding_slots(
    binding: Dict[str, Any], base: str, kind: str
) -> Tuple[Dict[str, Any], List[LegacySymbolRequirementV1]]:
    """Build one ``source``/``target`` IR node plus its two symbol requirements."""
    emitter_kind = f"connectoraction_{kind}"
    node = {"kind": kind, **_label_field(binding)}
    requirements: List[LegacySymbolRequirementV1] = []
    for field, ref_key, component_type in (
        ("connection_id", "connection_ref", "connector-settings"),
        ("operation_id", "operation_ref", "connector-action"),
    ):
        pointer = f"{base}/{field}"
        selector = _coerce_id(binding.get(field))
        if not selector:
            # Without this the empty selector fails LegacySymbolRequirementV1's
            # min_length as a RAW pydantic ValidationError, which is not in the
            # cut-over's caught tuple and would escape to a public caller.
            raise adapter_diagnostic(
                LEGACY_ADAPTER_SEMANTIC_LOSS,
                pointer,
                "connector binding is missing a resolved component id",
            )
        node[ref_key] = LEGACY_ADAPTER_ALIAS_PREFIX + pointer
        is_operation = component_type == "connector-action"
        requirements.append(
            LegacySymbolRequirementV1(
                role=f"{emitter_kind}.{'operation' if is_operation else 'connection'}",
                ir_ref=LEGACY_ADAPTER_ALIAS_PREFIX + pointer,
                legacy_selector=selector,
                source_pointer=pointer,
                expected_component_type=component_type,
                # Connector metadata rides ONLY on the operation requirement, so
                # lowering canonicalizes the operation's family — mirroring the
                # flow_sequence adapter and the #136 codec's `_resolve_binding`.
                connector_type=binding.get("connector_type") if is_operation else None,
                action_type=binding.get("action_type") if is_operation else None,
            )
        )
    return node, requirements


def _map_slot(
    transform: Dict[str, Any], base: str
) -> Tuple[Optional[Dict[str, Any]], List[LegacySymbolRequirementV1]]:
    """Build the optional ``map_ref`` node. ``passthrough`` appends NOTHING."""
    mode = str(transform.get("mode") or "passthrough").strip().lower()
    if mode not in _SUPPORTED_TRANSFORM_MODES:
        raise adapter_diagnostic(
            LEGACY_ADAPTER_UNSUPPORTED_KIND,
            f"{base}/mode",
            "transform mode is not representable for this dialect",
        )
    if mode == "passthrough":
        # Matches the legacy transform ladder, whose default arm appends no shape.
        return None, []
    # `_lower_map_stage` always writes `map_ref`; `map_id` is the legacy sibling
    # spelling, kept for totality on a direct build() bypass. Value resolution
    # mirrors the legacy ladder (`map_ref or map_id`, truthiness), and the pointer
    # names whichever key actually SUPPLIED it — falling back, when neither carries
    # a usable value, to the key the caller actually wrote, so an empty authored
    # `map_ref` is reported at `/transform/map_ref` and not at a key that was never
    # written (wrapper `selector_key` precedent).
    if transform.get("map_ref"):
        key = "map_ref"
    elif transform.get("map_id"):
        key = "map_id"
    else:
        key = "map_id" if "map_id" in transform and "map_ref" not in transform else "map_ref"
    pointer = f"{base}/{key}"
    selector = _coerce_id(transform.get("map_ref") or transform.get("map_id"))
    if not selector:
        raise adapter_diagnostic(
            LEGACY_ADAPTER_SEMANTIC_LOSS,
            pointer,
            "map transform is missing a resolved map component id",
        )
    node = {
        "kind": "map_ref",
        "map_ref": LEGACY_ADAPTER_ALIAS_PREFIX + pointer,
        **_label_field(transform),
    }
    return node, [
        LegacySymbolRequirementV1(
            role="map",
            ir_ref=LEGACY_ADAPTER_ALIAS_PREFIX + pointer,
            legacy_selector=selector,
            source_pointer=pointer,
            expected_component_type="transform.map",
        )
    ]


def adapt_sync_pipeline(
    config: Dict[str, Any], *, pointer_bases: _PointerBases = _CORE_BASES
) -> LegacyAdapterResultV1:
    """Normalize a lowered sync_pipeline linear core into IR + requirements.

    ``config`` is the output of ``SyncPipelineBuilder.lower_config`` — already
    validated, with every ``SYNC_PIPELINE_*`` structural error raised before this
    is reached. Every failure here is therefore an internal parity defect, raised
    as a value-free :class:`LegacyAdapterError` for the caller to translate into
    its existing external error family.

    ``pointer_bases`` says where the caller's own document keeps these fields, so
    aliases, ``source_pointer`` values and diagnostics all name a field that really
    exists in what the caller passed. It defaults to the lowered core's own layout;
    :func:`adapt_sync_pipeline_config` supplies raw-config bases instead.
    """
    _require_dict(config, "/", "lowered sync_pipeline config")
    unknown = sorted(set(config) - _KNOWN_ROOT_KEYS)
    if unknown:
        raise adapter_diagnostic(
            LEGACY_ADAPTER_UNSUPPORTED_KIND,
            f"/{unknown[0]}",
            "root key is not representable in ProcessIR v1 for this dialect",
        )

    source = _require_dict(
        config.get("source") or {}, pointer_bases.source, "source binding"
    )
    target = _require_dict(
        config.get("target") or {}, pointer_bases.target, "target binding"
    )
    transform = _require_dict(
        config.get("transform") or {"mode": "passthrough"},
        pointer_bases.transform,
        "transform",
    )

    if _is_listener(source):
        # Gate 2. The builder routes listener chains to the legacy renderer before
        # reaching here (Gate 1); this is the independent backstop, so a listener
        # can never be silently mis-shaped as a start_noaction + connectoraction
        # pair instead of the fused start_listen shape.
        raise adapter_diagnostic(
            LEGACY_ADAPTER_UNSUPPORTED_KIND,
            pointer_bases.listener,
            "listener entry is not representable in ProcessIR v1 — the legacy path "
            "fuses the start and connector shapes (#140)",
        )

    _check_binding(source, pointer_bases.source)
    _check_binding(target, pointer_bases.target)

    source_node, requirements = _binding_slots(source, pointer_bases.source, "source")
    steps: List[Dict[str, Any]] = [source_node]

    map_node, map_requirements = _map_slot(transform, pointer_bases.transform)
    if map_node is not None:
        steps.append(map_node)
        requirements.extend(map_requirements)

    target_node, target_requirements = _binding_slots(
        target, pointer_bases.target, "target"
    )
    steps.append(target_node)
    requirements.extend(target_requirements)

    # Always a Stop: `return_documents` is gated off for this dialect, so the
    # legacy terminal selector always resolves to stop(continue_=True).
    steps.append({"kind": "stop"})

    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": steps}})
    return LegacyAdapterResultV1(
        process_ir=ir,
        symbol_requirements=tuple(requirements),
        # Always empty, by construction: the dialect's config gate is a strict
        # allow-list at every level (root, PipelineSpec/StageSpec/PipelineEdgeSpec
        # are all `extra="forbid"`, and the per-stage key sets are allow-lists), so
        # there is no accepted-and-ignored key to record. Envelope keys are stripped
        # rather than dropped. A failure of the test pinning this means someone
        # loosened the config gate.
        compatibility_noop_paths=(),
        pipeline_view=None,
        pipeline_view_status="not_representable",
    )


def adapt_sync_pipeline_config(config: Dict[str, Any]) -> LegacyAdapterResultV1:
    """Registry entry point: a RAW validated ``sync_pipeline`` config -> IR.

    Every adapter in the registry takes the raw, already-validated config for ITS
    OWN dialect (`adapt_wrapper_subprocess` takes one carrying ``process_calls``,
    `adapt_flow_sequence` one carrying ``flow_sequence``). ``adapt_sync_pipeline``
    deliberately takes the *lowered* core instead — the shape the builder already
    holds by the time it reaches the cut-over, and the shape #140 will promote to
    the ordinary ``database_to_api_sync`` dialect — so registering it directly
    would break that contract: a caller handing `adapter_for("sync_pipeline")` a
    real sync_pipeline config would get ``LEGACY_ADAPTER_UNSUPPORTED_KIND`` at
    ``/pipeline`` for every input.

    So the registry gets this wrapper and the builder keeps calling
    ``adapt_sync_pipeline`` with the core it already lowered (no double lowering).

    ``lower_config`` is the dialect's own normalizer and cannot raise on input that
    passed ``validate_config`` — which runs it internally. On UNVALIDATED input it
    raises its ``SYNC_PIPELINE_*`` ``BuilderValidationError``, and that is allowed
    to propagate deliberately: it is the dialect's existing legacy error contract
    (ADR-001 asks adapters to preserve it), and it names the real defect far better
    than a re-wrapped adapter diagnostic could.

    A WSS listener config raises ``LEGACY_ADAPTER_UNSUPPORTED_KIND`` here rather
    than returning anything. That is correct and not a gap: an adapter returns IR
    or fails, so it *cannot* express "use the legacy renderer" — choosing the
    renderer is the builder's job (`_sync_pipeline_is_canonical`). This mirrors the
    registry entry's documented meaning: the dialect is cut over, not every one of
    its configs.

    Pointers are rebased onto the raw config, because ``source_pointer`` and the
    aliases that embed it are contractually the EXACT location a reference came
    from — in the document the CALLER handed over. Returning the lowered core's
    ``/source/...`` pointers here would name fields that do not exist in a raw
    sync_pipeline config at all.
    """
    from ....categories.components.builders.process_flow_builder import (
        SyncPipelineBuilder,
    )

    return adapt_sync_pipeline(
        SyncPipelineBuilder.lower_config(config),
        pointer_bases=_raw_pointer_bases(config),
    )


__all__ = ["adapt_sync_pipeline", "adapt_sync_pipeline_config"]
