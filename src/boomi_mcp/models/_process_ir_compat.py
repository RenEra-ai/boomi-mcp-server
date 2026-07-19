"""PRIVATE #136 compatibility codec: frozen ``flow_sequence`` <-> ``ProcessIRV1``.

Dark and unexported — no runtime code imports this module; only the #136 parity
tests do. It proves the losslessness acceptance criterion (legacy fixture ->
IR -> legacy -> IR is canonically identical) over the FROZEN semantic scope:

- a ``database_to_api_sync`` config carrying ``source`` + ``target`` +
  ``flow_sequence`` (+ optional ``return_documents``, a passthrough
  ``transform``, and a no-Try/Catch ``reliability`` treated as absent), or
- a ``wrapper_subprocess`` config carrying ``process_calls``
  (+ optional ``return_documents``).

Everything else — arbitrary root extras (the legacy root leniency stays #139's
gate, inventory §2.7), ``process_extensions``, Try/Catch reliability,
``dynamic_path``, sibling control blocks — is REJECTED here: #139 owns the full
production adapter, and this codec must never grow into a shadow of it. Any
compatibility-only machinery here (e.g. ``fallback_target``) has its removal
gate in #147.

Direction contracts:

- ``legacy_flow_sequence_to_ir`` normalizes aliases (``dataprocess`` ->
  ``data_process``, ``doccacheload`` -> ``cache_put``, ``doccacheretrieve`` ->
  ``document_cache_retrieve``, ``doccacheremove`` -> ``cache_remove``), expands
  defaults, strips connector metadata (only ``connection_id``/``operation_id``
  survive, as opaque refs), and hoists the root ``target`` into the decision
  TRUE arm (its emitted fallthrough). A root target made dead by a
  branch/exception terminal is dropped — the IR does not represent dead config.
- ``ir_to_legacy_flow_sequence`` reconstructs ``connector_type``/``action_type``
  SOLELY from the caller-supplied :class:`ConnectorResolutionContextV1` (a
  missing binding is a typed error, never a silent default) and re-synthesizes
  the legacy-required-but-unemitted root target from ``fallback_target`` for
  branch/exception-terminated sequences.

Equivalence is canonical-IR equality — ``canonical(legacy->IR) ==
canonical(legacy->IR->legacy->IR)`` — NOT legacy spelling identity.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from .process_ir import (
    ProcessIRDiagnostic,
    ProcessIRV1,
    ProcessIRValidationError,
    parse_process_ir_v1,
)
from ..errors import (
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_SCHEMA_INVALID,
)


class ConnectorBindingV1(BaseModel):
    """Connector metadata for ONE operation component — compiler/symbol-table
    territory, never authored into the IR."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    connector_type: str = Field(..., min_length=1)
    action_type: str = Field(..., min_length=1)


class ConnectorResolutionContextV1(BaseModel):
    """Read-only reverse-mapping context.

    ``operation_bindings`` maps an endpoint's ``operation_ref`` to its
    connector metadata. ``fallback_target`` re-synthesizes the legacy root
    target for sequences whose every path self-terminates (legacy requires the
    key even though it is never emitted) — compatibility-only, #147 removal.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_bindings: Mapping[str, ConnectorBindingV1] = Field(default_factory=dict)
    fallback_target: Optional[Dict[str, str]] = Field(
        default=None,
        description="Legacy target binding dict {connector_type, connection_id, operation_id, action_type}",
    )


def _reject(code: str, path: str, message: str) -> ProcessIRValidationError:
    return ProcessIRValidationError(
        [
            ProcessIRDiagnostic(
                code=code,
                path=path,
                message=message,
                remediation=(
                    "The #136 codec covers only the frozen flow_sequence scope; "
                    "the full legacy adapter is #139."
                ),
            )
        ]
    )


# ---------------------------------------------------------------------------
# legacy -> IR
# ---------------------------------------------------------------------------

_KIND_ALIASES = {
    "dataprocess": "data_process",
    "doccacheload": "cache_put",
    "doccacheretrieve": "document_cache_retrieve",
    "doccacheremove": "cache_remove",
}

_FLOW_ROOT_KEYS = frozenset(
    {
        "process_kind",
        "process_type",
        "source",
        "target",
        "flow_sequence",
        "return_documents",
        "transform",
        "reliability",
        "description",
    }
)
_WRAPPER_ROOT_KEYS = frozenset(
    {"process_kind", "process_type", "process_calls", "return_documents", "description"}
)
_BINDING_KEYS = frozenset(
    {"connector_type", "connection_id", "operation_id", "action_type"}
)

# Legacy per-kind step key allowlists (legacy spellings) — mirrors
# _FLOW_SEQUENCE_STEP_KEYS so an unknown legacy key is rejected, never dropped.
_LEGACY_STEP_KEYS: Dict[str, frozenset] = {
    "flow_control": frozenset({"kind", "label", "for_each_count"}),
    "message": frozenset({"kind", "label", "message_text"}),
    "map_ref": frozenset({"kind", "label", "map_ref"}),
    "dataprocess": frozenset({"kind", "label", "steps"}),
    "doccacheload": frozenset({"kind", "label", "document_cache_id"}),
    "doccacheretrieve": frozenset(
        {"kind", "label", "document_cache_id", "empty_cache_behavior", "load_all_documents"}
    ),
    "doccacheremove": frozenset(
        {"kind", "label", "document_cache_id", "remove_all_documents"}
    ),
    "set_ddp": frozenset({"kind", "label", "name", "source_values"}),
    "set_dpp": frozenset({"kind", "label", "name", "source_values", "persist"}),
    "cache_put": frozenset({"kind", "label", "document_cache_id"}),
    "cache_get": frozenset(
        {
            "kind",
            "label",
            "document_cache_id",
            "empty_cache_behavior",
            "doc_cache_index",
            "cache_key_values",
            "load_all_documents",
            "external_writer",
        }
    ),
    "decision": frozenset(
        {"kind", "label", "comparison", "left", "right", "true_steps", "false_steps"}
    ),
    "branch": frozenset({"kind", "label", "legs"}),
    "exception": frozenset(
        {"kind", "title", "message_template", "stop_single_document", "parameter_source"}
    ),
}


def _require_dict(value: Any, path: str, what: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise _reject(PROCESS_IR_SCHEMA_INVALID, path, f"{what} must be a JSON object")
    return value


def _check_step_keys(step: Dict[str, Any], legacy_kind: str, path: str) -> None:
    allowed = _LEGACY_STEP_KEYS[legacy_kind]
    extra = set(step) - allowed
    if extra:
        raise _reject(
            PROCESS_IR_SCHEMA_INVALID,
            path,
            f"unsupported key(s) for legacy kind '{legacy_kind}'",
        )


def _maybe_label(step: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
    if step.get("label") is not None:
        node["label"] = step["label"]
    return node


def _convert_property_source(source: Any, path: str) -> Dict[str, Any]:
    src = _require_dict(source, path, "source value")
    value_type = str(src.get("value_type") or "").strip()
    if value_type == "static":
        return {"value_type": "static", "value": src.get("value")}
    if value_type == "current":
        return {"value_type": "current"}
    if value_type == "profile":
        return {
            "value_type": "profile",
            "element_id": src.get("element_id"),
            "element_name": src.get("element_name"),
            "profile_ref": src.get("profile_id"),
            "profile_type": src.get("profile_type"),
        }
    if value_type in ("ddp", "dpp"):
        out: Dict[str, Any] = {
            "value_type": value_type,
            "property_name": src.get("property_name"),
        }
        if src.get("default_value") is not None:
            out["default_value"] = src.get("default_value")
        return out
    if value_type == "definedparameter":
        raise _reject(
            PROCESS_IR_CAPABILITY_UNSUPPORTED,
            path,
            "definedparameter property sources are capability-gated",
        )
    raise _reject(PROCESS_IR_SCHEMA_INVALID, path, "unknown property source value_type")


def _convert_operand(operand: Any, path: str) -> Dict[str, Any]:
    op = _require_dict(operand, path, "decision operand")
    value_type = str(op.get("value_type") or "").strip()
    if value_type == "track":
        out: Dict[str, Any] = {"value_type": "track", "property_id": op.get("property_id")}
        if op.get("property_name") is not None:
            out["property_name"] = op.get("property_name")
        if op.get("default_value") is not None:
            out["default_value"] = op.get("default_value")
        return out
    if value_type == "static":
        return {"value_type": "static", "static_value": op.get("static_value")}
    raise _reject(PROCESS_IR_SCHEMA_INVALID, path, "unknown decision operand value_type")


def _convert_dataprocess_op(step: Any, path: str) -> Dict[str, Any]:
    op = _require_dict(step, path, "data process operation")
    operation = str(op.get("operation") or "").strip()
    if operation == "custom_scripting":
        out: Dict[str, Any] = {"operation": "custom_scripting", "script": op.get("script")}
        if "language" in op:
            out["language"] = op.get("language")
        if "use_cache" in op:
            out["use_cache"] = op.get("use_cache")
        return out
    if operation in ("split_documents", "combine_documents"):
        out = {
            "operation": operation,
            "profile_type": op.get("profile_type"),
            "profile_ref": op.get("profile_id"),
            "link_element_key": op.get("link_element_key"),
            "link_element_name": op.get("link_element_name"),
        }
        if operation == "combine_documents" and "combine_into_link_element_key" in op:
            out["combine_into_link_element_key"] = op.get("combine_into_link_element_key")
        return out
    raise _reject(PROCESS_IR_SCHEMA_INVALID, path, "unsupported data process operation")


def _convert_target_binding(target: Any, path: str) -> Dict[str, Any]:
    tgt = _require_dict(target, path, "target binding")
    if tgt.get("dynamic_path") is not None:
        raise _reject(
            PROCESS_IR_CAPABILITY_UNSUPPORTED, f"{path}/dynamic_path",
            "dynamic_path is outside the frozen codec scope",
        )
    extra = set(tgt) - _BINDING_KEYS
    if extra:
        raise _reject(PROCESS_IR_SCHEMA_INVALID, path, "unsupported target binding key(s)")
    return {
        "kind": "target",
        "connection_ref": tgt.get("connection_id"),
        "operation_ref": tgt.get("operation_id"),
    }


def _convert_linear_step(step: Dict[str, Any], legacy_kind: str, path: str) -> Dict[str, Any]:
    """Convert one legacy LINEAR step (legacy spelling) to an IR node payload."""
    _check_step_keys(step, legacy_kind, path)
    kind = _KIND_ALIASES.get(legacy_kind, legacy_kind)
    if kind == "flow_control":
        return _maybe_label(step, {"kind": kind, "for_each_count": step.get("for_each_count")})
    if kind == "message":
        return _maybe_label(step, {"kind": kind, "text": step.get("message_text")})
    if kind == "map_ref":
        return _maybe_label(step, {"kind": kind, "map_ref": step.get("map_ref")})
    if kind == "data_process":
        raw_ops = step.get("steps")
        if not isinstance(raw_ops, list) or not raw_ops:
            raise _reject(PROCESS_IR_SCHEMA_INVALID, f"{path}/steps", "steps must be a non-empty list")
        ops = [
            _convert_dataprocess_op(op, f"{path}/steps/{i}") for i, op in enumerate(raw_ops)
        ]
        return _maybe_label(step, {"kind": kind, "steps": ops})
    if kind == "cache_put":
        return _maybe_label(step, {"kind": kind, "cache_ref": step.get("document_cache_id")})
    if kind == "document_cache_retrieve":
        node: Dict[str, Any] = {"kind": kind, "cache_ref": step.get("document_cache_id")}
        if "empty_cache_behavior" in step:
            node["empty_cache_behavior"] = step.get("empty_cache_behavior")
        if "load_all_documents" in step and step.get("load_all_documents") is not True:
            raise _reject(
                PROCESS_IR_CAPABILITY_UNSUPPORTED, f"{path}/load_all_documents",
                "keyed/indexed cache retrieval is capability-gated",
            )
        return _maybe_label(step, node)
    if kind == "cache_get":
        for gated in ("doc_cache_index", "cache_key_values"):
            if gated in step:
                raise _reject(
                    PROCESS_IR_CAPABILITY_UNSUPPORTED, f"{path}/{gated}",
                    "keyed/indexed cache retrieval is capability-gated",
                )
        if "load_all_documents" in step and step.get("load_all_documents") is not True:
            raise _reject(
                PROCESS_IR_CAPABILITY_UNSUPPORTED, f"{path}/load_all_documents",
                "keyed/indexed cache retrieval is capability-gated",
            )
        node = {"kind": kind, "cache_ref": step.get("document_cache_id")}
        if "empty_cache_behavior" in step:
            node["empty_cache_behavior"] = step.get("empty_cache_behavior")
        if "external_writer" in step:
            node["external_writer"] = step.get("external_writer")
        return _maybe_label(step, node)
    if kind == "cache_remove":
        node = {"kind": kind, "cache_ref": step.get("document_cache_id")}
        if "remove_all_documents" in step and step.get("remove_all_documents") is not True:
            raise _reject(
                PROCESS_IR_CAPABILITY_UNSUPPORTED, f"{path}/remove_all_documents",
                "keyed/indexed cache removal is capability-gated",
            )
        return _maybe_label(step, node)
    if kind in ("set_ddp", "set_dpp"):
        raw_sources = step.get("source_values")
        if not isinstance(raw_sources, list) or not raw_sources:
            raise _reject(
                PROCESS_IR_SCHEMA_INVALID, f"{path}/source_values",
                "source_values must be a non-empty list",
            )
        sources = [
            _convert_property_source(sv, f"{path}/source_values/{i}")
            for i, sv in enumerate(raw_sources)
        ]
        node = {"kind": kind, "name": step.get("name"), "source_values": sources}
        if kind == "set_dpp" and step.get("persist") is not None:
            node["persist"] = step.get("persist")
        return _maybe_label(step, node)
    raise _reject(PROCESS_IR_SCHEMA_INVALID, f"{path}/kind", "unsupported legacy step kind")


def _convert_exception_step(step: Dict[str, Any], path: str) -> Dict[str, Any]:
    _check_step_keys(step, "exception", path)
    node: Dict[str, Any] = {
        "kind": "exception",
        "message_template": step.get("message_template"),
    }
    if step.get("title") is not None:
        node["title"] = step.get("title")
    if step.get("stop_single_document") is not None:
        node["stop_single_document"] = step.get("stop_single_document")
    if step.get("parameter_source") is not None:
        node["parameter_source"] = step.get("parameter_source")
    return node


def _convert_branch_step(step: Dict[str, Any], path: str) -> Dict[str, Any]:
    _check_step_keys(step, "branch", path)
    raw_legs = step.get("legs")
    if not isinstance(raw_legs, list):
        raise _reject(PROCESS_IR_SCHEMA_INVALID, f"{path}/legs", "legs must be a list")
    legs: List[Dict[str, Any]] = []
    for i, raw_leg in enumerate(raw_legs):
        leg_path = f"{path}/legs/{i}"
        leg = _require_dict(raw_leg, leg_path, "branch leg")
        extra = set(leg) - {"steps", "target"}
        if extra:
            raise _reject(PROCESS_IR_SCHEMA_INVALID, leg_path, "unsupported branch leg key(s)")
        raw_steps = leg.get("steps") or []
        if not isinstance(raw_steps, list):
            raise _reject(PROCESS_IR_SCHEMA_INVALID, f"{leg_path}/steps", "steps must be a list")
        steps = [
            _convert_linear_step(
                _require_dict(s, f"{leg_path}/steps/{j}", "step"),
                str((s.get("kind") if isinstance(s, dict) else "") or "").strip(),
                f"{leg_path}/steps/{j}",
            )
            for j, s in enumerate(raw_steps)
        ]
        if leg.get("target") is not None:
            terminal = _convert_target_binding(leg.get("target"), f"{leg_path}/target")
        else:
            # Target-less staging leg: the trailing Add-to-Cache write IS the
            # terminal (the live staging pattern).
            if not steps or steps[-1]["kind"] != "cache_put":
                raise _reject(
                    PROCESS_IR_SCHEMA_INVALID, f"{leg_path}/target",
                    "a branch leg needs a target unless it ends in a cache_put staging write",
                )
            terminal = steps.pop()
        legs.append({"steps": steps, "terminal": terminal})
    node = {"kind": "branch", "legs": legs}
    return _maybe_label(step, node)


def _split_arm_steps(
    raw_steps: List[Any], path: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Convert a decision leg's legacy steps into (linear IR steps, terminal-or-None).

    The terminal is a trailing nested branch or exception, when present.
    """
    if not raw_steps:
        return [], None
    last = raw_steps[-1]
    last_kind = str((last.get("kind") if isinstance(last, dict) else "") or "").strip()
    terminal: Optional[Dict[str, Any]] = None
    linear_raw = raw_steps
    if last_kind == "branch":
        terminal = _convert_branch_step(
            _require_dict(last, f"{path}/{len(raw_steps) - 1}", "step"),
            f"{path}/{len(raw_steps) - 1}",
        )
        linear_raw = raw_steps[:-1]
    elif last_kind == "exception":
        terminal = _convert_exception_step(
            _require_dict(last, f"{path}/{len(raw_steps) - 1}", "step"),
            f"{path}/{len(raw_steps) - 1}",
        )
        linear_raw = raw_steps[:-1]
    steps = [
        _convert_linear_step(
            _require_dict(s, f"{path}/{j}", "step"),
            str((s.get("kind") if isinstance(s, dict) else "") or "").strip(),
            f"{path}/{j}",
        )
        for j, s in enumerate(linear_raw)
    ]
    return steps, terminal


def _convert_decision_step(
    step: Dict[str, Any], path: str, root_target: Dict[str, Any]
) -> Dict[str, Any]:
    _check_step_keys(step, "decision", path)
    true_raw = step.get("true_steps") or []
    false_raw = step.get("false_steps") or []
    if not isinstance(true_raw, list) or not isinstance(false_raw, list):
        raise _reject(PROCESS_IR_SCHEMA_INVALID, path, "true_steps/false_steps must be lists")
    true_steps, true_terminal = _split_arm_steps(true_raw, f"{path}/true_steps")
    false_steps, false_terminal = _split_arm_steps(false_raw, f"{path}/false_steps")
    node: Dict[str, Any] = {
        "kind": "decision",
        "comparison": step.get("comparison"),
        "left": _convert_operand(step.get("left"), f"{path}/left"),
        "right": _convert_operand(step.get("right"), f"{path}/right"),
        # D1: the root target is the true leg's emitted fallthrough — it lives
        # in IR as the TRUE-arm terminal.
        "true_arm": {"steps": true_steps, "terminal": true_terminal or dict(root_target)},
        "false_arm": {"steps": false_steps, "terminal": false_terminal or {"kind": "stop"}},
    }
    return _maybe_label(step, node)


def legacy_flow_sequence_to_ir(config: Any) -> ProcessIRV1:
    """Normalize a frozen-scope legacy config into a validated ProcessIRV1."""
    cfg = _require_dict(config, "", "legacy config")
    process_kind = str(cfg.get("process_kind") or cfg.get("process_type") or "").strip()

    if process_kind == "wrapper_subprocess":
        extra = set(cfg) - _WRAPPER_ROOT_KEYS
        if extra:
            raise _reject(
                PROCESS_IR_CAPABILITY_UNSUPPORTED, f"/{sorted(extra)[0]}",
                "root key outside the frozen wrapper codec scope (#139 owns the full adapter)",
            )
        calls = cfg.get("process_calls")
        if not isinstance(calls, list) or not calls:
            raise _reject(
                PROCESS_IR_SCHEMA_INVALID, "/process_calls",
                "process_calls must be a non-empty list",
            )
        steps: List[Dict[str, Any]] = []
        for i, raw_call in enumerate(calls):
            call = _require_dict(raw_call, f"/process_calls/{i}", "process call")
            extra_keys = set(call) - {"subprocess_ref", "process_id", "wait", "abort_on_error", "label"}
            if extra_keys:
                raise _reject(
                    PROCESS_IR_SCHEMA_INVALID, f"/process_calls/{i}",
                    "unsupported process call key(s)",
                )
            sref, pid = call.get("subprocess_ref"), call.get("process_id")
            has_sref = isinstance(sref, str) and sref.strip() != ""
            has_pid = isinstance(pid, str) and pid.strip() != ""
            if has_sref == has_pid:
                raise _reject(
                    PROCESS_IR_SCHEMA_INVALID, f"/process_calls/{i}",
                    "exactly one of subprocess_ref / process_id is required",
                )
            node: Dict[str, Any] = {"kind": "process_call", "process_ref": sref if has_sref else pid}
            if call.get("wait") is not None:
                node["wait"] = call.get("wait")
            if call.get("abort_on_error") is not None:
                node["abort_on_error"] = call.get("abort_on_error")
            steps.append(_maybe_label(call, node))
        steps.append(_terminal_from_return_documents(cfg))
        return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": steps}})

    if process_kind != "database_to_api_sync":
        raise _reject(
            PROCESS_IR_SCHEMA_INVALID, "/process_kind",
            "the codec covers only database_to_api_sync flow_sequence and wrapper_subprocess configs",
        )

    extra = set(cfg) - _FLOW_ROOT_KEYS
    if extra:
        raise _reject(
            PROCESS_IR_CAPABILITY_UNSUPPORTED, f"/{sorted(extra)[0]}",
            "root key outside the frozen flow_sequence codec scope (#139 owns the full adapter)",
        )
    transform = cfg.get("transform")
    if transform is not None:
        is_passthrough = (
            isinstance(transform, dict)
            and str(transform.get("mode") or "passthrough").strip().lower() == "passthrough"
        )
        if not is_passthrough:
            raise _reject(
                PROCESS_IR_CAPABILITY_UNSUPPORTED, "/transform",
                "a non-passthrough transform sibling is outside the codec scope",
            )
    reliability = cfg.get("reliability")
    if isinstance(reliability, dict):
        requests_try_catch = (
            (isinstance(reliability.get("retry_count"), int) and reliability.get("retry_count"))
            or str((reliability.get("dlq") or {}).get("mode") or "").strip().lower()
            in ("document_cache_ref", "error_subprocess_ref")
            or reliability.get("catch_exception") is not None
            or reliability.get("catch_notify") is not None
        )
        if requests_try_catch:
            raise _reject(
                PROCESS_IR_CAPABILITY_UNSUPPORTED, "/reliability",
                "Try/Catch reliability is outside the codec scope (scoped error handling is #142)",
            )

    source = _require_dict(cfg.get("source"), "/source", "source binding")
    if source.get("dynamic_path") is not None:
        raise _reject(
            PROCESS_IR_CAPABILITY_UNSUPPORTED, "/source/dynamic_path",
            "dynamic_path is outside the frozen codec scope",
        )
    if set(source) - _BINDING_KEYS:
        raise _reject(PROCESS_IR_SCHEMA_INVALID, "/source", "unsupported source binding key(s)")
    source_node = {
        "kind": "source",
        "connection_ref": source.get("connection_id"),
        "operation_ref": source.get("operation_id"),
    }
    target_node = _convert_target_binding(cfg.get("target"), "/target")

    seq = cfg.get("flow_sequence")
    if not isinstance(seq, list) or not seq:
        raise _reject(
            PROCESS_IR_SCHEMA_INVALID, "/flow_sequence",
            "flow_sequence must be a non-empty list",
        )

    last = seq[-1]
    last_kind = str((last.get("kind") if isinstance(last, dict) else "") or "").strip()
    rd = cfg.get("return_documents")
    rd_enabled = isinstance(rd, dict) and rd.get("enabled") is True

    steps = [{**source_node}]
    control_terminal: Optional[Dict[str, Any]] = None
    linear_raw = seq
    if last_kind in ("decision", "branch", "exception"):
        if rd_enabled:
            raise _reject(
                PROCESS_IR_SCHEMA_INVALID, "/return_documents",
                "return_documents composes only with a purely linear flow_sequence",
            )
        linear_raw = seq[:-1]
        last_dict = _require_dict(last, f"/flow_sequence/{len(seq) - 1}", "step")
        step_path = f"/flow_sequence/{len(seq) - 1}"
        if last_kind == "decision":
            control_terminal = _convert_decision_step(last_dict, step_path, target_node)
        elif last_kind == "branch":
            control_terminal = _convert_branch_step(last_dict, step_path)
        else:
            control_terminal = _convert_exception_step(last_dict, step_path)

    for j, raw_step in enumerate(linear_raw):
        step = _require_dict(raw_step, f"/flow_sequence/{j}", "step")
        legacy_kind = str(step.get("kind") or "").strip()
        if legacy_kind in ("decision", "branch", "exception"):
            raise _reject(
                PROCESS_IR_SCHEMA_INVALID, f"/flow_sequence/{j}",
                "control/terminal steps must be the last step of their sequence",
            )
        steps.append(_convert_linear_step(step, legacy_kind, f"/flow_sequence/{j}"))

    if control_terminal is not None:
        # D1/D2: the root target either lives in the decision TRUE arm or is
        # dead config (branch/exception) and is not represented.
        steps.append(control_terminal)
    else:
        steps.append(target_node)
        steps.append(_terminal_from_return_documents(cfg))
    return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": steps}})


def _terminal_from_return_documents(cfg: Dict[str, Any]) -> Dict[str, Any]:
    rd = cfg.get("return_documents")
    if isinstance(rd, dict):
        extra = set(rd) - {"enabled", "label"}
        if extra:
            raise _reject(
                PROCESS_IR_SCHEMA_INVALID, "/return_documents",
                "unsupported return_documents key(s)",
            )
        if rd.get("enabled") is True:
            node: Dict[str, Any] = {"kind": "return_documents"}
            if rd.get("label") is not None:
                node["label"] = rd.get("label")
            return node
    return {"kind": "stop"}


# ---------------------------------------------------------------------------
# IR -> legacy
# ---------------------------------------------------------------------------


def _resolve_binding(
    node: Any, context: ConnectorResolutionContextV1, path: str
) -> Dict[str, Any]:
    binding = context.operation_bindings.get(node.operation_ref)
    if binding is None:
        raise _reject(
            PROCESS_IR_SCHEMA_INVALID, path,
            "no connector binding for the operation reference in the resolution context",
        )
    return {
        "connector_type": binding.connector_type,
        "connection_id": node.connection_ref,
        "operation_id": node.operation_ref,
        "action_type": binding.action_type,
    }


def _legacy_label(node: Any, out: Dict[str, Any]) -> Dict[str, Any]:
    if getattr(node, "label", None) is not None:
        out["label"] = node.label
    return out


def _property_source_to_legacy(source: Any) -> Dict[str, Any]:
    vt = source.value_type
    if vt == "static":
        return {"value_type": "static", "value": source.value}
    if vt == "current":
        return {"value_type": "current"}
    if vt == "profile":
        return {
            "value_type": "profile",
            "element_id": source.element_id,
            "element_name": source.element_name,
            "profile_id": source.profile_ref,
            "profile_type": source.profile_type,
        }
    out: Dict[str, Any] = {"value_type": vt, "property_name": source.property_name}
    if source.default_value is not None:
        out["default_value"] = source.default_value
    return out


def _operand_to_legacy(operand: Any) -> Dict[str, Any]:
    if operand.value_type == "track":
        out: Dict[str, Any] = {"value_type": "track", "property_id": operand.property_id}
        if operand.property_name is not None:
            out["property_name"] = operand.property_name
        if operand.default_value is not None:
            out["default_value"] = operand.default_value
        return out
    return {"value_type": "static", "static_value": operand.static_value}


def _dataprocess_op_to_legacy(op: Any) -> Dict[str, Any]:
    if op.operation == "custom_scripting":
        return {
            "operation": "custom_scripting",
            "script": op.script,
            "language": op.language,
            "use_cache": op.use_cache,
        }
    out = {
        "operation": op.operation,
        "profile_type": op.profile_type,
        "profile_id": op.profile_ref,
        "link_element_key": op.link_element_key,
        "link_element_name": op.link_element_name,
    }
    if op.operation == "combine_documents":
        out["combine_into_link_element_key"] = op.combine_into_link_element_key
    return out


def _linear_node_to_legacy(node: Any) -> Dict[str, Any]:
    kind = node.kind
    if kind == "flow_control":
        return _legacy_label(node, {"kind": "flow_control", "for_each_count": node.for_each_count})
    if kind == "message":
        return _legacy_label(node, {"kind": "message", "message_text": node.text})
    if kind == "map_ref":
        return _legacy_label(node, {"kind": "map_ref", "map_ref": node.map_ref})
    if kind == "data_process":
        return _legacy_label(
            node,
            {"kind": "dataprocess", "steps": [_dataprocess_op_to_legacy(op) for op in node.steps]},
        )
    if kind == "cache_put":
        return _legacy_label(node, {"kind": "cache_put", "document_cache_id": node.cache_ref})
    if kind == "document_cache_retrieve":
        return _legacy_label(
            node,
            {
                "kind": "doccacheretrieve",
                "document_cache_id": node.cache_ref,
                "empty_cache_behavior": node.empty_cache_behavior,
                "load_all_documents": True,
            },
        )
    if kind == "cache_get":
        return _legacy_label(
            node,
            {
                "kind": "cache_get",
                "document_cache_id": node.cache_ref,
                "empty_cache_behavior": node.empty_cache_behavior,
                "external_writer": node.external_writer,
            },
        )
    if kind == "cache_remove":
        return _legacy_label(
            node,
            {
                "kind": "doccacheremove",
                "document_cache_id": node.cache_ref,
                "remove_all_documents": True,
            },
        )
    if kind == "set_ddp":
        return _legacy_label(
            node,
            {
                "kind": "set_ddp",
                "name": node.name,
                "source_values": [_property_source_to_legacy(sv) for sv in node.source_values],
            },
        )
    if kind == "set_dpp":
        return _legacy_label(
            node,
            {
                "kind": "set_dpp",
                "name": node.name,
                "source_values": [_property_source_to_legacy(sv) for sv in node.source_values],
                "persist": node.persist,
            },
        )
    raise _reject(PROCESS_IR_SCHEMA_INVALID, "", f"no legacy form for node kind '{kind}'")


def _exception_to_legacy(node: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "kind": "exception",
        "message_template": node.message_template,
        "stop_single_document": node.stop_single_document,
        "parameter_source": node.parameter_source,
    }
    if node.title is not None:
        out["title"] = node.title
    return out


def _branch_to_legacy(node: Any, context: ConnectorResolutionContextV1) -> Dict[str, Any]:
    legs: List[Dict[str, Any]] = []
    for leg in node.legs:
        steps = [_linear_node_to_legacy(s) for s in leg.steps]
        if leg.terminal.kind == "target":
            legs.append(
                {"steps": steps, "target": _resolve_binding(leg.terminal, context, "")}
            )
        else:  # staging cache_put terminal — target omitted
            steps.append(_linear_node_to_legacy(leg.terminal))
            legs.append({"steps": steps})
    out: Dict[str, Any] = {"kind": "branch", "legs": legs}
    return _legacy_label(node, out)


def ir_to_legacy_flow_sequence(
    ir: ProcessIRV1, context: ConnectorResolutionContextV1
) -> Dict[str, Any]:
    """Reconstruct the legacy dialect config for a frozen-scope IR document."""
    steps = list(ir.body.steps)
    kinds = [s.kind for s in steps]

    if "process_call" in kinds:
        config: Dict[str, Any] = {"process_kind": "wrapper_subprocess", "process_calls": []}
        for node in steps:
            if node.kind == "process_call":
                call: Dict[str, Any] = {
                    "wait": node.wait,
                    "abort_on_error": node.abort_on_error,
                }
                if node.process_ref.startswith("$ref:"):
                    call["subprocess_ref"] = node.process_ref
                else:
                    call["process_id"] = node.process_ref
                if node.label is not None:
                    call["label"] = node.label
                config["process_calls"].append(call)
            elif node.kind == "return_documents":
                config["return_documents"] = {
                    "enabled": True,
                    **({"label": node.label} if node.label is not None else {}),
                }
        return config

    source_node = steps[0]
    config = {
        "process_kind": "database_to_api_sync",
        "source": _resolve_binding(source_node, context, "/body/steps/0"),
    }
    terminal = steps[-1]
    flow_steps: List[Dict[str, Any]] = []

    if terminal.kind in ("stop", "return_documents"):
        target_node = steps[-2]
        config["target"] = _resolve_binding(target_node, context, "")
        for node in steps[1:-2]:
            flow_steps.append(_linear_node_to_legacy(node))
        if terminal.kind == "return_documents":
            config["return_documents"] = {
                "enabled": True,
                **({"label": terminal.label} if terminal.label is not None else {}),
            }
    else:
        for node in steps[1:-1]:
            flow_steps.append(_linear_node_to_legacy(node))
        if terminal.kind == "decision":
            true_terminal = terminal.true_arm.terminal
            true_steps = [_linear_node_to_legacy(s) for s in terminal.true_arm.steps]
            if true_terminal.kind == "target":
                config["target"] = _resolve_binding(true_terminal, context, "")
            elif true_terminal.kind == "branch":
                true_steps.append(_branch_to_legacy(true_terminal, context))
            else:
                true_steps.append(_exception_to_legacy(true_terminal))
            false_terminal = terminal.false_arm.terminal
            false_steps = [_linear_node_to_legacy(s) for s in terminal.false_arm.steps]
            if false_terminal.kind == "branch":
                false_steps.append(_branch_to_legacy(false_terminal, context))
            elif false_terminal.kind == "exception":
                false_steps.append(_exception_to_legacy(false_terminal))
            decision_step: Dict[str, Any] = {
                "kind": "decision",
                "comparison": terminal.comparison,
                "left": _operand_to_legacy(terminal.left),
                "right": _operand_to_legacy(terminal.right),
                "true_steps": true_steps,
                "false_steps": false_steps,
            }
            flow_steps.append(_legacy_label(terminal, decision_step))
        elif terminal.kind == "branch":
            flow_steps.append(_branch_to_legacy(terminal, context))
        else:  # exception
            flow_steps.append(_exception_to_legacy(terminal))
        if "target" not in config:
            # D2: legacy requires a root target even when every path
            # self-terminates; re-synthesize it from the context.
            if context.fallback_target is None:
                raise _reject(
                    PROCESS_IR_SCHEMA_INVALID, "/body/steps",
                    "a branch/exception-terminated sequence needs context.fallback_target "
                    "to reconstruct the legacy-required root target",
                )
            config["target"] = dict(context.fallback_target)

    config["flow_sequence"] = flow_steps
    return config
