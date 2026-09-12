"""One entry-recognition authority for deployment and routing (issue #158 / M12.20).

"Is this process a listener, and which operation does it listen on?" was answered
by two hand-written recognizers — ``integration_builder._process_config_has_wss_listen``
and ``deployment.orchestration._listener_operation_ref_from_process`` — each with its
own copy of the WSS alias set, and each recognizing exactly the two LEGACY config
shapes. A canonical ProcessIR root was invisible to both, so a listener authored the
canonical way could not be published or verified at all.

This module is the one answer, and it has two inputs of different trust:

* a CANONICAL root is classified by the COMPILER. Its recorded ProcessIR is parsed
  and lowered, and the compiler's entry policy says whether the CFG enters on a
  listener. That is the same authority that decided the root's Start form and its
  recorded execution profile, so routing cannot disagree with what was emitted.
  No second listener flag is persisted, and caller-supplied metadata is never
  consulted to decide it.
* a LEGACY process config is normalized from its ENTRY only — a ``sync_pipeline``
  ``listener`` stage, or a ``source`` binding with a Web Services Server family and
  the Listen action — through the resolver the WSS builder routes on. An unrelated
  WSS operation elsewhere in a spec never makes a process a listener, because only
  the process's own entry is read.

Deliberately NOT a full recompile at deployment: a recorded build carries no
trusted effect or replay context, and lowering needs neither to know the entry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

#: The two forms, named as the compiler's entry policy names them.
SCHEDULED = "scheduled"
LISTENER = "listener"


class RecordedEntryUnreadable(ValueError):
    """A recorded canonical root whose ProcessIR no longer parses or lowers.

    The build registry is in memory and written only by apply from a validated
    spec, so this is not reachable from any request — but a recognizer that
    crashed on it would take the whole deploy down with an untyped exception.
    Consumers translate it into their own malformed-record error.
    """


@dataclass(frozen=True)
class ProcessEntryV1:
    """How one process enters: ``form``, and for a listener its operation ref.

    ``operation_ref`` is the reference as authored — a ``$ref:KEY`` token or a
    literal component id — or ``None`` when the entry names none (a legacy
    listener stage with a blank id). A scheduled entry never carries one.
    """

    form: str
    operation_ref: Optional[str] = None

    @property
    def is_listener(self) -> bool:
        return self.form == LISTENER


_SCHEDULED_ENTRY = ProcessEntryV1(SCHEDULED)


def _clean_ref(value: Any) -> Optional[str]:
    return value.strip() if isinstance(value, str) and value.strip() else None


def legacy_config_entry(process_config: Any) -> ProcessEntryV1:
    """The entry of a LEGACY process config, normalized.

    Two recognized shapes, as before #158 — a ``sync_pipeline`` ``listener``
    stage, and a lowered or hand-authored ``source`` binding with a Web Services
    Server family and the Listen action — now read through the WSS builder's own
    alias resolver instead of two private alias sets. Anything else is scheduled.
    """
    from ..categories.components.builders.connector_builder import (
        _resolve_wss_connector_type,
    )

    if not isinstance(process_config, Mapping):
        return _SCHEDULED_ENTRY
    # #158 ARCH-158-r1-01 sibling / ARCH-158-r1-02: the entry is what the process
    # ENTERS on, so a `pipeline` block belongs to the entry only for the builder
    # that emits from it. A `database_to_api_sync` config carrying a stray
    # listener stage emits its scheduled source start, and reading the stage
    # there reported a listener entry for a process that has none. The
    # discriminator is the builder registry's own, and a config naming no kind
    # (the abbreviated recognizer shapes) is read as before.
    from ..categories.components.builders.process_flow_builder import (
        PROCESS_FLOW_BUILDERS,
        SyncPipelineBuilder,
    )

    kind = str(
        process_config.get("process_kind") or process_config.get("process_type") or ""
    ).strip().lower()
    pipeline_is_the_entry = kind not in PROCESS_FLOW_BUILDERS or kind == SyncPipelineBuilder.PROCESS_KIND
    pipeline = process_config.get("pipeline") if pipeline_is_the_entry else None
    if isinstance(pipeline, Mapping):
        for stage in pipeline.get("stages") or []:
            if not isinstance(stage, Mapping):
                continue
            if str(stage.get("kind") or "").strip().lower() != "listener":
                continue
            stage_config = stage.get("config")
            operation = (
                _clean_ref(stage_config.get("operation_id"))
                if isinstance(stage_config, Mapping)
                else None
            )
            return ProcessEntryV1(LISTENER, operation)
    source = process_config.get("source")
    if isinstance(source, Mapping):
        action_type = str(source.get("action_type") or "").strip()
        if (
            _resolve_wss_connector_type(source.get("connector_type")) is not None
            and action_type == "Listen"
        ):
            return ProcessEntryV1(LISTENER, _clean_ref(source.get("operation_id")))
    return _SCHEDULED_ENTRY


def canonical_root_entry(process_ir: Any) -> ProcessEntryV1:
    """The entry of a CANONICAL root, as the compiler's entry policy derives it.

    ``process_ir`` is a ``ProcessIRV1`` model or its recorded JSON form. It is
    parsed (so a recorded dict is re-validated rather than trusted) and lowered;
    the entry policy then classifies the CFG's own entry node. Raises
    :class:`RecordedEntryUnreadable` when the IR cannot be parsed or lowered.
    """
    from ..compiler.process_ir.diagnostics import ProcessIRCompileError
    from ..compiler.process_ir.entry_policy import LISTENER_SEMANTIC_KIND, classify_entry
    from ..compiler.process_ir.lowering import lower_process_ir_to_cfg
    from ..models.process_ir import (
        ProcessIRV1,
        ProcessIRValidationError,
        parse_process_ir_v1,
    )

    try:
        model = (
            process_ir
            if isinstance(process_ir, ProcessIRV1)
            else parse_process_ir_v1(process_ir)
        )
        cfg = lower_process_ir_to_cfg(model)
    except (ProcessIRValidationError, ProcessIRCompileError, TypeError, ValueError) as exc:
        raise RecordedEntryUnreadable(
            "the recorded process root no longer parses as ProcessIR"
        ) from exc
    if classify_entry(cfg) != LISTENER:
        return _SCHEDULED_ENTRY
    entry = next(node for node in cfg.nodes if node.node_id == cfg.entry_node_id)
    if entry.semantic.semantic_kind != LISTENER_SEMANTIC_KIND:  # pragma: no cover
        return _SCHEDULED_ENTRY
    return ProcessEntryV1(LISTENER, _clean_ref(entry.semantic.operation_ref))


__all__ = [
    "LISTENER",
    "SCHEDULED",
    "ProcessEntryV1",
    "RecordedEntryUnreadable",
    "canonical_root_entry",
    "legacy_config_entry",
]
