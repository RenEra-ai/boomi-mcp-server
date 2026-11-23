"""Render a shipped example's direct canonical process unit (issue #159 L7).

The six examples that used to author their process through a legacy adapter config now
carry ``integration_spec.processes`` (the typed ProcessIR unit) beside the supporting
``components``. This module is the ONE place that turns such a spec into the complete
process component file, through the public chain the golden corpus uses:

    plan_authoring_request_v1 (unit + request-local symbol table)
      -> build_materialization_plan (compiles through compile_process_ir_v1)
      -> late binding of the component keys to ids
      -> materialize_canonical_process_xml (the complete component file)

Expected bytes live in ``tests/fixtures/example_migration/`` (legacy-builder captures
frozen BEFORE the migration, provenance beside them) — never in this module.
"""

from __future__ import annotations

import copy
import json
import uuid
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple

_ROOT = Path(__file__).resolve().parent.parent
LEGACY_ORACLE_DIR = Path(__file__).resolve().parent / "fixtures" / "example_migration"


def example_spec(path: Path) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    assert payload["example_not_template"] is True
    assert payload["is_template"] is False
    assert payload["template_status"] == "example_only_not_reusable_template"
    return payload["integration_spec"]


def uuid5_registry(spec: Mapping[str, Any]) -> Dict[str, str]:
    """The oracle's id convention: ``uuid5(NAMESPACE_URL, "l7/" + key)`` per component key."""
    return {
        component["key"]: str(uuid.uuid5(uuid.NAMESPACE_URL, "l7/" + component["key"]))
        for component in spec["components"]
    }


def authoring_request(spec: Mapping[str, Any]):
    from boomi_mcp.models.authoring_workflow import (
        AuthoringRequestV1,
        ProcessIRAuthoringIntentV1,
    )

    naming = spec.get("naming") or {}
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name=spec.get("name") or naming["integration_name"],
            units=tuple(copy.deepcopy(spec["processes"])),
            components=tuple(copy.deepcopy(spec["components"])),
        )
    )


def plan_example(spec: Mapping[str, Any]) -> Tuple[Any, Any]:
    """The public typed plan: ``(plan_result, internals)``."""
    from boomi_mcp.authoring.workflow import plan_authoring_request_v1

    return plan_authoring_request_v1(
        authoring_request(spec), profile="golden", account_id="golden"
    )


def render_process_xml(spec: Mapping[str, Any], id_registry: Mapping[str, str]) -> str:
    """The complete ``process-component-v1`` file for the spec's FIRST unit."""
    from boomi_mcp.authoring.contract import get_authoring_revisions
    from boomi_mcp.authoring.process_materialization import build_materialization_plan
    from boomi_mcp.categories.components.canonical_process_apply import (
        materialize_canonical_process_xml,
    )
    from boomi_mcp.categories.integration_builder import _materializer_revision
    from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision

    _result, internals = plan_example(spec)
    unit = internals.normalized.integration_spec.processes[0]
    plan = build_materialization_plan(
        envelope=unit.envelope,
        process_ir=unit.process_ir,
        symbols=internals.symbols,
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision=_materializer_revision(),
    )
    rendered = materialize_canonical_process_xml(
        plan=plan, id_registry=dict(id_registry), symbols=internals.symbols
    )
    return rendered.decode("utf-8") if isinstance(rendered, bytes) else rendered
