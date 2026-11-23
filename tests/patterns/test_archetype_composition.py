"""Issue #14 (M8): compose_archetypes — archetype composition end-to-end.

Proves the composition contract and the full local chain through the public
action entry points, without live Boomi:

    compose_archetypes_action -> build_integration_action(plan)
                              -> ProcessFlowBuilder.build (Branch XML shape)

Acceptance criteria covered:
* Composition emits ONE coherent IntegrationSpecV1 (component_count matches,
  one main process on the flow_sequence map_ref + Branch surface).
* Invalid contract links fail with COMPOSITION_* codes BEFORE any spec exists
  (and therefore before any Boomi mutation).
* Composed output still plans clean through the normal build_integration path
  and the golden example spec round-trips byte-for-byte.

Payloads use sentinel placeholders only (``<<...>>``), never canned payloads,
raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

import copy
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.components.builders import ProcessFlowBuilder
from boomi_mcp.categories.integration_builder import _build_plan
from boomi_mcp.errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
    COMPOSITION_UNSUPPORTED_TOPOLOGY,
    ERROR_TAXONOMY,
)

_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"

_EXAMPLE_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "examples"
    / "m8"
    / "composed_db_to_api_fanout.integration.json"
)


_TESTS_DIR = str(Path(__file__).resolve().parent.parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import _example_unit_support as _units  # noqa: E402
from _fanout_v2_support import respell_oracle_credential_refs  # noqa: E402

_COMPONENT_FIELDS = ("key", "type", "action", "name", "config", "depends_on", "component_id")


def _example_payload(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["example_not_template"] is True
    assert payload["is_template"] is False
    assert payload["template_status"] == "example_only_not_reusable_template"
    return payload


def _recipe_normalized_spec(payload: Dict[str, Any]):
    """The CANONICAL route's spec for the example's ``recipe_request``.

    A typed recipe intent compiled through the public workflow; the normalized spec
    carries the supporting components and the direct process unit the fan-out
    recipe contributes (no legacy process component, no ``process_kind``).
    """
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    request = AuthoringRequestV1.model_validate(
        {
            "intent": {
                "intent_kind": "recipe",
                "integration_name": payload["integration_spec"]["name"],
                "invocations": [payload["recipe_request"]],
            }
        }
    )
    result, internals = compile_authoring_request_v1(request, profile="p")
    assert result.validation_report.is_valid
    return internals.normalized.integration_spec


def _assert_example_matches_recipe(payload: Dict[str, Any]) -> None:
    from boomi_mcp.models.process_ir import canonical_process_ir_json, parse_process_ir_v1

    shipped = payload["integration_spec"]
    assert not [c for c in shipped["components"] if c["type"] == "process"]
    composed = _recipe_normalized_spec(payload)

    # The ONE re-spelling fan-out 2.0.0 applies (a legacy credential_ref under the
    # opaque `credential://` scheme) goes on the EXPECTED side; every other byte of
    # every supporting component is compared as shipped.
    expected = [
        {field: component[field] for field in _COMPONENT_FIELDS}
        for component in respell_oracle_credential_refs(shipped["components"])
    ]
    actual = []
    for component in composed.components:
        data = component.model_dump(mode="json")
        actual.append({field: data[field] for field in _COMPONENT_FIELDS})
    assert actual == expected

    assert len(composed.processes) == len(shipped["processes"]) == 1
    unit, authored = composed.processes[0], shipped["processes"][0]
    envelope = authored["envelope"]
    assert unit.envelope.component_key == envelope["component_key"]
    assert unit.envelope.name == envelope["name"]
    assert unit.envelope.action == envelope["action"]
    assert set(unit.envelope.depends_on) == set(envelope["depends_on"])
    assert unit.envelope.process_extensions.model_dump(
        mode="json", exclude_none=True
    ) == envelope["process_extensions"]
    assert canonical_process_ir_json(unit.process_ir) == canonical_process_ir_json(
        parse_process_ir_v1(authored["process_ir"])
    )


def _payload_profile(*leaf_names: str) -> Dict[str, Any]:
    return {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": name, "kind": "simple", "data_type": "character"}
                for name in leaf_names
            ],
        },
    }


def _parts() -> List[Dict[str, Any]]:
    return [
        {
            "key": "db",
            "kind": "db_source",
            "parameters": {
                "binding": {
                    "mode": "create",
                    "settings": {
                        "driver": "microsoft_jdbc",
                        "auth_mode": "username_password",
                        "host": "db.example.invalid",
                        "database": "AppDB",
                        "username": "<<db user>>",
                        "credential_ref": "<<opaque credential reference>>",
                    },
                },
                "read_operation": {
                    "sql": "<<user-authored DB read statement>>",
                    "result_schema": {
                        "fields": [
                            {"name": "source_a", "data_type": "character"},
                        ],
                    },
                },
            },
        },
        {
            "key": "shape",
            "kind": "transform",
            "parameters": {
                "operations": [
                    {
                        "operation_type": "direct",
                        "source_field": "source_a",
                        "target_path": "Root/target_a",
                    },
                ],
            },
        },
        {
            "key": "orders",
            "kind": "rest_target",
            "label": "Orders",
            "parameters": {
                "binding": {
                    "mode": "create",
                    "settings": {
                        "base_url": "https://orders.example.invalid",
                        "auth_mode": "none",
                    },
                },
                "send_request": {"method": "POST", "path": "/v1/orders"},
                "payload_profile": _payload_profile("target_a"),
            },
        },
        {
            "key": "billing",
            "kind": "rest_target",
            "label": "Billing",
            "parameters": {
                "binding": {
                    "mode": "create",
                    "settings": {
                        "base_url": "https://billing.example.invalid",
                        "auth_mode": "none",
                    },
                },
                "send_request": {"method": "POST", "path": "/v1/billing"},
                "payload_profile": _payload_profile("target_a"),
            },
        },
    ]


def _options() -> Dict[str, Any]:
    return {
        "naming": {
            "integration_name": "demo-composed-fanout",
            "component_prefix": "DEMO",
        },
    }


def _compose(parts=None, options=None) -> Dict[str, Any]:
    # #159 S3: the compose tool and its action are retired; imported HERE so the
    # module (and the helpers other modules import from it) still collects once
    # the action is gone, and only the tests of the retired action fail.
    from boomi_mcp.categories.integration_authoring import compose_archetypes_action

    return compose_archetypes_action(
        parts=parts if parts is not None else _parts(),
        options=options if options is not None else _options(),
    )


# ---------------------------------------------------------------------------
# Success path — one coherent IntegrationSpecV1
# ---------------------------------------------------------------------------


def test_compose_registry_and_archetype_list_unchanged():
    from boomi_mcp.categories.integration_authoring import (
        list_integration_archetypes_action,
    )

    listed = list_integration_archetypes_action()
    assert listed["_success"] is True
    # #159 S3: discovery is recipe-only — the six default executables
    # (step-4 decision 1). No composition or adapter entry is listed, and fan-out
    # is served at its 2.0.0 default.
    served = {(r["recipe_id"], r["recipe_version"]) for r in listed["recipes"]}
    assert not any("compose_archetypes" in recipe_id for recipe_id, _v in served)
    assert served == {
        ("boomi.archetype.api_to_database_sync", "1.0.0"),
        ("boomi.archetype.database_to_api_sync", "1.0.0"),
        ("boomi.archetype.http_listener_to_db", "1.0.0"),
        ("boomi.archetype.http_listener_to_rest", "1.0.0"),
        ("boomi.archetype.stub_minimal", "1.0.0"),
        ("boomi.compose.db_rest_fanout", "2.0.0"),
    }
    assert listed["count"] == 6


def test_composition_no_longer_re_exports_the_scheduled_archetype_class():
    """`PatternRegistry.from_package` walks every class in each module's globals with
    no `__module__` filter, so a re-exported archetype class is a SECOND discovery
    path. #160 deletes the defining module; a surviving re-export elsewhere in the
    package would keep registering it (or break the walk). #151 replaced the class
    binding in composition.py with a module alias.
    """
    import boomi_mcp.patterns.archetypes.database_to_api_sync as _defining
    import boomi_mcp.patterns.composition as _composition

    assert "DatabaseToApiSyncArchetype" not in vars(_composition)
    # Positive control: the assertion above is only meaningful while the class is
    # still discoverable from its DEFINING module — otherwise it would pass
    # vacuously after an unrelated rename.
    assert "DatabaseToApiSyncArchetype" in vars(_defining)
    # The composition route itself still works, through the module alias (#159
    # migrates the route; #151 only moves how it is referenced).
    assert _composition._database_to_api_sync is _defining


def test_the_neutral_modules_do_not_import_a_deletion_scheduled_archetype():
    """The point of the #151 extraction: a survivor can reach the shared parameter
    and assembly layer without loading either module #160 deletes.

    Run in a CHILD interpreter — this process has almost certainly imported the
    archetypes already, so an in-process `sys.modules` check would pass vacuously.
    """
    import subprocess
    import sys as _sys

    probe = (
        "import sys;"
        "import boomi_mcp.patterns.archetype_parameters;"
        "import boomi_mcp.patterns.archetype_assembly;"
        "bad=[m for m in sys.modules if m.startswith("
        "'boomi_mcp.patterns.archetypes.')];"
        "print(sorted(bad));"
        "sys.exit(1 if bad else 0)"
    )
    proc = subprocess.run(
        [_sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[2]),
        env={**os.environ, "PYTHONPATH": "src"},
    )
    assert proc.returncode == 0, (
        "importing the neutral modules pulled in a scheduled archetype module: %s\n%s"
        % (proc.stdout.strip(), proc.stderr.strip())
    )


# ---------------------------------------------------------------------------
# Composed output flows through normal orchestration (plan + XML shape)
# ---------------------------------------------------------------------------


def test_golden_example_round_trips():
    payload = _example_payload(_EXAMPLE_PATH)
    # The example's typed recipe request reproduces its components and its direct
    # process unit (the legacy compose route no longer authors its process).
    _assert_example_matches_recipe(payload)


def test_golden_example_process_matches_the_frozen_legacy_bytes():
    spec = _units.example_spec(_EXAMPLE_PATH)
    xml = _units.render_process_xml(spec, _units.uuid5_registry(spec))
    frozen = (
        _units.LEGACY_ORACLE_DIR / "composed_db_to_api_fanout.process.legacy.xml"
    ).read_text(encoding="utf-8")
    assert xml == frozen
    shapes = [s.get("shapetype") for s in ET.fromstring(xml).iter("shape")]
    assert shapes.count("branch") == 1
    # DB Get source + one REST send per leg.
    assert shapes.count("connectoraction") == 3
    # Each Branch leg terminates in its own Stop.
    assert shapes.count("stop") == 2
    assert shapes.count("map") == 1


def test_golden_example_plans_clean():
    payload = json.loads(_EXAMPLE_PATH.read_text(encoding="utf-8"))
    with patch(_PAGINATE_TARGET, return_value=[]):
        plan = _build_plan(
            MagicMock(),
            {
                "integration_spec": payload["integration_spec"],
                "conflict_policy": "reuse",
            },
        )
    assert plan.get("_success", True) is not False
    for step in plan["steps"]:
        assert "validation_error" not in step, (step["key"], step.get("validation_error"))


# ---------------------------------------------------------------------------
# Contract mismatches fail with COMPOSITION_CONTRACT_MISMATCH (no spec emitted)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Unsupported topologies
# ---------------------------------------------------------------------------


def _cache_links(*cache_targets: str) -> List[Dict[str, Any]]:
    """The explicit star with document_cache declared on the given targets."""
    links: List[Dict[str, Any]] = [{"from_part": "db", "to_part": "shape"}]
    for target in ("orders", "billing"):
        link: Dict[str, Any] = {"from_part": "shape", "to_part": target}
        if target in cache_targets:
            link["handoff"] = {"mode": "document_cache"}
        links.append(link)
    return links


def _many_target_compose(count: int, cache: bool) -> Dict[str, Any]:
    parts = _parts()[:2]
    template = _parts()[2]
    links: List[Dict[str, Any]] = [{"from_part": "db", "to_part": "shape"}]
    for i in range(count):
        part = copy.deepcopy(template)
        part["key"] = f"t{i}"
        part["label"] = f"Leg {chr(ord('A') + i)}"
        parts.append(part)
        link: Dict[str, Any] = {"from_part": "shape", "to_part": part["key"]}
        if cache and i == 0:
            link["handoff"] = {"mode": "document_cache"}
        links.append(link)
    options = _options()
    options["links"] = links
    return _compose(parts=parts, options=options)


_CACHE_EXAMPLE_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "examples"
    / "m8"
    / "cache_handoff_staged_fanout.integration.json"
)


def test_cache_handoff_golden_example_round_trips():
    payload = _example_payload(_CACHE_EXAMPLE_PATH)
    _assert_example_matches_recipe(payload)


def test_cache_handoff_golden_example_process_matches_the_frozen_legacy_bytes():
    spec = _units.example_spec(_CACHE_EXAMPLE_PATH)
    xml = _units.render_process_xml(spec, _units.uuid5_registry(spec))
    frozen = (
        _units.LEGACY_ORACLE_DIR / "cache_handoff_staged_fanout.process.legacy.xml"
    ).read_text(encoding="utf-8")
    assert xml == frozen
    shapes = [s.get("shapetype") for s in ET.fromstring(xml).iter("shape")]
    assert shapes.count("doccacheload") == 1
    assert shapes.count("doccacheretrieve") == 2


def test_cache_handoff_golden_example_plans_clean():
    payload = json.loads(_CACHE_EXAMPLE_PATH.read_text(encoding="utf-8"))
    with patch(_PAGINATE_TARGET, return_value=[]):
        plan = _build_plan(
            MagicMock(),
            {
                "integration_spec": payload["integration_spec"],
                "conflict_policy": "reuse",
            },
        )
    assert plan.get("_success", True) is not False
    for step in plan["steps"]:
        assert "validation_error" not in step, (step["key"], step.get("validation_error"))


def test_the_shipped_m8_examples_apply_clean_in_dry_run():
    """#151 (M12.14): the shipped examples must PLAN AND APPLY through the entry.

    The issue's acceptance criterion says "plan and apply". The two tests above
    stop at `_build_plan`, which never reaches the apply entry point — the gap the
    architect review caught. Dry run, so this proves the apply path is reached AND
    that it refuses to mutate: `_apply_plan` returns before any create/update when
    `dry_run` is set, and the client is asserted untouched.
    """
    from boomi_mcp.categories.integration_builder import _apply_plan

    for path in (_EXAMPLE_PATH, _CACHE_EXAMPLE_PATH):
        payload = json.loads(path.read_text(encoding="utf-8"))
        spec = payload["integration_spec"]
        client = MagicMock()
        with patch(_PAGINATE_TARGET, return_value=[]):
            planned = _build_plan(
                MagicMock(),
                {"integration_spec": spec, "conflict_policy": "reuse"},
            )
            applied = _apply_plan(
                client,
                "test-profile",
                {
                    "integration_spec": spec,
                    "conflict_policy": "reuse",
                    "dry_run": True,
                },
            )
        assert applied.get("_success") is not False, path.name
        assert applied["dry_run"] is True, path.name
        # Apply must describe exactly what planning described.
        assert [s["key"] for s in applied["steps"]] == [
            s["key"] for s in planned["steps"]
        ], path.name
        assert [s["planned_action"] for s in applied["steps"]] == [
            s["planned_action"] for s in planned["steps"]
        ], path.name
        for step in applied["steps"]:
            assert "validation_error" not in step, (path.name, step["key"])
            assert step["planned_action"] in ("create", "reuse"), (path.name, step["key"])
        # A dry run must not touch the client AT ALL — asserted as "no calls",
        # not by matching method names. A name matcher was the first attempt and
        # was ineffective: the real SDK writes are `component.create_component(...)`,
        # `update_component_raw(...)`, `delete_component_metadata(...)`, none of
        # which contains the substrings `.create(` / `.update(` / `.delete(`, so a
        # genuine write would have passed. Zero calls is both stronger and immune
        # to the SDK renaming its methods; measured, the dry-run path makes none.
        assert client.mock_calls == [], (
            path.name,
            [str(call) for call in client.mock_calls],
        )


# ---------------------------------------------------------------------------
# Key / name collisions
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Parameter validation + envelope hygiene
# ---------------------------------------------------------------------------


def test_composition_error_codes_are_in_taxonomy():
    for code in (
        COMPOSITION_CONTRACT_MISMATCH,
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        COMPOSITION_COMPONENT_KEY_COLLISION,
    ):
        assert code in ERROR_TAXONOMY
        assert ERROR_TAXONOMY[code].category == "authoring"
        assert ERROR_TAXONOMY[code].retryable is False
        assert ERROR_TAXONOMY[code].owner == "#14"
