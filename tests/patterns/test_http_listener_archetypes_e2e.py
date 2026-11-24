"""M6 (issue #12): http_listener_to_db / http_listener_to_rest presets — end-to-end.

Proves the two listener archetypes' contracts and the full local chain through
the *public* action entry points, without live Boomi:

    build_from_archetype_action  ->  build_integration_action(plan)

Covered:
* Both presets emit a ``process_kind="sync_pipeline"`` main process with an
  intact ``listener -> map -> write|send`` stage graph (no pairwise
  source/target process config, no pre-lowering).
* The listener stage carries ONLY the wss_listen binding (operation $ref, no
  connection — WSS has no connection component).
* ``validation_rules.listener`` records the computed endpoint metadata
  orchestrate_deploy's listener_verify stage reads (endpoint_path formula,
  HTTP method derived from input_type, apiType requirement, no Test mode).
* inbound_validation is an opt-in build-time contract; the emitted request
  profile always satisfies it inside the archetype.
* Transform refs are validated against the listener payload profile leaves and
  the target index (write-profile columns / target payload leaves).

Payloads use sentinel placeholders only (``<<...>>``), never canned payloads,
raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_builder import build_integration_action


def build_from_archetype_action(*args, **kwargs):
    """The retired ``build_from_archetype`` tool's action (#159 S3 deletes it).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "build_from_archetype_action")(*args, **kwargs)


def get_integration_archetype_action(*args, **kwargs):
    """The discovery action #159 S3 reshapes to recipe-only (lane D).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "get_integration_archetype_action")(*args, **kwargs)


def list_integration_archetypes_action(*args, **kwargs):
    """The discovery action #159 S3 reshapes to recipe-only (lane D).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "list_integration_archetypes_action")(*args, **kwargs)


_DB_ARCHETYPE = "http_listener_to_db"
_REST_ARCHETYPE = "http_listener_to_rest"
_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_PROFILE = "qa-profile"  # never used to reach Boomi; plan calls are fully mocked

_DB_EXPECTED_KEYS = {
    "listener_request_profile",
    "source_wss_listener_operation",
    "target_db_connection",
    "target_db_write_profile",
    "target_db_write_operation",
    "transform_transform_map",
    "main_process",
}
_REST_EXPECTED_KEYS = {
    "listener_request_profile",
    "source_wss_listener_operation",
    "transform_target_profile",
    "transform_transform_map",
    "target_rest_connection",
    "target_rest_operation",
    "main_process",
}


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _payload_profile() -> Dict[str, Any]:
    return {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "event_id", "kind": "simple", "data_type": "character"},
                {"name": "amount", "kind": "simple", "data_type": "number"},
            ],
        },
    }


def _db_params(**overrides) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "naming": {"integration_name": "demo-listener-db", "component_prefix": "LDEMO"},
        "listener": {
            "object_name": "orderIntake",
            "operation_type": "EXECUTE",
            "payload_profile": _payload_profile(),
        },
        "target": {
            "connection": {"mode": "reuse", "component_id": "<<existing db conn id>>"},
            "write_profile": {
                "statement_type": "dynamicinsert",
                "table_name": "<<target table>>",
                "fields": [
                    {"name": "col_id", "data_type": "character", "mandatory": True},
                    {"name": "col_amount", "data_type": "number"},
                ],
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_path": "Root/event_id", "target_path": "Fields/col_id"},
                {"operation_type": "direct", "source_path": "Root/amount", "target_path": "Fields/col_amount"},
            ]
        },
    }
    params.update(overrides)
    return params


def _rest_params(**overrides) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "naming": {"integration_name": "demo-listener-rest", "component_prefix": "LRELAY"},
        "listener": {
            "object_name": "eventRelay",
            "payload_profile": _payload_profile(),
        },
        "target": {
            "binding": {
                "mode": "create",
                "settings": {"base_url": "https://target.example.com", "auth_mode": "none"},
            },
            "send_request": {"method": "POST", "path": "/v1/<<target resource>>"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "id", "kind": "simple", "data_type": "character"},
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_path": "Root/event_id", "target_path": "Root/id"},
            ]
        },
    }
    params.update(overrides)
    return params


def _spec(archetype: str, params: Dict[str, Any]) -> Dict[str, Any]:
    result = build_from_archetype_action(archetype, params)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _by_key(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c["key"]: c for c in spec["components"]}


# ===========================================================================
# Catalog / schema discovery
# ===========================================================================


class TestCatalogAndSchema:
    # #159 S3: discovery is recipe-only (step-4 decision 1); each listener
    # pattern is served as its 1.0.0 recipe ``boomi.archetype.<pattern>``. The
    # parametrize ids keep their S2 spelling.
    @pytest.mark.parametrize("archetype", [_DB_ARCHETYPE, _REST_ARCHETYPE])
    def test_listed_in_catalog(self, archetype):
        result = list_integration_archetypes_action(query="listener")
        assert result["_success"] is True
        listed = {r["recipe_id"]: r["recipe_version"] for r in result["recipes"]}
        assert listed.get(f"boomi.archetype.{archetype}") == "1.0.0"
        assert result["raw_xml_exposed"] is False

    @pytest.mark.parametrize("archetype", [_DB_ARCHETYPE, _REST_ARCHETYPE])
    def test_get_returns_strict_schema_without_xml(self, archetype):
        result = get_integration_archetype_action(f"boomi.archetype.{archetype}")
        assert result["_success"] is True
        descriptor = result["descriptor"]
        assert descriptor["capability_notes"]
        assert descriptor["limitations"]
        assert descriptor["examples"]
        for schema in (result["input_schema"], result["component_material_schema"]):
            assert schema.get("additionalProperties") is False
            assert schema["properties"]
        assert "<?xml" not in json.dumps(result)


# ===========================================================================
# Spec assembly — http_listener_to_db
# ===========================================================================


# ===========================================================================
# Spec assembly — http_listener_to_rest
# ===========================================================================


# ===========================================================================
# inbound_validation contract
# ===========================================================================


class TestInboundValidation:

    def test_primitive_rejects_profile_less_contract(self):
        """The primitive's own rejection path (unreachable through the archetype,
        which always generates a profile): a none/singledata listener cannot
        satisfy mode='profile_bound'."""
        from boomi_mcp.categories.components.builders.connector_builder import (
            BuilderValidationError,
        )
        from boomi_mcp.patterns.primitives.inbound_validate import (
            InboundValidateParameters,
            InboundValidatePrimitive,
        )

        with pytest.raises(BuilderValidationError) as exc:
            InboundValidatePrimitive.validate_contract(
                InboundValidateParameters(
                    listener_input_type="singledata",
                    listener_request_profile_id=None,
                )
            )
        assert exc.value.error_code == "INBOUND_VALIDATION_UNSATISFIABLE"

        with pytest.raises(BuilderValidationError) as exc:
            InboundValidatePrimitive.validate_contract(
                InboundValidateParameters(
                    listener_input_type="singlejson",
                    listener_request_profile_id=None,
                )
            )
        assert exc.value.error_code == "INBOUND_VALIDATION_UNSATISFIABLE"


# ===========================================================================
# Parameter validation
# ===========================================================================


# ===========================================================================
# asc_wrapper — API Service Component publish mode (M6.1 #133)
# ===========================================================================


# ===========================================================================
# #159 S2: the typed recipe presets (boomi.archetype.http_listener_to_db /
# http_listener_to_rest 1.0.0). Oracles: the frozen #157 rows of the three
# listener cases (replayed from each case's committed input.canonical.json
# through the public planner) and the legacy archetype route at HEAD (the same
# parameters through ``emit_spec`` and the sync_pipeline legacy arm).
# ===========================================================================

import copy as _copy
from pathlib import Path as _Path

from boomi_mcp.recipes import component_material as _component_material
from boomi_mcp.recipes import engine as _engine_module
from boomi_mcp.recipes.builtins import catalog as _catalog
from boomi_mcp.recipes.builtins import http_listener_to_db as _db_preset
from boomi_mcp.recipes.builtins import http_listener_to_rest as _rest_preset
from boomi_mcp.recipes.contracts import (
    RecipeCapabilityRequirementV1,
    RecipeConflictPolicyV1,
    RecipeRegistrationV1,
)
from boomi_mcp.recipes.errors import RecipeError
from boomi_mcp.recipes.registry import build_test_registry

_CASES_DIR = (
    _Path(__file__).resolve().parents[1] / "fixtures" / "governance" / "issue_157" / "cases"
)
_DB_RECIPE = "boomi.archetype.http_listener_to_db"
_REST_RECIPE = "boomi.archetype.http_listener_to_rest"
_PRESET_VERSION = "1.0.0"
_LISTENER_CASES = (
    "http_listener_to_db/advanced_runtime",
    "http_listener_to_db/plain",
    "http_listener_to_rest/plain",
)


def _listener_registrations():
    """The two registrations handed to the integrator for ``catalog.py``."""
    conflict = RecipeConflictPolicyV1(
        mode="error_unless_declared", merge_rules=("dedupe_identical_constraint",)
    )
    capabilities = tuple(
        RecipeCapabilityRequirementV1(
            authority="process_emitter", subject=subject, required_state="supported"
        )
        for subject in _db_preset.LISTENER_ROOT_CAPABILITIES
    )
    common = dict(
        recipe_version=_PRESET_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        output_types=("constraint_requirement", "process_unit"),
        capability_requirements=capabilities,
        conflict_policy=conflict,
    )
    return (
        RecipeRegistrationV1(
            recipe_id=_DB_RECIPE,
            input_model=_db_preset.HttpListenerToDbInputV1,
            executor=_db_preset.emit_http_listener_to_db,
            component_material_model=_db_preset.HttpListenerToDbMaterialV1,
            component_preparer=_db_preset.prepare_http_listener_to_db,
            implementation_dependency_modules=(_component_material.__name__,),
            **common,
        ),
        RecipeRegistrationV1(
            recipe_id=_REST_RECIPE,
            input_model=_rest_preset.HttpListenerToRestInputV1,
            executor=_rest_preset.emit_http_listener_to_rest,
            component_material_model=_rest_preset.HttpListenerToRestMaterialV1,
            component_preparer=_rest_preset.prepare_http_listener_to_rest,
            implementation_dependency_modules=(_component_material.__name__, _db_preset.__name__),
            **common,
        ),
    )


def _with_listener_presets():
    """The production registrations plus the two presets (unless the catalog
    already carries them, once the integrator lands them)."""
    present = {(reg.recipe_id, reg.recipe_version) for reg in _catalog.PRODUCTION_REGISTRATIONS}
    extra = tuple(
        reg
        for reg in _listener_registrations()
        if (reg.recipe_id, reg.recipe_version) not in present
    )
    return build_test_registry(_catalog.PRODUCTION_REGISTRATIONS + extra)


@pytest.fixture
def listener_preset_registry(monkeypatch):
    registry = _with_listener_presets()
    monkeypatch.setattr(_engine_module, "production_registry", lambda: registry)
    monkeypatch.setattr(_PAGINATE_TARGET, lambda *a, **k: [])
    return registry


def _canonical(case_id):
    return json.loads((_CASES_DIR / case_id / "input.canonical.json").read_text(encoding="utf-8"))


def _invocation(case_id):
    return _canonical(case_id)["intent"]["invocations"][0]


def _preset_for(recipe_id):
    return {
        _DB_RECIPE: (
            _db_preset.HttpListenerToDbInputV1,
            _db_preset.HttpListenerToDbMaterialV1,
            _db_preset.prepare_http_listener_to_db,
            _db_preset.emit_http_listener_to_db,
        ),
        _REST_RECIPE: (
            _rest_preset.HttpListenerToRestInputV1,
            _rest_preset.HttpListenerToRestMaterialV1,
            _rest_preset.prepare_http_listener_to_rest,
            _rest_preset.emit_http_listener_to_rest,
        ),
    }[recipe_id]


def _prepare(invocation, base_catalog=None):
    input_model, material_model, preparer, executor = _preset_for(invocation["recipe_id"])
    inp = input_model.model_validate(invocation["raw_input"])
    prepared = preparer(
        inp,
        material_model.model_validate(invocation["component_material"]),
        base_catalog=base_catalog or {},
    )
    return inp, prepared, executor(inp)


def _legacy_spec(case_id):
    """The legacy archetype at HEAD over the case's frozen parameters. The REST
    case's placeholder path is the one value re-spelled in its canonical input
    (see provenance.json), so the legacy route is given the same path."""
    from boomi_mcp.patterns.archetypes.http_listener_to_db import (
        HttpListenerToDbArchetype,
        HttpListenerToDbParameters,
    )
    from boomi_mcp.patterns.archetypes.http_listener_to_rest import (
        HttpListenerToRestArchetype,
        HttpListenerToRestParameters,
    )

    legacy = json.loads((_CASES_DIR / case_id / "input.legacy.json").read_text(encoding="utf-8"))
    params = legacy["input"]["parameters"]
    if legacy["input"]["archetype"] == _REST_ARCHETYPE:
        params["target"]["send_request"]["path"] = _invocation(case_id)["component_material"][
            "target"
        ]["operation"]["path"]
        return HttpListenerToRestArchetype.emit_spec(HttpListenerToRestParameters.model_validate(params))
    return HttpListenerToDbArchetype.emit_spec(HttpListenerToDbParameters.model_validate(params))


class TestTypedListenerPresets:
    @pytest.mark.parametrize("case_id", _LISTENER_CASES)
    def test_the_canonical_input_replays_every_frozen_row(self, case_id, listener_preset_registry):
        """The committed canonical input, through the PUBLIC planner, projects
        the case's complete ordered flow and endpoint sequences by full payload."""
        import _issue_157_flows_accounting as acct

        case = acct.load_cases()[case_id]
        assert case.input_canonical is not None
        invocation = case.input_canonical["intent"]["invocations"][0]
        assert (invocation["recipe_id"], invocation["recipe_version"]) in {
            (_DB_RECIPE, _PRESET_VERSION),
            (_REST_RECIPE, _PRESET_VERSION),
        }
        plan = acct.case_plan_result(case)
        edges = {(edge.component_key, edge.depends_on) for edge in plan.component_dependencies}
        # The API Service is materialized AFTER the process its route names.
        assert (("api_service", "main_process") in edges) is case_id.endswith("advanced_runtime")
        # The two DB cases reuse their connection BY NAME (the frozen input's
        # selector, which the endpoint row serves). Offline, no account answers
        # that name, so the one step the plan cannot execute is that reuse —
        # exactly as the legacy route's own plan of the same input (asserted
        # below). Nothing else may block.
        # Unconditional on purpose (the apply-boundary witness sweep refuses an
        # assertion behind a branch): each case's EXPECTED outcome is computed,
        # then every assertion runs for every case.
        from boomi_mcp.authoring.workflow import AuthoringWorkflowError, compile_authoring_request_v1
        from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

        name_bound = case_id.startswith("http_listener_to_db/")
        blocked = {(d.path, tuple(d.cause_codes)) for d in plan.errors}
        assert blocked == (
            {("/components/target_db_connection", ("error_missing_target",))} if name_bound else set()
        )
        assert plan.validation_report.is_valid is (not name_bound)
        legacy = _legacy_spec(case_id).model_dump(mode="json")
        # #159 S3: the legacy flat process body is refused at admission, so the
        # cross-check plans the legacy route's SUPPORTING components — the
        # blocked reuse is a supporting connection either way. A component that
        # names the process (the advanced case's API Service route) goes with it.
        processes = {c["key"] for c in legacy["components"] if c["type"] == "process"}
        legacy["components"] = [
            c
            for c in legacy["components"]
            if c["type"] != "process" and not processes & set(c.get("depends_on") or ())
        ]
        legacy_plan = build_integration_action(
            MagicMock(), _PROFILE, "plan", {"integration_spec": legacy, "conflict_policy": "reuse"}
        )
        legacy_blocked = {
            step["key"] for step in legacy_plan["steps"]
            if str(step.get("planned_action", "")).startswith("error")
        }
        assert legacy_blocked == ({"target_db_connection"} if name_bound else set())
        with patch(acct._GET_XML, acct.replay_component_get_xml()):
            try:
                compiled, _internals = compile_authoring_request_v1(
                    AuthoringRequestV1.model_validate(case.input_canonical),
                    boomi_client=MagicMock(),
                    profile="issue-157-replay",
                )
                outcome = (bool(compiled.validation_report.is_valid), None)
            except AuthoringWorkflowError as exc:
                outcome = (False, exc.code)
        assert outcome == ((False, "AUTHORING_COMPILE_BLOCKED") if name_bound else (True, None))
        flows = acct.derive_case_projection(case)
        endpoints = acct.derive_case_endpoints(case)
        assert [acct._digest(row) for row in flows] == [row["payload_sha256"] for row in case.flow_rows]
        assert [acct._digest(row) for row in endpoints] == [
            row["payload_sha256"] for row in case.endpoint_rows
        ]

    @pytest.mark.parametrize("case_id", _LISTENER_CASES)
    def test_the_supporting_components_equal_the_legacy_routes(self, case_id):
        """Component-set parity: types, keys, actions, names, configs and
        dependencies, in order — the legacy process entry excepted, which the
        preset emits as a unit whose name and edges are the legacy process's."""
        inp, prepared, contributions = _prepare(_invocation(case_id))
        legacy = [c.model_dump(mode="json") for c in _legacy_spec(case_id).components]
        legacy_process = next(c for c in legacy if c["type"] == "process")
        assert [c.model_dump(mode="json") for c in prepared.supporting_components] == [
            c for c in legacy if c["type"] != "process"
        ]
        unit = contributions[0].unit
        assert unit.envelope.component_key == legacy_process["key"]
        assert unit.envelope.name == legacy_process["name"]
        assert set(unit.envelope.depends_on) == set(legacy_process["depends_on"])
        assert unit.envelope.process_extensions.connections == ()

    @pytest.mark.parametrize("case_id", _LISTENER_CASES)
    def test_the_process_xml_equals_the_legacy_arm(self, case_id):
        """Process XML parity: the unit's canonical root, materialized with the
        same synthetic ids the legacy arm resolves, is byte-identical to the
        legacy sync_pipeline arm — and its entry-derived profile is listener."""
        from boomi_mcp.authoring.contract import get_authoring_revisions
        from boomi_mcp.authoring.process_materialization import build_materialization_plan
        from boomi_mcp.authoring.workflow import _connector_metadata_from_components
        from boomi_mcp.categories.components import canonical_process_apply as cpa
        from boomi_mcp.categories.components.process_component_materializer import (
            LISTENER_PROCESS_OPTIONS,
        )
        from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision
        from boomi_mcp.compiler.process_ir.legacy_adapters.emission import emit_legacy_result
        from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
            adapt_sync_pipeline_config,
        )
        from boomi_mcp.recipes.materialization import build_symbol_table

        _inp, prepared, contributions = _prepare(_invocation(case_id))
        components = list(prepared.supporting_components)
        unit = contributions[0].unit
        symbols = build_symbol_table(
            components, connector_metadata=_connector_metadata_from_components(components)
        )
        plan = build_materialization_plan(
            envelope=unit.envelope,
            process_ir=unit.process_ir,
            symbols=symbols,
            conflict_policy="reuse",
            compiler_revision=get_authoring_revisions()["compiler_revision"],
            emitter_revision=emitter_revision(),
            materializer_revision="sha256:" + "a" * 64,
        )
        assert plan.execution_profile == "listener"
        # ONE deterministic id table for both arms (apply refuses the legacy
        # placeholder spelling as an unbound reference, by design).
        ids = {c.key: "cid-" + c.key for c in components}
        canonical = cpa.materialize_canonical_process_xml(
            plan=plan, id_registry=ids, symbols=symbols
        )
        process = next(c for c in _legacy_spec(case_id).components if c.type == "process")
        legacy = emit_legacy_result(
            adapt_sync_pipeline_config(process.config),
            resolver=lambda selector: ids[selector[len("$ref:"):] if selector.startswith("$ref:") else selector],
            dialect="sync_pipeline",
        ).process_xml
        # The legacy arm emits the bare <process> graph; apply wraps it in the
        # component envelope with the process options the entry derives. So
        # the GRAPH is compared byte for byte, and the options are the
        # live-captured listener invariants.
        def graph(xml):
            return xml[xml.index("<shapes>") : xml.index("</shapes>") + len("</shapes>")]

        assert graph(canonical) == graph(legacy)
        assert legacy.startswith('<process xmlns=""><shapes>')
        assert '<process xmlns="" ' + LISTENER_PROCESS_OPTIONS + ">" + graph(legacy) in canonical

    # -- refusals: typed, value-free, before any component is assembled -----

    _REFUSALS = {
        # API Service material without publishing there.
        "db_api_service_material_on_bare_wss": (
            "http_listener_to_db/plain",
            lambda raw, mat: mat.update(api_service={}),
            "/component_material/api_service",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
        # #158: an all-inherit route for a GET operation serves GET, refused for input.
        "db_api_service_route_uncallable_by_operation": (
            "http_listener_to_db/advanced_runtime",
            lambda raw, mat: mat["listener"].update(operation_type="GET"),
            "/component_material/listener/operation_type",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        "db_api_service_route_uncallable_by_method": (
            "http_listener_to_db/advanced_runtime",
            lambda raw, mat: mat["api_service"].update(http_method="GET"),
            "/component_material/api_service/http_method",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        # CDX-159-S2-r1-01: an effective objectName carrying '/' is never
        # served (#158 QA-158-s2r1-01, measured), on either listener preset;
        # the pointer names the field the effective name came from.
        "db_api_service_route_unserved_by_override": (
            "http_listener_to_db/advanced_runtime",
            lambda raw, mat: mat["api_service"].update(object_name="orders/5ecret-7a1c"),
            "/component_material/api_service/object_name",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        "db_api_service_route_unserved_by_listener_name": (
            "http_listener_to_db/advanced_runtime",
            lambda raw, mat: mat["listener"].update(object_name="orders/5ecret-7a1c"),
            "/component_material/listener/object_name",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        "db_api_service_route_unserved_behind_a_blank_override": (
            "http_listener_to_db/advanced_runtime",
            lambda raw, mat: (
                mat["api_service"].update(object_name=" "),
                mat["listener"].update(object_name="orders/5ecret-7a1c"),
            ),
            "/component_material/listener/object_name",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        "rest_api_service_route_unserved_by_override": (
            "http_listener_to_rest/plain",
            lambda raw, mat: (
                raw["listener"].update(publish_mode="api_service"),
                mat.update(api_service={"object_name": "events/5ecret-7a1c"}),
            ),
            "/component_material/api_service/object_name",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        "rest_api_service_route_unserved_by_listener_name": (
            "http_listener_to_rest/plain",
            lambda raw, mat: (
                raw["listener"].update(publish_mode="api_service"),
                mat.update(api_service={}),
                mat["listener"].update(object_name="events/5ecret-7a1c"),
            ),
            "/component_material/listener/object_name",
            "LISTENER_ASC_ROUTE_UNCALLABLE",
        ),
        "db_unknown_source_leaf": (
            "http_listener_to_db/plain",
            lambda raw, mat: mat["transform"]["operations"][0].update(source_field="Root/nope"),
            "/component_material/transform/operations/0",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
        "db_unknown_target_column": (
            "http_listener_to_db/plain",
            lambda raw, mat: mat["transform"]["operations"][1].update(target_path="Fields/nope"),
            "/component_material/transform/operations/1",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
        "db_column_bound_twice": (
            "http_listener_to_db/plain",
            lambda raw, mat: mat["transform"]["operations"][1].update(target_path="Fields/id"),
            "/component_material/transform/operations/1",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
        "db_mandatory_column_unmapped": (
            "http_listener_to_db/plain",
            lambda raw, mat: mat["transform"].update(operations=mat["transform"]["operations"][1:]),
            "/component_material/transform",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
        "db_process_key_names_a_component": (
            "http_listener_to_db/plain",
            lambda raw, mat: raw.update(process_key="target_db_connection"),
            "/raw_input/process_key",
            "COMPOSITION_COMPONENT_KEY_COLLISION",
        ),
        "db_plaintext_credential": (
            "http_listener_to_db/plain",
            lambda raw, mat: (
                raw["target"].update(binding_mode="create", auth_mode="username_password"),
                mat["target"].update(
                    connection={
                        "mode": "create",
                        "driver": "microsoft_jdbc",
                        "host": "db.example.invalid",
                        "database": "AppDB",
                        "username": "svc",
                        "credential_ref": "plaintext-5ecret-7a1c",
                    }
                ),
            ),
            "/component_material/target/connection/credential_ref",
            "PLAINTEXT_SECRET_REJECTED",
        ),
        "rest_dynamic_path": (
            "http_listener_to_rest/plain",
            lambda raw, mat: mat["target"]["operation"].update(path="/v1/items/{id}"),
            "/component_material/target/operation/path",
            "RECIPE_INPUT_INVALID",
        ),
        "rest_watermark_query_parameter": (
            "http_listener_to_rest/plain",
            lambda raw, mat: mat["target"]["operation"].update(
                query_parameters=[{"name": "limit", "value_source": "watermark"}]
            ),
            "/component_material/target/operation/query_parameters/0/value_source",
            "COMPOSITION_UNSUPPORTED_TOPOLOGY",
        ),
        "rest_unpublished_query_name": (
            "http_listener_to_rest/plain",
            lambda raw, mat: mat["target"]["operation"].update(
                query_parameters=[{"name": "api_key", "value_source": "literal", "literal_value": "x"}]
            ),
            "/component_material/target/operation/query_parameters/0/name",
            "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED",
        ),
        "rest_unknown_target_leaf": (
            "http_listener_to_rest/plain",
            lambda raw, mat: mat["transform"]["operations"][0].update(target_path="Root/nope"),
            "/component_material/transform/operations/0",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
        "rest_material_connection_beside_a_base_key": (
            "http_listener_to_rest/plain",
            lambda raw, mat: raw["target"].update(connection_key="base_rest"),
            "/component_material/target/connection",
            "COMPOSITION_CONTRACT_MISMATCH",
        ),
    }

    def test_every_refusal_is_typed_value_free_and_precedes_assembly(self, monkeypatch):
        from boomi_mcp.models.integration_models import IntegrationComponentSpec

        built = []
        real_init = IntegrationComponentSpec.__init__

        def spy(self, *args, **kwargs):
            built.append(1)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(IntegrationComponentSpec, "__init__", spy)
        # Control: each unmutated case DOES assemble (the spy sees it).
        for case_id in _LISTENER_CASES:
            del built[:]
            _prepare(_invocation(case_id))
            assert built, case_id
        # Control (CDX-159-S2-r1-01): a SERVED API Service route still
        # assembles on both presets — an explicit slash-free override, and the
        # REST preset published with an all-inherit route.
        for case_id, mutate in (
            (
                "http_listener_to_db/advanced_runtime",
                lambda raw, mat: mat["api_service"].update(object_name="orderIntakeV2"),
            ),
            (
                "http_listener_to_rest/plain",
                lambda raw, mat: (
                    raw["listener"].update(publish_mode="api_service"),
                    mat.update(api_service={}),
                ),
            ),
            (
                "http_listener_to_rest/plain",
                lambda raw, mat: (
                    raw["listener"].update(publish_mode="api_service"),
                    mat.update(api_service={"object_name": "eventRelayV2"}),
                ),
            ),
        ):
            invocation = _copy.deepcopy(_invocation(case_id))
            mutate(invocation["raw_input"], invocation["component_material"])
            del built[:]
            _prepare(invocation)
            assert built, case_id
        for name, (case_id, mutate, pointer, cause) in sorted(self._REFUSALS.items()):
            invocation = _copy.deepcopy(_invocation(case_id))
            mutate(invocation["raw_input"], invocation["component_material"])
            del built[:]
            with pytest.raises(RecipeError) as caught:
                _prepare(invocation)
            (diagnostic,) = caught.value.diagnostics
            assert diagnostic.path == pointer, name
            assert cause in diagnostic.cause_codes, name
            # Value-free: no mutated value (the credential, an object name)
            # reaches the refusal; every such value carries this token.
            assert "5ecret-7a1c" not in repr(caught.value), name
            assert "5ecret-7a1c" not in str(caught.value), name
            assert built == [], name

    def test_the_root_is_a_listener_entry_and_publishing_requires_the_api_service(self):
        """The executor's root is listener -> map -> target -> stop; inbound
        validation rides the entry; the API Service is a REQUIRED component
        exactly when the input publishes there."""
        for case_id, validated, published in (
            ("http_listener_to_db/plain", True, False),
            ("http_listener_to_db/advanced_runtime", False, True),
            ("http_listener_to_rest/plain", True, False),
        ):
            _inp, _prepared, contributions = _prepare(_invocation(case_id))
            steps = contributions[0].unit.process_ir.body.steps
            assert [step.kind for step in steps] == ["listener", "map_ref", "target", "stop"]
            assert (steps[0].inbound_validation is not None) is validated
            required = {
                c.requirement.component_key
                for c in contributions
                if getattr(c, "requirement", None) is not None
                and c.requirement.kind == "component"
            }
            assert ("api_service" in required) is published

    def test_the_advertised_route_follows_the_requests_conflict_policy(self, listener_preset_registry):
        """#159 architect review r1, finding 4, through the PUBLIC planner: an API
        Service of the same name already exists. Under ``reuse`` apply binds it
        without writing its routes, so the listener row is served WITHOUT the
        route it would read off the candidate (``method``, ``endpoint_path``,
        ``publish_mode``) — and, since r2-01, nothing else changes: the listener
        row keeps its binding, and the database endpoint and every flow row are
        served exactly as under ``clone``, where apply writes the clone with this
        process's route and the frozen listener row is served. The policy
        reaches the projection from the request itself."""
        import _issue_157_flows_accounting as acct
        from boomi_mcp.authoring.workflow import plan_authoring_request_v1
        from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

        case_id = "http_listener_to_db/advanced_runtime"
        # the namesake's name comes from the legacy oracle, not the preset
        (service_name,) = [
            c.name for c in _legacy_spec(case_id).components if c.type == "webservice"
        ]
        rows = dict(acct.replay_identity_names())
        rows["<<namesake api service>>"] = {
            "component_id": "existing-api-service-id", "id": "existing-api-service-id",
            "name": service_name, "folder_name": "", "type": "webservice", "version": 1,
            "current_version": True, "deleted": False,
        }
        (frozen,) = [
            row["payload"]
            for row in acct.load_cases()[case_id].endpoint_rows
            if row["payload"].get("type") == "wss"
        ]

        def served(policy):
            payload = _canonical(case_id)
            payload["intent"]["conflict_policy"] = policy
            with patch(acct._GET_XML, acct.replay_component_get_xml()), patch(
                acct._PAGINATE, acct.replay_paginate_metadata(rows)
            ):
                result, _internals = plan_authoring_request_v1(
                    AuthoringRequestV1.model_validate(payload),
                    boomi_client=MagicMock(),
                    profile="issue-157-replay",
                )
            assert "existing-api-service-id" in json.dumps(result.model_dump(mode="json"))
            preview = result.integration_spec_preview
            return (
                [row.model_dump(mode="json", exclude_unset=True) for row in preview.flows],
                [row.model_dump(mode="json", exclude_unset=True) for row in preview.endpoints],
            )

        reused_flows, reused_endpoints = served("reuse")
        cloned_flows, cloned_endpoints = served("clone")
        assert [row for row in cloned_endpoints if row["type"] == "wss"] == [frozen]
        assert {row["type"] for row in cloned_endpoints} == {"wss", "database"}
        assert reused_flows == cloned_flows and len(reused_flows) > 1
        route_keys = {"method", "endpoint_path", "publish_mode"}
        assert route_keys <= set(frozen)
        assert reused_endpoints == [
            {k: v for k, v in row.items() if k not in route_keys} if row["type"] == "wss" else row
            for row in cloned_endpoints
        ]

    def test_the_preset_modules_import_no_legacy_producer(self):
        """The presets may never reach the retiring archetypes or the bridge."""
        import ast
        import inspect

        for module in (_db_preset, _rest_preset):
            tree = ast.parse(inspect.getsource(module))
            imported = {
                (node.module or "") for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
            } | {
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
                for alias in node.names
            }
            assert not {
                name for name in imported if "archetypes" in name.split(".") or name.endswith("recipe_bridge")
            }, module.__name__


def test_l2_served_method_text_states_the_get_with_input_refusal():
    """QA-159-S2-r4-01: every input these presets accept is a request body, so a
    GET-derived route is always refused; the served method text says so, in the
    method authority's own words, beside the code the refusal carries (the
    refusal itself is witnessed by the ``*_route_uncallable_*`` cases)."""
    from boomi_mcp.categories.components.wss_route_methods import (
        ASC_GET_WITH_INPUT_RULE,
    )
    from boomi_mcp.recipes.builtins.http_listener_to_db import HttpListenerToDbMaterialV1
    from boomi_mcp.recipes.builtins.http_listener_to_rest import HttpListenerToRestMaterialV1

    for model in (HttpListenerToDbMaterialV1, HttpListenerToRestMaterialV1):
        defs = model.model_json_schema()["$defs"]
        for owner, field in (
            ("ListenerOperationMaterialV1", "operation_type"),
            ("ApiServiceMaterialV1", "http_method"),
        ):
            text = defs[owner]["properties"][field]["description"]
            assert ASC_GET_WITH_INPUT_RULE in text, (model.__name__, owner)
            assert "LISTENER_ASC_ROUTE_UNCALLABLE" in text, (model.__name__, owner)
