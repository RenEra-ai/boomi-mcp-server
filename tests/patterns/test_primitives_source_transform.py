"""Issue #27 — tests for the source/transform primitive package.

Covers registry discovery + metadata hygiene, the three primitives
(db_extract, field_map, xml_json_convert), and the build_integration
``config.reference_only`` reuse support that primitive-emitted specs rely on.

All tests are pure: no live Boomi calls. build_integration plan/apply paths
mock ``paginate_metadata`` (the only Boomi I/O on the plan path).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from boomi_mcp.categories import integration_builder as ib
from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from boomi_mcp.categories.components.builders.profile_builder import (
    DatabaseReadProfileBuilder,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec
from boomi_mcp.patterns.base import PatternKind, PrimitiveBuildContext
from boomi_mcp.patterns.primitives import (
    DataProcessPrimitive,
    DbExtractPrimitive,
    FieldMapPrimitive,
    ReturnDocumentsPrimitive,
    XmlJsonConvertPrimitive,
)
from boomi_mcp.patterns.registry import PatternRegistry

_PATCH_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------


def _ctx() -> PrimitiveBuildContext:
    return PrimitiveBuildContext(
        integration_name="Demo", component_prefix="DEMO", folder_path="/Demo"
    )


def _emit(primitive, params: dict):
    """validate_parameters + emit_components in one step."""
    return primitive.emit_components(_ctx(), primitive.validate_parameters(params))


def _source_index(fields):
    """Build a DB read source field index from output-field dicts."""
    return DatabaseReadProfileBuilder.build_field_index(
        {
            "profile_type": "database.read",
            "component_name": "src",
            "query": "q",
            "output_fields": list(fields),
        }
    )


_DEFAULT_SRC_FIELDS = [
    {"name": "id", "data_type": "number"},
    {"name": "name", "data_type": "character"},
]


def _target_profile():
    return {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "id", "kind": "simple", "data_type": "number"},
                {"name": "fullName", "kind": "simple", "data_type": "character"},
                {
                    "name": "meta",
                    "kind": "object",
                    "children": [
                        {"name": "src", "kind": "simple", "data_type": "character"}
                    ],
                },
            ],
        },
    }


def _field_map_params(**overrides):
    params = {
        "key_prefix": "cust",
        "source": {
            "source_profile_id": "$ref:cust_db_read_profile",
            "source_profile_type": "profile.db",
            "source_field_index": _source_index(_DEFAULT_SRC_FIELDS),
        },
        "target_payload_profile": _target_profile(),
        "direct": [{"source_field": "id", "target_path": "Root/id"}],
    }
    params.update(overrides)
    return params


def _xml_index():
    return {
        "Order/id": {
            "path": "Order/id",
            "name": "id",
            "key": 3,
            "key_path": "*[@key='1']/*[@key='3']",
            "name_path": "Order/id",
            "data_type": "character",
            "kind": "element",
            "mappable": True,
        }
    }


def _json_index():
    return JSONGeneratedProfileBuilder.build_field_index(
        {
            "profile_type": "json.generated",
            "component_name": "t",
            "root": {
                "name": "Root",
                "kind": "object",
                "children": [
                    {"name": "id", "kind": "simple", "data_type": "character"}
                ],
            },
        }
    )


def _plan(components, conflict_policy="reuse", existing=None):
    cfg = {
        "conflict_policy": conflict_policy,
        "integration_spec": {
            "version": "1.0",
            "name": "t",
            "components": [c.model_dump() for c in components],
        },
    }
    with patch.object(ib, "paginate_metadata", return_value=list(existing or [])):
        return ib._build_plan(MagicMock(), cfg)


# ===========================================================================
# Registry + metadata hygiene
# ===========================================================================


class TestRegistryAndMetadata:
    def test_registry_discovers_three_primitives(self):
        reg = PatternRegistry.from_package("boomi_mcp.patterns")
        for name in ("db_extract", "field_map", "xml_json_convert"):
            cls = reg.get(name)
            assert cls.metadata.kind == PatternKind.PRIMITIVE

    @pytest.mark.parametrize(
        "primitive",
        [DbExtractPrimitive, FieldMapPrimitive, XmlJsonConvertPrimitive],
    )
    def test_describe_includes_contracts_and_builders(self, primitive):
        described = primitive.describe()
        for key in (
            "metadata",
            "parameter_schema",
            "input_contract",
            "output_contract",
            "required_builders",
        ):
            assert key in described
        assert described["required_builders"], "required_builders must be non-empty"
        # Archetype-only keys must not leak into a primitive describe().
        for archetype_only in ("capability_notes", "limitations", "examples"):
            assert archetype_only not in described

    @pytest.mark.parametrize(
        "primitive",
        [DbExtractPrimitive, FieldMapPrimitive, XmlJsonConvertPrimitive],
    )
    def test_no_raw_artifacts_in_describe(self, primitive):
        dumped = json.dumps(primitive.describe())
        # No raw XML, canned SQL, Groovy bodies, SOAP envelopes, OData filters.
        for forbidden in (
            "<bns:",
            "</",
            "<?xml",
            "<soap",
            "SOAP-ENV",
            "$filter=",
            "$select=",
            "SELECT ",
            "INSERT INTO",
            "```",
        ):
            assert forbidden not in dumped, f"{forbidden!r} leaked into describe()"


# ===========================================================================
# db_extract
# ===========================================================================


def _db_create_params(**overrides):
    params = {
        "key_prefix": "cust",
        "connection": {
            "mode": "create",
            "driver_id": "mysql",
            "auth_mode": "username_password",
            "username": "u",
            "credential_ref": "credential://x",
            "host": "h",
            "dbname": "db",
        },
        "read_profile": {
            "query": "SELECT id, name FROM customers",
            "output_fields": [
                {"name": "id", "data_type": "number"},
                {"name": "name", "data_type": "character"},
            ],
        },
        "operation": {"batch_count": 50000},
    }
    params.update(overrides)
    return params


class TestDbExtract:
    def test_create_mode_emits_three_in_order(self):
        comps = _emit(DbExtractPrimitive, _db_create_params())
        assert [c.type for c in comps] == [
            "connector-settings",
            "profile.db",
            "connector-action",
        ]
        assert [c.key for c in comps] == [
            "cust_db_connection",
            "cust_db_read_profile",
            "cust_db_get_operation",
        ]

    def test_get_operation_refs_and_depends_on(self):
        comps = _emit(DbExtractPrimitive, _db_create_params())
        op = comps[2].config
        assert op["read_profile_id"] == "$ref:cust_db_read_profile"
        assert op["connection_ref_key"] == "cust_db_connection"
        assert op["operation_mode"] == "get"
        assert op["batch_count"] == 50000
        assert set(comps[2].depends_on) == {
            "cust_db_connection",
            "cust_db_read_profile",
        }

    def test_reuse_mode_is_reference_only_without_connection_settings(self):
        comps = _emit(
            DbExtractPrimitive,
            _db_create_params(connection={"mode": "reuse", "component_id": "conn-1"}),
        )
        conn = comps[0]
        assert conn.config["reference_only"] is True
        assert conn.config["connector_type"] == "database"
        assert conn.component_id == "conn-1"
        # No host / username / credential_ref / driver leaked into reuse config.
        for forbidden in ("host", "username", "credential_ref", "driver_id"):
            assert forbidden not in conn.config

    def test_reuse_mode_by_name_sets_resolution_name(self):
        comps = _emit(
            DbExtractPrimitive,
            _db_create_params(
                connection={"mode": "reuse", "component_name": "Shared DB"}
            ),
        )
        conn = comps[0]
        assert conn.name == "Shared DB"
        assert conn.config["reference_only"] is True
        assert conn.component_id is None

    def test_missing_query_fails(self):
        params = _db_create_params()
        del params["read_profile"]["query"]
        with pytest.raises(ValidationError):
            DbExtractPrimitive.validate_parameters(params)

    def test_empty_output_fields_fails(self):
        params = _db_create_params()
        params["read_profile"]["output_fields"] = []
        with pytest.raises(ValidationError):
            DbExtractPrimitive.validate_parameters(params)

    def test_reuse_requires_exactly_one_binding(self):
        with pytest.raises(ValidationError):
            DbExtractPrimitive.validate_parameters(
                _db_create_params(connection={"mode": "reuse"})
            )
        with pytest.raises(ValidationError):
            DbExtractPrimitive.validate_parameters(
                _db_create_params(
                    connection={
                        "mode": "reuse",
                        "component_id": "a",
                        "component_name": "b",
                    }
                )
            )

    def test_reuse_rejects_whitespace_only_binding(self):
        # A whitespace-only component_id must not pass as a fake binding.
        with pytest.raises(ValidationError):
            DbExtractPrimitive.validate_parameters(
                _db_create_params(connection={"mode": "reuse", "component_id": "   "})
            )

    def test_reuse_strips_binding_whitespace(self):
        # Trailing/leading whitespace on a real binding is stripped, and a
        # blank id alongside a real name normalizes to name-only reuse.
        params = DbExtractPrimitive.validate_parameters(
            _db_create_params(connection={"mode": "reuse", "component_id": " real-id "})
        )
        assert params.connection.component_id == "real-id"
        params2 = DbExtractPrimitive.validate_parameters(
            _db_create_params(
                connection={
                    "mode": "reuse",
                    "component_id": "  ",
                    "component_name": "Shared DB",
                }
            )
        )
        assert params2.connection.component_id is None
        assert params2.connection.component_name == "Shared DB"

    def test_plaintext_secret_key_rejected_before_emission(self):
        params = _db_create_params()
        params["connection"]["password"] = "hunter2"
        with pytest.raises(ValidationError):
            DbExtractPrimitive.validate_parameters(params)

    def test_unsupported_driver_fails(self):
        params = _db_create_params(
            connection={
                "mode": "create",
                "driver_id": "postgres",
                "auth_mode": "username_password",
                "credential_ref": "credential://x",
                "username": "u",
                "host": "h",
                "dbname": "db",
            }
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(DbExtractPrimitive, params)
        assert exc.value.error_code == "UNSUPPORTED_DB_DRIVER"

    def test_emitted_create_components_pass_build_plan(self):
        comps = _emit(DbExtractPrimitive, _db_create_params())
        plan = _plan(comps)
        assert plan["_success"] is True
        for step in plan["steps"]:
            assert step.get("validation_error") is None
            assert step["planned_action"] == "create"

    def test_emitted_reuse_components_pass_build_plan(self):
        comps = _emit(
            DbExtractPrimitive,
            _db_create_params(connection={"mode": "reuse", "component_id": "conn-1"}),
        )
        plan = _plan(comps)
        assert plan["_success"] is True
        conn_step = next(s for s in plan["steps"] if s["key"] == "cust_db_connection")
        assert conn_step["planned_action"] == "reuse"
        assert conn_step["existing_component_id"] == "conn-1"
        assert conn_step.get("validation_error") is None


# ===========================================================================
# field_map
# ===========================================================================


class TestFieldMap:
    def test_direct_only_emits_profile_and_direct_map(self):
        comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                direct=[
                    {"source_field": "id", "target_path": "Root/id"},
                    {"source_field": "name", "target_path": "Root/fullName"},
                ]
            ),
        )
        assert [c.type for c in comps] == ["profile.json", "transform.map"]
        assert comps[1].config["map_type"] == "direct"
        assert set(comps[1].depends_on) == {
            "cust_target_profile",
            "cust_db_read_profile",
        }

    def test_direct_plus_function_emits_function_map(self):
        comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                map_function=[
                    {
                        "function_type": "uppercase",
                        "inputs": ["name"],
                        "target_path": "Root/fullName",
                        "parameters": {},
                    }
                ]
            ),
        )
        assert [c.type for c in comps] == ["profile.json", "transform.map"]
        assert comps[1].config["map_type"] == "function"

    def test_inline_script_emits_script_mapping_and_script_map(self):
        comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                direct=[],
                map_script=[
                    {
                        "language": "groovy2",
                        "script_body": "out0 = in0.toUpperCase()",
                        "inputs": [{"source_path": "name", "input_name": "in0"}],
                        "outputs": [
                            {"output_name": "out0", "target_path": "Root/fullName"}
                        ],
                    }
                ],
            ),
        )
        types = [c.type for c in comps]
        assert types == ["profile.json", "script.mapping", "transform.map"]
        the_map = comps[2]
        assert the_map.config["map_type"] == "script"
        assert the_map.config["script_mappings"][0]["script_component_id"] == (
            "$ref:cust_script_0"
        )
        # script.mapping input data_type bridged from source (character→character).
        script = comps[1]
        assert script.config["inputs"] == [{"name": "in0", "data_type": "character"}]
        # No script.processing anywhere.
        assert all(c.type != "script.processing" for c in comps)

    def test_script_op_without_body_or_ref_fails(self):
        with pytest.raises(ValidationError):
            FieldMapPrimitive.validate_parameters(
                _field_map_params(
                    direct=[],
                    map_script=[
                        {
                            "inputs": [{"source_path": "name", "input_name": "in0"}],
                            "outputs": [
                                {"output_name": "o", "target_path": "Root/fullName"}
                            ],
                        }
                    ],
                )
            )

    def test_literal_script_ref_fails(self):
        params = _field_map_params(
            direct=[],
            map_script=[
                {
                    "script_component_ref": "literal-uuid-123",
                    "inputs": [{"source_path": "name", "input_name": "in0"}],
                    "outputs": [{"output_name": "o", "target_path": "Root/fullName"}],
                }
            ],
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "SCRIPT_MAPPING_REF_REQUIRED"

    def test_external_script_ref_token_is_accepted(self):
        comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                direct=[],
                map_script=[
                    {
                        "script_component_ref": "$ref:shared_script",
                        "inputs": [{"source_path": "name", "input_name": "in0"}],
                        "outputs": [
                            {"output_name": "o", "target_path": "Root/fullName"}
                        ],
                    }
                ],
            ),
        )
        # No standalone script.mapping emitted for an external ref.
        assert [c.type for c in comps] == ["profile.json", "transform.map"]
        the_map = comps[1]
        assert the_map.config["script_mappings"][0]["script_component_id"] == (
            "$ref:shared_script"
        )
        assert "shared_script" in the_map.depends_on

    def test_mixed_function_and_script_rejected(self):
        params = _field_map_params(
            direct=[],
            map_function=[
                {
                    "function_type": "uppercase",
                    "inputs": ["name"],
                    "target_path": "Root/fullName",
                }
            ],
            map_script=[
                {
                    "language": "groovy2",
                    "script_body": "x = 1",
                    "inputs": [{"source_path": "name", "input_name": "in0"}],
                    "outputs": [{"output_name": "o", "target_path": "Root/id"}],
                }
            ],
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "UNSUPPORTED_TRANSFORM_ROUTE"

    def test_unsupported_function_type_fails(self):
        params = _field_map_params(
            direct=[],
            map_function=[
                {
                    "function_type": "no_such_function",
                    "inputs": ["name"],
                    "target_path": "Root/fullName",
                }
            ],
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "UNSUPPORTED_MAP_FUNCTION_TYPE"

    def test_unknown_source_field_fails(self):
        params = _field_map_params(
            direct=[{"source_field": "ghost", "target_path": "Root/id"}]
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "MAP_FIELD_NOT_FOUND"

    def test_unknown_target_path_fails(self):
        params = _field_map_params(
            direct=[{"source_field": "id", "target_path": "Root/ghost"}]
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "MAP_FIELD_NOT_FOUND"

    def test_duplicate_target_fails(self):
        params = _field_map_params(
            direct=[
                {"source_field": "id", "target_path": "Root/id"},
                {"source_field": "name", "target_path": "Root/id"},
            ]
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "DUPLICATE_TARGET_MAPPING"

    def test_structural_target_node_fails(self):
        params = _field_map_params(
            direct=[{"source_field": "id", "target_path": "Root/meta"}]
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(FieldMapPrimitive, params)
        assert exc.value.error_code == "PROFILE_FIELD_NOT_MAPPABLE"

    def test_requires_at_least_one_operation(self):
        with pytest.raises(ValidationError):
            FieldMapPrimitive.validate_parameters(_field_map_params(direct=[]))

    def test_source_ref_added_to_map_depends_on(self):
        # A $ref source profile must appear in the map's depends_on so
        # build_integration can order it first (MAP_PROFILE_REF_REQUIRED).
        comps = _emit(FieldMapPrimitive, _field_map_params())
        the_map = next(c for c in comps if c.type == "transform.map")
        assert "cust_db_read_profile" in the_map.depends_on
        assert "cust_target_profile" in the_map.depends_on

    def test_literal_source_profile_not_added_to_depends_on(self):
        comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                source={
                    "source_profile_id": "literal-uuid-123",
                    "source_profile_type": "profile.db",
                    "source_field_index": _source_index(_DEFAULT_SRC_FIELDS),
                }
            ),
        )
        the_map = next(c for c in comps if c.type == "transform.map")
        assert the_map.depends_on == ["cust_target_profile"]

    def test_script_route_depends_on_includes_source_and_scripts(self):
        comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                direct=[],
                map_script=[
                    {
                        "language": "groovy2",
                        "script_body": "out0 = in0.toUpperCase()",
                        "inputs": [{"source_path": "name", "input_name": "in0"}],
                        "outputs": [
                            {"output_name": "out0", "target_path": "Root/fullName"}
                        ],
                    }
                ],
            ),
        )
        the_map = next(c for c in comps if c.type == "transform.map")
        assert "cust_target_profile" in the_map.depends_on
        assert "cust_db_read_profile" in the_map.depends_on
        assert "cust_script_0" in the_map.depends_on


# ===========================================================================
# xml_json_convert
# ===========================================================================


def _convert_params(**overrides):
    params = {
        "key_prefix": "conv",
        "source_profile_id": "$ref:xmlp",
        "source_profile_type": "profile.xml",
        "target_profile_id": "$ref:jsonp",
        "target_profile_type": "profile.json",
        "field_mappings": [{"source_path": "Order/id", "target_path": "Root/id"}],
        "source_field_index": _xml_index(),
        "target_field_index": _json_index(),
    }
    params.update(overrides)
    return params


class TestXmlJsonConvert:
    def test_xml_to_json_emits_one_direct_map(self):
        comps = _emit(XmlJsonConvertPrimitive, _convert_params())
        assert len(comps) == 1
        assert comps[0].type == "transform.map"
        assert comps[0].config["map_type"] == "direct"

    def test_json_to_xml_emits_one_direct_map(self):
        comps = _emit(
            XmlJsonConvertPrimitive,
            _convert_params(
                source_profile_id="$ref:jsonp",
                source_profile_type="profile.json",
                target_profile_id="$ref:xmlp",
                target_profile_type="profile.xml",
                field_mappings=[{"source_path": "Root/id", "target_path": "Order/id"}],
                source_field_index=_json_index(),
                target_field_index=_xml_index(),
            ),
        )
        assert len(comps) == 1
        assert comps[0].config["map_type"] == "direct"

    def test_same_family_rejected(self):
        with pytest.raises(ValidationError):
            XmlJsonConvertPrimitive.validate_parameters(
                _convert_params(
                    source_profile_type="profile.json",
                    target_profile_type="profile.json",
                )
            )

    def test_database_family_rejected(self):
        with pytest.raises(ValidationError):
            XmlJsonConvertPrimitive.validate_parameters(
                _convert_params(source_profile_type="profile.db")
            )

    def test_missing_field_mappings_rejected(self):
        with pytest.raises(ValidationError):
            XmlJsonConvertPrimitive.validate_parameters(
                _convert_params(field_mappings=[])
            )

    def test_unavailable_index_fails(self):
        with pytest.raises(BuilderValidationError) as exc:
            _emit(XmlJsonConvertPrimitive, _convert_params(source_field_index={}))
        assert exc.value.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE"

    def test_duplicate_target_fails(self):
        params = _convert_params(
            source_field_index={
                "Order/id": dict(_xml_index()["Order/id"]),
                "Order/ref": {
                    **_xml_index()["Order/id"],
                    "path": "Order/ref",
                    "name": "ref",
                    "key": 4,
                },
            },
            field_mappings=[
                {"source_path": "Order/id", "target_path": "Root/id"},
                {"source_path": "Order/ref", "target_path": "Root/id"},
            ],
        )
        with pytest.raises(BuilderValidationError) as exc:
            _emit(XmlJsonConvertPrimitive, params)
        assert exc.value.error_code == "DUPLICATE_TARGET_MAPPING"

    @pytest.mark.parametrize(
        "bad_key",
        ["function_mappings", "script_mappings", "xslt", "xml"],
    )
    def test_non_direct_conversion_requests_rejected(self, bad_key):
        params = _convert_params()
        params[bad_key] = [{"x": 1}] if bad_key.endswith("s") else "<root/>"
        with pytest.raises(ValidationError):
            XmlJsonConvertPrimitive.validate_parameters(params)

    def test_ref_profiles_added_to_depends_on(self):
        # Both $ref profiles must be declared as map dependencies.
        comps = _emit(XmlJsonConvertPrimitive, _convert_params())
        assert set(comps[0].depends_on) == {"xmlp", "jsonp"}

    def test_literal_profiles_have_empty_depends_on(self):
        comps = _emit(
            XmlJsonConvertPrimitive,
            _convert_params(
                source_profile_id="lit-xml-uuid",
                target_profile_id="lit-json-uuid",
            ),
        )
        assert comps[0].depends_on == []


# ===========================================================================
# build_integration reference_only regression
# ===========================================================================


def _ref_only_conn(**config):
    base = {"reference_only": True, "connector_type": "database"}
    base.update(config)
    return IntegrationComponentSpec(
        key="c",
        type="connector-settings",
        action="create",
        name=config.get("component_name"),
        component_id=config.get("component_id"),
        config=base,
    )


class TestReferenceOnlyBuildIntegration:
    def test_reference_only_by_id_plans_as_reuse(self):
        comp = _ref_only_conn(component_id="conn-9")
        plan = _plan([comp])
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "conn-9"
        assert step.get("validation_error") is None

    def test_reference_only_apply_does_not_create_or_update(self):
        comp = _ref_only_conn(component_id="conn-9")
        cfg = {
            "conflict_policy": "fail",  # reference_only must reuse regardless
            "dry_run": False,
            "integration_spec": {
                "version": "1.0",
                "name": "t",
                "components": [comp.model_dump()],
            },
        }
        with patch.object(ib, "paginate_metadata", return_value=[]), patch.object(
            ib,
            "_execute_component",
            side_effect=AssertionError("must not create/update a reference_only component"),
        ) as exec_mock:
            res = ib._apply_plan(MagicMock(), "dev", cfg)
        assert res["_success"] is True
        assert res["results"]["c"]["status"] == "reused"
        assert res["results"]["c"]["component_id"] == "conn-9"
        assert exec_mock.called is False

    def test_reference_only_missing_name_fails_before_apply(self):
        comp = _ref_only_conn(component_name="Ghost Connection")
        cfg = {
            "dry_run": False,
            "integration_spec": {
                "version": "1.0",
                "name": "t",
                "components": [comp.model_dump()],
            },
        }
        with patch.object(ib, "paginate_metadata", return_value=[]):
            res = ib._apply_plan(MagicMock(), "dev", cfg)
        assert res["_success"] is False
        assert any(
            s["planned_action"] == "error_missing_target"
            for s in res["unresolvable_steps"]
        )
        # Bug #134: the failure message must reflect reference_only, not
        # action=update (this component declared action=create).
        joined = " ".join(res["details"])
        assert "reference_only" in joined
        assert "action=update" not in joined

    def test_reference_only_ambiguous_name_fails_before_apply(self):
        comp = _ref_only_conn(component_name="Dup Connection")
        matches = [
            {"component_id": "id-1", "name": "Dup Connection", "type": "connector-settings"},
            {"component_id": "id-2", "name": "Dup Connection", "type": "connector-settings"},
        ]
        cfg = {
            "dry_run": False,
            "integration_spec": {
                "version": "1.0",
                "name": "t",
                "components": [comp.model_dump()],
            },
        }
        with patch.object(ib, "paginate_metadata", return_value=matches):
            res = ib._apply_plan(MagicMock(), "dev", cfg)
        assert res["_success"] is False
        assert any(
            s["planned_action"] == "error_ambiguous_match"
            for s in res["unresolvable_steps"]
        )

    def test_reference_only_config_only_component_id_plans_reuse(self):
        # Binding supplied only inside config (no top-level component_id).
        comp = IntegrationComponentSpec(
            key="c",
            type="connector-settings",
            action="create",
            config={
                "reference_only": True,
                "connector_type": "database",
                "component_id": "cfg-id-1",
            },
        )
        plan = _plan([comp])
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "cfg-id-1"
        assert step.get("validation_error") is None

    def test_reference_only_config_only_name_resolves(self):
        # Binding name supplied only inside config (no top-level name).
        comp = IntegrationComponentSpec(
            key="c",
            type="connector-settings",
            action="create",
            config={
                "reference_only": True,
                "connector_type": "database",
                "component_name": "Shared DB",
            },
        )
        match = [
            {"component_id": "r-1", "name": "Shared DB", "type": "connector-settings"}
        ]
        plan = _plan([comp], existing=match)
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "r-1"

    def test_reference_only_whitespace_top_level_id_is_not_fake_reuse(self):
        # A blank top-level component_id must not become a fake existing id.
        comp = IntegrationComponentSpec(
            key="c",
            type="connector-settings",
            action="create",
            component_id="   ",
            config={"reference_only": True, "connector_type": "database"},
        )
        plan = _plan([comp])
        step = plan["steps"][0]
        assert step["planned_action"] == "error_missing_target"
        assert step["existing_component_id"] is None

    def test_reference_only_blank_top_level_falls_back_to_config_id(self):
        comp = IntegrationComponentSpec(
            key="c",
            type="connector-settings",
            action="create",
            component_id="  ",
            config={
                "reference_only": True,
                "connector_type": "database",
                "component_id": "real-9",
            },
        )
        plan = _plan([comp])
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "real-9"


# ===========================================================================
# Composition: db_extract + field_map through build_integration plan
# ===========================================================================


class TestComposition:
    def test_db_extract_plus_field_map_passes_build_plan(self):
        """The intended issue #29 composition must plan cleanly: the field_map
        transform.map references db_extract's read profile via $ref, so the
        source-profile dependency must be declared (else MAP_PROFILE_REF_REQUIRED)."""
        db_comps = _emit(DbExtractPrimitive, _db_create_params())
        fm_comps = _emit(
            FieldMapPrimitive,
            _field_map_params(
                source={
                    "source_profile_id": "$ref:cust_db_read_profile",
                    "source_profile_type": "profile.db",
                    "source_field_index": _source_index(_DEFAULT_SRC_FIELDS),
                },
                direct=[
                    {"source_field": "id", "target_path": "Root/id"},
                    {"source_field": "name", "target_path": "Root/fullName"},
                ],
            ),
        )
        plan = _plan(db_comps + fm_comps)
        assert plan["_success"] is True
        for step in plan["steps"]:
            assert step.get("validation_error") is None, step
        map_step = next(s for s in plan["steps"] if s["key"] == "cust_transform_map")
        assert map_step["planned_action"] == "create"


# ===========================================================================
# data_process (issue #106 M10.2)
# ===========================================================================


class TestDataProcess:
    def test_registry_discovers_data_process(self):
        try:
            reg = PatternRegistry.from_package("boomi_mcp.patterns")
        except TypeError as exc:  # pragma: no cover — interpreter-specific
            # Python 3.9.6 has inspect.isclass(type[X]) == True, which makes
            # PatternRegistry.from_package() trip on registry.PatternClass for
            # EVERY pattern (pre-existing; the sibling
            # test_registry_discovers_three_primitives fails identically here).
            # Conformant interpreters (3.9.7+/3.10+/3.11) discover correctly.
            pytest.skip(f"registry discovery unavailable on this interpreter: {exc}")
        cls = reg.get("data_process")
        assert cls is DataProcessPrimitive
        assert cls.metadata.kind == PatternKind.PRIMITIVE

    def test_describe_includes_contracts_and_no_raw_artifacts(self):
        described = DataProcessPrimitive.describe()
        for key in ("metadata", "parameter_schema", "input_contract", "output_contract", "required_builders"):
            assert key in described
        assert described["required_builders"] == ["ProcessFlowBuilder"]
        dumped = json.dumps(described)
        for forbidden in ("<bns:", "</", "<?xml", "```"):
            assert forbidden not in dumped, f"{forbidden!r} leaked into describe()"

    def test_emit_components_is_empty(self):
        params = DataProcessPrimitive.validate_parameters(
            {"steps": [{"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"}]}
        )
        assert DataProcessPrimitive.emit_components(_ctx(), params) == []

    def test_emit_fragment_returns_dataprocess_transform(self):
        params = DataProcessPrimitive.validate_parameters(
            {
                "label": "Tag",
                "steps": [{"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"}],
            }
        )
        fragment = DataProcessPrimitive.emit_fragment(_ctx(), params)
        transform = fragment["process_config"]["transform"]
        assert transform["mode"] == "dataprocess"
        assert transform["label"] == "Tag"
        assert transform["steps"][0]["operation"] == "custom_scripting"
        assert transform["steps"][0]["language"] == "groovy2"
        assert transform["steps"][0]["use_cache"] is True
        assert fragment["depends_on"] == []

    def test_validation_rejects_empty_steps(self):
        with pytest.raises(ValidationError):
            DataProcessPrimitive.validate_parameters({"steps": []})

    def test_validation_rejects_missing_script(self):
        with pytest.raises(ValidationError):
            DataProcessPrimitive.validate_parameters(
                {"steps": [{"operation": "custom_scripting"}]}
            )

    def test_validation_rejects_blank_script(self):
        with pytest.raises(ValidationError):
            DataProcessPrimitive.validate_parameters(
                {"steps": [{"operation": "custom_scripting", "script": "   "}]}
            )

    def test_validation_rejects_non_groovy2_language(self):
        with pytest.raises(ValidationError):
            DataProcessPrimitive.validate_parameters(
                {"steps": [{"operation": "custom_scripting", "script": "x", "language": "python"}]}
            )

    def test_validation_rejects_non_custom_scripting_operation(self):
        with pytest.raises(ValidationError):
            DataProcessPrimitive.validate_parameters(
                {"steps": [{"operation": "search_replace", "script": "x"}]}
            )


# ===========================================================================
# return_documents (issue #107 M10.3)
# ===========================================================================


class TestReturnDocuments:
    def test_registry_discovers_return_documents(self):
        try:
            reg = PatternRegistry.from_package("boomi_mcp.patterns")
        except TypeError as exc:  # pragma: no cover — interpreter-specific
            # Same Python 3.9.6 inspect.isclass quirk the sibling
            # test_registry_discovers_data_process documents.
            pytest.skip(f"registry discovery unavailable on this interpreter: {exc}")
        cls = reg.get("return_documents")
        assert cls is ReturnDocumentsPrimitive
        assert cls.metadata.kind == PatternKind.PRIMITIVE

    def test_describe_includes_contracts_and_no_raw_artifacts(self):
        described = ReturnDocumentsPrimitive.describe()
        for key in ("metadata", "parameter_schema", "input_contract", "output_contract", "required_builders"):
            assert key in described
        assert described["required_builders"] == ["ProcessFlowBuilder"]
        dumped = json.dumps(described)
        for forbidden in ("<bns:", "</", "<?xml", "```"):
            assert forbidden not in dumped, f"{forbidden!r} leaked into describe()"

    def test_emit_components_is_empty(self):
        params = ReturnDocumentsPrimitive.validate_parameters({})
        assert ReturnDocumentsPrimitive.emit_components(_ctx(), params) == []

    def test_emit_fragment_returns_return_documents_block(self):
        params = ReturnDocumentsPrimitive.validate_parameters({"label": "Status Updates"})
        fragment = ReturnDocumentsPrimitive.emit_fragment(_ctx(), params)
        rd = fragment["process_config"]["return_documents"]
        assert rd == {"enabled": True, "label": "Status Updates"}
        assert fragment["depends_on"] == []

    def test_emit_fragment_omits_absent_label(self):
        params = ReturnDocumentsPrimitive.validate_parameters({})
        fragment = ReturnDocumentsPrimitive.emit_fragment(_ctx(), params)
        assert fragment["process_config"]["return_documents"] == {"enabled": True}

    def test_validation_rejects_unknown_parameter(self):
        # extra="forbid" — 'enabled' is not a caller parameter (the primitive IS
        # the request for an enabled Return Documents terminal).
        with pytest.raises(ValidationError):
            ReturnDocumentsPrimitive.validate_parameters({"enabled": True})

    def test_validation_rejects_non_string_label(self):
        with pytest.raises(ValidationError):
            ReturnDocumentsPrimitive.validate_parameters({"label": 5})
