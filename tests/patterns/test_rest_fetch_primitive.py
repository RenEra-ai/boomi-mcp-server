"""Issue #72 (M5.4) — tests for the ``rest_fetch`` REST source primitive.

Covers registry discovery + metadata hygiene, connection create/reuse, the
GET-only execute operation, the required explicit output shape, the validated
pagination / conditional-request metadata, the operation slot declarations (for
#96), and the source fragment. All tests are pure: no live Boomi calls — every
byte of XML and structured validation is delegated to the REST Client builders.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.patterns.base import PatternKind, PrimitiveBuildContext
from boomi_mcp.patterns.primitives import RestFetchPrimitive
from boomi_mcp.patterns.registry import PatternRegistry


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _ctx() -> PrimitiveBuildContext:
    return PrimitiveBuildContext(
        integration_name="Demo", component_prefix="DEMO", folder_path="/Demo"
    )


def _emit(params: dict):
    return RestFetchPrimitive.emit_components(
        _ctx(), RestFetchPrimitive.validate_parameters(params)
    )


def _fragment(params: dict):
    return RestFetchPrimitive.emit_fragment(
        _ctx(), RestFetchPrimitive.validate_parameters(params)
    )


def _params(**overrides):
    params = {
        "key_prefix": "cust",
        "connection": {"mode": "create", "base_url": "https://api.example.com", "auth": "NONE"},
        "operation": {"path": "/v1/items"},
        "response": {
            "profile_id": "$ref:cust_resp_profile",
            "profile_type": "profile.json",
            "field_index": {"Root/id": {"data_type": "character", "mappable": True}},
        },
    }
    params.update(overrides)
    return params


# ---------------------------------------------------------------------------
# Registry + metadata hygiene
# ---------------------------------------------------------------------------


class TestRegistryAndMetadata:
    def test_registry_discovers_rest_fetch(self):
        try:
            reg = PatternRegistry.from_package("boomi_mcp.patterns")
        except TypeError as exc:  # pragma: no cover — interpreter-specific
            # Python 3.9.6 inspect.isclass quirk (documented by the sibling
            # source/transform registry tests); conformant interpreters discover.
            pytest.skip(f"registry discovery unavailable on this interpreter: {exc}")
        cls = reg.get("rest_fetch")
        assert cls is RestFetchPrimitive
        assert cls.metadata.kind == PatternKind.PRIMITIVE
        assert "source" in cls.metadata.tags and "rest" in cls.metadata.tags

    def test_describe_includes_contracts_and_builders(self):
        described = RestFetchPrimitive.describe()
        for key in ("metadata", "parameter_schema", "output_contract", "required_builders"):
            assert key in described
        assert described["required_builders"] == [
            "RestClientConnectionBuilder",
            "RestClientOperationBuilder",
        ]
        # Archetype-only keys must not leak into a primitive describe().
        for archetype_only in ("capability_notes", "limitations", "examples"):
            assert archetype_only not in described

    def test_no_raw_artifacts_in_describe(self):
        dumped = json.dumps(RestFetchPrimitive.describe())
        for forbidden in (
            "<bns:", "</", "<?xml", "<soap", "SOAP-ENV",
            "$filter=", "$select=", "SELECT ", "INSERT INTO", "```",
        ):
            assert forbidden not in dumped, f"{forbidden!r} leaked into describe()"


# ---------------------------------------------------------------------------
# Component emission (connection + GET operation)
# ---------------------------------------------------------------------------


class TestEmission:
    def test_create_mode_emits_connection_then_operation(self):
        comps = _emit(_params())
        assert [c.type for c in comps] == ["connector-settings", "connector-action"]
        assert [c.key for c in comps] == ["cust_rest_source_connection", "cust_rest_source_operation"]

    def test_operation_is_get_only_with_response_shape(self):
        op = _emit(_params())[1].config
        assert op["method"] == "GET"
        assert op["operation_mode"] == "execute"
        assert op["connection_ref_key"] == "cust_rest_source_connection"
        assert op["response_profile_id"] == "$ref:cust_resp_profile"
        assert op["response_profile_type"] == "json"
        # No request profile fields (#50 freeze + #72 empty-request guarantee).
        assert "request_profile_id" not in op
        assert "request_profile_type" not in op

    def test_xml_profile_type_maps_to_xml(self):
        op = _emit(
            _params(response={
                "profile_id": "$ref:cust_resp_profile",
                "profile_type": "profile.xml",
                "field_index": {"Order/id": {"data_type": "character"}},
            })
        )[1].config
        assert op["response_profile_type"] == "xml"

    def test_ref_response_profile_in_depends_on(self):
        op = _emit(_params())[1]
        assert set(op.depends_on) == {"cust_rest_source_connection", "cust_resp_profile"}

    def test_literal_response_profile_not_in_depends_on(self):
        op = _emit(
            _params(response={
                "profile_id": "11111111-1111-1111-1111-111111111111",
                "profile_type": "profile.json",
                "field_index": {"Root/id": {"data_type": "character"}},
            })
        )[1]
        assert op.depends_on == ["cust_rest_source_connection"]

    def test_reuse_by_id_is_reference_only(self):
        conn = _emit(_params(connection={"mode": "reuse", "component_id": "conn-1"}))[0]
        assert conn.config["reference_only"] is True
        assert conn.config["connector_type"] == "rest"
        assert conn.component_id == "conn-1"
        for forbidden in ("base_url", "auth", "username"):
            assert forbidden not in conn.config

    def test_reuse_by_name_sets_resolution_name(self):
        conn = _emit(_params(connection={"mode": "reuse", "component_name": "Shared API"}))[0]
        assert conn.name == "Shared API"
        assert conn.config["reference_only"] is True
        assert conn.component_id is None

    def test_static_query_and_headers_pass_builder(self):
        op = _emit(
            _params(operation={
                "path": "/v1/items",
                "query_parameters": {"limit": "100"},
                "request_headers": {"Accept": "application/json"},
            })
        )[1].config
        assert op["query_parameters"] == {"limit": "100"}
        assert op["request_headers"] == {"Accept": "application/json"}

    def test_secret_shaped_header_value_rejected_at_emit(self):
        # The REST operation builder rejects a credential-shaped header value.
        with pytest.raises(BuilderValidationError) as exc:
            _emit(_params(operation={
                "path": "/v1/items",
                "request_headers": {"X-Custom": "Bearer aaaa.bbbb.cccc"},
            }))
        assert exc.value.error_code == "REST_SECRET_VALUE_FORBIDDEN"


# ---------------------------------------------------------------------------
# Response shape validation
# ---------------------------------------------------------------------------


class TestResponseShape:
    def test_missing_response_rejected(self):
        params = _params()
        del params["response"]
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(params)

    def test_blank_profile_id_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(response={
                    "profile_id": "   ",
                    "profile_type": "profile.json",
                    "field_index": {"a": {"data_type": "character"}},
                })
            )

    def test_empty_field_index_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(response={
                    "profile_id": "$ref:r",
                    "profile_type": "profile.json",
                    "field_index": {},
                })
            )

    def test_bad_profile_type_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(response={
                    "profile_id": "$ref:r",
                    "profile_type": "profile.csv",
                    "field_index": {"a": {"data_type": "character"}},
                })
            )


# ---------------------------------------------------------------------------
# Operation slots (declared here; #96 binds them at runtime)
# ---------------------------------------------------------------------------


class TestOperationSlots:
    def test_valid_slots_land_in_fragment(self):
        frag = _fragment(
            _params(
                operation={"path": "/v1/items/{id}"},
                path_slots=[{"name": "id", "description": "record id"}],
                query_parameter_slots=[{"name": "since"}],
                request_header_slots=[{"name": "X-Tenant", "required": False}],
            )
        )
        slots = frag["metadata"]["rest_fetch"]["operation_slots"]
        assert slots["path"] == [{"name": "id", "required": True, "description": "record id"}]
        assert slots["query_parameter"] == [{"name": "since", "required": True}]
        assert slots["request_header"] == [{"name": "X-Tenant", "required": False}]

    def test_blank_slot_name_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(query_parameter_slots=[{"name": "  "}])
            )

    def test_duplicate_slot_name_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(query_parameter_slots=[{"name": "since"}, {"name": "since"}])
            )

    def test_path_slot_not_in_path_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(operation={"path": "/v1/items"}, path_slots=[{"name": "id"}])
            )

    def test_static_query_param_dup_of_slot_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(
                    operation={"path": "/v1/items", "query_parameters": {"since": "x"}},
                    query_parameter_slots=[{"name": "since"}],
                )
            )

    def test_secret_shaped_slot_name_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(request_header_slots=[{"name": "api_key"}])
            )


# ---------------------------------------------------------------------------
# Pagination metadata (validated config, not request templates)
# ---------------------------------------------------------------------------


class TestPagination:
    def test_default_is_none(self):
        frag = _fragment(_params())
        assert frag["metadata"]["rest_fetch"]["pagination"] == {"mode": "none"}

    def test_page_mode_valid(self):
        frag = _fragment(
            _params(pagination={"mode": "page", "page_parameter": "page",
                                "page_size_parameter": "size", "page_size": 100, "max_pages": 50})
        )
        pg = frag["metadata"]["rest_fetch"]["pagination"]
        assert pg["mode"] == "page" and pg["page_parameter"] == "page" and pg["max_pages"] == 50

    def test_offset_mode_valid(self):
        frag = _fragment(_params(pagination={"mode": "offset", "offset_parameter": "offset"}))
        assert frag["metadata"]["rest_fetch"]["pagination"]["offset_parameter"] == "offset"

    def test_cursor_mode_valid(self):
        frag = _fragment(
            _params(pagination={"mode": "cursor", "cursor_parameter": "cursor",
                                "next_cursor_path": "meta/next"})
        )
        assert frag["metadata"]["rest_fetch"]["pagination"]["next_cursor_path"] == "meta/next"

    def test_link_header_mode_defaults(self):
        frag = _fragment(_params(pagination={"mode": "link_header"}))
        assert frag["metadata"]["rest_fetch"]["pagination"]["mode"] == "link_header"

    def test_page_mode_requires_page_parameter(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(_params(pagination={"mode": "page"}))

    def test_cursor_mode_requires_both_fields(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(pagination={"mode": "cursor", "cursor_parameter": "c"})
            )

    def test_none_mode_rejects_mode_specific_field(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(pagination={"mode": "none", "page_parameter": "page"})
            )

    def test_cross_mode_field_rejected(self):
        # An offset field under page mode is rejected (never silently dropped).
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(pagination={"mode": "page", "page_parameter": "p", "offset_parameter": "o"})
            )


# ---------------------------------------------------------------------------
# Conditional-request metadata
# ---------------------------------------------------------------------------


class TestConditionalRequest:
    def test_disabled_default(self):
        frag = _fragment(_params())
        assert frag["metadata"]["rest_fetch"]["conditional_request"] == {"enabled": False}

    def test_etag_defaults(self):
        frag = _fragment(_params(conditional_request={"enabled": True, "validator": "etag"}))
        cr = frag["metadata"]["rest_fetch"]["conditional_request"]
        assert cr["request_header"] == "If-None-Match"
        assert cr["response_header"] == "ETag"
        assert cr["on_not_modified"] == "skip"

    def test_last_modified_defaults(self):
        frag = _fragment(
            _params(conditional_request={"enabled": True, "validator": "last_modified"})
        )
        cr = frag["metadata"]["rest_fetch"]["conditional_request"]
        assert cr["request_header"] == "If-Modified-Since"
        assert cr["response_header"] == "Last-Modified"

    def test_enabled_requires_validator(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(conditional_request={"enabled": True})
            )

    def test_field_without_enabled_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(conditional_request={"enabled": False, "validator": "etag"})
            )

    def test_blank_header_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(conditional_request={"enabled": True, "validator": "etag", "request_header": "  "})
            )

    def test_secret_shaped_header_rejected(self):
        with pytest.raises(ValidationError):
            RestFetchPrimitive.validate_parameters(
                _params(conditional_request={"enabled": True, "validator": "etag", "request_header": "Api-Key"})
            )


# ---------------------------------------------------------------------------
# Source fragment
# ---------------------------------------------------------------------------


class TestFragment:
    def test_fragment_source_binding(self):
        frag = _fragment(_params())
        source = frag["process_config"]["source"]
        assert source["connector_type"] == "rest"
        assert source["action_type"] == "GET"
        assert source["connection_id"] == "$ref:cust_rest_source_connection"
        assert source["operation_id"] == "$ref:cust_rest_source_operation"
        assert frag["depends_on"] == ["cust_rest_source_connection", "cust_rest_source_operation"]

    def test_fragment_metadata_output_shape_and_guarantees(self):
        meta = _fragment(_params())["metadata"]["rest_fetch"]
        assert meta["output_shape"]["profile_id"] == "$ref:cust_resp_profile"
        assert meta["output_shape"]["profile_type"] == "profile.json"
        assert meta["output_shape"]["field_index"] == {"Root/id": {"data_type": "character", "mappable": True}}
        assert meta["request_document"] == "empty"
        assert meta["response_replaces_document"] is True

    def test_fragment_has_no_dynamic_properties(self):
        # #96 owns runtime dynamicProperties; #72 must NOT emit any.
        dumped = json.dumps(_fragment(
            _params(operation={"path": "/v1/items/{id}"}, path_slots=[{"name": "id"}])
        ))
        assert "dynamicProperties" not in dumped
        assert "dynamic_path" not in dumped


# ---------------------------------------------------------------------------
# Component-key disambiguation vs rest_send (api_to_api_sync, same key_prefix)
# ---------------------------------------------------------------------------


class TestKeyDisambiguation:
    def test_rest_fetch_and_rest_send_keys_do_not_collide(self):
        # An api_to_api_sync flow emits a rest_fetch SOURCE and a rest_send TARGET
        # under the SAME key_prefix; their component keys must be distinct so the
        # integration spec does not fail duplicate-key validation (rest_fetch uses
        # source-specific roles, mirroring how db_extract + rest_send stay distinct).
        from boomi_mcp.patterns.primitives import RestSendWithRetryPrimitive

        fetch_comps = _emit(_params())
        send_params = {
            "key_prefix": "cust",
            "connection": {"mode": "create", "base_url": "https://api.example.com", "auth": "NONE"},
            "operation": {"method": "POST", "path": "/v1/items"},
        }
        send_comps = RestSendWithRetryPrimitive.emit_components(
            _ctx(), RestSendWithRetryPrimitive.validate_parameters(send_params)
        )
        fetch_keys = {c.key for c in fetch_comps}
        send_keys = {c.key for c in send_comps}
        assert fetch_keys.isdisjoint(send_keys), (fetch_keys, send_keys)
        assert fetch_keys == {"cust_rest_source_connection", "cust_rest_source_operation"}
        assert send_keys == {"cust_rest_connection", "cust_rest_operation"}

        # The emitted CREATE display names must also be unique (case-insensitive) —
        # _lint_component_names hard-rejects duplicate create names
        # (COMPONENT_NAME_NOT_UNIQUE), so distinct keys alone are not enough for a
        # same-prefix api_to_api_sync assembly to plan.
        names = [c.config["component_name"] for c in (*fetch_comps, *send_comps)]
        lowered = [n.lower() for n in names]
        assert len(set(lowered)) == len(lowered), names
        # The source connection name differs from the target connection name.
        fetch_conn = next(c for c in fetch_comps if c.type == "connector-settings")
        send_conn = next(c for c in send_comps if c.type == "connector-settings")
        assert fetch_conn.config["component_name"] != send_conn.config["component_name"]
