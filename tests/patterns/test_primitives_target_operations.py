"""Issue #28 — tests for the target/operational primitive package.

Covers registry discovery + metadata hygiene, the REST target primitive
(rest_send_with_retry), the five operational primitives (schedule_envelope,
watermark_state, error_classifier, dlq_writer, run_metadata), and composition
through build_integration plan + the ProcessFlowBuilder retry/DLQ gate.

All tests are pure: no live Boomi calls. build_integration plan paths mock
``paginate_metadata`` (the only Boomi I/O on the plan path).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from boomi_mcp.categories import integration_builder as ib
from boomi_mcp.categories.components.builders import ProcessFlowBuilder
from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.profile_builder import (
    DatabaseReadProfileBuilder,
)
from boomi_mcp.patterns.base import PatternKind, PrimitiveBuildContext
from boomi_mcp.patterns.primitives import (
    DbExtractPrimitive,
    DlqWriterPrimitive,
    ErrorClassifierPrimitive,
    FieldMapPrimitive,
    RestSendWithRetryPrimitive,
    RunMetadataPrimitive,
    ScheduleEnvelopePrimitive,
    WatermarkStatePrimitive,
)
from boomi_mcp.patterns.registry import PatternRegistry

_NEW_PRIMITIVES = [
    RestSendWithRetryPrimitive,
    ScheduleEnvelopePrimitive,
    WatermarkStatePrimitive,
    ErrorClassifierPrimitive,
    DlqWriterPrimitive,
    RunMetadataPrimitive,
]
_NEW_PRIMITIVE_NAMES = [
    "rest_send_with_retry",
    "schedule_envelope",
    "watermark_state",
    "error_classifier",
    "dlq_writer",
    "run_metadata",
]


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


def _fragment(primitive, params: dict):
    """validate_parameters + emit_fragment in one step."""
    return primitive.emit_fragment(_ctx(), primitive.validate_parameters(params))


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


def _rest_create_params(**overrides):
    params = {
        "key_prefix": "cust",
        "connection": {"mode": "create", "base_url": "https://api.invalid", "auth": "NONE"},
        "operation": {"method": "PATCH", "path": "/resource"},
    }
    params.update(overrides)
    return params


def _source_index(fields):
    return DatabaseReadProfileBuilder.build_field_index(
        {
            "profile_type": "database.read",
            "component_name": "src",
            "query": "q",
            "output_fields": list(fields),
        }
    )


# ===========================================================================
# Registry + metadata hygiene
# ===========================================================================


class TestRegistryAndMetadata:
    def test_registry_discovers_new_primitives(self):
        reg = PatternRegistry.from_package("boomi_mcp.patterns")
        for name in _NEW_PRIMITIVE_NAMES:
            cls = reg.get(name)
            assert cls.metadata.kind == PatternKind.PRIMITIVE

    @pytest.mark.parametrize("primitive", _NEW_PRIMITIVES)
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
        # Archetype-only keys must not leak into a primitive describe().
        for archetype_only in ("capability_notes", "limitations", "examples"):
            assert archetype_only not in described

    def test_rest_send_declares_required_builders(self):
        described = RestSendWithRetryPrimitive.describe()
        assert set(described["required_builders"]) == {
            "RestClientConnectionBuilder",
            "RestClientOperationBuilder",
        }

    @pytest.mark.parametrize("primitive", _NEW_PRIMITIVES)
    def test_no_raw_artifacts_in_describe(self, primitive):
        dumped = json.dumps(primitive.describe())
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
            "Bearer ",
            "```",
        ):
            assert forbidden not in dumped, f"{forbidden!r} leaked into describe()"


# ===========================================================================
# rest_send_with_retry
# ===========================================================================


class TestRestSendComponents:
    def test_create_emits_connection_then_operation(self):
        comps = _emit(RestSendWithRetryPrimitive, _rest_create_params())
        assert [c.key for c in comps] == ["cust_rest_connection", "cust_rest_operation"]
        assert [c.type for c in comps] == ["connector-settings", "connector-action"]

        conn, op = comps
        assert conn.config["connector_type"] == "rest"
        assert conn.config["base_url"] == "https://api.invalid"
        assert conn.config["auth"] == "NONE"
        assert "reference_only" not in conn.config

        assert op.config["operation_mode"] == "execute"
        assert op.config["connector_type"] == "rest"
        assert op.config["connection_ref_key"] == "cust_rest_connection"
        assert op.config["method"] == "PATCH"
        assert op.config["path"] == "/resource"
        assert op.depends_on == ["cust_rest_connection"]

    def test_reuse_by_id_emits_reference_only(self):
        comps = _emit(
            RestSendWithRetryPrimitive,
            _rest_create_params(connection={"mode": "reuse", "component_id": "abc-123"}),
        )
        conn = comps[0]
        assert conn.config["reference_only"] is True
        assert conn.config["connector_type"] == "rest"
        assert conn.config["component_id"] == "abc-123"
        assert conn.component_id == "abc-123"
        # No create config body leaked.
        assert "base_url" not in conn.config
        assert "auth" not in conn.config
        assert "component_name" not in conn.config

    def test_reuse_by_name_emits_name_resolution(self):
        comps = _emit(
            RestSendWithRetryPrimitive,
            _rest_create_params(connection={"mode": "reuse", "component_name": "Existing REST"}),
        )
        conn = comps[0]
        assert conn.config["reference_only"] is True
        assert conn.config["component_name"] == "Existing REST"
        assert conn.name == "Existing REST"
        assert "component_id" not in conn.config

    def test_whitespace_only_reuse_binding_rejected(self):
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(connection={"mode": "reuse", "component_id": "   "})
            )

    def test_reuse_requires_exactly_one_binding(self):
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(
                    connection={"mode": "reuse", "component_id": "a", "component_name": "b"}
                )
            )

    def test_unsupported_method_surfaces_builder_error(self):
        with pytest.raises(BuilderValidationError) as exc:
            _emit(
                RestSendWithRetryPrimitive,
                _rest_create_params(operation={"method": "FETCH", "path": "/x"}),
            )
        assert exc.value.error_code == "UNSUPPORTED_REST_METHOD"

    def test_invalid_profile_type_surfaces_builder_error(self):
        with pytest.raises(BuilderValidationError) as exc:
            _emit(
                RestSendWithRetryPrimitive,
                _rest_create_params(
                    operation={"method": "POST", "path": "/x", "request_profile_type": "yaml"}
                ),
            )
        assert exc.value.error_code == "REST_OPERATION_VALIDATION_FAILED"

    def test_secret_shaped_header_surfaces_builder_error(self):
        with pytest.raises(BuilderValidationError) as exc:
            _emit(
                RestSendWithRetryPrimitive,
                _rest_create_params(
                    operation={
                        "method": "POST",
                        "path": "/x",
                        "request_headers": {"Authorization": "value"},
                    }
                ),
            )
        assert exc.value.error_code == "REST_SECRET_VALUE_FORBIDDEN"

    def test_missing_method_rejected_at_param_boundary(self):
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(operation={"path": "/x"})
            )

    def test_profile_refs_added_to_depends_on(self):
        comps = _emit(
            RestSendWithRetryPrimitive,
            _rest_create_params(
                operation={
                    "method": "POST",
                    "path": "/x",
                    "request_profile_id": "$ref:req_profile",
                    "response_profile_id": "$ref:resp_profile",
                    "request_profile_type": "json",
                }
            ),
        )
        op = comps[1]
        assert set(op.depends_on) == {"cust_rest_connection", "req_profile", "resp_profile"}

    def test_literal_profile_uuid_not_a_dependency(self):
        comps = _emit(
            RestSendWithRetryPrimitive,
            _rest_create_params(
                operation={
                    "method": "POST",
                    "path": "/x",
                    "request_profile_id": "11111111-1111-1111-1111-111111111111",
                }
            ),
        )
        assert comps[1].depends_on == ["cust_rest_connection"]

    def test_emitted_components_pass_build_plan(self):
        comps = _emit(RestSendWithRetryPrimitive, _rest_create_params())
        plan = _plan(comps)
        assert plan["_success"] is True
        for step in plan["steps"]:
            assert step.get("validation_error") is None
            assert step["planned_action"] == "create"

    @pytest.mark.parametrize("bad_timeout", [True, False, "5", 1.5])
    def test_non_int_timeout_rejected_at_param_boundary(self, bad_timeout):
        # Codex P2: Optional[int] coerced bool/str (True->1) and bypassed the
        # builder's timeout type check, emitting an altered timeout. StrictInt
        # rejects them before the value can reach the builder.
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(
                    connection={
                        "mode": "create",
                        "base_url": "https://api.invalid",
                        "auth": "NONE",
                        "connect_timeout_ms": bad_timeout,
                    }
                )
            )

    def test_negative_timeout_accepted(self):
        # Negative / zero = "wait indefinitely" per Boomi; must still be valid.
        comps = _emit(
            RestSendWithRetryPrimitive,
            _rest_create_params(
                connection={
                    "mode": "create",
                    "base_url": "https://api.invalid",
                    "auth": "NONE",
                    "connect_timeout_ms": -1,
                    "read_timeout_ms": 0,
                }
            ),
        )
        assert comps[0].config["connect_timeout_ms"] == -1
        assert comps[0].config["read_timeout_ms"] == 0

    @pytest.mark.parametrize("bad", ["false", "true", 1, 0])
    def test_non_bool_preemptive_rejected(self, bad):
        # Codex P2: Optional[bool] coerced "false"/1 past the builder's non-bool
        # check; StrictBool rejects them at the param boundary.
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(
                    connection={
                        "mode": "create",
                        "base_url": "https://api.invalid",
                        "auth": "BASIC",
                        "username": "u",
                        "credential_ref": "credential://x",
                        "preemptive": bad,
                    }
                )
            )

    @pytest.mark.parametrize("field", ["return_application_errors", "track_response"])
    @pytest.mark.parametrize("bad", ["false", 1])
    def test_non_bool_operation_flags_rejected(self, field, bad):
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(operation={"method": "GET", "path": "/x", field: bad})
            )

    def test_real_bool_operation_flags_accepted(self):
        comps = _emit(
            RestSendWithRetryPrimitive,
            _rest_create_params(
                operation={
                    "method": "GET",
                    "path": "/x",
                    "return_application_errors": False,
                    "track_response": True,
                }
            ),
        )
        assert comps[1].config["return_application_errors"] is False
        assert comps[1].config["track_response"] is True


class TestRestSendFragment:
    def test_target_fragment_shape(self):
        frag = _fragment(RestSendWithRetryPrimitive, _rest_create_params())
        target = frag["process_config"]["target"]
        assert target["connector_type"] == "rest"
        assert target["connection_id"] == "$ref:cust_rest_connection"
        assert target["operation_id"] == "$ref:cust_rest_operation"
        assert target["action_type"] == "PATCH"
        assert set(frag["depends_on"]) == {"cust_rest_connection", "cust_rest_operation"}

    def test_action_type_uppercases_method(self):
        frag = _fragment(
            RestSendWithRetryPrimitive,
            _rest_create_params(operation={"method": "post", "path": "/x"}),
        )
        assert frag["process_config"]["target"]["action_type"] == "POST"

    def test_retry_policy_is_metadata_only(self):
        frag = _fragment(
            RestSendWithRetryPrimitive,
            _rest_create_params(retry_policy={"max_attempts": 3}),
        )
        assert frag["metadata"]["retry_policy"] == {"max_attempts": 3}
        # Must NOT flow into a reliability.retry_count the process builder reads.
        assert "reliability" not in frag["process_config"]

    def test_retry_policy_attempts_bounded(self):
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(retry_policy={"max_attempts": 6})
            )

    @pytest.mark.parametrize("bad", [True, "3"])
    def test_retry_policy_attempts_rejects_non_int(self, bad):
        with pytest.raises(ValidationError):
            RestSendWithRetryPrimitive.validate_parameters(
                _rest_create_params(retry_policy={"max_attempts": bad})
            )


# ===========================================================================
# schedule_envelope
# ===========================================================================


class TestScheduleEnvelope:
    def test_manual_emits_manual_trigger(self):
        frag = _fragment(ScheduleEnvelopePrimitive, {"mode": "manual"})
        assert frag["process_config"]["execution"]["trigger"] == {"mode": "manual"}

    def test_manual_rejects_cron(self):
        with pytest.raises(ValidationError):
            ScheduleEnvelopePrimitive.validate_parameters(
                {"mode": "manual", "cron": "*/5 * * * *"}
            )

    def test_scheduled_requires_cron(self):
        with pytest.raises(ValidationError):
            ScheduleEnvelopePrimitive.validate_parameters({"mode": "scheduled"})

    def test_scheduled_rejects_malformed_cron(self):
        with pytest.raises(ValidationError):
            ScheduleEnvelopePrimitive.validate_parameters(
                {"mode": "scheduled", "cron": "* * *"}
            )

    def test_scheduled_emits_cron_trigger(self):
        frag = _fragment(
            ScheduleEnvelopePrimitive,
            {"mode": "scheduled", "cron": "0 * * * *", "timezone": "UTC", "max_retry": 2},
        )
        trigger = frag["process_config"]["execution"]["trigger"]
        assert trigger == {"mode": "scheduled", "cron": "0 * * * *", "timezone": "UTC"}
        assert frag["metadata"]["schedule"]["applies_after_deploy"] is True
        assert frag["metadata"]["schedule"]["max_retry"] == 2

    def test_blank_timezone_treated_as_absent(self):
        # Codex P3: blank optional strings must not be emitted verbatim.
        frag = _fragment(
            ScheduleEnvelopePrimitive,
            {"mode": "scheduled", "cron": "0 * * * *", "timezone": "  "},
        )
        assert "timezone" not in frag["process_config"]["execution"]["trigger"]

    def test_blank_timezone_allowed_in_manual(self):
        # Blank == absent, so manual mode accepts it (a real timezone does not).
        frag = _fragment(ScheduleEnvelopePrimitive, {"mode": "manual", "timezone": ""})
        assert frag["process_config"]["execution"]["trigger"] == {"mode": "manual"}

    def test_max_retry_bounded(self):
        with pytest.raises(ValidationError):
            ScheduleEnvelopePrimitive.validate_parameters(
                {"mode": "scheduled", "cron": "0 * * * *", "max_retry": 6}
            )

    @pytest.mark.parametrize("bad", [True, "2"])
    def test_max_retry_rejects_non_int(self, bad):
        with pytest.raises(ValidationError):
            ScheduleEnvelopePrimitive.validate_parameters(
                {"mode": "scheduled", "cron": "0 * * * *", "max_retry": bad}
            )


# ===========================================================================
# watermark_state
# ===========================================================================


class TestWatermarkState:
    def test_disabled_emits_disabled_metadata(self):
        frag = _fragment(WatermarkStatePrimitive, {"enabled": False})
        assert frag["metadata"]["watermark"] == {"enabled": False}

    def test_disabled_rejects_config_fields(self):
        with pytest.raises(ValidationError):
            WatermarkStatePrimitive.validate_parameters(
                {"enabled": False, "field": "updated_at"}
            )

    def test_enabled_requires_field_kind_persistence(self):
        with pytest.raises(ValidationError):
            WatermarkStatePrimitive.validate_parameters(
                {"enabled": True, "field": "updated_at", "kind": "timestamp"}
            )

    def test_enabled_dpp_fragment(self):
        frag = _fragment(
            WatermarkStatePrimitive,
            {
                "enabled": True,
                "field": "updated_at",
                "kind": "timestamp",
                "persistence": "dpp",
                "dpp_name": "wm",
                "initial_value": "0",
            },
        )
        wm = frag["metadata"]["watermark"]
        assert wm["field"] == "updated_at"
        assert wm["kind"] == "timestamp"
        assert wm["persistence"] == "dpp"
        assert wm["dpp_name"] == "wm"
        assert wm["initial_value"] == "0"

    def test_blank_dpp_name_treated_as_absent(self):
        # Codex P3: a blank dpp_name must not be emitted as "".
        frag = _fragment(
            WatermarkStatePrimitive,
            {
                "enabled": True,
                "field": "updated_at",
                "kind": "timestamp",
                "persistence": "dpp",
                "dpp_name": "  ",
            },
        )
        assert "dpp_name" not in frag["metadata"]["watermark"]

    def test_external_store_requires_store_ref(self):
        with pytest.raises(ValidationError):
            WatermarkStatePrimitive.validate_parameters(
                {"enabled": True, "field": "seq", "kind": "sequence", "persistence": "external_store"}
            )

    def test_dpp_rejects_store_ref(self):
        with pytest.raises(ValidationError):
            WatermarkStatePrimitive.validate_parameters(
                {
                    "enabled": True,
                    "field": "seq",
                    "kind": "sequence",
                    "persistence": "dpp",
                    "store_ref": "s",
                }
            )


# ===========================================================================
# error_classifier
# ===========================================================================


class TestErrorClassifier:
    def test_defaults_match_contract(self):
        frag = _fragment(ErrorClassifierPrimitive, {})
        clf = frag["process_config"]["reliability"]["error_classifier"]
        assert clf["retriable_status_codes"] == [502, 503, 504]
        assert clf["terminal_status_codes"] == [400, 401, 403, 404, 422]
        assert clf["custom_rules"] == []

    def test_custom_rules_are_opaque_labels(self):
        frag = _fragment(ErrorClassifierPrimitive, {"custom_rules": ["rate_limited"]})
        assert frag["process_config"]["reliability"]["error_classifier"]["custom_rules"] == [
            "rate_limited"
        ]

    def test_overlap_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(
                ErrorClassifierPrimitive,
                {"retriable_status_codes": [500], "terminal_status_codes": [500]},
            )
        assert exc.value.error_code == "STATUS_CODE_OVERLAP"

    def test_duplicate_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(ErrorClassifierPrimitive, {"retriable_status_codes": [503, 503]})
        assert exc.value.error_code == "INVALID_STATUS_CODE"

    def test_bool_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(ErrorClassifierPrimitive, {"retriable_status_codes": [True]})
        assert exc.value.error_code == "INVALID_STATUS_CODE"

    def test_string_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(ErrorClassifierPrimitive, {"retriable_status_codes": ["503"]})
        assert exc.value.error_code == "INVALID_STATUS_CODE"

    def test_out_of_range_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(ErrorClassifierPrimitive, {"retriable_status_codes": [999]})
        assert exc.value.error_code == "INVALID_STATUS_CODE"


# ===========================================================================
# dlq_writer
# ===========================================================================


class TestDlqWriter:
    def test_disabled_emits_disabled(self):
        frag = _fragment(DlqWriterPrimitive, {"mode": "disabled"})
        assert frag["process_config"]["reliability"]["dlq"] == {"mode": "disabled"}
        assert "depends_on" not in frag

    def test_disabled_rejects_target_fields(self):
        with pytest.raises(ValidationError):
            DlqWriterPrimitive.validate_parameters(
                {"mode": "disabled", "document_cache_id": "dc"}
            )

    def test_document_cache_id_fragment(self):
        frag = _fragment(
            DlqWriterPrimitive, {"mode": "document_cache_ref", "document_cache_id": "dc-1"}
        )
        dlq = frag["process_config"]["reliability"]["dlq"]
        assert dlq == {"mode": "document_cache_ref", "document_cache_id": "dc-1"}
        assert "depends_on" not in frag

    def test_document_cache_ref_key_adds_dependency(self):
        frag = _fragment(
            DlqWriterPrimitive,
            {"mode": "document_cache_ref", "document_cache_ref_key": "cache_comp"},
        )
        dlq = frag["process_config"]["reliability"]["dlq"]
        assert dlq["document_cache_ref_key"] == "cache_comp"
        assert frag["depends_on"] == ["cache_comp"]

    def test_document_cache_requires_exactly_one(self):
        with pytest.raises(ValidationError):
            DlqWriterPrimitive.validate_parameters({"mode": "document_cache_ref"})
        with pytest.raises(ValidationError):
            DlqWriterPrimitive.validate_parameters(
                {
                    "mode": "document_cache_ref",
                    "document_cache_id": "a",
                    "document_cache_ref_key": "b",
                }
            )

    def test_error_subprocess_ref_key_adds_dependency(self):
        frag = _fragment(
            DlqWriterPrimitive,
            {"mode": "error_subprocess_ref", "process_ref_key": "err_proc"},
        )
        dlq = frag["process_config"]["reliability"]["dlq"]
        assert dlq["process_ref_key"] == "err_proc"
        assert frag["depends_on"] == ["err_proc"]

    def test_error_subprocess_requires_exactly_one(self):
        with pytest.raises(ValidationError):
            DlqWriterPrimitive.validate_parameters({"mode": "error_subprocess_ref"})

    def test_mode_mismatch_rejected(self):
        with pytest.raises(ValidationError):
            DlqWriterPrimitive.validate_parameters(
                {"mode": "document_cache_ref", "process_id": "p"}
            )


# ===========================================================================
# run_metadata
# ===========================================================================


class TestRunMetadata:
    def test_static_metadata_fragment(self):
        frag = _fragment(RunMetadataPrimitive, {"static_metadata": {"owner": "team"}})
        assert frag["process_config"]["execution"]["run_metadata"] == {"owner": "team"}

    def test_dynamic_properties_and_correlation(self):
        frag = _fragment(
            RunMetadataPrimitive,
            {
                "static_metadata": {"owner": "team"},
                "dynamic_process_properties": {"last_run": "watermark source"},
                "correlation_id_property": "correlation_id",
            },
        )
        assert frag["metadata"]["dynamic_process_properties"] == {"last_run": "watermark source"}
        assert frag["process_config"]["execution"]["correlation_id_property"] == "correlation_id"

    def test_secret_shaped_static_key_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(RunMetadataPrimitive, {"static_metadata": {"password": "x"}})
        assert exc.value.error_code == "SECRET_SHAPED_KEY"

    def test_secret_shaped_dynamic_key_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(
                RunMetadataPrimitive,
                {"dynamic_process_properties": {"api_key": "x"}},
            )
        assert exc.value.error_code == "SECRET_SHAPED_KEY"

    @pytest.mark.parametrize(
        "key",
        [
            "secret_key",
            "db_password",
            "aws_secret_access_key",
            "auth_token",
            "private_key",
            "passphrase",
            "client_secret",
            "encryption_key",
            "signing_key",
            "api_secret",
        ],
    )
    def test_composite_secret_key_rejected(self, key):
        # Bug #135: anchored builder regex missed composite credential names.
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(RunMetadataPrimitive, {"static_metadata": {key: "v"}})
        assert exc.value.error_code == "SECRET_SHAPED_KEY"

    @pytest.mark.parametrize(
        "good_key",
        ["sort_key", "partition_key", "idempotency_key", "owner", "region", "correlation_id"],
    )
    def test_benign_keys_allowed(self, good_key):
        # Substring stems must not flag benign *_key / business metadata names.
        frag = _fragment(RunMetadataPrimitive, {"static_metadata": {good_key: "v"}})
        assert frag["process_config"]["execution"]["run_metadata"] == {good_key: "v"}

    def test_secret_shaped_value_rejected(self):
        # Value backstop: even an innocuous key cannot carry credential material.
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r"
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(RunMetadataPrimitive, {"static_metadata": {"note": jwt}})
        assert exc.value.error_code == "SECRET_SHAPED_VALUE"

    def test_secret_shaped_dynamic_value_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(
                RunMetadataPrimitive,
                {"dynamic_process_properties": {"label": "Bearer abc.def.ghi"}},
            )
        assert exc.value.error_code == "SECRET_SHAPED_VALUE"

    def test_blank_static_value_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(RunMetadataPrimitive, {"static_metadata": {"owner": "  "}})
        assert exc.value.error_code == "BLANK_METADATA_VALUE"

    def test_blank_dynamic_name_rejected(self):
        with pytest.raises(BuilderValidationError) as exc:
            _fragment(
                RunMetadataPrimitive,
                {"dynamic_process_properties": {"   ": "x"}},
            )
        assert exc.value.error_code == "BLANK_METADATA_KEY"


# ===========================================================================
# Composition
# ===========================================================================


_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
_REST_OP_ID = "44444444-4444-4444-4444-444444444444"


def _db_extract_components():
    return _emit(
        DbExtractPrimitive,
        {
            "key_prefix": "cust",
            "connection": {
                "mode": "create",
                "driver_id": "mysql",
                "auth_mode": "username_password",
                "username": "u",
                "credential_ref": "credential://x",
                "host": "h",
                "dbname": "d",
            },
            "read_profile": {
                "query": "q",
                "output_fields": [
                    {"name": "id", "data_type": "number"},
                    {"name": "name", "data_type": "character"},
                ],
            },
        },
    )


def _field_map_components():
    return _emit(
        FieldMapPrimitive,
        {
            "key_prefix": "cust",
            "source": {
                "source_profile_id": "$ref:cust_db_read_profile",
                "source_profile_type": "profile.db",
                "source_field_index": _source_index(
                    [
                        {"name": "id", "data_type": "number"},
                        {"name": "name", "data_type": "character"},
                    ]
                ),
            },
            "target_payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "id", "kind": "simple", "data_type": "number"}
                    ],
                },
            },
            "direct": [{"source_field": "id", "target_path": "Root/id"}],
        },
    )


class TestComposition:
    def test_db_extract_field_map_rest_send_plan_deterministically(self):
        comps = (
            _db_extract_components()
            + _field_map_components()
            + _emit(RestSendWithRetryPrimitive, _rest_create_params())
        )
        plan = _plan(comps)
        assert plan["_success"] is True
        for step in plan["steps"]:
            assert step.get("validation_error") is None
        # The plan must be a valid topological order: every key is present
        # exactly once, and each component's depends_on precede it.
        planned_keys = [s["key"] for s in plan["steps"]]
        assert set(planned_keys) == {c.key for c in comps}
        assert len(planned_keys) == len(comps)
        position = {key: i for i, key in enumerate(planned_keys)}
        for comp in comps:
            for dep in comp.depends_on:
                assert position[dep] < position[comp.key], (
                    f"{dep} must be planned before {comp.key}"
                )

    def _process_config(self, reliability=None):
        cfg = {
            "process_kind": "database_to_api_sync",
            "source": {
                "connector_type": "database",
                "connection_id": _DB_CONN_ID,
                "operation_id": _DB_OP_ID,
                "action_type": "Get",
            },
            "transform": {"mode": "passthrough"},
            "target": {
                "connector_type": "rest",
                "connection_id": _REST_CONN_ID,
                "operation_id": _REST_OP_ID,
                "action_type": "POST",
            },
        }
        if reliability is not None:
            cfg["reliability"] = reliability
        return cfg

    def test_retry_zero_dlq_disabled_plans_cleanly(self):
        err_frag = _fragment(ErrorClassifierPrimitive, {})
        dlq_frag = _fragment(DlqWriterPrimitive, {"mode": "disabled"})
        reliability = {
            "retry_count": 0,
            **err_frag["process_config"]["reliability"],
            **dlq_frag["process_config"]["reliability"],
        }
        err = ProcessFlowBuilder.validate_config(
            self._process_config(reliability), depends_on=[]
        )
        assert err is None

    def test_retry_positive_still_gated(self):
        err = ProcessFlowBuilder.validate_config(
            self._process_config({"retry_count": 1, "dlq": {"mode": "disabled"}}),
            depends_on=[],
        )
        assert err is not None
        assert err.error_code == "PROCESS_RETRY_UNVERIFIED"

    def test_enabled_dlq_with_binding_now_supported(self):
        # Issue #51 M3.R1a: a dlq_writer fragment with retry_count == 0 and a
        # bound document cache is now consumed into a verified Try/Catch + DLQ
        # catch-path, so it validates cleanly (was PROCESS_RETRY_UNVERIFIED).
        dlq_frag = _fragment(
            DlqWriterPrimitive,
            {"mode": "document_cache_ref", "document_cache_id": "dc-1"},
        )
        reliability = {"retry_count": 0, **dlq_frag["process_config"]["reliability"]}
        err = ProcessFlowBuilder.validate_config(
            self._process_config(reliability), depends_on=[]
        )
        assert err is None

    def test_enabled_dlq_with_retry_still_gated(self):
        # retry_count > 0 + DLQ stays gated until issue #51 R1b verifies the
        # retryCount->interval mapping against a live export.
        dlq_frag = _fragment(
            DlqWriterPrimitive,
            {"mode": "document_cache_ref", "document_cache_id": "dc-1"},
        )
        reliability = {"retry_count": 1, **dlq_frag["process_config"]["reliability"]}
        err = ProcessFlowBuilder.validate_config(
            self._process_config(reliability), depends_on=[]
        )
        assert err is not None
        assert err.error_code == "PROCESS_RETRY_UNVERIFIED"
