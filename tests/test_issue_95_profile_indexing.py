"""Issue #95 (M7.5): live existing-profile index discovery + literal-UUID map validation.

Covers four layers:

1. The pure indexer ``index_existing_profile_xml`` over sanitized live-export
   fixtures (profile.json / profile.xml / profile.db) — canonical paths,
   platform keys, key/name paths, structural-vs-leaf mappability, and every
   structured parse failure.
2. ``validate_supplied_profile_index`` — the supplied-index gate.
3. The read-only handler ``index_profile_component_action`` — default omits raw
   XML, opt-in exposes it, and every failure stays a structured read-only
   envelope that never leaks raw XML.
4. ``build_integration`` plan (and the reused validation) accepting a literal
   existing-profile UUID transform.map when an index is supplied or discoverable,
   and keeping ``MAP_PROFILE_INDEX_UNAVAILABLE`` otherwise.

Plus the MCP wrapper (registration / annotations / SDK delegation).
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from src.boomi_mcp.categories.components.builders.profile_generation import (
    MAP_PROFILE_INDEX_UNAVAILABLE,
    PROFILE_INDEX_DUPLICATE_PATH,
    PROFILE_INDEX_PARSE_FAILED,
    PROFILE_INDEX_STRUCTURE_INVALID,
    PROFILE_INDEX_UNSUPPORTED_TYPE,
    index_existing_profile_xml,
    validate_supplied_profile_index,
)
from src.boomi_mcp.categories import profile_index as profile_index_mod
from src.boomi_mcp.categories.profile_index import index_profile_component_action
from src.boomi_mcp.categories.integration_builder import (
    _apply_plan,
    _build_plan,
    _normalize_to_spec,
)
from src.boomi_mcp.categories.components.builders.transform_map_validation import (
    resolve_map_profile_index,
)
from src.boomi_mcp.categories.transformation_review import review_transformation_action

_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "profile_components" / "issue_95"

# Fixture componentIds (match the componentId attribute inside each fixture).
JSON_UUID = "11111111-1111-1111-1111-111111111111"
XML_UUID = "22222222-2222-2222-2222-222222222222"
DB_UUID = "33333333-3333-3333-3333-333333333333"
JSON2_UUID = "55555555-5555-5555-5555-555555555555"


def _fixture(name):
    return (_FIXTURE_DIR / f"{name}.xml").read_text()


def _supplied_index(component_id, fixture_name):
    """Build a profile_indexes_by_component_id entry from a fixture."""
    indexed = index_existing_profile_xml(_fixture(fixture_name))
    return {
        "component_id": component_id,
        "profile_component_type": indexed["profile_component_type"],
        "field_index_by_path": indexed["field_index_by_path"],
    }


# ===========================================================================
# 1. Pure indexer
# ===========================================================================


class TestPureIndexerJson:
    def setup_method(self):
        self.res = index_existing_profile_xml(_fixture("profile_json"))
        self.idx = self.res["field_index_by_path"]

    def test_component_type(self):
        assert self.res["profile_component_type"] == "profile.json"

    def test_mappable_paths_are_only_mappable_leaves(self):
        assert self.res["mappable_paths"] == [
            "Root/customer_code",
            "Root/address/city",
            "Root/address/postal_code",
            "Root/lines[]/sku",
            "Root/lines[]/quantity",
        ]

    def test_structural_containers_present_but_non_mappable(self):
        for path in ("Root", "Root/address", "Root/lines"):
            assert path in self.idx
            assert self.idx[path]["mappable"] is False
            assert self.idx[path]["is_mappable"] is False
            assert self.idx[path]["structural"] is True

    def test_explicit_non_mappable_leaf(self):
        # isMappable="false" leaf is indexed but non-mappable.
        assert self.idx["Root/internal_ref"]["mappable"] is False
        assert "Root/internal_ref" not in self.res["mappable_paths"]

    def test_non_mappable_leaf_is_not_structural(self):
        # structural reflects NODE SHAPE, independent of mappable — a
        # non-mappable scalar LEAF is a leaf, not a container.
        assert self.idx["Root/internal_ref"]["structural"] is False
        # containers are structural.
        assert self.idx["Root/address"]["structural"] is True
        assert self.idx["Root/lines"]["structural"] is True
        # mappable leaves are non-structural.
        assert self.idx["Root/customer_code"]["structural"] is False

    def test_leaf_key_and_paths_match_builder_emit(self):
        e = self.idx["Root/customer_code"]
        assert e["key"] == "3"
        assert e["key_path"] == "*[@key='1']/*[@key='2']/*[@key='3']"
        assert e["name_path"] == "Root/Object/customer_code"

    def test_array_child_key_and_name_paths(self):
        e = self.idx["Root/lines[]/sku"]
        assert e["key"] == "13"
        assert e["key_path"] == (
            "*[@key='1']/*[@key='2']/*[@key='9']/*[@key='10']/"
            "*[@key='11']/*[@key='12']/*[@key='13']"
        )
        assert e["name_path"] == "Root/Object/lines/Array/lines/Object/sku"

    def test_every_entry_stamped_with_profile_component_type(self):
        assert all(
            e["profile_component_type"] == "profile.json" for e in self.idx.values()
        )


class TestPureIndexerXml:
    def setup_method(self):
        self.res = index_existing_profile_xml(_fixture("profile_xml"))
        self.idx = self.res["field_index_by_path"]

    def test_component_type(self):
        assert self.res["profile_component_type"] == "profile.xml"

    def test_namespaces_type_definitions_not_indexed(self):
        # The <Namespaces><Types><Type><XMLElement name="ShouldBeIgnoredTypeDef">
        # subtree must NEVER surface as a data path.
        assert not any("ShouldBeIgnored" in p for p in self.idx)

    def test_attribute_indexed_with_at_segment(self):
        e = self.idx["Order/@id"]
        assert e["kind"] == "attribute"
        assert e["mappable"] is True
        assert e["key_path"] == "*[@key='1']/*[@key='2']"
        assert e["name_path"] == "Order/@id"

    def test_repeating_element_children_get_bracket_segment(self):
        assert "Order/lines[]/sku" in self.idx
        assert "Order/lines[]/qty" in self.idx
        assert self.idx["Order/lines[]/sku"]["key_path"] == (
            "*[@key='1']/*[@key='5']/*[@key='6']"
        )
        # The repeating container itself is structural (non-mappable).
        assert self.idx["Order/lines"]["mappable"] is False

    def test_explicit_non_mappable_leaf(self):
        assert self.idx["Order/internal_token"]["mappable"] is False

    def test_mappable_paths(self):
        assert self.res["mappable_paths"] == [
            "Order/@id",
            "Order/customer_code",
            "Order/lines[]/sku",
            "Order/lines[]/qty",
        ]


class TestPureIndexerDb:
    def setup_method(self):
        self.res = index_existing_profile_xml(_fixture("profile_db"))
        self.idx = self.res["field_index_by_path"]

    def test_component_type(self):
        assert self.res["profile_component_type"] == "profile.db"

    def test_columns_keyed_by_bare_name_matching_build_field_index(self):
        e = self.idx["customer_code"]
        assert e["key"] == "5"
        assert e["key_path"] == "*[@key='2']/*[@key='3']/*[@key='5']"
        assert e["name_path"] == "Statement/Fields/customer_code"

    def test_non_mappable_column(self):
        assert self.idx["row_hash"]["mappable"] is False
        assert "row_hash" not in self.res["mappable_paths"]

    def test_db_parameters_not_indexed(self):
        # DBParameter "Statement" must not collide with anything or appear.
        assert self.res["mappable_paths"] == ["customer_code", "updated_at"]
        assert len(self.idx) == 3


class TestPureIndexerDbWrite:
    """A dbwrite profile namespaces columns (Fields/) and WHERE keys (Conditions/)
    exactly like DatabaseWriteProfileBuilder.build_field_index."""

    def setup_method(self):
        self.res = index_existing_profile_xml(_fixture("profile_db_write"))
        self.idx = self.res["field_index_by_path"]

    def test_fields_namespaced_and_keyed(self):
        e = self.idx["Fields/display_name"]
        assert e["key"] == "5"
        assert e["key_path"] == "*[@key='2']/*[@key='3']/*[@key='5']"
        assert e["name_path"] == "Statement/Fields/display_name"
        assert e["mappable"] is True

    def test_conditions_namespaced_under_key_4(self):
        e = self.idx["Conditions/record_id"]
        assert e["key"] == "7"
        assert e["key_path"] == "*[@key='2']/*[@key='4']/*[@key='7']"
        assert e["name_path"] == "Statement/Conditions/record_id"
        assert e["mappable"] is True

    def test_bare_column_name_not_used_for_write(self):
        # Write profiles must NOT key by bare name (that's the read-profile shape).
        assert "display_name" not in self.idx
        assert self.res["mappable_paths"] == [
            "Fields/display_name",
            "Fields/status",
            "Conditions/record_id",
        ]

    def test_dynamicdelete_conditions_only(self):
        xml = (
            '<DatabaseProfile xmlns=""><ProfileProperties>'
            '<DatabaseGeneralInfo executionType="dbwrite"/></ProfileProperties>'
            '<DataElements><DBStatement key="2" name="Statement" '
            'statementType="dynamicdelete"><DBConditions key="4" name="Conditions">'
            '<DBCondition key="5" name="record_id" dataType="character" '
            'isMappable="true"/></DBConditions></DBStatement></DataElements>'
            "</DatabaseProfile>"
        )
        res = index_existing_profile_xml(xml)
        assert res["mappable_paths"] == ["Conditions/record_id"]

    def test_dbread_still_uses_bare_column_names(self):
        # Regression: a read profile keeps bare-name keys (no Fields/ prefix).
        res = index_existing_profile_xml(_fixture("profile_db"))
        assert "customer_code" in res["field_index_by_path"]
        assert "Fields/customer_code" not in res["field_index_by_path"]

    def test_conditions_are_required_fields_are_from_mandatory(self):
        # DatabaseWriteProfileBuilder marks every condition required=True and
        # honors field mandatory. Review's validate_unmapped reads "required".
        assert self.idx["Conditions/record_id"]["required"] is True
        assert self.idx["Fields/display_name"]["required"] is False

    def test_data_type_inferred_from_dataformat(self):
        # dbwrite exports omit dataType on character columns; type is encoded in
        # the <DataFormat> child.
        assert self.idx["Fields/display_name"]["data_type"] == "character"


class TestDbDataTypeInference:
    """DB exports omit the dataType attribute — the indexer decodes it from the
    <DataFormat> child so review keeps type metadata (issue #95 fix)."""

    def setup_method(self):
        self.idx = index_existing_profile_xml(_fixture("profile_db"))["field_index_by_path"]

    def test_character_column(self):
        assert self.idx["customer_code"]["data_type"] == "character"

    def test_date_column(self):
        assert self.idx["updated_at"]["data_type"] == "datetime"


class TestPureIndexerFailures:
    def test_malformed_xml(self):
        with pytest.raises(BuilderValidationError) as exc:
            index_existing_profile_xml("<not-closed>")
        assert exc.value.error_code == PROFILE_INDEX_PARSE_FAILED

    def test_empty_input(self):
        with pytest.raises(BuilderValidationError) as exc:
            index_existing_profile_xml("   ")
        assert exc.value.error_code == PROFILE_INDEX_PARSE_FAILED

    def test_unsupported_root_type(self):
        with pytest.raises(BuilderValidationError) as exc:
            index_existing_profile_xml(
                '<bns:Component xmlns:bns="http://api.platform.boomi.com/">'
                "<bns:object><NotAProfile/></bns:object></bns:Component>"
            )
        assert exc.value.error_code == PROFILE_INDEX_UNSUPPORTED_TYPE

    def test_missing_platform_key(self):
        xml = (
            '<JSONProfile xmlns=""><DataElements>'
            '<JSONRootValue name="Root"><JSONObject key="2" name="Object">'
            '<JSONObjectEntry name="x" isMappable="true"/></JSONObject>'
            "</JSONRootValue></DataElements></JSONProfile>"
        )
        with pytest.raises(BuilderValidationError) as exc:
            index_existing_profile_xml(xml)
        assert exc.value.error_code == PROFILE_INDEX_STRUCTURE_INVALID

    def test_duplicate_canonical_path(self):
        xml = (
            '<JSONProfile xmlns=""><DataElements>'
            '<JSONRootValue key="1" name="Root"><JSONObject key="2" name="Object">'
            '<JSONObjectEntry key="3" name="dup" isMappable="true"/>'
            '<JSONObjectEntry key="4" name="dup" isMappable="true"/>'
            "</JSONObject></JSONRootValue></DataElements></JSONProfile>"
        )
        with pytest.raises(BuilderValidationError) as exc:
            index_existing_profile_xml(xml)
        assert exc.value.error_code == PROFILE_INDEX_DUPLICATE_PATH

    def test_bare_profile_root_without_component_envelope(self):
        # Accept a bare <JSONProfile> (no <bns:Component> wrapper).
        xml = (
            '<JSONProfile xmlns=""><DataElements>'
            '<JSONRootValue key="1" name="Root"><JSONObject key="2" name="Object">'
            '<JSONObjectEntry key="3" name="x" dataType="character" isMappable="true"/>'
            "</JSONObject></JSONRootValue></DataElements></JSONProfile>"
        )
        res = index_existing_profile_xml(xml)
        assert res["mappable_paths"] == ["Root/x"]


# ===========================================================================
# 2. Supplied-index validation
# ===========================================================================


class TestSuppliedIndexValidation:
    def test_valid_supplied_index_passes(self):
        entry = _supplied_index(JSON_UUID, "profile_json")
        assert validate_supplied_profile_index(JSON_UUID, entry) is None

    def test_non_mapping_rejected(self):
        assert validate_supplied_profile_index(JSON_UUID, "nope") is not None

    def test_component_id_mismatch_rejected(self):
        entry = _supplied_index("different-uuid", "profile_json")
        err = validate_supplied_profile_index(JSON_UUID, entry)
        assert err is not None
        assert err.error_code == MAP_PROFILE_INDEX_UNAVAILABLE

    def test_unsupported_profile_component_type_rejected(self):
        entry = _supplied_index(JSON_UUID, "profile_json")
        entry["profile_component_type"] = "profile.edi"
        err = validate_supplied_profile_index(JSON_UUID, entry)
        assert err is not None and err.error_code == MAP_PROFILE_INDEX_UNAVAILABLE

    def test_empty_field_index_rejected(self):
        entry = _supplied_index(JSON_UUID, "profile_json")
        entry["field_index_by_path"] = {}
        assert validate_supplied_profile_index(JSON_UUID, entry) is not None

    def test_entry_missing_key_path_rejected(self):
        entry = _supplied_index(JSON_UUID, "profile_json")
        first = next(iter(entry["field_index_by_path"]))
        entry["field_index_by_path"][first] = dict(
            entry["field_index_by_path"][first]
        )
        entry["field_index_by_path"][first].pop("key_path")
        assert validate_supplied_profile_index(JSON_UUID, entry) is not None

    def test_non_boolean_mappable_rejected(self):
        entry = _supplied_index(JSON_UUID, "profile_json")
        first = next(iter(entry["field_index_by_path"]))
        entry["field_index_by_path"][first] = dict(
            entry["field_index_by_path"][first]
        )
        entry["field_index_by_path"][first]["mappable"] = "true"  # str, not bool
        assert validate_supplied_profile_index(JSON_UUID, entry) is not None

    def test_non_string_key_path_rejected(self):
        # key_path / name_path reach the string-only XML escaper — an int there
        # would pass planning and crash rendering mid-apply.
        for bad_key in ("key_path", "name_path"):
            entry = _supplied_index(JSON_UUID, "profile_json")
            first = next(iter(entry["field_index_by_path"]))
            entry["field_index_by_path"][first] = dict(entry["field_index_by_path"][first])
            entry["field_index_by_path"][first][bad_key] = 123  # int, not str
            err = validate_supplied_profile_index(JSON_UUID, entry)
            assert err is not None and err.error_code == MAP_PROFILE_INDEX_UNAVAILABLE

    def test_integer_key_still_accepted(self):
        # key (the platform key) may legitimately be an int.
        entry = _supplied_index(JSON_UUID, "profile_json")
        first = next(iter(entry["field_index_by_path"]))
        entry["field_index_by_path"][first] = dict(entry["field_index_by_path"][first])
        entry["field_index_by_path"][first]["key"] = 5
        assert validate_supplied_profile_index(JSON_UUID, entry) is None

    def test_boolean_key_rejected(self):
        # bool subclasses int, but True/False would render as invalid map keys.
        for bad in (True, False):
            entry = _supplied_index(JSON_UUID, "profile_json")
            first = next(iter(entry["field_index_by_path"]))
            entry["field_index_by_path"][first] = dict(entry["field_index_by_path"][first])
            entry["field_index_by_path"][first]["key"] = bad
            err = validate_supplied_profile_index(JSON_UUID, entry)
            assert err is not None and err.error_code == MAP_PROFILE_INDEX_UNAVAILABLE


# ===========================================================================
# 3. Read-only handler
# ===========================================================================


_HANDLER_GET = "src.boomi_mcp.categories.profile_index.component_get_xml"


class TestIndexProfileComponentAction:
    def test_default_omits_raw_xml(self):
        with patch(_HANDLER_GET, return_value={"type": "profile.json", "id": JSON_UUID, "xml": _fixture("profile_json")}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is True
        assert res["read_only"] is True
        assert res["boomi_mutation"] is False
        assert res["raw_xml_exposed"] is False
        assert "raw_xml" not in res
        assert res["profile_component_type"] == "profile.json"
        assert "Root/customer_code" in res["field_index_by_path"]

    def test_opt_in_includes_raw_xml(self):
        with patch(_HANDLER_GET, return_value={"type": "profile.json", "id": JSON_UUID, "xml": _fixture("profile_json")}):
            res = index_profile_component_action(MagicMock(), JSON_UUID, include_raw_xml=True)
        assert res["raw_xml_exposed"] is True
        assert res["raw_xml"] == _fixture("profile_json")

    def test_success_carries_import_provenance_marker(self):
        # import_integration_draft only accepts a live index whose
        # produced_by == "index_profile_component".
        with patch(_HANDLER_GET, return_value={"type": "profile.json", "id": JSON_UUID, "xml": _fixture("profile_json")}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["produced_by"] == "index_profile_component"

    def test_blank_component_id_is_structured_error(self):
        res = index_profile_component_action(MagicMock(), "   ")
        assert res["_success"] is False
        assert res["error_code"] == "INDEX_PROFILE_COMPONENT_INVALID_INPUT"
        assert res["read_only"] is True
        assert res["raw_xml_exposed"] is False

    def test_fetch_failure_is_read_only_envelope_no_raw_xml(self):
        with patch(_HANDLER_GET, side_effect=Exception("GET failed (HTTP 404): not found")):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == "INDEX_PROFILE_COMPONENT_FETCH_FAILED"
        assert res["read_only"] is True
        assert res["boomi_mutation"] is False
        assert res["raw_xml_exposed"] is False
        assert "raw_xml" not in res

    def test_unsupported_type_is_structured_error(self):
        bad = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/">'
            "<bns:object><NotAProfile/></bns:object></bns:Component>"
        )
        with patch(_HANDLER_GET, return_value={"type": "process", "id": JSON_UUID, "xml": bad}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == PROFILE_INDEX_UNSUPPORTED_TYPE
        assert res["raw_xml_exposed"] is False
        assert "raw_xml" not in res

    def test_parse_failure_is_structured_error(self):
        with patch(_HANDLER_GET, return_value={"type": "profile.json", "id": JSON_UUID, "xml": "<broken"}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == PROFILE_INDEX_PARSE_FAILED
        assert res["raw_xml_exposed"] is False

    def test_non_profile_metadata_type_rejected_even_with_profile_subtree(self):
        # A non-profile component whose XML embeds a profile-shaped subtree must
        # be rejected on its declared metadata type — not mis-indexed.
        sneaky = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process">'
            "<bns:object>" + _fixture("profile_json").split("<bns:object>")[1]
        )
        with patch(_HANDLER_GET, return_value={"type": "process", "id": JSON_UUID, "xml": sneaky}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == PROFILE_INDEX_UNSUPPORTED_TYPE
        assert res["raw_xml_exposed"] is False

    def test_returned_id_mismatch_rejected(self):
        with patch(_HANDLER_GET, return_value={"type": "profile.json", "id": "other-id", "xml": _fixture("profile_json")}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == "INDEX_PROFILE_COMPONENT_FETCH_FAILED"

    def test_missing_exported_id_fails_closed(self):
        # component_get_xml returns 'id'='' when the exported XML omits componentId;
        # the identity check must FAIL CLOSED (not fall through to indexing).
        with patch(_HANDLER_GET, return_value={"type": "profile.json", "id": "", "xml": _fixture("profile_json")}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == "INDEX_PROFILE_COMPONENT_FETCH_FAILED"

    def test_missing_metadata_type_fails_closed(self):
        # A blank/absent declared type must be rejected fail-closed, never
        # falling through to XML-root inference.
        with patch(_HANDLER_GET, return_value={"type": "", "id": JSON_UUID, "xml": _fixture("profile_json")}):
            res = index_profile_component_action(MagicMock(), JSON_UUID)
        assert res["_success"] is False
        assert res["error_code"] == PROFILE_INDEX_UNSUPPORTED_TYPE


# ===========================================================================
# 4. build_integration plan — literal-UUID map validation
# ===========================================================================

_PLAN_PAGINATE = "src.boomi_mcp.categories.integration_builder.paginate_metadata"
_PLAN_GET = "src.boomi_mcp.categories.integration_builder.component_get_xml"


def _literal_map_config(
    field_mappings,
    *,
    profile_indexes=None,
    source_uuid=JSON_UUID,
    target_uuid=XML_UUID,
):
    """Config for a build_integration plan of a single literal-UUID direct map."""
    spec = {
        "version": "1.0",
        "name": "issue-95-map",
        "components": [
            {
                "key": "literal_map",
                "type": "transform.map",
                "action": "create",
                "name": "Literal UUID Map",
                "config": {
                    "component_type": "transform.map",
                    "map_type": "direct",
                    "component_name": "Literal UUID Map",
                    "source_profile_id": source_uuid,
                    "source_profile_type": "profile.json",
                    "target_profile_id": target_uuid,
                    "target_profile_type": "profile.xml",
                    "field_mappings": field_mappings,
                },
                "depends_on": [],
            }
        ],
    }
    if profile_indexes is not None:
        spec["profile_indexes_by_component_id"] = profile_indexes
    return {"conflict_policy": "reuse", "integration_spec": spec}


def _map_step(plan):
    return next(s for s in plan["steps"] if s["key"] == "literal_map")


class TestLiteralUuidMapPlan:
    def _both_supplied(self):
        return {
            JSON_UUID: _supplied_index(JSON_UUID, "profile_json"),
            XML_UUID: _supplied_index(XML_UUID, "profile_xml"),
        }

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_supplied_indexes_validate_clean(self, _pag):
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
            profile_indexes=self._both_supplied(),
        )
        step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "create"
        assert "validation_error" not in step or step.get("validation_error") is None

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_missing_target_field_reports_map_field_not_found(self, _pag):
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/nonexistent"}],
            profile_indexes=self._both_supplied(),
        )
        step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "MAP_FIELD_NOT_FOUND"

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_structural_target_reports_not_mappable(self, _pag):
        cfg = _literal_map_config(
            # Order/lines is a structural (repeating) container -> non-mappable.
            [{"source_path": "Root/customer_code", "target_path": "Order/lines"}],
            profile_indexes=self._both_supplied(),
        )
        step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "PROFILE_FIELD_NOT_MAPPABLE"

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_explicit_non_mappable_leaf_target_rejected(self, _pag):
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/internal_token"}],
            profile_indexes=self._both_supplied(),
        )
        step = _map_step(_build_plan(MagicMock(), cfg))
        ve = step["validation_error"]
        assert ve["error_code"] == "PROFILE_FIELD_NOT_MAPPABLE"
        # A non-mappable LEAF must not be diagnosed as a structural node
        # (the diagnostic branches on the index's structural flag).
        assert ve["details"]["structural"] is False
        assert "structural node" not in ve["error"]

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_index_type_mismatch_reports_unavailable(self, _pag):
        # A supplied index whose profile type conflicts with the endpoint's
        # declared source/target_profile_type is rejected pre-mutation.
        indexes = {
            JSON_UUID: _supplied_index(JSON_UUID, "profile_xml"),  # xml index for a profile.json endpoint
            XML_UUID: _supplied_index(XML_UUID, "profile_xml"),
        }
        cfg = _literal_map_config(
            [{"source_path": "Order/customer_code", "target_path": "Order/customer_code"}],
            profile_indexes=indexes,
        )
        step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"
        assert step["validation_error"]["field"] == "source_profile_type"

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_no_index_and_no_discovery_reports_unavailable(self, _pag):
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
        )
        # No supplied index; live discovery fails.
        with patch(_PLAN_GET, side_effect=Exception("not found")):
            step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_live_discovery_resolves_and_validates_clean(self, _pag):
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
        )

        def _fake_get(_client, component_id, *a, **k):
            fixture = "profile_json" if component_id == JSON_UUID else "profile_xml"
            return {"type": f"profile.{'json' if fixture=='profile_json' else 'xml'}",
                    "id": component_id, "xml": _fixture(fixture)}

        with patch(_PLAN_GET, side_effect=_fake_get):
            step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "create"
        assert step.get("validation_error") is None

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_malformed_supplied_index_falls_through_to_discovery(self, _pag):
        # Supplied index for source has a component_id mismatch (malformed) -> the
        # resolver falls through to live discovery, which succeeds for both.
        bad = _supplied_index("wrong-uuid", "profile_json")
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
            profile_indexes={JSON_UUID: bad},
        )

        def _fake_get(_client, component_id, *a, **k):
            fixture = "profile_json" if component_id == JSON_UUID else "profile_xml"
            return {"type": "profile.json" if component_id == JSON_UUID else "profile.xml",
                    "id": component_id, "xml": _fixture(fixture)}

        with patch(_PLAN_GET, side_effect=_fake_get):
            step = _map_step(_build_plan(MagicMock(), cfg))
        assert step["planned_action"] == "create"

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_all_ref_spec_makes_zero_live_calls(self, _pag):
        # A spec with no literal-UUID map endpoint must not call discovery.
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
            source_uuid="$ref:missing_src",
            target_uuid="$ref:missing_tgt",
        )
        with patch(_PLAN_GET) as m_get:
            _build_plan(MagicMock(), cfg)
        m_get.assert_not_called()


# ---- Fix regressions: $ref normalization, top-level config, apply drift, review ----

_APPLY_RESOLVE = "src.boomi_mcp.categories.integration_builder._resolve_literal_profile_indexes"
_APPLY_EXEC = "src.boomi_mcp.categories.integration_builder._execute_component"


class TestResolveMapProfileIndexNormalization:
    def test_ref_with_leading_whitespace_treated_as_literal(self):
        # A padded " $ref:x" must NOT resolve as a $ref (depends_on coverage and
        # _resolve_dependency_tokens test the unstripped value); it stays a
        # literal -> None -> MAP_PROFILE_INDEX_UNAVAILABLE, consistently.
        assert resolve_map_profile_index(" $ref:x", None, {}) is None
        assert resolve_map_profile_index(" $ref:x", {"x": object()}) is None

    def test_literal_uuid_stripped_only_for_index_lookup(self):
        idx = {"f": {"key": "5", "key_path": "kp", "name_path": "np", "mappable": True}}
        wrapper = {"profile_component_type": "profile.json", "field_index_by_path": idx}
        assert resolve_map_profile_index("  U1  ", None, {"U1": wrapper}) is idx

    def test_unknown_literal_uuid_returns_none(self):
        assert resolve_map_profile_index("nope", None, {"U1": {}}) is None


class TestTopLevelConfigPreservesIndexes:
    def test_top_level_form_preserves_supplied_indexes(self):
        supplied = {JSON_UUID: _supplied_index(JSON_UUID, "profile_json")}
        spec = _normalize_to_spec(
            {"name": "x", "components": [], "profile_indexes_by_component_id": supplied}
        )
        assert spec.profile_indexes_by_component_id == supplied

    def test_integration_spec_form_preserves_supplied_indexes(self):
        supplied = {JSON_UUID: _supplied_index(JSON_UUID, "profile_json")}
        spec = _normalize_to_spec(
            {
                "integration_spec": {
                    "name": "x",
                    "components": [],
                    "profile_indexes_by_component_id": supplied,
                }
            }
        )
        assert spec.profile_indexes_by_component_id == supplied


class TestApplyDriftFailFast:
    @patch(_PLAN_PAGINATE, return_value=[])
    def test_apply_time_index_drift_fails_before_mutation(self, _pag):
        # Plan resolves valid indexes (map validates clean); apply RE-resolves an
        # empty map (a live profile changed/removed between plan and apply) ->
        # fail fast BEFORE any _execute_component, so no partial mutation.
        valid = {
            JSON_UUID: _supplied_index(JSON_UUID, "profile_json"),
            XML_UUID: _supplied_index(XML_UUID, "profile_xml"),
        }
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
        )
        cfg["dry_run"] = False
        with (
            patch(_APPLY_RESOLVE, side_effect=[valid, {}]),
            patch(_APPLY_EXEC) as m_exec,
        ):
            result = _apply_plan(MagicMock(), "work", cfg)
        assert result["_success"] is False
        assert result["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"
        assert result["failed_step"] == "literal_map"
        m_exec.assert_not_called()

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_stable_indexes_apply_without_drift_error(self, _pag):
        # Sanity: with stable indexes across plan+apply, the drift guard does not
        # false-trigger — the map reaches the execution loop.
        valid = {
            JSON_UUID: _supplied_index(JSON_UUID, "profile_json"),
            XML_UUID: _supplied_index(XML_UUID, "profile_xml"),
        }
        cfg = _literal_map_config(
            [{"source_path": "Root/customer_code", "target_path": "Order/customer_code"}],
        )
        cfg["dry_run"] = False
        with (
            patch(_APPLY_RESOLVE, side_effect=[valid, valid]),
            patch(_APPLY_EXEC, return_value={"_success": True, "component_id": "new-map"}) as m_exec,
        ):
            result = _apply_plan(MagicMock(), "work", cfg)
        assert result.get("_success", True) is not False
        m_exec.assert_called()

    @patch(_PLAN_PAGINATE, return_value=[])
    def test_raw_xml_map_with_literal_ids_not_blocked_by_drift_guard(self, _pag):
        # A raw-XML (config.xml) map with literal IDs skips structured validation
        # everywhere else; the drift guard must not run validate_transform_map on
        # it (which would fail UNSUPPORTED_TRANSFORM_ROUTE). It reaches apply.
        spec = {
            "version": "1.0",
            "name": "rawxml",
            "components": [
                {
                    "key": "raw_map",
                    "type": "transform.map",
                    "action": "create",
                    "name": "Raw Map",
                    "config": {
                        "component_type": "transform.map",
                        "xml": "<bns:Component type=\"transform.map\"><bns:object/></bns:Component>",
                        "source_profile_id": JSON_UUID,
                        "target_profile_id": XML_UUID,
                    },
                    "depends_on": [],
                }
            ],
        }
        cfg = {"conflict_policy": "reuse", "dry_run": False, "integration_spec": spec}
        with (
            patch(_APPLY_RESOLVE, side_effect=[{}, {}]),
            patch(_APPLY_EXEC, return_value={"_success": True, "component_id": "x"}) as m_exec,
        ):
            result = _apply_plan(MagicMock(), "work", cfg)
        m_exec.assert_called()
        assert result.get("_success", True) is not False


class TestTransformationReviewLiteralIndexes:
    def _review_config(self, with_index):
        spec = {
            "version": "1.0",
            "name": "rev",
            "components": [
                {
                    "key": "m",
                    "type": "transform.map",
                    "name": "M",
                    "config": {
                        "component_type": "transform.map",
                        "map_type": "direct",
                        "component_name": "M",
                        "source_profile_id": JSON_UUID,
                        "source_profile_type": "profile.json",
                        "target_profile_id": JSON2_UUID,
                        "target_profile_type": "profile.json",
                        "field_mappings": [
                            {"source_path": "Root/customer_code", "target_path": "Root/customer_code"}
                        ],
                    },
                    "depends_on": [],
                }
            ],
        }
        if with_index:
            spec["profile_indexes_by_component_id"] = {
                JSON_UUID: _supplied_index(JSON_UUID, "profile_json"),
                JSON2_UUID: _supplied_index(JSON2_UUID, "profile_json"),
            }
        return {"integration_spec": spec}

    def test_review_without_supplied_index_reports_unavailable(self):
        res = review_transformation_action("list_fields", self._review_config(False))
        assert res["_success"] is False
        assert "PROFILE_INDEX_UNAVAILABLE" in json.dumps(res)

    def test_review_with_supplied_index_resolves(self):
        # Build accepts this spec; review must not diverge (no unavailable error).
        res = review_transformation_action("list_fields", self._review_config(True))
        assert res["_success"] is True
        assert "PROFILE_INDEX_UNAVAILABLE" not in json.dumps(res)


# ===========================================================================
# 5. MCP wrapper (registration / annotations / delegation)
# ===========================================================================

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def _annotation_value(annotations, key):
    if annotations is None:
        return None
    if hasattr(annotations, key):
        return getattr(annotations, key)
    if isinstance(annotations, dict):
        return annotations.get(key)
    if hasattr(annotations, "model_dump"):
        return annotations.model_dump().get(key)
    raise AssertionError(f"Cannot read annotation {key!r}")


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestIndexProfileComponentWrapper:
    def test_registered_read_only_idempotent(self):
        t = _run_async(server.mcp.get_tool("index_profile_component"))
        assert t is not None
        assert _annotation_value(t.annotations, "readOnlyHint") is True
        assert _annotation_value(t.annotations, "destructiveHint") is False
        assert _annotation_value(t.annotations, "idempotentHint") is True

    def test_schema_exposes_expected_params(self):
        by = {t.name: t for t in _run_async(server.mcp.list_tools())}
        props = set(by["index_profile_component"].parameters["properties"])
        assert props == {"profile", "component_id", "include_raw_xml"}

    def test_wrapper_builds_sdk_and_delegates(self):
        handler = MagicMock(return_value={"_success": True, "field_index_by_path": {}})
        with (
            patch.object(server, "get_current_user", return_value="user@example.com"),
            patch.object(
                server,
                "get_secret",
                return_value={"account_id": "acct", "username": "u", "password": "p"},
            ),
            patch.object(server, "Boomi", return_value="SDK") as m_boomi,
            patch.object(server, "index_profile_component_action", handler),
        ):
            result = server.index_profile_component(
                profile="work", component_id=JSON_UUID, include_raw_xml=True
            )
        assert result == {"_success": True, "field_index_by_path": {}}
        m_boomi.assert_called_once()
        handler.assert_called_once_with("SDK", JSON_UUID, True)

    def test_wrapper_disabled_profile_returns_error(self):
        with (
            patch.object(server, "get_current_user", return_value="user@example.com"),
            patch.object(
                server,
                "get_secret",
                side_effect=server.DisabledProfileError("Profile 'work' is disabled"),
            ),
        ):
            result = server.index_profile_component(profile="work", component_id=JSON_UUID)
        assert result["_success"] is False
        assert "disabled" in result["error"].lower()

    def test_wrapper_errors_carry_read_only_contract_flags(self):
        # Pre-handler failures must still honor the advertised read-only envelope
        # (flags + structured error code, never raw XML).
        with (
            patch.object(server, "get_current_user", return_value="user@example.com"),
            patch.object(
                server, "get_secret", side_effect=ValueError("no such profile")
            ),
        ):
            result = server.index_profile_component(profile="nope", component_id=JSON_UUID)
        assert result["_success"] is False
        assert result["read_only"] is True
        assert result["boomi_mutation"] is False
        assert result["raw_xml_exposed"] is False
        assert result["error_code"] == "PROFILE_NOT_FOUND"
