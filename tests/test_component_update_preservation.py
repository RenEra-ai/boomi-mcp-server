"""Unit tests for the component_update_preservation module (issue #45)."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.component_update_preservation import (
    OwnedPath,
    PreservationPolicy,
    merge_for_update,
)


NS = {"bns": "http://api.platform.boomi.com/"}


# ---------------------------------------------------------------------------
# Fixtures: minimal-but-realistic current and desired component XMLs.
# ---------------------------------------------------------------------------

_DESIRED_DB_CONN = """\
<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database"
               name="conn-renamed" folderFullPath="DB/Conns">
  <bns:encryptedValues/>
  <bns:description>desired-desc</bns:description>
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="newdb" host="db.example.com" port="5432"/>
  </bns:object>
</bns:Component>
"""

_CURRENT_DB_CONN = """\
<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database"
               name="conn-old" folderId="42" futureFlag="opaque-value">
  <bns:encryptedValues>
    <bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/>
  </bns:encryptedValues>
  <bns:description>old-desc</bns:description>
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="olddb" host="old.example.com"
                                port="3306" futureAttr="boomi-added"/>
    <UnknownSibling xmlns="" id="must-survive"/>
  </bns:object>
  <bns:processOverrides xmlns="">
    <override path="//DatabaseConnectionSettings/@host" environmentId="env-1" value="prod.db"/>
  </bns:processOverrides>
</bns:Component>
"""


_DB_POLICY = PreservationPolicy(
    component_type="connector-settings",
    subtype="database",
    owned_root_attrs=("name", "folderId", "folderFullPath"),
    owned_paths=(OwnedPath(path="bns:object/DatabaseConnectionSettings"),),
)


def _parse(xml: str) -> ET.Element:
    return ET.fromstring(xml)


# ---------------------------------------------------------------------------
# Happy-path preservation
# ---------------------------------------------------------------------------


def test_unknown_root_attribute_survives():
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    assert root.attrib.get("futureFlag") == "opaque-value"


def test_owned_root_attributes_get_replaced():
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    assert root.attrib["name"] == "conn-renamed"
    assert root.attrib["folderFullPath"] == "DB/Conns"


def test_unknown_root_child_survives():
    """bns:processOverrides is not owned by any builder — must survive."""
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    assert root.find("bns:processOverrides", NS) is not None


def test_unknown_object_sibling_survives():
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    obj = root.find("bns:object", NS)
    assert obj is not None
    assert obj.find("UnknownSibling") is not None
    assert obj.find("UnknownSibling").attrib["id"] == "must-survive"


def test_owned_subtree_replaced_with_desired():
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    settings = root.find("bns:object/DatabaseConnectionSettings", NS)
    assert settings is not None
    assert settings.attrib["dbname"] == "newdb"
    assert settings.attrib["host"] == "db.example.com"
    # The `futureAttr` was on the current's DatabaseConnectionSettings. Per
    # policy, the entire owned subtree is replaced — so it's gone. Anything
    # the builder doesn't own should live OUTSIDE the owned subtree.
    assert "futureAttr" not in settings.attrib


def test_description_replaced_when_desired_non_empty():
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    desc = root.find("bns:description", NS)
    assert desc is not None
    assert desc.text == "desired-desc"


def test_description_preserved_when_desired_empty():
    desired_empty_desc = _DESIRED_DB_CONN.replace(
        "<bns:description>desired-desc</bns:description>",
        "<bns:description></bns:description>",
    )
    merged = merge_for_update(_CURRENT_DB_CONN, desired_empty_desc, _DB_POLICY)
    root = _parse(merged)
    desc = root.find("bns:description", NS)
    assert desc is not None
    assert desc.text == "old-desc"


# ---------------------------------------------------------------------------
# encryptedValues handling
# ---------------------------------------------------------------------------


def test_existing_encrypted_value_entries_survive():
    merged = merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, _DB_POLICY)
    root = _parse(merged)
    ev = root.find("bns:encryptedValues", NS)
    assert ev is not None
    entries = ev.findall("bns:encryptedValue", NS)
    paths = {e.attrib.get("path") for e in entries}
    assert "//DatabaseConnectionSettings/@password" in paths


def test_desired_encrypted_value_added_when_missing_from_current():
    desired_with_new_secret = _DESIRED_DB_CONN.replace(
        "<bns:encryptedValues/>",
        (
            "<bns:encryptedValues>"
            '<bns:encryptedValue path="//DatabaseConnectionSettings/@token" isSet="false"/>'
            "</bns:encryptedValues>"
        ),
    )
    merged = merge_for_update(_CURRENT_DB_CONN, desired_with_new_secret, _DB_POLICY)
    root = _parse(merged)
    ev = root.find("bns:encryptedValues", NS)
    entries = ev.findall("bns:encryptedValue", NS)
    paths = {e.attrib.get("path") for e in entries}
    assert "//DatabaseConnectionSettings/@password" in paths  # current preserved
    assert "//DatabaseConnectionSettings/@token" in paths  # desired added


def test_desired_encrypted_value_does_not_clobber_existing_isset_secret():
    desired_overwriting_existing = _DESIRED_DB_CONN.replace(
        "<bns:encryptedValues/>",
        (
            "<bns:encryptedValues>"
            '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="false"/>'
            "</bns:encryptedValues>"
        ),
    )
    merged = merge_for_update(_CURRENT_DB_CONN, desired_overwriting_existing, _DB_POLICY)
    root = _parse(merged)
    ev = root.find("bns:encryptedValues", NS)
    entries = ev.findall("bns:encryptedValue", NS)
    paths = {e.attrib.get("path") for e in entries}
    isset_values = {
        e.attrib.get("path"): e.attrib.get("isSet")
        for e in entries
    }
    assert paths == {"//DatabaseConnectionSettings/@password"}
    # Existing isSet=true must NOT have been overwritten to isSet=false.
    assert isset_values["//DatabaseConnectionSettings/@password"] == "true"


# ---------------------------------------------------------------------------
# Type / subType mismatch
# ---------------------------------------------------------------------------


def test_type_mismatch_raises_structured_error():
    wrong_type_current = _CURRENT_DB_CONN.replace(
        'type="connector-settings"', 'type="connector-action"'
    )
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(wrong_type_current, _DESIRED_DB_CONN, _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_TYPE_MISMATCH"
    assert exc.value.field == "type"


def test_subtype_mismatch_raises_structured_error():
    wrong_subtype_current = _CURRENT_DB_CONN.replace(
        'subType="database"', 'subType="rest"'
    )
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(wrong_subtype_current, _DESIRED_DB_CONN, _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_TYPE_MISMATCH"
    assert exc.value.field == "subType"


# ---------------------------------------------------------------------------
# Malformed XML
# ---------------------------------------------------------------------------


def test_malformed_current_xml_raises_parse_error():
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update("<not-valid", _DESIRED_DB_CONN, _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_XML_PARSE_FAILED"
    assert exc.value.field == "current_xml"


def test_malformed_desired_xml_raises_parse_error():
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(_CURRENT_DB_CONN, "<not-valid", _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_XML_PARSE_FAILED"
    assert exc.value.field == "desired_xml"


def test_empty_xml_raises_parse_error():
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update("", _DESIRED_DB_CONN, _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_XML_PARSE_FAILED"


# ---------------------------------------------------------------------------
# Missing owned subtree
# ---------------------------------------------------------------------------


def test_missing_owned_subtree_in_current_raises_object_missing():
    current_no_settings = _CURRENT_DB_CONN.replace(
        '<DatabaseConnectionSettings xmlns="" dbname="olddb" host="old.example.com"\n'
        '                                port="3306" futureAttr="boomi-added"/>',
        "",
    )
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(current_no_settings, _DESIRED_DB_CONN, _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_OBJECT_MISSING"


def test_missing_owned_subtree_in_desired_raises_object_missing():
    desired_no_settings = re.sub(
        r"<DatabaseConnectionSettings[^>]*/>",
        "",
        _DESIRED_DB_CONN,
    )
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(_CURRENT_DB_CONN, desired_no_settings, _DB_POLICY)
    assert exc.value.error_code == "UPDATE_PRESERVATION_OBJECT_MISSING"


# ---------------------------------------------------------------------------
# Policy required
# ---------------------------------------------------------------------------


def test_missing_policy_raises_policy_unsupported():
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(_CURRENT_DB_CONN, _DESIRED_DB_CONN, None)  # type: ignore[arg-type]
    assert exc.value.error_code == "UPDATE_PRESERVATION_POLICY_UNSUPPORTED"


# ---------------------------------------------------------------------------
# Key-merge mode (REST Client field id pattern)
# ---------------------------------------------------------------------------


_REST_CURRENT = """\
<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest-old">
  <bns:encryptedValues/>
  <bns:description>desc</bns:description>
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://old.example.com"/>
      <field id="auth_type" type="string" value="basic"/>
      <field id="future_custom_field" type="string" value="boomi-added-this"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""

_REST_DESIRED = """\
<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest-renamed">
  <bns:encryptedValues/>
  <bns:description>new-desc</bns:description>
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://new.example.com"/>
      <field id="auth_type" type="string" value="oauth"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""


_REST_POLICY = PreservationPolicy(
    component_type="connector-settings",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/GenericConnectionConfig",
            mode="key_merge",
            key_attr="id",
        ),
    ),
)


def test_key_merge_replaces_owned_field_ids():
    merged = merge_for_update(_REST_CURRENT, _REST_DESIRED, _REST_POLICY)
    root = _parse(merged)
    fields = root.findall("bns:object/GenericConnectionConfig/field", NS)
    by_id = {f.attrib["id"]: f.attrib.get("value") for f in fields}
    assert by_id["url"] == "https://new.example.com"
    assert by_id["auth_type"] == "oauth"


def test_key_merge_preserves_unknown_field_ids():
    merged = merge_for_update(_REST_CURRENT, _REST_DESIRED, _REST_POLICY)
    root = _parse(merged)
    fields = root.findall("bns:object/GenericConnectionConfig/field", NS)
    by_id = {f.attrib["id"]: f.attrib.get("value") for f in fields}
    assert by_id.get("future_custom_field") == "boomi-added-this"


def test_key_merge_preserves_relative_order_of_existing_fields():
    merged = merge_for_update(_REST_CURRENT, _REST_DESIRED, _REST_POLICY)
    root = _parse(merged)
    field_ids = [
        f.attrib["id"]
        for f in root.findall("bns:object/GenericConnectionConfig/field", NS)
    ]
    # url and auth_type were in current at indices 0,1 — they should still
    # be at indices 0,1 after merge. future_custom_field stays at index 2.
    assert field_ids[0:3] == ["url", "auth_type", "future_custom_field"]


def test_key_merge_appends_new_field_ids_at_end():
    desired_with_new_field = _REST_DESIRED.replace(
        '<field id="auth_type" type="string" value="oauth"/>',
        (
            '<field id="auth_type" type="string" value="oauth"/>'
            '<field id="newly_added_owned_field" type="string" value="present"/>'
        ),
    )
    merged = merge_for_update(_REST_CURRENT, desired_with_new_field, _REST_POLICY)
    root = _parse(merged)
    field_ids = [
        f.attrib["id"]
        for f in root.findall("bns:object/GenericConnectionConfig/field", NS)
    ]
    # Order: url, auth_type, future_custom_field (preserved), newly_added_owned_field (appended)
    assert field_ids == [
        "url",
        "auth_type",
        "future_custom_field",
        "newly_added_owned_field",
    ]


# ---------------------------------------------------------------------------
# Key-merge attribute handling (REST Client operation pattern)
# ---------------------------------------------------------------------------


_REST_OP_CURRENT = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op-old">
  <bns:object>
    <Operation xmlns="" returnApplicationErrors="true" trackResponse="false">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE"
                                futureBoomiAttr="opaque">
          <field id="path" type="string" value="/old/path"/>
          <field id="future_owned_field" type="string" value="user-added"/>
          <Options/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""

_REST_OP_DESIRED = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op-renamed">
  <bns:object>
    <Operation xmlns="" returnApplicationErrors="false" trackResponse="true">
      <Configuration>
        <GenericOperationConfig customOperationType="POST" operationType="EXECUTE"
                                requestProfileType="JSON" responseProfileType="JSON"
                                requestProfile="rp-123" responseProfile="rsp-456">
          <field id="path" type="string" value="/new/path"/>
          <Options/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""


_REST_OP_POLICY = PreservationPolicy(
    component_type="connector-action",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/Operation/Configuration/GenericOperationConfig",
            mode="key_merge",
            key_attr="id",
        ),
    ),
)


def test_key_merge_overwrites_owned_attributes():
    merged = merge_for_update(_REST_OP_CURRENT, _REST_OP_DESIRED, _REST_OP_POLICY)
    root = _parse(merged)
    cfg = root.find("bns:object/Operation/Configuration/GenericOperationConfig", NS)
    assert cfg is not None
    assert cfg.attrib["customOperationType"] == "POST"
    assert cfg.attrib["requestProfileType"] == "JSON"
    assert cfg.attrib["requestProfile"] == "rp-123"


def test_key_merge_preserves_unknown_attributes():
    """Unknown attribute on the keyed element (e.g., a future Boomi attr)
    must survive even when builder sets other owned attrs."""
    merged = merge_for_update(_REST_OP_CURRENT, _REST_OP_DESIRED, _REST_OP_POLICY)
    root = _parse(merged)
    cfg = root.find("bns:object/Operation/Configuration/GenericOperationConfig", NS)
    assert cfg is not None
    assert cfg.attrib.get("futureBoomiAttr") == "opaque"


def test_key_merge_preserves_unknown_field_id_in_operation():
    merged = merge_for_update(_REST_OP_CURRENT, _REST_OP_DESIRED, _REST_OP_POLICY)
    root = _parse(merged)
    field_ids = {
        f.attrib["id"]
        for f in root.findall(
            "bns:object/Operation/Configuration/GenericOperationConfig/field", NS
        )
    }
    assert "future_owned_field" in field_ids
    assert "path" in field_ids


# ---------------------------------------------------------------------------
# Codex review r1 — owned_keys removal in key_merge
# ---------------------------------------------------------------------------


_KEY_MERGE_OWNED_KEYS_CURRENT = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/items"/>
          <field id="followRedirects" type="string" value="NONE"/>
          <field id="future_user_extra" type="string" value="must-survive"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""


_KEY_MERGE_OWNED_KEYS_DESIRED_PATCH = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="PATCH" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""


_KEY_MERGE_OWNED_KEYS_POLICY = PreservationPolicy(
    component_type="connector-action",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/Operation/Configuration/GenericOperationConfig",
            mode="key_merge",
            key_attr="id",
            owned_keys=("path", "followRedirects"),
        ),
    ),
)


def test_owned_keys_remove_stale_when_desired_omits():
    """When the builder owns a key but desired XML omits it, the current
    XML's entry must be REMOVED (treated as a builder-cleared field).
    Codex r1 P2: GET→PATCH method change must drop the stale
    followRedirects field, not preserve it."""
    merged = merge_for_update(
        _KEY_MERGE_OWNED_KEYS_CURRENT,
        _KEY_MERGE_OWNED_KEYS_DESIRED_PATCH,
        _KEY_MERGE_OWNED_KEYS_POLICY,
    )
    root = _parse(merged)
    field_ids = {
        f.attrib["id"]
        for f in root.findall(
            "bns:object/Operation/Configuration/GenericOperationConfig/field", NS
        )
    }
    assert "followRedirects" not in field_ids
    assert "path" in field_ids  # still owned + still present in desired
    # Truly unknown ids (not in owned_keys) still survive.
    assert "future_user_extra" in field_ids


# ---------------------------------------------------------------------------
# Codex review r1 — unkeyed children replaced by tag name (no duplicates)
# ---------------------------------------------------------------------------


_OPTIONS_DUP_CURRENT = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/items"/>
          <Options/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""


_OPTIONS_DUP_DESIRED = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/items/new"/>
          <Options/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""


_OPTIONS_DUP_POLICY = PreservationPolicy(
    component_type="connector-action",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/Operation/Configuration/GenericOperationConfig",
            mode="key_merge",
            key_attr="id",
        ),
    ),
)


def test_key_merge_unkeyed_children_replaced_by_tag_no_duplicates():
    """Both current and desired emit a single <Options/> placeholder
    (no @id). The merge must REPLACE current's Options with desired's
    by tag-name match, not preserve current + append desired (which
    would produce a duplicate). Codex r1 P2."""
    merged = merge_for_update(
        _OPTIONS_DUP_CURRENT, _OPTIONS_DUP_DESIRED, _OPTIONS_DUP_POLICY
    )
    root = _parse(merged)
    options = root.findall(
        "bns:object/Operation/Configuration/GenericOperationConfig/Options", NS
    )
    assert len(options) == 1


# ---------------------------------------------------------------------------
# Codex review r1 — attrs_only mode for envelope attribute ownership
# ---------------------------------------------------------------------------


_ATTRS_ONLY_CURRENT = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="" returnApplicationErrors="true" trackResponse="false"
               futureBoomiAttr="opaque">
      <Configuration><GenericOperationConfig/></Configuration>
      <Tracking><TrackedFields><TrackedField name="userAdded" path="//x"/></TrackedFields></Tracking>
    </Operation>
  </bns:object>
</bns:Component>
"""


_ATTRS_ONLY_DESIRED = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="" returnApplicationErrors="false" trackResponse="true">
      <Configuration><GenericOperationConfig/></Configuration>
      <Tracking><TrackedFields/></Tracking>
    </Operation>
  </bns:object>
</bns:Component>
"""


_ATTRS_ONLY_POLICY = PreservationPolicy(
    component_type="connector-action",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/Operation",
            mode="attrs_only",
            owned_attrs=("returnApplicationErrors", "trackResponse"),
        ),
    ),
)


def test_attrs_only_mode_overwrites_owned_attrs():
    merged = merge_for_update(
        _ATTRS_ONLY_CURRENT, _ATTRS_ONLY_DESIRED, _ATTRS_ONLY_POLICY
    )
    root = _parse(merged)
    op = root.find("bns:object/Operation", NS)
    assert op.attrib["returnApplicationErrors"] == "false"
    assert op.attrib["trackResponse"] == "true"


def test_attrs_only_mode_preserves_unknown_attrs():
    merged = merge_for_update(
        _ATTRS_ONLY_CURRENT, _ATTRS_ONLY_DESIRED, _ATTRS_ONLY_POLICY
    )
    root = _parse(merged)
    op = root.find("bns:object/Operation", NS)
    assert op.attrib.get("futureBoomiAttr") == "opaque"


def test_attrs_only_mode_preserves_children():
    merged = merge_for_update(
        _ATTRS_ONLY_CURRENT, _ATTRS_ONLY_DESIRED, _ATTRS_ONLY_POLICY
    )
    root = _parse(merged)
    tracked = root.find(
        "bns:object/Operation/Tracking/TrackedFields/TrackedField", NS
    )
    assert tracked is not None
    assert tracked.attrib["name"] == "userAdded"


# ---------------------------------------------------------------------------
# Codex review r2 — owned_attrs removal in key_merge
# ---------------------------------------------------------------------------


_OWNED_ATTRS_KEY_MERGE_CURRENT = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="JSON"
                                requestProfile="old-profile-id"
                                responseProfileType="NONE"
                                futureBoomiAttr="opaque">
          <field id="path" type="string" value="/v1/old"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""

_OWNED_ATTRS_KEY_MERGE_DESIRED = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="NONE"
                                responseProfileType="NONE">
          <field id="path" type="string" value="/v1/new"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""

_OWNED_ATTRS_KEY_MERGE_POLICY = PreservationPolicy(
    component_type="connector-action",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/Operation/Configuration/GenericOperationConfig",
            mode="key_merge",
            key_attr="id",
            owned_attrs=(
                "customOperationType",
                "operationType",
                "requestProfile",
                "requestProfileType",
                "responseProfile",
                "responseProfileType",
            ),
        ),
    ),
)


def test_key_merge_owned_attrs_removes_stale_attr_when_desired_omits():
    """Codex r2 P2: when owned_attrs is set, an attr listed there but
    absent from desired must be REMOVED from current (builder cleared
    it). E.g., switching request_profile_type to NONE clears the stale
    requestProfile id."""
    merged = merge_for_update(
        _OWNED_ATTRS_KEY_MERGE_CURRENT,
        _OWNED_ATTRS_KEY_MERGE_DESIRED,
        _OWNED_ATTRS_KEY_MERGE_POLICY,
    )
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    assert "requestProfile" not in cfg.attrib  # stale removed
    assert cfg.attrib["requestProfileType"] == "NONE"  # owned + desired = updated


def test_key_merge_owned_attrs_preserves_unknown_attrs():
    """Codex r2 P2: attrs NOT in owned_attrs survive regardless of
    desired's content."""
    merged = merge_for_update(
        _OWNED_ATTRS_KEY_MERGE_CURRENT,
        _OWNED_ATTRS_KEY_MERGE_DESIRED,
        _OWNED_ATTRS_KEY_MERGE_POLICY,
    )
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    assert cfg.attrib.get("futureBoomiAttr") == "opaque"


# ---------------------------------------------------------------------------
# Codex review r2 — default policy excludes folder attrs
# ---------------------------------------------------------------------------


def test_default_owned_root_attrs_only_contains_name():
    """Codex r2 P2: PreservationPolicy default owned_root_attrs is just
    ('name',). Folder attrs (folderName, folderId, folderFullPath) are
    explicitly NOT in the default — builders emit folderName='Home' when
    callers omit folder_name, which would silently move components on
    every structured update."""
    policy = PreservationPolicy(component_type="connector-settings")
    assert policy.owned_root_attrs == ("name",)
    assert "folderName" not in policy.owned_root_attrs
    assert "folderId" not in policy.owned_root_attrs
    assert "folderFullPath" not in policy.owned_root_attrs


def test_folder_attrs_preserved_when_builder_emits_default_home():
    """Concrete regression: a structured update where desired emits
    folderName='Home' (builder default when caller omits folder_name)
    must NOT overwrite current's actual folder."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database"
               name="conn" folderName="Production/DB" folderId="prod-folder-id">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="x" host="h" port="1"/>
  </bns:object>
</bns:Component>
"""
    desired_with_home_default = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database"
               name="conn" folderName="Home">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="newdb" host="h" port="1"/>
  </bns:object>
</bns:Component>
"""
    default_policy = PreservationPolicy(
        component_type="connector-settings",
        subtype="database",
        owned_paths=(OwnedPath(path="bns:object/DatabaseConnectionSettings"),),
    )
    merged = merge_for_update(current, desired_with_home_default, default_policy)
    root = _parse(merged)
    assert root.attrib["folderName"] == "Production/DB"
    assert root.attrib["folderId"] == "prod-folder-id"


# ---------------------------------------------------------------------------
# Codex review r6 — preserve_keys keeps live OAuth2 token state
# ---------------------------------------------------------------------------


_OAUTH_PRESERVE_CURRENT = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest">
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://example.com"/>
      <field id="oauthContext" type="oauth">
        <OAuth2Config grantType="client_credentials">
          <credentials accessTokenKey="live-token-key-cached-by-boomi"
                       clientId="client-id"
                       clientSecret=""/>
          <authorizationTokenEndpoint url=""><sslOptions/></authorizationTokenEndpoint>
          <authorizationParameters/>
          <accessTokenEndpoint url="https://example.com/oauth/token"><sslOptions/></accessTokenEndpoint>
          <accessTokenParameters/>
          <scope/>
          <jwtParameters><expiration>0</expiration></jwtParameters>
          <accessToken>LIVE_ACCESS_TOKEN_CIPHERTEXT</accessToken>
        </OAuth2Config>
      </field>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""


_OAUTH_PRESERVE_DESIRED = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest">
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://NEW.example.com"/>
      <field id="oauthContext" type="oauth">
        <OAuth2Config grantType="client_credentials">
          <credentials accessTokenKey="" clientId="client-id" clientSecret=""/>
          <authorizationTokenEndpoint url=""><sslOptions/></authorizationTokenEndpoint>
          <authorizationParameters/>
          <accessTokenEndpoint url="https://NEW.example.com/oauth/token"><sslOptions/></accessTokenEndpoint>
          <accessTokenParameters/>
          <scope/>
          <jwtParameters><expiration>0</expiration></jwtParameters>
        </OAuth2Config>
      </field>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""


_OAUTH_PRESERVE_POLICY = PreservationPolicy(
    component_type="connector-settings",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/GenericConnectionConfig",
            mode="key_merge",
            key_attr="id",
            owned_keys=("url", "oauthContext"),
            preserve_keys=("oauthContext",),
        ),
    ),
)


def test_preserve_keys_keeps_live_oauth_token_cache():
    """Codex r6 P1: REST connection updates must NOT reset the live
    OAuth2 token cache. Builder emits oauthContext as a token-not-set
    skeleton; preserve_keys=('oauthContext',) makes current win even
    when desired emits a same-keyed entry."""
    merged = merge_for_update(
        _OAUTH_PRESERVE_CURRENT, _OAUTH_PRESERVE_DESIRED, _OAUTH_PRESERVE_POLICY
    )
    # Non-preserved url got updated.
    assert 'value="https://NEW.example.com"' in merged
    # Live token cache survived (accessTokenKey + accessToken element).
    assert 'accessTokenKey="live-token-key-cached-by-boomi"' in merged
    assert "LIVE_ACCESS_TOKEN_CIPHERTEXT" in merged
    # Builder's empty-skeleton accessTokenKey did NOT win.
    assert 'accessTokenKey=""' not in merged


def test_preserve_keys_adds_desired_entry_when_current_lacks_it():
    """Initial setup: when current XML doesn't have the preserved key
    yet, desired's value is added (so the OAuth handshake bootstrap
    still works on first apply)."""
    current_no_oauth = _OAUTH_PRESERVE_CURRENT.replace(
        '<field id="oauthContext" type="oauth">', '<field id="_marker_" type="x">'
    )
    # Make the section a valid-ish XML by closing markers around the OAuth block.
    current_no_oauth = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest">
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://example.com"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""
    merged = merge_for_update(
        current_no_oauth, _OAUTH_PRESERVE_DESIRED, _OAUTH_PRESERVE_POLICY
    )
    # Desired's oauthContext was appended.
    assert 'id="oauthContext"' in merged
    assert 'grantType="client_credentials"' in merged


def test_rest_connection_policy_does_not_preserve_oauth_context():
    """Codex r7 P1 trade-off: oauthContext is fully owned by the
    builder. Preserving it across structured updates would block
    legitimate auth-mode changes (NONE/BASIC → OAUTH2) by keeping the
    current's empty skeleton over desired's populated config. The
    accepted trade-off is that structured REST connection updates
    require OAuth re-authorization; metadata-only smart-merge (rename/
    description/folder) is the safe path for non-body changes.
    Documented in the REST connector update_note."""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_CONNECTION_POLICY,
    )
    cfg_path = next(
        op
        for op in _REST_CLIENT_CONNECTION_POLICY.owned_paths
        if op.path == "bns:object/GenericConnectionConfig"
    )
    # preserve_keys must NOT contain oauthContext, otherwise desired's
    # OAuth config for an auth-mode change silently fails to apply.
    preserve = cfg_path.preserve_keys or ()
    assert "oauthContext" not in preserve


# ---------------------------------------------------------------------------
# Codex review r7 — owned_encrypted_paths prunes stale secret slots
# ---------------------------------------------------------------------------


def test_owned_encrypted_paths_prune_stale_slot_when_desired_omits():
    """Codex r7 P2: auth-mode change from BASIC to NONE → builder
    emits empty <bns:encryptedValues/>. Stale password slot in
    current must be PRUNED from the merged output, not preserved as
    if it were unknown."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest-old">
  <bns:encryptedValues>
    <bns:encryptedValue path="//GenericConnectionConfig/field[@type='password']" isSet="true"/>
  </bns:encryptedValues>
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://example.com"/>
      <field id="auth" type="string" value="BASIC"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest-new">
  <bns:encryptedValues/>
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://example.com"/>
      <field id="auth" type="string" value="NONE"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="connector-settings",
        subtype="officialboomi-X3979C-rest-prod",
        owned_paths=(
            OwnedPath(
                path="bns:object/GenericConnectionConfig",
                mode="key_merge",
                key_attr="id",
            ),
        ),
        owned_encrypted_paths=(
            "//GenericConnectionConfig/field[@type='password']",
        ),
    )
    merged = merge_for_update(current, desired, policy)
    # Stale password slot is gone.
    assert "field[@type='password']" not in merged
    # The unknown entries (had there been any) would survive.


def test_owned_encrypted_paths_preserves_isset_true_secret_when_desired_has_same_path():
    """Auth mode unchanged: builder still emits the marker for the
    same path. The current isSet=true value MUST survive — the prune
    only fires when desired omits the path."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest">
  <bns:encryptedValues>
    <bns:encryptedValue path="//GenericConnectionConfig/field[@type='password']" isSet="true"/>
  </bns:encryptedValues>
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://example.com"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="officialboomi-X3979C-rest-prod"
               name="rest">
  <bns:encryptedValues>
    <bns:encryptedValue path="//GenericConnectionConfig/field[@type='password']" isSet="false"/>
  </bns:encryptedValues>
  <bns:object>
    <GenericConnectionConfig xmlns="">
      <field id="url" type="string" value="https://NEW.example.com"/>
    </GenericConnectionConfig>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="connector-settings",
        subtype="officialboomi-X3979C-rest-prod",
        owned_paths=(
            OwnedPath(
                path="bns:object/GenericConnectionConfig",
                mode="key_merge",
                key_attr="id",
            ),
        ),
        owned_encrypted_paths=(
            "//GenericConnectionConfig/field[@type='password']",
        ),
    )
    merged = merge_for_update(current, desired, policy)
    root = _parse(merged)
    entry = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    # The isSet=true entry survived; desired's isSet=false did NOT win.
    assert entry is not None
    assert entry.attrib["isSet"] == "true"


def test_owned_encrypted_paths_unknown_path_always_preserved():
    """Codex r7 P2: paths NOT in owned_encrypted_paths must always
    survive regardless of whether desired emits them."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database"
               name="db">
  <bns:encryptedValues>
    <bns:encryptedValue path="//SomeFutureBoomiPath" isSet="true"/>
  </bns:encryptedValues>
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="db" host="h" port="1"/>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database"
               name="db">
  <bns:encryptedValues/>
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="db" host="newhost" port="1"/>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="connector-settings",
        subtype="database",
        owned_paths=(OwnedPath(path="bns:object/DatabaseConnectionSettings"),),
        owned_encrypted_paths=("//DatabaseConnectionSettings/@password",),
    )
    merged = merge_for_update(current, desired, policy)
    # Unknown path survived even though desired omitted it.
    assert "//SomeFutureBoomiPath" in merged


def test_rest_connection_policy_declares_both_password_xpaths_as_owned():
    """The shipped REST policy must enumerate both encryptedValue
    paths the builder can emit (OAUTH2 clientSecret, password-mode
    password field) so auth-mode changes prune the stale slot."""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_CONNECTION_POLICY,
    )
    owned = set(_REST_CLIENT_CONNECTION_POLICY.owned_encrypted_paths)
    assert (
        "//GenericConnectionConfig/field/OAuth2Config/credentials/@clientSecret"
        in owned
    )
    assert "//GenericConnectionConfig/field[@type='password']" in owned


# ---------------------------------------------------------------------------
# Codex review r8 — preserve_when_desired_empty + subtype_marker
# ---------------------------------------------------------------------------


def test_preserve_when_desired_empty_keeps_live_custom_properties():
    """Codex r8 P2: REST op path-only update emits empty customProperties
    placeholders; live UI-added custom properties must survive."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/old"/>
          <field id="queryParameters" type="customproperties">
            <customProperties>
              <properties key="Authorization-Header" value="Bearer LIVE-TOKEN"/>
              <properties key="api-version" value="2"/>
            </customProperties>
          </field>
          <field id="requestHeaders" type="customproperties">
            <customProperties>
              <properties key="X-Custom" value="live-header-value"/>
            </customProperties>
          </field>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/new"/>
          <field id="queryParameters" type="customproperties"><customProperties/></field>
          <field id="requestHeaders" type="customproperties"><customProperties/></field>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="connector-action",
        subtype="officialboomi-X3979C-rest-prod",
        owned_paths=(
            OwnedPath(
                path="bns:object/Operation/Configuration/GenericOperationConfig",
                mode="key_merge",
                key_attr="id",
                owned_keys=("path", "queryParameters", "requestHeaders"),
                preserve_when_desired_empty=("queryParameters", "requestHeaders"),
            ),
        ),
    )
    merged = merge_for_update(current, desired, policy)
    # path was updated.
    assert 'value="/v1/new"' in merged
    # Live UI-added custom properties survived.
    assert 'key="Authorization-Header"' in merged
    assert 'value="Bearer LIVE-TOKEN"' in merged
    assert 'key="X-Custom"' in merged


def test_preserve_when_desired_empty_yields_to_populated_desired():
    """When caller DOES supply queryParameters, desired wins — the
    preserve_when_desired_empty check only triggers when desired is
    actually empty."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="queryParameters" type="customproperties">
            <customProperties>
              <properties key="old_param" value="old"/>
            </customProperties>
          </field>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="queryParameters" type="customproperties">
            <customProperties>
              <properties key="new_param" value="new"/>
            </customProperties>
          </field>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="connector-action",
        subtype="officialboomi-X3979C-rest-prod",
        owned_paths=(
            OwnedPath(
                path="bns:object/Operation/Configuration/GenericOperationConfig",
                mode="key_merge",
                key_attr="id",
                owned_keys=("queryParameters",),
                preserve_when_desired_empty=("queryParameters",),
            ),
        ),
    )
    merged = merge_for_update(current, desired, policy)
    # New value won; old value cleared (desired populated → desired wins).
    assert 'key="new_param"' in merged
    assert 'key="old_param"' not in merged


def test_subtype_marker_mismatch_raises_type_mismatch():
    """Codex r8 P2 narrow-risk guard: DB read builder rejects a write
    profile by checking the executionType marker, even though root
    type='profile.db' matches."""
    current_write = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="profile.db" name="write">
  <bns:object>
    <DatabaseProfile xmlns="" strict="true" version="2">
      <ProfileProperties>
        <DatabaseGeneralInfo executionType="dbwrite"/>
      </ProfileProperties>
      <DataElements/>
    </DatabaseProfile>
  </bns:object>
</bns:Component>
"""
    desired_read = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="profile.db" name="read">
  <bns:object>
    <DatabaseProfile xmlns="" strict="true" version="2">
      <ProfileProperties>
        <DatabaseGeneralInfo executionType="dbread"/>
      </ProfileProperties>
      <DataElements/>
    </DatabaseProfile>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="profile.db",
        owned_paths=(OwnedPath(path="bns:object/DatabaseProfile/DataElements"),),
        subtype_marker_xpath="bns:object/DatabaseProfile/ProfileProperties/DatabaseGeneralInfo",
        subtype_marker_attr="executionType",
        subtype_marker_expected="dbread",
    )
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(current_write, desired_read, policy)
    assert exc.value.error_code == "UPDATE_PRESERVATION_TYPE_MISMATCH"
    assert exc.value.field == "subtype_marker"
    assert exc.value.details["actual"] == "dbwrite"
    assert exc.value.details["expected"] == "dbread"


def test_subtype_marker_match_allows_merge():
    """When the marker matches, the merge proceeds normally."""
    current_read = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="profile.db" name="read">
  <bns:object>
    <DatabaseProfile xmlns="" strict="true" version="2">
      <ProfileProperties>
        <DatabaseGeneralInfo executionType="dbread"/>
      </ProfileProperties>
      <DataElements old="yes"/>
    </DatabaseProfile>
  </bns:object>
</bns:Component>
"""
    desired_read = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="profile.db" name="read">
  <bns:object>
    <DatabaseProfile xmlns="" strict="true" version="2">
      <ProfileProperties>
        <DatabaseGeneralInfo executionType="dbread"/>
      </ProfileProperties>
      <DataElements new="yes"/>
    </DatabaseProfile>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="profile.db",
        owned_paths=(OwnedPath(path="bns:object/DatabaseProfile/DataElements"),),
        subtype_marker_xpath="bns:object/DatabaseProfile/ProfileProperties/DatabaseGeneralInfo",
        subtype_marker_attr="executionType",
        subtype_marker_expected="dbread",
    )
    merged = merge_for_update(current_read, desired_read, policy)
    # Owned subtree replaced.
    assert 'new="yes"' in merged
    assert 'old="yes"' not in merged


def test_db_read_profile_policy_has_subtype_marker():
    """The shipped DB read profile policy must declare the
    executionType marker so a write profile slipping through plan-time
    validation fails the merge cleanly."""
    from boomi_mcp.categories.components.builders.profile_builder import (
        _DATABASE_READ_PROFILE_POLICY,
    )
    assert (
        _DATABASE_READ_PROFILE_POLICY.subtype_marker_xpath
        == "bns:object/DatabaseProfile/ProfileProperties/DatabaseGeneralInfo"
    )
    assert _DATABASE_READ_PROFILE_POLICY.subtype_marker_attr == "executionType"
    assert _DATABASE_READ_PROFILE_POLICY.subtype_marker_expected == "dbread"


def test_rest_operation_policy_preserves_custom_properties_when_desired_empty():
    """REST operation policy must list queryParameters and requestHeaders
    in preserve_when_desired_empty so path-only updates don't wipe live
    UI-added custom props."""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    cfg = next(
        op
        for op in _REST_CLIENT_OPERATION_POLICY.owned_paths
        if op.mode == "key_merge"
    )
    preserve_empty = set(cfg.preserve_when_desired_empty or ())
    assert "queryParameters" in preserve_empty
    assert "requestHeaders" in preserve_empty


# ---------------------------------------------------------------------------
# Codex review r11 — newly-added keyed children insert before unkeyed
# ---------------------------------------------------------------------------


def test_newly_added_keyed_child_inserts_before_unkeyed():
    """Codex r11 P2: when key_merge adds a keyed child not present in
    current, it must insert BEFORE unkeyed children (e.g. <Options/>)
    to preserve canonical builder/live order."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="PATCH" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="path" type="string" value="/v1/items"/>
          <Options/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    # GET method adds followRedirects which current lacks.
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="GET" operationType="EXECUTE"
                                requestProfileType="NONE" responseProfileType="NONE">
          <field id="followRedirects" type="string" value="NONE"/>
          <field id="path" type="string" value="/v1/items"/>
          <Options/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    policy = PreservationPolicy(
        component_type="connector-action",
        subtype="officialboomi-X3979C-rest-prod",
        owned_paths=(
            OwnedPath(
                path="bns:object/Operation/Configuration/GenericOperationConfig",
                mode="key_merge",
                key_attr="id",
                owned_keys=("path", "followRedirects"),
            ),
        ),
    )
    merged = merge_for_update(current, desired, policy)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    # Collect child tags in order — keyed fields must precede <Options/>.
    child_tags = [(ch.tag, ch.attrib.get("id")) for ch in list(cfg)]
    options_idx = next(i for i, (tag, _) in enumerate(child_tags) if tag == "Options")
    followredirects_idx = next(
        i for i, (tag, key) in enumerate(child_tags)
        if tag == "field" and key == "followRedirects"
    )
    assert followredirects_idx < options_idx


# ---------------------------------------------------------------------------
# Codex review r16 — REST op path-only update preserves profile bindings
# ---------------------------------------------------------------------------


def test_rest_op_path_only_update_preserves_live_profile_bindings():
    """Scope B (#50) — path-only case: a path-only / method-only
    structured update on a REST operation with existing profile
    bindings (requestProfile, responseProfile, *ProfileType set in
    live XML) must NOT clobber them. Under #50 conditional emission the
    builder emits NO requestProfile/responseProfile AND NO
    requestProfileType/responseProfileType when the caller omits those
    fields, so the policy's additive merge preserves all four live
    attrs."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="JSON"
                                requestProfile="live-request-profile-id"
                                responseProfileType="JSON"
                                responseProfile="live-response-profile-id">
          <field id="path" type="string" value="/v1/old"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    # Desired: caller only changes path. Under #50 conditional emission
    # the builder emits NEITHER the optional requestProfile/responseProfile
    # ids NOR the requestProfileType/responseProfileType attrs (caller
    # supplied none of them), so desired carries none of the four.
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE">
          <field id="path" type="string" value="/v1/new"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    merged = merge_for_update(current, desired, _REST_CLIENT_OPERATION_POLICY)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    # Method updated.
    assert cfg.attrib["customOperationType"] == "POST"
    # Path field updated.
    fields = {
        f.attrib["id"]: f.attrib.get("value")
        for f in cfg.findall("field")
    }
    assert fields.get("path") == "/v1/new"
    # Live profile UUIDs preserved (additive merge: desired omitted them).
    assert cfg.attrib.get("requestProfile") == "live-request-profile-id"
    assert cfg.attrib.get("responseProfile") == "live-response-profile-id"
    # Live profile TYPES preserved too — #50 made the type attrs
    # conditionally emitted, so a path-only update omits them and the
    # additive merge keeps the live JSON type (previously the builder's
    # default "xml" clobbered this; the coupled_attr_groups hack that
    # worked around it is now retired).
    assert cfg.attrib.get("requestProfileType") == "JSON"
    assert cfg.attrib.get("responseProfileType") == "JSON"


# ---------------------------------------------------------------------------
# Codex review r17 — owned_attrs_additive (conditional builder emission)
# ---------------------------------------------------------------------------


def test_owned_attrs_additive_overwrites_when_desired_supplies():
    """Codex r17 P2: when the builder emits a conditionally-owned attr
    (because the caller explicitly supplied a value), the merge MUST
    overwrite current's value with desired's."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfile="old-request-profile-id">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfile="new-request-profile-id">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    merged = merge_for_update(current, desired, _REST_CLIENT_OPERATION_POLICY)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    # User's new requestProfile binding overwrote the old value.
    assert cfg.attrib["requestProfile"] == "new-request-profile-id"


def test_owned_attrs_additive_preserves_when_desired_omits():
    """Codex r17 P2: when desired XML omits a conditionally-owned attr
    (path-only update — caller didn't supply request_profile_id), the
    merge MUST preserve current's value rather than clearing it."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfile="live-binding">
          <field id="path" type="string" value="/v1/old"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE">
          <field id="path" type="string" value="/v1/new"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    merged = merge_for_update(current, desired, _REST_CLIENT_OPERATION_POLICY)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    # path updated; live requestProfile preserved (desired omitted it).
    assert cfg.attrib.get("requestProfile") == "live-binding"
    fields = {f.attrib["id"]: f.attrib.get("value") for f in cfg.findall("field")}
    assert fields["path"] == "/v1/new"


def test_rest_op_explicit_profile_binding_lands_with_correct_type():
    """Codex r18 P2 fix: when caller supplies request_profile_id +
    request_profile_type, both the UUID and the type must propagate
    to merged XML (not just the UUID with stale type)."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="NONE"
                                responseProfileType="NONE">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    # Builder output when user supplies request_profile_id + request_profile_type.
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfile="new-uuid"
                                requestProfileType="json"
                                responseProfileType="xml">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    merged = merge_for_update(current, desired, _REST_CLIENT_OPERATION_POLICY)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    # New profile UUID lands.
    assert cfg.attrib["requestProfile"] == "new-uuid"
    # New profile type lands too (no stale "NONE" survives).
    assert cfg.attrib["requestProfileType"] == "json"


def test_rest_op_type_only_update_applies_type_and_preserves_live_id():
    """Scope B (#50) — type-only case: a type-only update
    (request_profile_type supplied WITHOUT request_profile_id) now
    APPLIES the new type while preserving the live profile id. Before
    #50 this was a no-op (the live type survived) because the builder
    default-emitted the type and the merge coupled it to the id; now
    the type is conditionally emitted and merges additively."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="JSON"
                                requestProfile="live-request-id">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    # Builder output when the caller supplies ONLY request_profile_type
    # (a type-only update): requestProfileType emitted, requestProfile
    # (the id) omitted.
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="xml">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    merged = merge_for_update(current, desired, _REST_CLIENT_OPERATION_POLICY)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    # The new type is applied...
    assert cfg.attrib["requestProfileType"] == "xml"
    # ...and the live profile id is preserved (desired omitted it).
    assert cfg.attrib["requestProfile"] == "live-request-id"


def test_rest_op_binding_update_applies_id_and_type_together():
    """Scope B (#50) — binding case: supplying request_profile_id +
    request_profile_type applies BOTH the id and the type to the merged
    XML, overwriting the live binding."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="xml"
                                requestProfile="old-id">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod"
               name="op">
  <bns:object>
    <Operation xmlns="">
      <Configuration>
        <GenericOperationConfig customOperationType="POST"
                                operationType="EXECUTE"
                                requestProfileType="json"
                                requestProfile="new-id">
          <field id="path" type="string" value="/v1/items"/>
        </GenericOperationConfig>
      </Configuration>
    </Operation>
  </bns:object>
</bns:Component>
"""
    from boomi_mcp.categories.components.builders.connector_builder import (
        _REST_CLIENT_OPERATION_POLICY,
    )
    merged = merge_for_update(current, desired, _REST_CLIENT_OPERATION_POLICY)
    root = _parse(merged)
    cfg = root.find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    assert cfg.attrib["requestProfile"] == "new-id"
    assert cfg.attrib["requestProfileType"] == "json"


# ---------------------------------------------------------------------------
# Review follow-up — subtree_merge mode (granular connector-body merge)
# ---------------------------------------------------------------------------


_SUBTREE_MERGE_POLICY = PreservationPolicy(
    component_type="connector-settings",
    subtype="database",
    owned_paths=(
        OwnedPath(
            path="bns:object/DatabaseConnectionSettings",
            mode="subtree_merge",
            owned_attrs=("dbname", "host", "port"),
            owned_child_tags=("WriteOptions", "AdapterPoolInfo"),
        ),
    ),
)


def _db_subtree_current(extra_attr="", extra_child=""):
    return f"""\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database" name="conn">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="olddb" host="old.example.com"
                                port="3306"{extra_attr}>
      <WriteOptions sqlOption="0"/>
      <AdapterPoolInfo maxActive="0"/>{extra_child}
    </DatabaseConnectionSettings>
  </bns:object>
</bns:Component>
"""


_DB_SUBTREE_DESIRED = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database" name="conn">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="newdb" host="new.example.com"
                                port="5432">
      <WriteOptions sqlOption="1"/>
      <AdapterPoolInfo maxActive="-1"/>
    </DatabaseConnectionSettings>
  </bns:object>
</bns:Component>
"""


def test_subtree_merge_updates_owned_attrs():
    merged = merge_for_update(
        _db_subtree_current(), _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY
    )
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    assert s.attrib["dbname"] == "newdb"
    assert s.attrib["host"] == "new.example.com"
    assert s.attrib["port"] == "5432"


def test_subtree_merge_replaces_owned_child_blocks():
    merged = merge_for_update(
        _db_subtree_current(), _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY
    )
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    assert s.find("WriteOptions").attrib["sqlOption"] == "1"
    assert s.find("AdapterPoolInfo").attrib["maxActive"] == "-1"
    # No duplicate child blocks.
    assert len(s.findall("WriteOptions")) == 1
    assert len(s.findall("AdapterPoolInfo")) == 1


def test_subtree_merge_preserves_unknown_attr_on_owned_element():
    current = _db_subtree_current(extra_attr=' futureBoomiAttr="opaque"')
    merged = merge_for_update(current, _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY)
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    # Unknown attr survives; owned attrs still updated.
    assert s.attrib.get("futureBoomiAttr") == "opaque"
    assert s.attrib["dbname"] == "newdb"


def test_subtree_merge_preserves_unknown_child_on_owned_element():
    current = _db_subtree_current(
        extra_child='<FutureSection retained="yes"/>'
    )
    merged = merge_for_update(current, _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY)
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    assert s.find("FutureSection") is not None
    assert s.find("FutureSection").attrib["retained"] == "yes"
    # Owned child still replaced.
    assert s.find("WriteOptions").attrib["sqlOption"] == "1"


def test_subtree_merge_adds_owned_child_when_current_lacks_it():
    """Robustness: if a live component (e.g. raw-XML-created) lacks an
    owned child block, subtree_merge adds desired's rather than raising
    OBJECT_MISSING."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database" name="conn">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="olddb" host="h" port="1">
      <WriteOptions sqlOption="0"/>
    </DatabaseConnectionSettings>
  </bns:object>
</bns:Component>
"""
    merged = merge_for_update(current, _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY)
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    # AdapterPoolInfo was absent in current — added from desired.
    assert s.find("AdapterPoolInfo") is not None
    assert s.find("AdapterPoolInfo").attrib["maxActive"] == "-1"


def test_subtree_merge_adds_missing_leading_owned_child_in_canonical_order():
    """When current lacks an owned block that precedes a present one in
    desired order, the added block lands in desired (canonical) order
    rather than appended after the present block."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database" name="conn">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="olddb" host="h" port="1">
      <AdapterPoolInfo maxActive="0"/>
    </DatabaseConnectionSettings>
  </bns:object>
</bns:Component>
"""
    merged = merge_for_update(current, _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY)
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    tags = [c.tag for c in list(s)]
    assert tags == ["WriteOptions", "AdapterPoolInfo"]
    assert s.find("WriteOptions").attrib["sqlOption"] == "1"
    assert s.find("AdapterPoolInfo").attrib["maxActive"] == "-1"


def test_subtree_merge_reorders_owned_children_to_desired_order():
    """Current carrying owned blocks in a non-canonical order is normalized
    to desired document order."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-settings" subType="database" name="conn">
  <bns:object>
    <DatabaseConnectionSettings xmlns="" dbname="olddb" host="h" port="1">
      <AdapterPoolInfo maxActive="0"/>
      <WriteOptions sqlOption="0"/>
    </DatabaseConnectionSettings>
  </bns:object>
</bns:Component>
"""
    merged = merge_for_update(current, _DB_SUBTREE_DESIRED, _SUBTREE_MERGE_POLICY)
    s = _parse(merged).find("bns:object/DatabaseConnectionSettings", NS)
    tags = [c.tag for c in list(s)]
    assert tags == ["WriteOptions", "AdapterPoolInfo"]


def test_subtree_merge_requires_owned_attrs_or_child_tags():
    bad_policy = PreservationPolicy(
        component_type="connector-settings",
        subtype="database",
        owned_paths=(
            OwnedPath(path="bns:object/DatabaseConnectionSettings", mode="subtree_merge"),
        ),
    )
    with pytest.raises(BuilderValidationError) as exc:
        merge_for_update(_db_subtree_current(), _DB_SUBTREE_DESIRED, bad_policy)
    assert exc.value.error_code == "UPDATE_PRESERVATION_MERGE_FAILED"


# ---------------------------------------------------------------------------
# Review follow-up — coupled_attr_groups (profile type follows binding)
# ---------------------------------------------------------------------------


_COUPLED_POLICY = PreservationPolicy(
    component_type="connector-action",
    subtype="officialboomi-X3979C-rest-prod",
    owned_paths=(
        OwnedPath(
            path="bns:object/Operation/Configuration/GenericOperationConfig",
            mode="key_merge",
            key_attr="id",
            owned_attrs=("customOperationType", "operationType"),
            owned_attrs_additive=("requestProfile", "responseProfile"),
            coupled_attr_groups=(
                ("requestProfile", ("requestProfileType",)),
                ("responseProfile", ("responseProfileType",)),
            ),
        ),
    ),
)


def test_coupled_attr_preserved_when_trigger_absent():
    """Path-only update: desired has no requestProfile, so the coupled
    requestProfileType is NOT applied — live JSON type preserved."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod" name="op">
  <bns:object><Operation xmlns=""><Configuration>
    <GenericOperationConfig customOperationType="POST" operationType="EXECUTE"
                            requestProfile="live-id" requestProfileType="JSON">
      <field id="path" type="string" value="/old"/>
    </GenericOperationConfig>
  </Configuration></Operation></bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod" name="op">
  <bns:object><Operation xmlns=""><Configuration>
    <GenericOperationConfig customOperationType="POST" operationType="EXECUTE"
                            requestProfileType="xml">
      <field id="path" type="string" value="/new"/>
    </GenericOperationConfig>
  </Configuration></Operation></bns:object>
</bns:Component>
"""
    merged = merge_for_update(current, desired, _COUPLED_POLICY)
    cfg = _parse(merged).find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    assert cfg.attrib["requestProfile"] == "live-id"
    assert cfg.attrib["requestProfileType"] == "JSON"  # coupled, preserved


def test_coupled_attr_applied_when_trigger_present():
    """Binding update: desired supplies requestProfile, so the coupled
    requestProfileType is applied."""
    current = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod" name="op">
  <bns:object><Operation xmlns=""><Configuration>
    <GenericOperationConfig customOperationType="POST" operationType="EXECUTE"
                            requestProfileType="NONE">
      <field id="path" type="string" value="/x"/>
    </GenericOperationConfig>
  </Configuration></Operation></bns:object>
</bns:Component>
"""
    desired = """\
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               type="connector-action" subType="officialboomi-X3979C-rest-prod" name="op">
  <bns:object><Operation xmlns=""><Configuration>
    <GenericOperationConfig customOperationType="POST" operationType="EXECUTE"
                            requestProfile="new-id" requestProfileType="JSON">
      <field id="path" type="string" value="/x"/>
    </GenericOperationConfig>
  </Configuration></Operation></bns:object>
</bns:Component>
"""
    merged = merge_for_update(current, desired, _COUPLED_POLICY)
    cfg = _parse(merged).find(
        "bns:object/Operation/Configuration/GenericOperationConfig", NS
    )
    assert cfg.attrib["requestProfile"] == "new-id"
    assert cfg.attrib["requestProfileType"] == "JSON"  # coupled, applied
