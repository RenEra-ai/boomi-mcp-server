"""Action-level tests for the M9.7 safe existing-component edit workflow (#97).

Exercises prepare_component_edit_action / apply_component_edit_action against a
fake Boomi client: read-only preview, confirmation gate, raw-XML rejection,
encrypted/unknown XML preservation, drift + patch-mismatch aborts, and the
post-write version comparison.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from boomi_mcp.categories.components import safe_edit_component
from boomi_mcp.categories.components.safe_edit_component import (
    prepare_component_edit_action,
    apply_component_edit_action,
)
from boomi_mcp.categories.components.component_update_preservation import (
    OwnedPath,
    PreservationPolicy,
)

NS = {"bns": "http://api.platform.boomi.com/"}
COMPONENT_ID = "abc-123"


# ---------------------------------------------------------------------------
# Fixtures + fake client
# ---------------------------------------------------------------------------

_CURRENT_DB_CONN = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="connector-settings" subType="database" name="conn-old" '
    'folderId="42" futureFlag="opaque-value">'
    '<bns:encryptedValues>'
    '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/>'
    '</bns:encryptedValues>'
    '<bns:description>old-desc</bns:description>'
    '<bns:object>'
    '<DatabaseConnectionSettings xmlns="" dbname="olddb" host="old.example.com"/>'
    '<UnknownSibling xmlns="" id="must-survive"/>'
    '</bns:object>'
    '</bns:Component>'
)

_DESIRED_DB_CONN = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="connector-settings" subType="database" name="conn-renamed">'
    '<bns:encryptedValues/>'
    '<bns:object>'
    '<DatabaseConnectionSettings xmlns="" dbname="newdb" host="db.internal"/>'
    '</bns:object>'
    '</bns:Component>'
)

_DB_POLICY = PreservationPolicy(
    component_type="connector-settings",
    subtype="database",
    owned_root_attrs=("name", "folderId"),
    owned_paths=(OwnedPath(path="bns:object/DatabaseConnectionSettings"),),
)

_CURRENT_MAP = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="transform.map" name="m-old">'
    '<bns:object><Map xmlns=""/></bns:object>'
    '</bns:Component>'
)


def _with_version(xml_text: str, version: int) -> str:
    root = ET.fromstring(xml_text)
    root.set("version", str(version))
    return ET.tostring(root, encoding="unicode")


class _FakeComponentApi:
    def __init__(self, xml_text: str, version: int = 3):
        self._xml = xml_text
        self._version = version
        self.pushed = []

    def get_component(self, component_id):
        return _with_version(self._xml, self._version)

    def update_component_raw(self, component_id, xml_text):
        self.pushed.append((component_id, xml_text))
        self._xml = xml_text
        self._version += 1
        return b"<updated/>"


class _FakeConnectorApi:
    def __init__(self):
        self.calls = []

    def get_connector(self, connector_type):
        self.calls.append(connector_type)
        return {"type": connector_type}


class _FakeClient:
    def __init__(self, xml_text: str = _CURRENT_DB_CONN, version: int = 3):
        self.component = _FakeComponentApi(xml_text, version)
        self.connector = _FakeConnectorApi()


def _spy_compare(monkeypatch):
    calls = []

    def _fake_compare(boomi_client, profile, component_id, config):
        calls.append(config)
        return {"_success": True, "diff": {"changed": True}}

    monkeypatch.setattr(safe_edit_component, "compare_versions", _fake_compare)
    return calls


def _prepare_then_token(client, patch):
    prep = prepare_component_edit_action(client, "prof", COMPONENT_ID, patch)
    assert prep["_success"] is True, prep
    return prep["confirmation_token"]


# ---------------------------------------------------------------------------
# Read-only preview
# ---------------------------------------------------------------------------

def test_prepare_is_read_only_no_mutation():
    client = _FakeClient()
    prep = prepare_component_edit_action(
        client, "prof", COMPONENT_ID, {"config": {"name": "conn-new"}}
    )
    assert prep["_success"] is True
    assert prep["read_only"] is True
    assert prep["boomi_mutation"] is False
    assert prep["confirmation_token"]
    assert prep["diff"]  # a rename produces a diff
    assert client.component.pushed == []  # no write happened


def test_prepare_no_change_flag_when_patch_is_noop():
    client = _FakeClient()
    # Re-set the existing name -> merged XML equals current -> empty diff.
    prep = prepare_component_edit_action(
        client, "prof", COMPONENT_ID, {"config": {"name": "conn-old"}}
    )
    assert prep["_success"] is True
    assert prep["no_change"] is True
    assert prep["diff"] == []
    assert client.component.pushed == []


# ---------------------------------------------------------------------------
# Confirmation gate + raw XML rejection
# ---------------------------------------------------------------------------

def test_apply_requires_confirmation():
    client = _FakeClient()
    token = _prepare_then_token(client, {"config": {"name": "conn-new"}})
    res = apply_component_edit_action(
        client, "prof", COMPONENT_ID,
        {"config": {"name": "conn-new"}}, token, confirm_apply=False,
    )
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_CONFIRMATION_REQUIRED"
    assert res["boomi_mutation"] is False
    assert client.component.pushed == []


@pytest.mark.parametrize("action", [prepare_component_edit_action, apply_component_edit_action])
def test_raw_xml_patch_rejected(action):
    client = _FakeClient()
    patch = {"config": {"xml": "<Component/>"}}
    if action is prepare_component_edit_action:
        res = action(client, "prof", COMPONENT_ID, patch)
    else:
        res = action(client, "prof", COMPONENT_ID, patch, "tok", confirm_apply=True)
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_RAW_XML_UNSUPPORTED"
    assert client.component.pushed == []


def test_type_mismatch_rejected():
    client = _FakeClient()
    prep = prepare_component_edit_action(
        client, "prof", COMPONENT_ID,
        {"component_type": "process", "config": {"name": "x"}},
    )
    assert prep["_success"] is False
    assert prep["error_code"] == "COMPONENT_EDIT_TYPE_MISMATCH"


# ---------------------------------------------------------------------------
# Metadata smart-merge: preserves everything, applies, compares versions
# ---------------------------------------------------------------------------

def test_metadata_apply_preserves_and_compares(monkeypatch):
    calls = _spy_compare(monkeypatch)
    client = _FakeClient()
    patch = {"config": {"name": "conn-renamed", "description": "new-desc"}}
    token = _prepare_then_token(client, patch)

    res = apply_component_edit_action(
        client, "prof", COMPONENT_ID, patch, token, confirm_apply=True
    )
    assert res["_success"] is True
    assert res["boomi_mutation"] is True
    assert res["update_mode"] == "metadata_smart_merge"
    assert res["base_version"] == 3
    assert res["new_version"] == 4

    # version comparison was requested with base + post-write versions.
    assert calls == [{"source_version": 3, "target_version": 4}]
    assert "version_comparison" in res

    # The pushed XML renamed the component but kept the encrypted secret and the
    # unknown sibling.
    pushed_id, pushed_xml = client.component.pushed[-1]
    assert pushed_id == COMPONENT_ID
    root = ET.fromstring(pushed_xml)
    assert root.get("name") == "conn-renamed"
    ev = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    assert ev is not None and ev.get("isSet") == "true"
    assert root.find(".//UnknownSibling") is not None


# ---------------------------------------------------------------------------
# Body patch: structured builder + preservation merge (build is mocked so the
# test focuses on safe-edit's merge + push orchestration, not builder fields).
# ---------------------------------------------------------------------------

def test_body_patch_preserves_encrypted_and_unknown(monkeypatch):
    _spy_compare(monkeypatch)

    def _fake_build(boomi_client, comp, payload, **kwargs):
        return {"_success": True, "built_xml": _DESIRED_DB_CONN, "policy": _DB_POLICY}

    monkeypatch.setattr(safe_edit_component, "build_structured_update_xml", _fake_build)
    client = _FakeClient()
    patch = {"component_type": "connector-settings", "config": {"host": "db.internal"}}

    prep = prepare_component_edit_action(client, "prof", COMPONENT_ID, patch)
    assert prep["_success"] is True
    assert prep["update_mode"] == "read_merge_write"
    assert client.component.pushed == []

    res = apply_component_edit_action(
        client, "prof", COMPONENT_ID, patch, prep["confirmation_token"], confirm_apply=True
    )
    assert res["_success"] is True
    pushed_xml = client.component.pushed[-1][1]
    root = ET.fromstring(pushed_xml)
    # Owned attr replaced from desired; unknown root attr preserved from current.
    assert root.get("name") == "conn-renamed"
    assert root.get("futureFlag") == "opaque-value"
    # Encrypted secret + unknown sibling survive the merge.
    ev = root.find("bns:encryptedValues/bns:encryptedValue", NS)
    assert ev is not None and ev.get("isSet") == "true"
    assert root.find(".//UnknownSibling") is not None


def test_transform_map_body_without_map_context_errors():
    client = _FakeClient(_CURRENT_MAP)
    patch = {"config": {"map_type": "direct", "mappings": [{"from": "a", "to": "b"}]}}
    prep = prepare_component_edit_action(client, "prof", COMPONENT_ID, patch)
    assert prep["_success"] is False
    assert prep["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"
    assert client.component.pushed == []


# ---------------------------------------------------------------------------
# Drift / token integrity aborts
# ---------------------------------------------------------------------------

def test_drift_detected_aborts(monkeypatch):
    _spy_compare(monkeypatch)
    client = _FakeClient()
    patch = {"config": {"name": "conn-renamed"}}
    token = _prepare_then_token(client, patch)

    # Simulate someone else editing the component after preview.
    client.component._version += 1

    res = apply_component_edit_action(
        client, "prof", COMPONENT_ID, patch, token, confirm_apply=True
    )
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_DRIFT_DETECTED"
    assert res["current_version"] == 4
    assert res["expected_version"] == 3
    assert client.component.pushed == []


def test_patch_mismatch_aborts():
    client = _FakeClient()
    token = _prepare_then_token(client, {"config": {"name": "conn-renamed"}})
    # Apply a DIFFERENT patch than the one the token fingerprints.
    res = apply_component_edit_action(
        client, "prof", COMPONENT_ID,
        {"config": {"name": "totally-different"}}, token, confirm_apply=True,
    )
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_PATCH_MISMATCH"
    assert client.component.pushed == []


def test_malformed_token_rejected():
    client = _FakeClient()
    res = apply_component_edit_action(
        client, "prof", COMPONENT_ID,
        {"config": {"name": "conn-renamed"}}, "!!!not-base64!!!", confirm_apply=True,
    )
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_TOKEN_INVALID"
    assert client.component.pushed == []


def test_token_for_other_component_rejected():
    client = _FakeClient()
    token = _prepare_then_token(client, {"config": {"name": "conn-renamed"}})
    res = apply_component_edit_action(
        client, "prof", "different-id",
        {"config": {"name": "conn-renamed"}}, token, confirm_apply=True,
    )
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_TOKEN_INVALID"
