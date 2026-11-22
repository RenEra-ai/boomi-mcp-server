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


# ---------------------------------------------------------------------------
# #159 S3: the canonical process update arm (patch.authoring_request)
# ---------------------------------------------------------------------------
#
# FIXTURE PROVENANCE (every served byte below is a platform capture; nothing is
# derived from the code under test):
# - the live PROCESS being edited: `cap155-e3b-patch-canonical/component_process.xml`,
#   the stored process of an executed-green run (its PROVENANCE.md), with three
#   test-local additions that make the preservation assertions non-vacuous — a
#   `processOverrides` child, an encrypted value and an unknown `bns:Component`
#   sibling (the same device the #97 tests above use, `UnknownSibling`);
# - the REST GET / PATCH OPERATIONS: `cap155-e3b-patch-canonical/component_op_src.xml`
#   and `component_op_patch.xml`, stored readbacks;
# - the REST CONNECTIONS: `replay_identity/cap159-s1-reuse-identity/rest-conn-aa5c5042.xml`;
# - the MAP and PROFILES (mapped witness only): `process_ir/issue184/captures/
#   cap184-fixtures/components/{map,profile}.stored.xml`.
# Only the `componentId` attribute (and the map's from/to profile ids) is substituted,
# with the frozen oracle ids the #184 goldens carry (`issue155_symbols()` /
# `issue184_symbols()` in `_wave_gate_golden_corpus.py`), so emitted bytes compare
# against the frozen goldens. Every declared operation path equals the stored one.

import copy as _copy
import json as _json
import re as _re
from pathlib import Path as _Path

_TESTS = _Path(__file__).resolve().parent
_FIX = _TESTS / "fixtures"
_CAP155 = _FIX / "boomi/connector_replay/captures/cap155-e3b-patch-canonical"
_CAP_CONN = _FIX / "boomi/replay_identity/cap159-s1-reuse-identity/rest-conn-aa5c5042.xml"
_CAP184 = _FIX / "boomi/process_ir/issue184/captures/cap184-fixtures/components"
_IR184 = _FIX / "process_ir/issue184"

CANONICAL_PROCESS_ID = "26d9d546-3813-47b5-bee1-207206c9fcfb"  # the captured process's own id
_GET_PATH = "/admin/cdscm/api/v1/clients/1ccd7554-7f2e-4e05-83ac-194d20b16b2d"
_PATCH_PATH = "/admin/cdscm/api/v1/clients/41172938-b9bd-4495-b411-9851f5ac7b00"
_CONN_URL = "http://host.docker.internal:8081"
_CAPTURED_MAP_PROFILE = "61449393-c75f-4ec0-a035-3f841ccc4f18"


def _captured(path, component_id):
    text = _Path(path).read_text(encoding="utf-8")
    text, count = _re.subn(r'componentId="[^"]*"', f'componentId="{component_id}"', text, count=1)
    assert count == 1, path
    return text


def _live_process_xml():
    text = _captured(_CAP155 / "component_process.xml", CANONICAL_PROCESS_ID)
    text = text.replace(
        "<bns:encryptedValues/>",
        '<bns:encryptedValues><bns:encryptedValue path="//process/@secret" isSet="true"/>'
        "</bns:encryptedValues>",
        1,
    )
    text = text.replace(
        "<bns:processOverrides/>",
        '<bns:processOverrides><Overrides xmlns=""><Connection id="s3a-override"/></Overrides>'
        '</bns:processOverrides><bns:FutureSibling marker="must-survive"/>',
        1,
    )
    return text


def canonical_account_store(mapped=False):
    """``{component id: stored XML}`` for the canonical-arm fake account."""
    store = {
        CANONICAL_PROCESS_ID: _live_process_xml(),
        "RCONN": _captured(_CAP_CONN, "RCONN"),
        "CONN-UUID": _captured(_CAP_CONN, "CONN-UUID"),
        "ROP": _captured(_CAP155 / "component_op_src.xml", "ROP"),
        "OP-UUID": _captured(_CAP155 / "component_op_patch.xml", "OP-UUID"),
    }
    if mapped:
        store["MAP-UUID"] = (
            _captured(_CAP184 / "map.stored.xml", "MAP-UUID")
            .replace(f'fromProfile="{_CAPTURED_MAP_PROFILE}"', 'fromProfile="PROFILE-READ"')
            .replace(f'toProfile="{_CAPTURED_MAP_PROFILE}"', 'toProfile="PROFILE-REQUEST"')
        )
        store["PROFILE-READ"] = _captured(_CAP184 / "profile.stored.xml", "PROFILE-READ")
        store["PROFILE-REQUEST"] = _captured(_CAP184 / "profile.stored.xml", "PROFILE-REQUEST")
    return store


def _versioned(xml_text, version):
    return _re.sub(r'version="\d+"', f'version="{version}"', xml_text, count=1)


class CanonicalComponentApi:
    """The account's component transport — the ONLY thing faked on this arm."""

    def __init__(self, store):
        self.store = dict(store)
        self.versions = {key: 1 for key in self.store}
        self.reads = []
        self.pushed = []
        self.on_read = None

    def get_component(self, component_id):
        self.reads.append(component_id)
        if self.on_read is not None:
            self.on_read(self, component_id)
        if component_id not in self.store:
            raise Exception(f"404 component {component_id} not found")
        return _versioned(self.store[component_id], self.versions[component_id]).encode("utf-8")

    def update_component_raw(self, component_id, xml_text):
        self.pushed.append((component_id, xml_text))
        self.store[component_id] = xml_text
        self.versions[component_id] += 1
        return b"<updated/>"


class CanonicalAccount:
    account_id = "acct-s3a"

    def __init__(self, store):
        self.component = CanonicalComponentApi(store)


def _connection_row(key, component_id):
    return {"key": key, "type": "connector-settings", "name": key, "action": "create",
            "component_id": component_id,
            "config": {"connector_type": "rest", "component_name": key,
                       "base_url": _CONN_URL, "auth": "NONE"}}


def _operation_row(key, component_id, connection_key, method, path):
    return {"key": key, "type": "connector-action", "name": key, "action": "create",
            "component_id": component_id, "depends_on": [connection_key],
            "config": {"connector_type": "rest", "operation_mode": "execute",
                       "component_name": key, "connection_ref_key": connection_key,
                       "method": method, "path": path}}


def _bound_row(key, component_type, component_id):
    return {"key": key, "type": component_type, "name": key, "action": "create",
            "config": {"reference_only": True, "component_id": component_id}}


def canonical_patch(mapped=False, *, name="S3a canonical process", **envelope_overrides):
    """``patch`` carrying the frozen #184 IR fixture, every reference bound to its oracle id.

    ``mapped=False`` is golden-000080's source-only spelling
    (`issue184/source_dynamic_path_dpp.json`); ``mapped=True`` is golden-000081's mapped
    both-sides spelling (`issue184/both_sides_dynamic_path.json`).
    """
    fixture = "both_sides_dynamic_path.json" if mapped else "source_dynamic_path_dpp.json"
    rows = [
        _connection_row("RCONN", "RCONN"),
        _connection_row("CONN", "CONN-UUID"),
        _operation_row("ROP", "ROP", "RCONN", "GET", _GET_PATH),
        _operation_row("OP", "OP-UUID", "CONN", "PATCH", _PATCH_PATH),
    ]
    if mapped:
        rows += [
            _bound_row("MAP", "transform.map", "MAP-UUID"),
            _bound_row("PREAD", "profile.json", "PROFILE-READ"),
            _bound_row("PREQ", "profile.json", "PROFILE-REQUEST"),
        ]
    envelope = {"component_key": "proc", "name": name, "action": "update",
                "component_id": CANONICAL_PROCESS_ID,
                "depends_on": sorted(row["key"] for row in rows)}
    envelope.update(envelope_overrides)
    return {"authoring_request": {"intent": {
        "intent_kind": "process_ir", "integration_name": "S3a safe edit",
        "units": [{"envelope": envelope,
                   "process_ir": _json.loads((_IR184 / fixture).read_text(encoding="utf-8"))}],
        "components": rows,
    }}}


def shapes_without_layout(xml_text):
    """The `<shapes>` tree with only the layout coordinates removed."""
    root = ET.fromstring(xml_text)
    shapes = next(root.iter("shapes"))
    for element in shapes.iter():
        element.attrib.pop("x", None)
        element.attrib.pop("y", None)
    return ET.tostring(shapes, encoding="unicode")


def assert_live_process_preserved(pushed_xml):
    root = ET.fromstring(pushed_xml)
    assert root.get("folderId") == "Rjo4NjY5Njc2"
    assert root.get("folderName") == "Retired-Account-A"
    assert root.find("bns:processOverrides/Overrides/Connection", NS).get("id") == "s3a-override"
    assert root.find("bns:FutureSibling", NS).get("marker") == "must-survive"
    assert root.find("bns:encryptedValues/bns:encryptedValue", NS).get("path") == "//process/@secret"


def _canonical_prepare(client, patch):
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch)
    assert res["_success"] is True, res
    return res


def test_canonical_update_previews_read_only_then_writes_the_golden_shapes_once(monkeypatch):
    _spy_compare(monkeypatch)
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    prep = _canonical_prepare(client, patch)
    assert prep["update_mode"] == "canonical_process_update"
    assert client.component.pushed == []

    res = apply_component_edit_action(
        client, "prof", CANONICAL_PROCESS_ID, patch, prep["confirmation_token"], confirm_apply=True,
    )
    assert res["_success"] is True, res
    assert [cid for cid, _ in client.component.pushed] == [CANONICAL_PROCESS_ID]
    pushed = client.component.pushed[0][1]
    golden = (_FIX / "golden_xml/issue184_source_dynamic_path_dpp.xml").read_text(encoding="utf-8")
    assert shapes_without_layout(pushed) == shapes_without_layout(golden)
    assert ET.fromstring(pushed).get("name") == "S3a canonical process"
    assert_live_process_preserved(pushed)
    # Every supporting id was read from the account, not only the connectors.
    assert {"RCONN", "CONN-UUID", "ROP", "OP-UUID"} <= set(client.component.reads)


def test_canonical_update_refuses_apply_when_a_dependency_changed_since_prepare(monkeypatch):
    """Same patch, same previewed bytes — only the live GET operation moved (a new
    version) between prepare and apply. The proposed-bytes check cannot see it (the
    emitted ids are unchanged), so only the dependency binding the token carries can."""
    _spy_compare(monkeypatch)
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    client.component.versions["ROP"] += 1

    res = apply_component_edit_action(
        client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True,
    )
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_DRIFT_DETECTED"
    assert "resolved_dependencies" in res["changed"]
    assert res["boomi_mutation"] is False
    assert client.component.pushed == []


def test_canonical_update_stale_replay_writes_nothing(monkeypatch):
    _spy_compare(monkeypatch)
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    first = apply_component_edit_action(
        client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True,
    )
    assert first["_success"] is True, first
    replay = apply_component_edit_action(
        client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True,
    )
    assert replay["_success"] is False
    assert replay["error_code"] == "COMPONENT_EDIT_DRIFT_DETECTED"
    assert replay["boomi_mutation"] is False
    assert len(client.component.pushed) == 1


def test_canonical_update_rechecks_the_target_after_its_dependency_reads(monkeypatch):
    """The target changes WHILE apply reads a dependency (after apply's first drift
    check passed): the re-read before the write refuses, and nothing is pushed."""
    _spy_compare(monkeypatch)
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    fired = []

    def _concurrent_edit(api, component_id):
        if component_id == "OP-UUID" and not fired:
            fired.append(component_id)
            api.versions[CANONICAL_PROCESS_ID] += 1

    client.component.on_read = _concurrent_edit
    res = apply_component_edit_action(
        client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True,
    )
    assert fired, "the dependency read never happened"
    assert res["_success"] is False
    assert res["error_code"] == "COMPONENT_EDIT_DRIFT_DETECTED"
    assert client.component.pushed == []


def test_canonical_process_metadata_only_patch_keeps_the_live_body(monkeypatch):
    _spy_compare(monkeypatch)
    client = CanonicalAccount(canonical_account_store())
    patch = {"config": {"name": "S3a renamed only"}}
    prep = _canonical_prepare(client, patch)
    assert prep["update_mode"] == "metadata_smart_merge"
    res = apply_component_edit_action(
        client, "prof", CANONICAL_PROCESS_ID, patch, prep["confirmation_token"], confirm_apply=True,
    )
    assert res["_success"] is True, res
    pushed = client.component.pushed[0][1]
    assert ET.fromstring(pushed).get("name") == "S3a renamed only"
    live = _live_process_xml()
    # The body is untouched, coordinates included: a metadata patch never re-arranges.
    assert ET.tostring(next(ET.fromstring(pushed).iter("shapes"))) == ET.tostring(
        next(ET.fromstring(live).iter("shapes"))
    )
    assert_live_process_preserved(pushed)


@pytest.mark.parametrize("variant", [
    "mixed_config", "mixed_map_context", "two_units", "create_action",
    "other_component", "written_supporting_row", "unbound_supporting_row",
])
def test_canonical_update_admission_refuses_before_any_write(variant):
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    intent = patch["authoring_request"]["intent"]
    expected_field = None
    if variant == "mixed_config":
        patch["config"] = {"name": "x"}
        expected_field = "config"
    elif variant == "mixed_map_context":
        patch["map_context"] = {"source_index": {}}
        expected_field = "map_context"
    elif variant == "two_units":
        second = _copy.deepcopy(intent["units"][0])
        second["envelope"].update(component_key="proc2", component_id="other-process")
        intent["units"].append(second)
    elif variant == "create_action":
        intent["units"][0]["envelope"]["action"] = "create"
    elif variant == "other_component":
        intent["units"][0]["envelope"]["component_id"] = "some-other-process"
    elif variant == "written_supporting_row":
        intent["components"][2]["action"] = "update"
    elif variant == "unbound_supporting_row":
        del intent["components"][2]["component_id"]
    # An apply token that passes every check BEFORE admission (component, policy,
    # layout, patch digest, live version and XML), so apply's refusal is admission's.
    live = client.component.get_component(CANONICAL_PROCESS_ID).decode("utf-8")
    token = safe_edit_component._make_confirmation_token(
        CANONICAL_PROCESS_ID, 1, safe_edit_component._sha256(live),
        safe_edit_component._sha256(safe_edit_component._canonical_json(patch)),
    )
    for action in ("prepare", "apply"):
        if action == "prepare":
            res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch)
        else:
            res = apply_component_edit_action(
                client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True,
            )
        assert res["_success"] is False, (variant, action, res)
        assert res["boomi_mutation"] is False
        assert res["error_code"] in (
            "PROCESS_COMPONENT_SCHEMA_INVALID", "PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY",
        ), (variant, action, res)
        if expected_field:
            assert res["field"] == expected_field
    assert client.component.pushed == []
