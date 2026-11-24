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


# #159 S3 (lane S3b): the mapped both-bound witness, bound to PROVEN-OPERABLE captures.
#
# FIXTURE PROVENANCE: `replay_identity/cap159-s3-bound-profiles/` (its PROVENANCE.md) — a REST
# GET operation storing `responseProfile`, a REST PATCH operation storing `requestProfile`, a
# map between those two JSON profiles, the profiles and the connection, executed GREEN on the
# platform runtime (`execution-76ba0751-…`) with the mapped PATCH effect observed. The bytes
# were emitted by this repo's builders at ed17dde; the green run is the independent evidence.
# The second connection is the S1 capture `cap159-s1-reuse-identity/rest-conn-aa5c5042.xml`
# (same base URL). Every byte is served VERBATIM: no attribute is substituted. The oracle
# comparison maps these real ids onto golden-000079's ids (`ORACLE_IDS_BY_CAPTURED_ID`) and
# drops layout coordinates — nothing else is normalized.
# Two documented limits: (1) the operations' `requestProfile`/`responseProfile` SPELLING was emitted
# by the builders; the green run proves the set operable, not that the REST runtime reads those two
# attributes (its PROVENANCE.md). (2) golden-000081's IR labels the target writer's element "3" as
# `requestId (Root/Object/requestId)`; in the captured PATCH request profile key 3 is `name`.
# Nothing offline reads that label (the emitter writes the authored id and label); a LIVE run of
# this witness will meet it.
_CAP159S3 = _FIX / "boomi/replay_identity/cap159-s3-bound-profiles"
_CAP159S1_CONN = _FIX / "boomi/replay_identity/cap159-s1-reuse-identity/rest-conn-aa5c5042.xml"
CAPTURED_GET_OP = "3cbe300a-1369-49b7-ad1d-9487b55d1df8"
CAPTURED_PATCH_OP = "db9108fc-7b28-4fbc-bd5a-4229216f1742"
CAPTURED_MAP = "a3e236b6-a736-4811-92c4-8b8a23ea8cb7"
CAPTURED_GET_RESPONSE_PROFILE = "dfa08e0a-479c-4aa8-b043-2113af48dad4"
CAPTURED_PATCH_REQUEST_PROFILE = "d65fda99-ae94-4608-b830-6754ebf229c5"
CAPTURED_GET_CONN = "b0d70f36-9661-4639-9ad1-b6e74369799e"
CAPTURED_PATCH_CONN = "aa5c5042-75d1-4bd3-85df-a65739cb920e"
_CAPTURED_PATH = "/admin/refdata/api/v1/rdcds/currency/QA_currency_E0D9B5299C"
#: The documented id substitution of the oracle comparison: captured id -> golden-000079 id.
ORACLE_IDS_BY_CAPTURED_ID = {
    CAPTURED_GET_CONN: "RCONN",
    CAPTURED_GET_OP: "ROP",
    CAPTURED_PATCH_CONN: "CONN-UUID",
    CAPTURED_PATCH_OP: "OP-UUID",
    CAPTURED_MAP: "MAP-UUID",
    CAPTURED_GET_RESPONSE_PROFILE: "PROFILE-READ",
    CAPTURED_PATCH_REQUEST_PROFILE: "PROFILE-REQUEST",
}


def captured_bound_profiles_store():
    """``{component id: stored XML}``: the live process plus the cap159-s3 set, verbatim."""
    store = {CANONICAL_PROCESS_ID: _live_process_xml()}
    for name in ("op-get-3cbe300a.xml", "op-patch-db9108fc.xml", "map-a3e236b6.xml",
                 "profile-get-response-dfa08e0a.xml", "profile-patch-request-d65fda99.xml",
                 "rest-conn-b0d70f36.xml"):
        text = (_CAP159S3 / name).read_text(encoding="utf-8")
        store[_re.search(r'componentId="([^"]+)"', text).group(1)] = text
    conn = _CAP159S1_CONN.read_text(encoding="utf-8")
    store[_re.search(r'componentId="([^"]+)"', conn).group(1)] = conn
    return store


def captured_bound_profiles_patch(**envelope_overrides):
    """golden-000081's mapped both-sides IR, every reference bound to a cap159-s3 capture."""
    patch = canonical_patch(mapped=True, **envelope_overrides)
    intent = patch["authoring_request"]["intent"]
    intent["components"] = [
        _connection_row("RCONN", CAPTURED_GET_CONN),
        _connection_row("CONN", CAPTURED_PATCH_CONN),
        _operation_row("ROP", CAPTURED_GET_OP, "RCONN", "GET", _CAPTURED_PATH),
        _operation_row("OP", CAPTURED_PATCH_OP, "CONN", "PATCH", _CAPTURED_PATH),
        _bound_row("MAP", "transform.map", CAPTURED_MAP),
        _bound_row("PREAD", "profile.json", CAPTURED_GET_RESPONSE_PROFILE),
        _bound_row("PREQ", "profile.json", CAPTURED_PATCH_REQUEST_PROFILE),
    ]
    return patch


def with_oracle_ids(xml_text):
    """``xml_text`` with every attribute value that IS a captured id replaced by the oracle's."""
    root = ET.fromstring(xml_text)
    for element in root.iter():
        for name, value in element.attrib.items():
            if value in ORACLE_IDS_BY_CAPTURED_ID:
                element.set(name, ORACLE_IDS_BY_CAPTURED_ID[value])
    return ET.tostring(root, encoding="unicode")


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
        # #159 S3 (S3-D2): each admission refusal serves its dedicated code.
        assert res["error_code"] == {
            "mixed_config": "COMPONENT_EDIT_AUTHORING_REQUEST_INVALID",
            "mixed_map_context": "COMPONENT_EDIT_AUTHORING_REQUEST_INVALID",
            "two_units": "COMPONENT_EDIT_AUTHORING_REQUEST_INVALID",
            "create_action": "COMPONENT_EDIT_AUTHORING_REQUEST_INVALID",
            "other_component": "COMPONENT_EDIT_TARGET_MISMATCH",
            "written_supporting_row": "COMPONENT_EDIT_SUPPORTING_WRITE_REFUSED",
            # A create row with no id would be CREATED by apply: a write.
            "unbound_supporting_row": "COMPONENT_EDIT_SUPPORTING_WRITE_REFUSED",
        }[variant], (variant, action, res)
        if expected_field:
            assert res["field"] == expected_field
    assert client.component.pushed == []


def test_canonical_update_refuses_a_dependency_the_account_holds_as_another_type():
    """#159 S3 (S3-D2): dependency verification serves its dedicated code. The captured
    request with one bound profile row declared as an XML profile: the account holds a
    JSON profile at that id, so prepare refuses COMPONENT_EDIT_DEPENDENCY_UNVERIFIED at
    that row and nothing is written."""
    client = CanonicalAccount(captured_bound_profiles_store())
    patch = captured_bound_profiles_patch()
    rows = patch["authoring_request"]["intent"]["components"]
    index = next(i for i, row in enumerate(rows) if row["key"] == "PREAD")
    rows[index]["type"] = "profile.xml"
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch)
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    assert res["error_code"] == "COMPONENT_EDIT_DEPENDENCY_UNVERIFIED", res
    assert res["field"] == f"authoring_request.intent.components.{index}"
    assert client.component.pushed == []


@pytest.mark.parametrize("spelling", ["component_id", "reference_only"])
def test_canonical_update_refuses_a_declared_verb_the_bound_operation_does_not_store(spelling):
    """The stored PATCH operation (`component_op_patch.xml`, customOperationType="PATCH")
    is declared a POST. The declaration must never reach the emitted connector shape:
    bound by the top-level id, the snapshot's identity reading refuses it; bound only
    through `config.component_id` (which the snapshot does not read), admission refuses
    the spelling. Either way prepare refuses and nothing is written."""
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    row = patch["authoring_request"]["intent"]["components"][3]
    assert row["key"] == "OP"
    row["config"]["method"] = "POST"
    if spelling == "reference_only":
        row["config"].update(reference_only=True, component_id=row.pop("component_id"))
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch)
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    if spelling == "reference_only":
        assert res["error_code"] == "COMPONENT_EDIT_BINDING_REQUIRED"
        assert res["field"] == "authoring_request.intent.components.3.component_id"
    else:
        codes = {d.get("code") for d in res.get("authoring_diagnostics") or ()}
        assert "CONNECTOR_REPLAY_IDENTITY_MISMATCH" in codes, res
    assert client.component.pushed == []


# #159 S3 (lane S3b): controls for the mapped both-bound witness
# (`test_issue_184_cutover.py::test_the_public_safe_edit_applies_the_canonical_mapped_both_sides_update`).
# Each mutates ONE captured byte of the cap159-s3 set in the fake account — the control's whole point —
# and asserts the refusal's code and pointer, and that nothing is written.

_MAP_POINTER = "/body/steps/2/map_ref"
_UNREAD_PROFILE = "0b0b0b0b-1111-4222-8333-444444444444"  # a valid id the account does not hold


def _profile_mismatch_at_the_map(result):
    return any(
        "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH" in (item.get("cause_codes") or ())
        and item.get("path") == _MAP_POINTER
        for item in result.get("authoring_diagnostics") or ()
    )


def _captured_map_with_target(store, profile_id):
    store[CAPTURED_MAP] = store[CAPTURED_MAP].replace(
        f'toProfile="{CAPTURED_PATCH_REQUEST_PROFILE}"', f'toProfile="{profile_id}"', 1)
    assert f'toProfile="{profile_id}"' in store[CAPTURED_MAP]
    return store


@pytest.mark.parametrize("stored_target", ["another_profile", "unreadable_profile"])
def test_captured_mapped_update_refuses_a_stored_map_target_the_patch_does_not_accept(stored_target):
    """Non-vacuity of the stored reading: the bound map's STORED `toProfile` names another real
    profile (the GET's response profile) or a profile the account cannot read. Prepare refuses
    PROFILE_MISMATCH at the map, value-free, and writes nothing."""
    profile_id = CAPTURED_GET_RESPONSE_PROFILE if stored_target == "another_profile" else _UNREAD_PROFILE
    client = CanonicalAccount(_captured_map_with_target(captured_bound_profiles_store(), profile_id))
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, captured_bound_profiles_patch())
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    assert _profile_mismatch_at_the_map(res), res
    assert profile_id not in _json.dumps(res)
    assert client.component.pushed == []


def test_captured_mapped_update_needs_the_stored_reading(monkeypatch):
    """With the stored-binding projection switched off, the very request the witness applies refuses
    at the map — so the witness's success is the stored reading's, not something else's."""
    from boomi_mcp.recipes import materialization

    monkeypatch.setattr(materialization, "_stored_binding_profile_facts",
                        lambda component, snapshot, facts: (facts, ()))
    client = CanonicalAccount(captured_bound_profiles_store())
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, captured_bound_profiles_patch())
    assert res["_success"] is False and _profile_mismatch_at_the_map(res), res
    assert client.component.pushed == []


def test_captured_mapped_update_refuses_apply_when_the_stored_map_changed_since_prepare(monkeypatch):
    """Prepare previews against the stored map; before apply the account's map is re-pointed at
    another profile (a new version). Apply refuses before the write: nothing is pushed."""
    _spy_compare(monkeypatch)
    client = CanonicalAccount(captured_bound_profiles_store())
    patch = captured_bound_profiles_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    _captured_map_with_target(client.component.store, CAPTURED_GET_RESPONSE_PROFILE)
    client.component.versions[CAPTURED_MAP] += 1
    res = apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True)
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    # Apply RECOMPILES against the fresh account: the stored map now maps into the GET's profile,
    # which the PATCH does not accept.
    assert res["error_code"] == "AUTHORING_COMPILE_BLOCKED", res
    assert _profile_mismatch_at_the_map(res), res
    assert client.component.pushed == []


def test_captured_mapped_update_refuses_apply_when_a_linked_profile_became_unreadable(monkeypatch):
    """The profile the map and the PATCH link is gone from the account between prepare and apply:
    apply refuses before the write and pushes nothing."""
    _spy_compare(monkeypatch)
    client = CanonicalAccount(captured_bound_profiles_store())
    patch = captured_bound_profiles_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    del client.component.store[CAPTURED_PATCH_REQUEST_PROFILE]
    res = apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True)
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    # The map's stored target is now UNKNOWN: refused at the map, by the fresh compile, value-free.
    assert res["error_code"] == "AUTHORING_COMPILE_BLOCKED", res
    assert _profile_mismatch_at_the_map(res), res
    assert CAPTURED_PATCH_REQUEST_PROFILE not in _json.dumps(res)
    assert client.component.pushed == []


def _captured_unmapped_patch():
    """golden-000080's UNMAPPED source-only IR with the operations bound to the profiled cap159-s3
    captures (the IR's request path is a binding; the declared path equals the stored one)."""
    patch = canonical_patch()
    rows = {row["key"]: row for row in patch["authoring_request"]["intent"]["components"]}
    rows["RCONN"]["component_id"] = CAPTURED_GET_CONN
    rows["CONN"]["component_id"] = CAPTURED_PATCH_CONN
    for key, op_id in (("ROP", CAPTURED_GET_OP), ("OP", CAPTURED_PATCH_OP)):
        rows[key]["component_id"] = op_id
        rows[key]["config"]["path"] = _CAPTURED_PATH
    return patch


@pytest.mark.parametrize("linked_profiles", ["readable", "unreadable"])
def test_unknown_stored_operation_profiles_change_nothing_in_an_unmapped_update(linked_profiles):
    """An unknown stored link refuses only where a profile is REQUIRED (the map pointer above).
    In an unmapped flow the operations' linked profiles are never required, so the same update
    prepares identically whether the account can read them or not (measured, not assumed:
    the unknown side sets ``profiles_unknown`` on the operation symbols)."""
    store = captured_bound_profiles_store()
    if linked_profiles == "unreadable":
        del store[CAPTURED_GET_RESPONSE_PROFILE]
        del store[CAPTURED_PATCH_REQUEST_PROFILE]
    client = CanonicalAccount(store)
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, _captured_unmapped_patch())
    assert res["_success"] is True, res
    assert client.component.pushed == []
    # The reader DID consult the account for both linked profiles, so the unknown arm was reached.
    assert {CAPTURED_GET_RESPONSE_PROFILE, CAPTURED_PATCH_REQUEST_PROFILE} <= set(client.component.reads)


# ---------------------------------------------------------------------------
# #159 S3 (cutover decision 2): the legacy body arm on a process is refused
# ---------------------------------------------------------------------------

_CURRENT_PROCESS = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="process" name="p-old" folderId="42">'
    '<bns:object><process xmlns=""><shapes/></process></bns:object>'
    '</bns:Component>'
)


@pytest.mark.parametrize(
    "body",
    [
        {"process_kind": "database_to_api_sync", "password": "CANARY_V"},
        {"process_type": "CANARY_V"},
        {"flow_sequence": [{"CANARY_K": "CANARY_V"}]},
        {"name": "p-new", "source": {"CANARY_K": "CANARY_V"}},
    ],
)
def test_a_legacy_process_body_patch_is_refused_at_prepare_and_apply(body, monkeypatch):
    """A structured BODY patch on a process names no canonical unit, so safe edit
    refuses it with the unit-required code at prepare, value-free, without dispatching
    any builder; apply has no token that covers it, so nothing is written. A
    metadata-only patch on the same process stays on the smart-merge path."""
    _spy_compare(monkeypatch)
    client = _FakeClient(_CURRENT_PROCESS)
    patch = {"config": dict(body)}
    prep = prepare_component_edit_action(client, "p", COMPONENT_ID, patch)
    assert prep["_success"] is False
    assert prep["error_code"] == "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED"
    assert prep["field"] == "config"
    assert "patch.authoring_request" in prep["hint"]
    assert "CANARY_K" not in repr(prep) and "CANARY_V" not in repr(prep)
    # Apply can never reach a write for it: no prepare issues a token for the body,
    # and a token prepared for a metadata patch does not cover it.
    metadata = prepare_component_edit_action(
        client, "p", COMPONENT_ID, {"config": {"name": "p-renamed"}})
    assert metadata["_success"] is True, metadata
    for token, code in (
        ("bogus-token", "COMPONENT_EDIT_TOKEN_INVALID"),
        (metadata["confirmation_token"], "COMPONENT_EDIT_PATCH_MISMATCH"),
    ):
        applied = apply_component_edit_action(
            client, "p", COMPONENT_ID, patch, token, confirm_apply=True)
        assert applied["_success"] is False
        assert applied["error_code"] == code, applied
        assert "CANARY_K" not in repr(applied) and "CANARY_V" not in repr(applied)
    assert client.component.pushed == []


# ---------------------------------------------------------------------------
# #159 architect review r1 #1 / #11: every read safe edit writes from is the named
# component's OWN envelope, and the canonical arm spends ONE read budget per call.
# ---------------------------------------------------------------------------
#
# Every byte below is a capture already served above; a fault rewrites ONLY the
# envelope's `componentId` (or the root's name), so nothing but the envelope check
# can tell the faulty answer from the true one.

_ANOTHER_ID = "99999999-aaaa-4bbb-8ccc-dddddddddddd"


def _envelope_fault(xml_text, fault, own_id):
    if fault == "another_id":
        text = xml_text.replace(f'componentId="{own_id}"', f'componentId="{_ANOTHER_ID}"', 1)
    elif fault == "no_id":
        text = xml_text.replace(f'componentId="{own_id}"', "", 1)
    else:  # not_a_component: the expected id and type, on a root that is no ComponentGet envelope
        text = _re.sub(r"<bns:Component\b", "<NotAComponent", xml_text, count=1).replace(
            "</bns:Component>", "</NotAComponent>")
    assert text != xml_text, fault
    return text


def _refused_unwritten(res, client, *, field="component_id"):
    """Refused, value-free, nothing written. Another id fails at the ONE read (a failed read);
    a root that is no envelope, or a missing id on the canonical arm, at safe edit's own check."""
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    if field is not None:
        assert res["field"] == field, res
    assert _ANOTHER_ID not in _json.dumps(res)
    assert client.component.pushed == []


@pytest.mark.parametrize("fault", ["another_id", "no_id", "not_a_component"])
def test_canonical_update_refuses_a_target_answer_that_is_not_its_own_envelope(fault, monkeypatch):
    """ARCH-r1 #1 (CRITICAL): the account answers the read of process A with bytes that are not
    A's envelope. Before the fix prepare AND apply succeeded and pushed those bytes — B's id
    included — to A's update endpoint. Prepare now refuses; and apply, whose token was minted
    while A answered truly, refuses when A's answer turns faulty: nothing is written."""
    _spy_compare(monkeypatch)
    store = canonical_account_store()
    store[CANONICAL_PROCESS_ID] = _envelope_fault(store[CANONICAL_PROCESS_ID], fault, CANONICAL_PROCESS_ID)
    client = CanonicalAccount(store)
    _refused_unwritten(prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, canonical_patch()),
                       client)

    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    client.component.store[CANONICAL_PROCESS_ID] = _envelope_fault(
        client.component.store[CANONICAL_PROCESS_ID], fault, CANONICAL_PROCESS_ID)
    res = apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True)
    _refused_unwritten(res, client)
    assert "error_code" not in res  # the envelope refusal, not the drift check behind it


@pytest.mark.parametrize("fault", ["another_id", "no_id", "not_a_component"])
def test_canonical_update_refuses_a_dependency_answer_that_is_not_its_own_envelope(fault, monkeypatch):
    """ARCH-r1 #1 (CRITICAL, supporting bindings): the bound GET connection's id answers with the
    same captured bytes under another envelope. Before the fix the type check passed and the
    update was written against a dependency the account never answered for. Now: another id is a
    FAILED READ at the one read, so the compile's connector identity is unavailable and it refuses;
    a root that is no envelope, or no id, refuses COMPONENT_EDIT_DEPENDENCY_UNVERIFIED at that row.
    The same at apply after a clean prepare. Nothing is written."""
    _spy_compare(monkeypatch)
    pointer = "authoring_request.intent.components.0"

    def _assert_refused(res, client):
        if fault == "another_id":
            _refused_unwritten(res, client, field=None)
            assert res["error_code"] == "AUTHORING_COMPILE_BLOCKED", res
            codes = {d.get("code") for d in res.get("authoring_diagnostics") or ()}
            assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" in codes, res
        else:
            _refused_unwritten(res, client, field=pointer)
            assert res["error_code"] == "COMPONENT_EDIT_DEPENDENCY_UNVERIFIED", res

    store = canonical_account_store()
    store["RCONN"] = _envelope_fault(store["RCONN"], fault, "RCONN")
    client = CanonicalAccount(store)
    _assert_refused(prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, canonical_patch()),
                    client)

    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    client.component.store["RCONN"] = _envelope_fault(client.component.store["RCONN"], fault, "RCONN")
    _assert_refused(
        apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True),
        client)


def test_canonical_update_never_previews_bytes_that_name_another_component(monkeypatch):
    """ARCH-r1 #1: whatever produced them, bytes naming another component are never previewed,
    so no token covers them (and the one write asserts the same invariant)."""
    original = safe_edit_component.merge_for_update

    def _merge_naming_another(current, built, policy):
        return original(current, built, policy).replace(
            f'componentId="{CANONICAL_PROCESS_ID}"', f'componentId="{_ANOTHER_ID}"', 1)

    monkeypatch.setattr(safe_edit_component, "merge_for_update", _merge_naming_another)
    client = CanonicalAccount(canonical_account_store())
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, canonical_patch())
    _refused_unwritten(res, client)
    assert res.get("confirmation_token") is None


@pytest.mark.parametrize("fault", ["another_id", "not_a_component"])
def test_legacy_edit_refuses_a_target_answer_naming_another_component(fault, monkeypatch):
    """ARCH-r1 #1, the sibling arm (INHERITED from ed17dde, measured): the metadata smart-merge
    arm reads its target through the same read. A target answering with another component's
    envelope is refused at prepare and at apply; a missing id (the pre-S3 fixtures' spelling)
    stays accepted there — it names no other component."""
    _spy_compare(monkeypatch)
    own = _CURRENT_DB_CONN.replace("<bns:Component ", f'<bns:Component componentId="{COMPONENT_ID}" ', 1)
    faulty = _envelope_fault(own, fault, COMPONENT_ID)
    patch = {"config": {"name": "conn-renamed"}}

    # Another id is a failed READ (the legacy arm's read-failure envelope names no field).
    field = None if fault == "another_id" else "component_id"
    client = _FakeClient(faulty)
    _refused_unwritten(prepare_component_edit_action(client, "prof", COMPONENT_ID, patch), client,
                       field=field)

    client = _FakeClient(own)
    token = _prepare_then_token(client, patch)
    client.component._xml = faulty
    res = apply_component_edit_action(client, "prof", COMPONENT_ID, patch, token, confirm_apply=True)
    _refused_unwritten(res, client, field=field)
    assert res.get("error_code") != "COMPONENT_EDIT_DRIFT_DETECTED", res

    # Controls: the component's own id, and no id at all, still edit.
    for live in (own, _CURRENT_DB_CONN):
        client = _FakeClient(live)
        token = _prepare_then_token(client, patch)
        res = apply_component_edit_action(client, "prof", COMPONENT_ID, patch, token, confirm_apply=True)
        assert res["_success"] is True, res
        assert [cid for cid, _ in client.component.pushed] == [COMPONENT_ID]


def test_canonical_update_spends_one_read_budget_from_the_target_read(monkeypatch):
    """ARCH-r1 #11 / ARCH-r2 #3: prepare and apply each spend ONE read budget, starting at the
    target's read. EVERY account read of the call — the post-write re-read included — is handed
    the budget's REMAINING slice, so none runs under a standalone deadline (``None``) and the
    slices never grow. Before r1 the target was read under its own deadline and a fresh budget
    started after it; before r2 the post-write re-read ran under a fresh deadline."""
    from boomi_mcp.categories.components import _shared

    _spy_compare(monkeypatch)
    real = _shared.component_get_xml
    calls = []
    post_write = []

    def _spy(client, component_id, deadline_seconds=None):
        (post_write if client.component.pushed else calls).append((component_id, deadline_seconds))
        return real(client, component_id, deadline_seconds=deadline_seconds)

    monkeypatch.setattr(_shared, "component_get_xml", _spy)
    monkeypatch.setattr(safe_edit_component, "component_get_xml", _spy)
    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    prepare_calls = list(calls)
    calls.clear()
    res = apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True)
    assert res["_success"] is True, res
    # Non-vacuity: the post-write re-read happened, on the target, inside apply's budget.
    assert [cid for cid, _ in post_write] == [CANONICAL_PROCESS_ID], post_write
    apply_calls = calls + post_write
    for phase, seen in (("prepare", prepare_calls), ("apply", apply_calls)):
        assert seen and seen[0][0] == CANONICAL_PROCESS_ID, (phase, seen)
        deadlines = [deadline for _cid, deadline in seen]
        assert None not in deadlines, (phase, seen)
        assert deadlines == sorted(deadlines, reverse=True), (phase, seen)


def test_canonical_update_compares_versions_inside_the_calls_one_budget(monkeypatch):
    """ARCH-r2 #3: the post-write version comparison is an account call of the canonical apply,
    so it runs on the REMAINING budget: a comparison that stalls past it is abandoned and served
    as a comparison error beside the landed write. Before the fix it ran unbounded, so the call
    outlived its budget by the whole stall."""
    import time as _t
    from boomi_mcp.categories.components import _shared

    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    compared = []

    def _slow_compare(boomi_client, profile, component_id, config):
        compared.append(config)
        _t.sleep(2.5)
        return {"_success": True, "diff": {"changed": True}}

    monkeypatch.setattr(safe_edit_component, "compare_versions", _slow_compare)
    monkeypatch.setattr(_shared, "_component_get_deadline_seconds", lambda: 1)
    started = _t.monotonic()
    res = apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True)
    elapsed = _t.monotonic() - started
    assert res["_success"] is True and res["boomi_mutation"] is True, res
    assert compared, "the comparison must have been started (non-vacuity)"
    assert "version_comparison" not in res and res.get("version_comparison_error"), res
    # It names what ran out — the comparison, not a component GET (QA r6-02).
    assert "version comparison" in res["version_comparison_error"], res
    assert "Component GET" not in res["version_comparison_error"], res
    assert elapsed < 2.0, elapsed


def test_captured_mapped_update_refuses_a_linked_profile_that_is_no_component_envelope():
    """ARCH-r1 #3: the PATCH request profile the map targets answers with its expected id and type
    on a root that is no ComponentGet envelope (`<NotAComponent>`). Before the fix that verified as
    the profile and prepare succeeded; now the link is UNKNOWN and prepare refuses PROFILE_MISMATCH
    at the map, writing nothing."""
    store = captured_bound_profiles_store()
    store[CAPTURED_PATCH_REQUEST_PROFILE] = _envelope_fault(
        store[CAPTURED_PATCH_REQUEST_PROFILE], "not_a_component", CAPTURED_PATCH_REQUEST_PROFILE)
    assert f'componentId="{CAPTURED_PATCH_REQUEST_PROFILE}"' in store[CAPTURED_PATCH_REQUEST_PROFILE]
    client = CanonicalAccount(store)
    res = prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, captured_bound_profiles_patch())
    assert res["_success"] is False, res
    assert res["boomi_mutation"] is False
    assert _profile_mismatch_at_the_map(res), res
    assert client.component.pushed == []


# ---------------------------------------------------------------------------
# #159 ARCH-r1 #1, the STRUCTURAL fix: the one read (`_shared.component_get_xml`)
# fails an answer whose envelope names another component, for every caller.
# ---------------------------------------------------------------------------

_A_ID = "11111111-aaaa-4bbb-8ccc-000000000001"
_B_ID = "22222222-aaaa-4bbb-8ccc-000000000002"


def _db_conn_named(component_id):
    return _CURRENT_DB_CONN.replace(
        "<bns:Component ", f'<bns:Component componentId="{component_id}" version="3" ', 1)


class _AnsweringApi:
    """Answers EVERY GET with the envelope of ``answered_id`` (B answering for A, or A for A),
    whatever was requested, and records every write."""

    def __init__(self, answered_id):
        self.answered_id = answered_id
        self.reads, self.pushed = [], []

    def get_component(self, component_id):
        self.reads.append(component_id)
        return _db_conn_named(self.answered_id).encode("utf-8")

    def update_component_raw(self, component_id, xml_text):
        self.pushed.append((component_id, xml_text))
        return b"<updated/>"


class _AnsweringClient:
    def __init__(self, answered_id):
        self.component = _AnsweringApi(answered_id)


@pytest.mark.parametrize(
    "requested",
    [_A_ID, _A_ID.upper(), f" {_A_ID} ", f"{_A_ID}~dev-branch", f"{_A_ID}~7"],
    ids=["bare", "upper_case", "padded", "branch_qualified", "version_qualified"],
)
def test_the_one_read_fails_an_answer_naming_another_component(requested):
    """Non-vacuity at the authority: B's envelope answering a request for A (bare, any case,
    padded, or in the `~branch` / `~version` forms merge_versions sends) is a READ FAILURE,
    value-free; A's own envelope reads; an envelope stating no id keeps the requested-id fallback."""
    from boomi_mcp.categories.components import _shared

    with pytest.raises(_shared.ComponentGetIdentityMismatch) as refused:
        _shared.component_get_xml(_AnsweringClient(_B_ID), requested)
    assert not isinstance(refused.value, ET.ParseError)  # a failed read, never "unparseable"
    assert _A_ID not in str(refused.value) and _B_ID not in str(refused.value)

    assert _shared.component_get_xml(_AnsweringClient(_A_ID), requested)["id"] == _A_ID
    no_id = _FakeClient(_CURRENT_DB_CONN)
    assert _shared.component_get_xml(no_id, requested)["component_id"] == requested


def test_a_budgeted_read_of_another_components_answer_reads_nothing():
    """The phase budget (`workflow.BudgetedComponentReads`) reads through the one read: an answer
    naming another component is an unread component there (None), not a reading of it."""
    from boomi_mcp.authoring.workflow import BudgetedComponentReads

    assert BudgetedComponentReads(_AnsweringClient(_B_ID)).fetch(_A_ID) is None
    assert BudgetedComponentReads(_AnsweringClient(_A_ID)).fetch(_A_ID)["id"] == _A_ID


def _route_manage_component(client):
    from boomi_mcp.categories.components.manage_component import update_component

    return update_component(client, "p", _A_ID, {"name": "renamed"})


def _route_connectors(client):
    from boomi_mcp.categories.components.connectors import update_connector

    return update_connector(client, "p", _A_ID, {"name": "renamed"})


def _route_folders(client):
    from boomi_mcp.categories.folders import _action_move_component

    try:
        return _action_move_component(client, "p", component_id=_A_ID, target_folder_id="F9")
    except Exception as exc:  # this route lets a failed read propagate to the tool layer
        return {"_success": False, "raised": type(exc).__name__}


def _route_merge_versions(client):
    from boomi_mcp.categories.components.analyze_component import merge_versions

    return merge_versions(client, "p", _A_ID, {"source_branch": "dev", "target_branch": "main"})


def _route_apply_structured_update(client):
    from boomi_mcp.categories.integration_builder import _apply_structured_update

    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    comp = IntegrationComponentSpec(key="db", type="connector-settings", name="conn-renamed",
                                    action="update", component_id=_A_ID,
                                    config={"connector_type": "database"})
    return _apply_structured_update(client, "p", _A_ID, comp, _DESIRED_DB_CONN, _DB_POLICY)


@pytest.mark.parametrize("route", [
    _route_manage_component, _route_connectors, _route_folders, _route_merge_versions,
    _route_apply_structured_update,
], ids=["manage_component_update", "manage_connector_update", "folders_move_component",
        "merge_versions", "build_integration_apply_structured_update"])
def test_read_merge_write_routes_never_write_another_components_bytes(route, monkeypatch):
    """Every read-merge-write route outside safe edit: when the account answers A's read with B's
    envelope, the route used to write B's bytes to A (measured on ed17dde and 1a5a3fc). Now the read
    fails and nothing is written; with A's own envelope the same route writes A."""
    _spy_compare(monkeypatch)
    from boomi_mcp.categories.components import analyze_component

    monkeypatch.setattr(analyze_component, "compare_versions", lambda *a, **k: {"_success": True})
    foreign = _AnsweringClient(_B_ID)
    res = route(foreign)
    assert res.get("_success") is not True, res
    assert foreign.component.pushed == [], foreign.component.pushed
    assert _B_ID not in _json.dumps(res)

    own = _AnsweringClient(_A_ID)
    res = route(own)
    assert res.get("_success") is True, res
    assert [cid for cid, _ in own.component.pushed] == [_A_ID]


def test_canonical_target_read_timeout_still_serves_the_deadline_code(monkeypatch):
    """ARCH-r1 #11 keeps the served code: the canonical target is now read through the call's ONE
    budget, and a target read that times out still serves COMPONENT_GET_DEADLINE_EXCEEDED (as the
    standalone read did), at prepare and at apply; nothing is written."""
    from boomi_mcp.categories.components import _shared

    client = CanonicalAccount(canonical_account_store())
    patch = canonical_patch()
    token = _canonical_prepare(client, patch)["confirmation_token"]
    real = _shared.component_get_xml

    def _stalled_target(c, component_id, deadline_seconds=None):
        if component_id == CANONICAL_PROCESS_ID:
            raise _shared.ComponentGetDeadlineExceeded(component_id, deadline_seconds, 0.0)
        return real(c, component_id, deadline_seconds=deadline_seconds)

    monkeypatch.setattr(_shared, "component_get_xml", _stalled_target)
    for res in (
        prepare_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch),
        apply_component_edit_action(client, "prof", CANONICAL_PROCESS_ID, patch, token, confirm_apply=True),
    ):
        assert res["_success"] is False, res
        assert res["error_code"] == "COMPONENT_GET_DEADLINE_EXCEEDED", res
        assert res["retryable"] is True
    assert client.component.pushed == []


class _VerifyFailsApi(_AnsweringApi):
    """Answers A's own envelope until the write lands; the post-write verification read then
    answers B's envelope (``mode="foreign"``) or fails outright (``mode="error"``)."""

    def __init__(self, mode):
        super().__init__(_A_ID)
        self.mode = mode

    def get_component(self, component_id):
        if self.pushed:
            self.reads.append(component_id)
            if self.mode == "error":
                raise RuntimeError("verification read failed")
            return _db_conn_named(_B_ID).encode("utf-8")
        return super().get_component(component_id)


class _VerifyFailsClient:
    def __init__(self, mode):
        self.component = _VerifyFailsApi(mode)


@pytest.mark.parametrize("mode", ["foreign", "error"])
@pytest.mark.parametrize("route", [_route_folders, _route_merge_versions],
                         ids=["folders_move_component", "merge_versions"])
def test_a_failed_verification_read_never_reports_a_landed_write_as_failed(route, mode, monkeypatch):
    """Mutation accounting (#159 S3 fix-only r1): when the write succeeded and only the post-write
    verification read fails (another component's envelope, or an error), the route reports the
    write as done (`boomi_mutation`), says verification failed, writes once, and echoes no id."""
    from boomi_mcp.categories.components import analyze_component

    monkeypatch.setattr(analyze_component, "compare_versions", lambda *a, **k: {"_success": True})
    client = _VerifyFailsClient(mode)
    res = route(client)
    assert res.get("_success") is True, res
    assert res.get("boomi_mutation") is True, res
    assert res["verification"]["verified"] is False
    assert [cid for cid, _ in client.component.pushed] == [_A_ID]
    assert _B_ID not in _json.dumps(res)
    assert "verification read failed" not in _json.dumps(res)
    # Nothing confirmed is claimed: no version, and no note asserting an increment.
    assert "incremented" not in res.get("note", "")
    assert res.get("new_version", "") == ""


class _VersioningApi(_AnsweringApi):
    """Answers A's own envelope: at version 3 until the write, then at ``after`` (4 = the write
    produced a version, 3 = the re-read shows none — a no-op write or a stale read)."""

    def __init__(self, after):
        super().__init__(_A_ID)
        self.after = after

    def get_component(self, component_id):
        self.reads.append(component_id)
        version = self.after if self.pushed else 3
        return _db_conn_named(_A_ID).replace('version="3"', f'version="{version}"', 1).encode("utf-8")


class _VersioningClient:
    def __init__(self, after):
        self.component = _VersioningApi(after)


@pytest.mark.parametrize("config, after, claims_increment, shown", [
    ({"source_branch": "dev", "target_branch": "main"}, 4, True, "incremented"),
    ({"source_branch": "dev", "target_branch": "main"}, 3, False, "shows no new version"),
    ({"source_version": 1, "target_version": 2}, 4, False, "shows version 4"),
], ids=["head_target_advanced", "head_target_not_advanced", "snapshot_target_observed"])
def test_a_merge_claims_a_version_increment_only_when_the_re_read_shows_one(
        config, after, claims_increment, shown, monkeypatch):
    """ARCH-r2 #2 (structural, second instance of CDX fx2-01): the merge's served version claim is
    DERIVED from the post-write reading (`_shared.post_write_version`), against the pre-write
    version of the SAME object. A re-read at the old version claims no increment; a target read
    as a version snapshot (`{id}~{n}`) is no baseline for the head, so only the observed version
    is served. Before the fix any successful re-read claimed an increment."""
    from boomi_mcp.categories.components import analyze_component
    from boomi_mcp.categories.components.analyze_component import merge_versions

    monkeypatch.setattr(analyze_component, "compare_versions", lambda *a, **k: {"_success": True})
    client = _VersioningClient(after)
    res = merge_versions(client, "p", _A_ID, config)
    assert res["_success"] is True and res["boomi_mutation"] is True, res
    assert [cid for cid, _ in client.component.pushed] == [_A_ID]
    assert ("incremented" in res["note"]) is claims_increment, res["note"]
    assert shown in res["note"], res["note"]
    assert res["new_version"] == after
    assert "verification" not in res


def _reading(root):
    """A post-write reading as the one read returns it: the dict keeps the raw XML."""
    return {"version": 0, "xml": root}


_A_ENV = '<bns:Component xmlns:bns="http://api.platform.boomi.com/" componentId="A" type="x"{0}/>'


@pytest.mark.parametrize("before, after, state, version", [
    (3, None, "unconfirmed", None),
    (None, _reading(_A_ENV.format(' version="4"')), "observed", 4),
    (3, _reading(_A_ENV.format(' version="4"')), "advanced", 4),
    (3, _reading(_A_ENV.format(' version="3"')), "unchanged", 3),
    (3, _reading(_A_ENV.format(' version="2"')), "unchanged", 2),
    # ARCH-r3 #1: no version evidence -> nothing is claimed.
    (3, _reading(_A_ENV.format('')), "unconfirmed", None),
    (None, _reading(_A_ENV.format('')), "unconfirmed", None),
    (3, _reading(_A_ENV.format(' version=""')), "unconfirmed", None),
    (3, _reading('<NotAComponent componentId="A" version="999"/>'), "unconfirmed", None),
    (3, _reading("not xml"), "unconfirmed", None),
])
def test_post_write_version_covers_every_case_of_its_authority(before, after, state, version):
    """ARCH-r2 #2 / ARCH-r3 #1 coverage claim: reading present/absent × the reading stating a
    version in a genuine ComponentGet envelope or not × same-object baseline present/absent ×
    advanced or not. A root that is no component, or a component stating no version, is NO
    evidence (the read's dict defaults a missing version to 0) — before r3 those served version
    999 as an increment and "version 0" as observed."""
    from boomi_mcp.categories.components._shared import post_write_version

    assert post_write_version(before, after) == (state, version)


class _NoEvidenceApi(_VersioningApi):
    """After the write, answers A's id on a root that establishes no version."""

    def __init__(self, answer):
        super().__init__(after=None)
        self.answer = answer

    def get_component(self, component_id):
        if self.pushed:
            self.reads.append(component_id)
            return self.answer.encode("utf-8")
        return super().get_component(component_id)


@pytest.mark.parametrize("answer", [
    f'<NotAComponent componentId="{_A_ID}" version="999"/>',
    _db_conn_named(_A_ID).replace('version="3" ', "", 1),
], ids=["not_a_component_envelope", "envelope_stating_no_version"])
@pytest.mark.parametrize("config", [
    {"source_branch": "dev", "target_branch": "main"}, {"source_version": 1, "target_version": 2},
], ids=["head_target", "snapshot_target"])
def test_a_merge_claims_no_version_a_re_read_does_not_establish(answer, config, monkeypatch):
    """ARCH-r3 #1: a post-write answer that is no component envelope, or a component envelope
    stating no version, establishes no version: the merge claims neither an increment nor an
    observed version, serves no new_version, and still reports the landed write."""
    from boomi_mcp.categories.components import analyze_component
    from boomi_mcp.categories.components.analyze_component import merge_versions

    monkeypatch.setattr(analyze_component, "compare_versions", lambda *a, **k: {"_success": True})
    client = _VersioningClient(3)
    client.component = _NoEvidenceApi(answer)
    res = merge_versions(client, "p", _A_ID, config)
    assert res["_success"] is True and res["boomi_mutation"] is True, res
    assert [cid for cid, _ in client.component.pushed] == [_A_ID]
    assert "incremented" not in res["note"] and "shows version" not in res["note"], res["note"]
    assert "could not be confirmed" in res["note"], res["note"]
    assert res["new_version"] == ""
