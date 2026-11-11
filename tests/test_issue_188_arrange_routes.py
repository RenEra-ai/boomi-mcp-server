"""#188: every process write route arranges exactly once — or is exempt, with a reason.

Two halves:

* **Dispositions.** Every row of the #149 write-route inventory (``WRITE_ROUTES``) has exactly one
  disposition here — ``direct`` (the route arranges), ``transitive`` (it reaches a direct route), or
  ``exempt`` (with the reason). A route added to the inventory without one fails, and a
  non-vacuity witness proves the validator rejects that case.
* **Behaviour.** Transport spies prove, per route, what the platform receives: arranged bytes once,
  unchanged bytes under ``preserve`` and on every exempt route, and no transport call at all on a
  layout refusal.
"""
from __future__ import annotations

import copy
import hashlib
import sys
from pathlib import Path

import pytest

_TESTS = str(Path(__file__).resolve().parent)
if _TESTS not in sys.path:
    sys.path.insert(0, _TESTS)

import _m12_12_legacy_inventory as inv  # noqa: E402

from boomi_mcp import errors  # noqa: E402
from boomi_mcp.arrange import arrange_for_submission, arrange_process_body, compare  # noqa: E402
from boomi_mcp.categories import integration_builder as ib  # noqa: E402
from boomi_mcp.categories.components import (  # noqa: E402
    analyze_component, connectors, manage_component, safe_edit_component)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "arrange"

# ----------------------------------------------------------------------------- dispositions

DIRECT, TRANSITIVE, EXEMPT = "direct", "transitive", "exempt"

#: route_id -> (disposition, owner-or-reason). A transitive row names the direct routes it reaches.
DISPOSITIONS = {
    "WRT-canonical-process-materialization": (DIRECT, "_execute_canonical_process arranges after placement, before the digest (receipt to create_component); updates via _apply_structured_update"),
    "WRT-manage-component-create": (DIRECT, "create_component raw arm arranges after placement, before _create_component_raw"),
    "WRT-manage-component-update": (DIRECT, "update_component raw arm arranges before update_component_raw"),
    "WRT-manage-component-clone": (DIRECT, "clone_component arranges the final clone body before _create_component_raw"),
    "WRT-manage-connector-create": (DIRECT, "create_connector raw arm arranges after placement"),
    "WRT-manage-connector-update": (DIRECT, "update_connector raw arm arranges before update_component_raw"),
    "WRT-build-integration-structured-process": (DIRECT, "legacy process create -> create_component(layout_mode); update -> _apply_structured_update"),
    "WRT-build-integration-preservation-merge": (DIRECT, "_apply_structured_update arranges after the merge, before the digest and on_pre_push"),
    "WRT-analyze-component-merge": (DIRECT, "merge_versions arranges before the write; the diff is from the submitted bytes"),
    "WRT-manage-component-dispatch": (TRANSITIVE, ("WRT-manage-component-create", "WRT-manage-component-update", "WRT-manage-component-clone")),
    "WRT-build-integration-generic": (TRANSITIVE, ("WRT-manage-component-create", "WRT-manage-component-update", "WRT-manage-connector-create", "WRT-manage-connector-update")),
    "WRT-shared-raw-create-sink": (TRANSITIVE, ("WRT-manage-component-create", "WRT-manage-component-clone", "WRT-manage-connector-create", "WRT-canonical-process-materialization")),
    "WRT-manage-component-typed-create": (EXEMPT, "typed builders emit non-process component types only"),
    "WRT-connector-typed-build": (EXEMPT, "connector builders emit connector components only"),
    "WRT-build-integration-typed-nonprocess": (EXEMPT, "typed non-process builders (profiles, maps, caches, properties, API services) cannot emit a process body"),
    "WRT-manage-component-metadata": (EXEMPT, "owner decision: metadata-only update re-submits the live body; a rename must not move shapes"),
    "WRT-manage-connector-metadata": (EXEMPT, "owner decision: metadata-only update keeps geometry"),
    "WRT-safe-edit-metadata": (EXEMPT, "owner decision: safe-edit metadata patch keeps geometry (the body-patch path arranges in _compute_merged_xml)"),
    "WRT-folders-move-component": (EXEMPT, "owner decision: a folder move re-submits the live body unchanged"),
    "WRT-shared-dormant-writer": (EXEMPT, "dormant: _update_component_xml has no production caller"),
    "WRT-shared-channel-lossless-read": (EXEMPT, "a read, not a write"),
    "WRT-external-transport-oauth-callback": (EXEMPT, "not a component write"),
    "WRT-external-transport-listener-probe": (EXEMPT, "not a component write"),
    "WRT-external-transport-marketplace": (EXEMPT, "not a component write"),
    "WRT-external-transport-schema-discovery": (EXEMPT, "not a component write"),
    "WRT-raw-api-component": (EXEMPT, "owner decision: invoke_boomi_api is a raw passthrough"),
}


def validate_dispositions(routes, dispositions):
    """Every problem with the disposition table, as strings; [] when it is total and well-formed."""
    problems = []
    ids = [r["route_id"] for r in routes]
    if len(ids) != len(set(ids)):
        problems.append("duplicate route ids in the inventory")
    for rid in sorted(set(ids) - set(dispositions)):
        problems.append(f"{rid}: no #188 layout disposition")
    for rid in sorted(set(dispositions) - set(ids)):
        problems.append(f"{rid}: disposition for a route the inventory no longer has")
    for rid, (kind, detail) in sorted(dispositions.items()):
        if kind not in (DIRECT, TRANSITIVE, EXEMPT):
            problems.append(f"{rid}: unknown disposition {kind!r}")
        elif kind == TRANSITIVE:
            owners = tuple(detail) if isinstance(detail, (tuple, list)) else ()
            if not owners or any(dispositions.get(o, (None,))[0] != DIRECT for o in owners):
                problems.append(f"{rid}: a transitive route must name direct routes")
        elif not (isinstance(detail, str) and detail.strip()):
            problems.append(f"{rid}: a {kind} disposition needs its reason")
    return problems


def test_every_write_route_has_a_layout_disposition():
    assert len(inv.WRITE_ROUTES) == len(DISPOSITIONS) == 26
    assert validate_dispositions(inv.WRITE_ROUTES, DISPOSITIONS) == []


def test_a_new_route_without_a_disposition_fails():
    """Non-vacuity: the validator rejects exactly the case it exists to catch."""
    routes = list(copy.deepcopy(inv.WRITE_ROUTES))
    routes.append({**routes[0], "route_id": "WRT-synthetic-new"})
    assert validate_dispositions(routes, DISPOSITIONS) == [
        "WRT-synthetic-new: no #188 layout disposition"]
    broken = dict(DISPOSITIONS)
    broken["WRT-manage-component-dispatch"] = (TRANSITIVE, ("WRT-raw-api-component",))
    broken["WRT-raw-api-component"] = (EXEMPT, " ")
    assert validate_dispositions(inv.WRITE_ROUTES, broken) == [
        "WRT-manage-component-dispatch: a transitive route must name direct routes",
        "WRT-raw-api-component: a exempt disposition needs its reason"]


def test_the_hooks_left_the_route_inventory_reconciled():
    """No hook helper became a write sink of its own: every derived sink is still claimed."""
    census = inv.build_inventory(include_served=False)["census"]
    reconciliation = inv.reconcile_routes(census)
    assert reconciliation["unclassified"] == []
    assert reconciliation["stale_claims"] == []
    assert inv.dormant_writer_callers(census) == []


def test_the_raw_api_passthrough_never_arranges():
    for module in ("src/boomi_mcp/categories/meta_tools.py", "src/boomi_mcp/categories/folders.py"):
        source = (Path(__file__).resolve().parent.parent / module).read_text(encoding="utf-8")
        for marker in ("boomi_mcp.arrange", "..arrange", "arrange_process_body", "arrange_for_submission"):
            assert marker not in source, (module, marker)


# ----------------------------------------------------------------------------- transport spies

#: A recorded body with unset dragpoints: a second arrangement would move them again (p09).
PROCESS = (FIXTURES / "before" / "p09_unset_dragpoints.saved.xml").read_text(encoding="utf-8")
ARRANGED = arrange_process_body(PROCESS)[0]
UNARRANGEABLE = (FIXTURES / "before" / "p17_orphan_cycle.saved.xml").read_text(encoding="utf-8")
CONNECTOR = ('<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="connector-settings" '
             'name="c"><bns:object><X xmlns="" a="1"/></bns:object></bns:Component>')


def _created(name="AFL", cid="new-id"):
    return (f'<bns:Component xmlns:bns="http://api.platform.boomi.com/" componentId="{cid}" '
            f'name="{name}" type="process" version="1"/>').encode()


class _Api:
    def __init__(self, live=PROCESS, version=3):
        self.live, self.version = live, version
        self.created, self.updated = [], []

    def create_component(self, xml):
        self.created.append(xml)
        return _created()

    def update_component_raw(self, cid, xml):
        self.updated.append((cid, xml))
        self.live, self.version = xml, self.version + 1
        return _created(cid=cid)

    def get_component(self, cid):
        import re
        body = self.live
        if ' version="' in body[:400]:
            body = re.sub(r' version="\d+"', f' version="{self.version}"', body, count=1)
        else:
            body = body.replace("<bns:Component ", f'<bns:Component version="{self.version}" ', 1)
        return body.encode()


class _Client:
    def __init__(self, **kw):
        self.component = _Api(**kw)


def _sent(client):
    return [x for x in client.component.created] + [x for _, x in client.component.updated]


def _arranged_once(xml):
    """The bytes are exactly ONE arrangement of the input: connected geometry and unset
    dragpoints both match, so a second pass (which moves p09's unset dragpoints) is excluded."""
    return xml == ARRANGED


@pytest.mark.parametrize("call", [
    lambda c, m: manage_component.manage_component_action(c, "p", "create", config={"xml": PROCESS, **m}),
    lambda c, m: manage_component.manage_component_action(c, "p", "update", component_id="x", config={"xml": PROCESS, **m}),
    lambda c, m: connectors.manage_connector_action(c, "p", "create", config={"xml": PROCESS, **m}),
    lambda c, m: connectors.manage_connector_action(c, "p", "update", component_id="x", config={"xml": PROCESS, **m}),
], ids=["component-create", "component-update", "connector-create", "connector-update"])
def test_raw_writes_arrange_once_and_preserve_passes_through(call):
    client = _Client()
    result = call(client, {})
    assert result["_success"], result
    assert [_arranged_once(x) for x in _sent(client)] == [True]
    assert result["layout"]["applied"] and result["layout"]["mode"] == "arrange"

    client = _Client()
    result = call(client, {"layout": "preserve"})
    assert result["_success"], result
    assert _sent(client) == [PROCESS]
    assert result["layout"] == {"applied": False, "mode": "preserve", "reason": "preserved",
                                "columns": None, "rows": None, "estimated_extent": None,
                                "warnings": []}


@pytest.mark.parametrize("action", ["create", "update"])
@pytest.mark.parametrize("router", ["component", "connector"])
def test_raw_writes_refuse_before_any_transport(router, action):
    route = (manage_component.manage_component_action if router == "component"
             else connectors.manage_connector_action)
    extra = {"component_id": "x"} if action == "update" else {}
    for body, mode, code in ((UNARRANGEABLE, None, "PROCESS_LAYOUT_UNARRANGEABLE"),
                             ("<bns:Component xmlns:bns='u' type='process'><bns:object>", None,
                              "PROCESS_LAYOUT_XML_INVALID"),
                             ("<bns:Component xmlns:bns='u' type='process'><bns:object>", "preserve",
                              "PROCESS_LAYOUT_XML_INVALID"),
                             (PROCESS, "auto", "PROCESS_LAYOUT_MODE_INVALID")):
        client = _Client()
        config = {"xml": body, **({"layout": mode} if mode else {})}
        result = route(client, "p", action, config=config, **extra)
        assert result["_success"] is False and result["error_code"] == code, result
        assert result["write_attempted"] is False
        assert _sent(client) == []
    client = _Client()
    result = route(client, "p", action, config={"xml": UNARRANGEABLE, "layout": "preserve"}, **extra)
    assert result["_success"] and _sent(client) == [UNARRANGEABLE]
    assert result["layout"]["warnings"] == ["PROCESS_LAYOUT_UNARRANGEABLE"]


def test_a_non_process_raw_write_is_byte_exact_and_reports_no_layout():
    client = _Client()
    result = manage_component.manage_component_action(client, "p", "create", config={"xml": CONNECTOR})
    assert result["_success"] and _sent(client) == [CONNECTOR] and "layout" not in result


def test_raw_create_places_before_arranging():
    client = _Client()
    result = manage_component.create_component(client, "p", {"xml": PROCESS}, folder_id="F1")
    assert result["_success"]
    (sent,) = _sent(client)
    assert 'folderId="F1"' in sent
    assert compare(sent, ARRANGED) == []


def test_a_receipt_submits_its_bytes_unchanged_and_a_stale_one_refuses():
    receipt = arrange_for_submission(PROCESS)
    client = _Client()
    result = manage_component.create_component(client, "p", {"xml": receipt.xml}, _layout_receipt=receipt)
    assert result["_success"] and _sent(client) == [receipt.xml]
    client = _Client()
    result = manage_component.create_component(client, "p", {"xml": receipt.xml + " "}, _layout_receipt=receipt)
    assert result["error_code"] == "PROCESS_LAYOUT_INTERNAL_ERROR" and _sent(client) == []


def test_clone_arranges_the_final_body_and_preserve_keeps_it():
    client = _Client()
    result = manage_component.manage_component_action(client, "p", "clone", component_id="src", config={"name": "Copy"})
    assert result["_success"] and result["layout"]["reason"] == "arranged"
    (sent,) = _sent(client)
    assert 'name="Copy"' in sent and compare(sent, ARRANGED) == []
    client = _Client()
    result = manage_component.manage_component_action(
        client, "p", "clone", component_id="src", config={"name": "Copy", "layout": "preserve"})
    (sent,) = _sent(client)
    assert compare(sent, PROCESS) == [] and result["layout"]["mode"] == "preserve"


def test_metadata_routes_keep_the_live_geometry():
    client = _Client()
    result = manage_component.manage_component_action(client, "p", "update", component_id="x", config={"name": "Renamed"})
    (sent,) = _sent(client)
    assert compare(sent, PROCESS) == [] and result["layout"]["reason"] == "metadata_only"
    client = _Client()
    connectors.manage_connector_action(client, "p", "update", component_id="x", config={"name": "Renamed"})
    (sent,) = _sent(client)
    assert compare(sent, PROCESS) == []
    from boomi_mcp.categories import folders
    client = _Client()
    folders._action_move_component(client, "p", component_id="x", target_folder_id="F2")
    (sent,) = _sent(client)
    assert compare(sent, PROCESS) == []


def test_merge_arranges_and_its_diff_shows_the_submitted_coordinates():
    client = _Client()
    result = analyze_component.analyze_component_action(
        client, "p", "merge", component_id="x", config={"source_version": 1})
    assert result["_success"], result
    (sent,) = _sent(client)
    assert compare(sent, ARRANGED) == [] and result["layout"]["reason"] == "arranged"
    assert 'x="48.0"' in result["diff_preview"]  # coordinate churn is visible
    client = _Client()
    result = analyze_component.analyze_component_action(
        client, "p", "merge", component_id="x", config={"source_version": 1, "layout": "preserve"})
    (sent,) = _sent(client)
    assert compare(sent, PROCESS) == []


# ---- build_integration: the structured update boundary


def _structured_update(client, monkeypatch, **kw):
    monkeypatch.setattr(ib, "merge_for_update", lambda current, built, policy: built)
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY)
    spec = ib.IntegrationComponentSpec(key="p", type="process", action="update", name="p", config={})
    return ib._apply_structured_update(client, "p", "x", spec, PROCESS, PROCESS_PRESERVATION_POLICY, **kw)


def test_the_structured_update_digests_the_arranged_bytes(monkeypatch):
    client = _Client()
    pre_push = []
    result = _structured_update(client, monkeypatch, on_pre_push=lambda: pre_push.append(1))
    (sent,) = _sent(client)
    assert _arranged_once(sent) and pre_push == [1]
    assert result["submitted_xml"] == sent
    assert result["submitted_xml_digest"] == "sha256:" + hashlib.sha256(sent.encode()).hexdigest()
    assert result["layout"]["reason"] == "arranged"


def test_the_structured_update_keeps_the_digest_on_a_push_failure(monkeypatch):
    client = _Client()

    def boom(cid, xml):
        client.component.updated.append((cid, xml))
        raise RuntimeError("down")
    client.component.update_component_raw = boom
    result = _structured_update(client, monkeypatch)
    (sent,) = _sent(client)
    assert result["write_attempted"] is True and _arranged_once(sent)
    assert result["submitted_xml_digest"] == "sha256:" + hashlib.sha256(sent.encode()).hexdigest()


def test_a_structured_update_layout_refusal_precedes_pre_push_and_transport(monkeypatch):
    client = _Client()
    pre_push = []
    monkeypatch.setattr(ib, "merge_for_update", lambda current, built, policy: UNARRANGEABLE)
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY)
    spec = ib.IntegrationComponentSpec(key="p", type="process", action="update", name="p", config={})
    result = ib._apply_structured_update(client, "p", "x", spec, PROCESS, PROCESS_PRESERVATION_POLICY,
                                         on_pre_push=lambda: pre_push.append(1))
    assert result["error_code"] == "PROCESS_LAYOUT_UNARRANGEABLE"
    assert result["write_attempted"] is False and pre_push == [] and _sent(client) == []


def test_the_structured_update_preserves_on_request(monkeypatch):
    client = _Client()
    result = _structured_update(client, monkeypatch, layout_mode="preserve")
    assert _sent(client) == [PROCESS] and result["layout"]["mode"] == "preserve"


def test_build_integration_refuses_an_invalid_layout_before_dispatch():
    result = ib.build_integration_action(object(), "p", "apply", {"layout": "sideways"})
    assert result["error_code"] == "PROCESS_LAYOUT_MODE_INVALID" and result["write_attempted"] is False
    result = ib.build_integration_action(object(), "p", "apply", {"layout": "arrange"}, layout_mode="preserve")
    assert result["error_code"] == "PROCESS_LAYOUT_MODE_INVALID"


def test_build_integration_strips_layout_before_planning(monkeypatch):
    seen = []
    monkeypatch.setattr(ib, "_build_plan", lambda client, cfg: seen.append(cfg) or {"_success": True})
    monkeypatch.setattr(ib, "_apply_plan", lambda client, profile, cfg, **kw: seen.append((cfg, kw)) or {"_success": True})
    cfg = {"components": [], "layout": "preserve"}
    ib.build_integration_action(object(), "p", "plan", cfg)
    ib.build_integration_action(object(), "p", "apply", cfg)
    assert seen == [{"components": []}, ({"components": []}, {"layout_mode": "preserve"})]
    assert cfg == {"components": [], "layout": "preserve"}  # the caller's dict is untouched


# ---- safe edit


@pytest.fixture
def safe_edit_process(monkeypatch):
    """A body patch whose structured build and merge yield PROCESS (the merge itself is
    covered elsewhere): what is under test is the layout step after it."""
    monkeypatch.setattr(safe_edit_component, "build_structured_update_xml",
                        lambda *a, **k: {"_success": True, "built_xml": PROCESS, "policy": None})
    monkeypatch.setattr(safe_edit_component, "merge_for_update", lambda current, built, policy: built)
    monkeypatch.setattr(safe_edit_component, "compare_versions",
                        lambda *a, **k: {"_success": True, "diff": {}})
    return {"component_type": "process", "config": {"process_kind": "x"}}


@pytest.mark.parametrize("mode", [None, "preserve"])
def test_safe_edit_prepare_and_apply_write_the_previewed_bytes(safe_edit_process, mode):
    patch = dict(safe_edit_process, **({"layout": mode} if mode else {}))
    client = _Client(live=PROCESS.replace('name="AFL', 'name="OLD', 1))
    prep = safe_edit_component.prepare_component_edit_action(client, "p", "x", patch)
    assert prep["_success"], prep
    apply = safe_edit_component.apply_component_edit_action(
        client, "p", "x", patch, prep["confirmation_token"], confirm_apply=True)
    assert apply["_success"], apply
    (sent,) = _sent(client)
    assert sent == (PROCESS if mode else ARRANGED)
    assert prep["layout"] == apply["layout"]
    assert prep["layout"]["mode"] == (mode or "arrange")
    if not mode:
        assert any(line.startswith("+") and 'x="48.0"' in line for line in prep["diff"])


def test_safe_edit_internal_preserve_needs_no_patch_key(safe_edit_process):
    client = _Client()
    prep = safe_edit_component.prepare_component_edit_action(client, "p", "x", safe_edit_process,
                                                             layout_mode="preserve")
    apply = safe_edit_component.apply_component_edit_action(
        client, "p", "x", safe_edit_process, prep["confirmation_token"], confirm_apply=True,
        layout_mode="preserve")
    assert apply["_success"] and _sent(client) == [PROCESS]


def test_safe_edit_refuses_old_tokens_and_changed_modes(safe_edit_process):
    import base64, json
    client = _Client()
    prep = safe_edit_component.prepare_component_edit_action(client, "p", "x", safe_edit_process)
    token = json.loads(base64.urlsafe_b64decode(prep["confirmation_token"]))
    old = {k: token[k] for k in ("component_id", "version", "base_xml_sha256", "patch_sha256")}
    old_token = base64.urlsafe_b64encode(json.dumps(old).encode()).decode()
    result = safe_edit_component.apply_component_edit_action(
        client, "p", "x", safe_edit_process, old_token, confirm_apply=True)
    assert result["error_code"] == "COMPONENT_EDIT_TOKEN_INVALID" and _sent(client) == []
    result = safe_edit_component.apply_component_edit_action(
        client, "p", "x", safe_edit_process, prep["confirmation_token"], confirm_apply=True,
        layout_mode="preserve")
    assert result["error_code"] == "COMPONENT_EDIT_TOKEN_INVALID" and _sent(client) == []


def test_safe_edit_refuses_bytes_that_differ_from_the_preview(safe_edit_process, monkeypatch):
    client = _Client()
    prep = safe_edit_component.prepare_component_edit_action(client, "p", "x", safe_edit_process)
    monkeypatch.setattr(safe_edit_component, "merge_for_update",
                        lambda current, built, policy: built.replace('userlabel="', 'userlabel="z', 1))
    result = safe_edit_component.apply_component_edit_action(
        client, "p", "x", safe_edit_process, prep["confirmation_token"], confirm_apply=True)
    assert result["error_code"] == "COMPONENT_EDIT_DRIFT_DETECTED" and _sent(client) == []


def test_safe_edit_layout_refusal_is_read_only(safe_edit_process, monkeypatch):
    monkeypatch.setattr(safe_edit_component, "merge_for_update", lambda current, built, policy: UNARRANGEABLE)
    client = _Client()
    prep = safe_edit_component.prepare_component_edit_action(client, "p", "x", safe_edit_process)
    assert prep["error_code"] == "PROCESS_LAYOUT_UNARRANGEABLE" and prep["boomi_mutation"] is False
    assert _sent(client) == []


# ---- served codes


def test_every_layout_code_is_owned_and_emitted_by_a_route():
    codes = {c for c, s in errors.ERROR_TAXONOMY.items() if s.category == "process_layout"}
    assert codes == {"PROCESS_LAYOUT_MODE_INVALID", "PROCESS_LAYOUT_XML_INVALID",
                     "PROCESS_LAYOUT_CANVAS_UNSUPPORTED", "PROCESS_LAYOUT_UNARRANGEABLE",
                     "PROCESS_LAYOUT_INTERNAL_ERROR"}
    assert {errors.ERROR_TAXONOMY[c].owner for c in codes} == {"#188"}
    emitted = set()
    for body, cfg in ((UNARRANGEABLE, {}), ("<x", {}), (PROCESS, {"layout": "?"}),
                      ("<list><process><shapes/></process></list>", {})):
        result = manage_component.manage_component_action(_Client(), "p", "create", config={"xml": body, **cfg})
        emitted.add(result["error_code"])
    receipt = arrange_for_submission(PROCESS)
    emitted.add(manage_component.create_component(
        _Client(), "p", {"xml": PROCESS}, _layout_receipt=receipt)["error_code"])
    assert emitted == codes
