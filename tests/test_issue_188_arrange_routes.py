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
    # the apply contract's accounting fields, as on every other apply refusal (CDX-188-r1-01)
    assert (result["action"], result["mutation_status"], result["mutation_performed"]) == ("apply", "none", False)
    typed = ib.build_integration_action(object(), "p", "apply",
                                        {"layout": "sideways", "authoring_request": {"contract_version": "2"}})
    assert (typed["mutation_status"], typed["mutation_performed"]) == ("none", False)
    plan = ib.build_integration_action(object(), "p", "plan", {"layout": "sideways"})
    assert plan["error_code"] == "PROCESS_LAYOUT_MODE_INVALID" and "mutation_status" not in plan
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


# ---- build_integration: the two process CREATE arms, through the real create_component

from unittest.mock import MagicMock, patch  # noqa: E402

from _m12_11_support import appliable_process_ir_request  # noqa: E402
from boomi_mcp.arrange import layout as _arrange_layout  # noqa: E402
from boomi_mcp.authoring.workflow import compile_authoring_request_v1  # noqa: E402
from boomi_mcp.categories.components import manage_component as _mc  # noqa: E402

_PROC_ID = "process-cid-188"


def _typed_payload():
    request = appliable_process_ir_request()
    with patch.object(ib, "paginate_metadata", return_value=[]):  # a MagicMock pages forever
        result, _ = compile_authoring_request_v1(request, boomi_client=MagicMock(), profile="p")
    payload = request.model_dump(mode="json")
    payload["expected_capability_revision"] = result.revision_binding.capability_revision
    payload["expected_compile_hash"] = result.revision_binding.compile_hash
    return payload


def _typed_apply(*, layout=None, layout_mode=None, arrange_hook=None):
    """A real typed apply; the non-process components and the transport are faked. The
    process root reaches the REAL create_component, whose raw transport is spied."""
    sent, arranged_inputs, arrange_calls = [], [], []
    real_arrange_for_submission = ib.arrange_for_submission

    def spy_submission(xml, mode):
        arranged_inputs.append(xml)
        if arrange_hook is not None:
            arrange_hook(xml, mode)
        return real_arrange_for_submission(xml, mode)

    real_layout_arrange = _arrange_layout.arrange

    def count_layout(shapes):
        arrange_calls.append(1)
        return real_layout_arrange(shapes)

    def raw_create(_client, xml, **_kw):
        sent.append(xml)
        return {"component_id": _PROC_ID, "name": "p", "type": "process", "sub_type": "",
                "folder_name": "", "version": "1"}

    def live(_client, component_id, *_a, **_k):
        if component_id == _PROC_ID and sent:
            return {"type": "process", "xml": sent[-1]}
        return {"type": "connector-settings",
                "xml": '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
                       'type="connector-settings"><bns:object><c/></bns:object></bns:Component>'}

    counter = {"n": 0}

    def other(*_a, **_k):
        counter["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % counter["n"]}

    config = {"authoring_request": _typed_payload(), "dry_run": False}
    if layout is not None:
        config["layout"] = layout
    ib._BUILD_REGISTRY.clear()
    with patch.object(ib, "paginate_metadata", return_value=[]), \
            patch.object(ib, "_execute_component", side_effect=other), \
            patch.object(ib, "component_get_xml", side_effect=live), \
            patch.object(ib, "arrange_for_submission", side_effect=spy_submission), \
            patch.object(_arrange_layout, "arrange", side_effect=count_layout), \
            patch.object(_mc, "_create_component_raw", side_effect=raw_create):
        kwargs = {"layout_mode": layout_mode} if layout_mode else {}
        result = ib.build_integration_action(MagicMock(), "p", "apply", config=config, **kwargs)
    return result, sent, arranged_inputs, arrange_calls


def test_a_process_ir_create_is_arranged_once_and_attested_over_the_arranged_bytes():
    result, sent, inputs, calls = _typed_apply()
    assert result["_success"], result.get("error")
    (submitted,) = sent
    (emitted,) = inputs
    assert submitted == arrange_process_body(emitted)[0]
    assert submitted != emitted, "non-vacuous: the emitter's geometry is not Arrange's"
    assert len(calls) == 1, "the body was arranged more than once in one call chain"
    (mutation,) = result["process_mutations"]
    assert mutation["submitted_xml_digest"] == "sha256:" + hashlib.sha256(submitted.encode()).hexdigest()
    assert result["results"]["proc"]["layout"]["reason"] == "arranged"


@pytest.mark.parametrize("how", ["config", "internal"])
def test_a_process_ir_create_preserves_on_request(how):
    result, sent, inputs, _ = _typed_apply(**({"layout": "preserve"} if how == "config"
                                              else {"layout_mode": "preserve"}))
    assert result["_success"], result.get("error")
    assert sent == inputs
    assert result["results"]["proc"]["layout"]["mode"] == "preserve"


def test_a_process_ir_layout_refusal_writes_nothing_for_the_root_and_keeps_earlier_writes():
    from boomi_mcp.arrange import ProcessLayoutError

    def refuse(xml, mode):
        raise ProcessLayoutError("PROCESS_LAYOUT_UNARRANGEABLE", "synthetic")

    result, sent, _, _ = _typed_apply(arrange_hook=refuse)
    assert result["_success"] is False
    assert sent == []
    partial = result.get("partial_results") or result.get("results") or {}
    assert partial["proc"]["status"] == "refused"
    assert partial["proc"]["result"]["error_code"] == "PROCESS_LAYOUT_UNARRANGEABLE"
    # the two dependency components written before the root stay accounted for
    assert {k for k, v in partial.items() if (v.get("component_id") or "").startswith("cid-")} == {"conn", "op"}
    assert not result.get("process_mutations")


def _legacy_process():
    return ib.IntegrationComponentSpec(
        key="p1", type="process", action="create", name="Legacy",
        config={"name": "Legacy", "process_kind": "wrapper_subprocess",
                "process_calls": [{"process_id": "99999999-9999-9999-9999-999999999999"}]})


@pytest.mark.parametrize("mode", ["arrange", "preserve"])
def test_a_legacy_process_create_is_placed_then_arranged_once(mode):
    comp = _legacy_process()
    client = _Client()
    real = ib._execute_component

    with patch.object(_mc, "arrange_process_body", wraps=_mc.arrange_process_body) as spy:
        result = real(client, "p", comp, comp.config,
                      placement=ib._ComponentPlacement(folder_id="F9", requested_name=None),
                      layout_mode=mode)
    assert result["_success"], result
    (sent,) = _sent(client)
    assert spy.call_count == 1
    (call,) = spy.call_args_list
    assert 'folderId="F9"' in call.args[0], "placement precedes arrangement"
    if mode == "arrange":
        assert sent == arrange_process_body(call.args[0])[0] != call.args[0]
    else:
        assert sent == call.args[0]
    assert result["layout"]["mode"] == mode


# ---- one explicit internal-caller preserve per hook (#182 forward contract)


def test_every_hook_accepts_an_explicit_internal_preserve(monkeypatch):
    calls = {
        "create_component": lambda c: manage_component.create_component(c, "p", {"xml": PROCESS}, layout_mode="preserve"),
        "update_component": lambda c: manage_component.update_component(c, "p", "x", {"xml": PROCESS}, layout_mode="preserve"),
        "clone_component": lambda c: manage_component.clone_component(c, "p", "x", {"name": "C"}, layout_mode="preserve"),
        "create_connector": lambda c: connectors.create_connector(c, "p", {"xml": PROCESS}, layout_mode="preserve"),
        "update_connector": lambda c: connectors.update_connector(c, "p", "x", {"xml": PROCESS}, layout_mode="preserve"),
        "merge_versions": lambda c: analyze_component.merge_versions(c, "p", "x", {"source_version": 1}, layout_mode="preserve"),
        "manage_component_action": lambda c: manage_component.manage_component_action(c, "p", "create", layout_mode="preserve", config={"xml": PROCESS}),
        "manage_connector_action": lambda c: connectors.manage_connector_action(c, "p", "create", layout_mode="preserve", config={"xml": PROCESS}),
        "analyze_component_action": lambda c: analyze_component.analyze_component_action(c, "p", "merge", component_id="x", layout_mode="preserve", config={"source_version": 1}),
    }
    for name, call in calls.items():
        client = _Client()
        result = call(client)
        assert result["_success"], (name, result)
        (sent,) = _sent(client)
        assert compare(sent, PROCESS) == [], name
        assert result["layout"]["mode"] == "preserve", name
    # _apply_structured_update, _execute_canonical_process, _execute_component, safe edit and
    # build_integration_action are covered above with layout_mode="preserve".


# ---- #188 architect review, evaluation 1


def test_a_canonical_create_whose_transport_fails_was_arranged_once_and_attests_the_sent_bytes():
    """Exactly-once and the digest on the FAILURE arm, through the real create_component and
    its receipt (the earlier canonical-failure tests replace create_component entirely)."""
    import hashlib as _hashlib

    sent = []

    def raw_create_fails(_client, xml, **_kw):
        sent.append(xml)
        raise RuntimeError("connection reset after the body left")

    calls = []
    real_layout_arrange = _arrange_layout.arrange

    def count_layout(shapes):
        calls.append(1)
        return real_layout_arrange(shapes)

    emitted = []
    real_submission = ib.arrange_for_submission

    def spy(xml, mode):
        emitted.append(xml)
        return real_submission(xml, mode)

    counter = {"n": 0}

    def other(*_a, **_k):
        counter["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % counter["n"]}

    ib._BUILD_REGISTRY.clear()
    config = {"authoring_request": _typed_payload(), "dry_run": False}
    with patch.object(ib, "paginate_metadata", return_value=[]), \
            patch.object(ib, "_execute_component", side_effect=other), \
            patch.object(ib, "component_get_xml", side_effect=lambda *a, **k: {"type": "x", "xml": "<x/>"}), \
            patch.object(ib, "arrange_for_submission", side_effect=spy), \
            patch.object(_arrange_layout, "arrange", side_effect=count_layout), \
            patch.object(_mc, "_create_component_raw", side_effect=raw_create_fails):
        result = ib.build_integration_action(MagicMock(), "p", "apply", config=config)
    assert result["_success"] is False
    (submitted,) = sent
    assert submitted == arrange_process_body(emitted[0])[0] and len(calls) == 1
    assert result["mutation_status"] != "none"  # dependencies were written; the root's write is uncertain
    partial = result.get("partial_results") or {}
    assert partial["proc"]["status"] == "failed"
    # the write note of an unidentified, mutation-uncertain submission carries the digest of
    # the bytes that left — the arranged ones
    notes = (result.get("process_writes") or []) + (result.get("process_mutations") or [])
    digests = {n.get("submitted_xml_digest") for n in notes}
    assert "sha256:" + _hashlib.sha256(submitted.encode()).hexdigest() in digests, notes


def _write_calls_without_layout_mode(source: str):
    """Names of component-write calls inside ``_execute_component`` that do not forward
    ``layout_mode`` (trading partners take a typed model, not a component body)."""
    import ast
    import textwrap

    tree = ast.parse(textwrap.dedent(source))
    func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_execute_component")
    writes = {"create_component", "update_component", "create_connector", "update_connector",
              "_apply_structured_update"}
    missing = []
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in writes:
            if not any(k.arg == "layout_mode" for k in node.keywords):
                missing.append((node.func.id, node.lineno))
    return missing


def test_every_component_write_in_execute_component_forwards_the_layout_mode():
    import inspect
    assert _write_calls_without_layout_mode(inspect.getsource(ib._execute_component)) == []


def test_the_forwarding_check_catches_a_dropped_layout_mode():
    """Non-vacuity: the exact defect architect evaluation 1 found (a structured connector
    update dropping the mode) is caught."""
    import inspect
    source = inspect.getsource(ib._execute_component)
    broken = source.replace("layout_mode=layout_mode,", "", 1)
    assert broken != source
    assert _write_calls_without_layout_mode(broken)


def test_a_structured_connector_update_keeps_preserve_for_a_process_sibling(monkeypatch):
    """Architect evaluation 1, finding 1: a structured connector update whose merged body
    carries an ``object/process`` sibling must honour ``preserve``."""
    with_process = PROCESS  # stands in for the merged body
    monkeypatch.setattr(ib, "merge_for_update", lambda current, built, policy: with_process)
    seen = []
    real = ib._apply_structured_update

    def spy(*a, **k):
        seen.append(k.get("layout_mode"))
        return real(*a, **k)

    monkeypatch.setattr(ib, "_apply_structured_update", spy)
    comp = ib.IntegrationComponentSpec(
        key="conn", type="connector-settings", action="update", name="REST",
        config={"connector_type": "rest", "component_name": "REST", "base_url": "https://api.example.com",
                "auth": "NONE"})
    client = _Client()
    client.connector = MagicMock()  # connector-type validation reads the account's catalog
    result = ib._execute_component(client, "p", comp, comp.config, target_id="x", layout_mode="preserve")
    assert seen == ["preserve"], (seen, result)
    assert _sent(client) == [PROCESS]
