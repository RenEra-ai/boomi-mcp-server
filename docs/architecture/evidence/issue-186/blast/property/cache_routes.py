"""#186 item 3 probe: a typed write into a reused / reference-only document cache, judged on typed plan,
typed compile and raw apply against a mocked account that stores the cache with a given profile.

Usage: cache_routes.py <tree>"""
import copy
import sys
from unittest import mock

tree = sys.argv[1]
sys.path.insert(0, tree + "/src")
sys.path.insert(0, tree + "/tests")
import boomi_mcp  # noqa: E402

assert boomi_mcp.__file__.startswith(tree)
import test_issue_184_component_identity as C  # noqa: E402
from boomi_mcp.authoring.workflow import _normalize_intent, compile_authoring_request_v1, plan_authoring_request_v1  # noqa: E402
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.components import _shared  # noqa: E402

P_STORED = "11111111-2222-4333-8444-555555555555"
P_OTHER = "99999999-2222-4333-8444-555555555555"


def stored_xml(profile_id, profile_type="profile.json"):
    attrs = ("" if profile_id is None else ' profile="%s"' % profile_id) + (
        "" if profile_type is None else ' profileType="%s"' % profile_type)
    return ('<?xml version="1.0" encoding="UTF-8"?><bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'componentId="%s" type="documentcache" name="stored cache"><bns:object><DocumentCache xmlns=""%s>'
            '</DocumentCache></bns:object></bns:Component>' % (C._CACHE_ID, attrs))


IR = {"version": "1", "body": {"kind": "sequence", "steps": [
    {"kind": "connector_call", "operation_ref": "$ref:op_get1"},
    {"kind": "branch", "legs": [{"steps": [], "terminal": {"kind": "cache_put", "cache_ref": "$ref:c"}},
                                {"steps": [{"kind": "message", "text": "m"}], "terminal": {"kind": "stop"}}]}]}}


def components(binding, p_a_reused):
    p_a = C._profile("p_a", "a1")
    if p_a_reused:
        p_a["component_id"] = P_STORED
    cache = C._cache("c", "p_a", "a1", binding)
    if binding == "by_name":
        cache = C._named(C._cache("c", "p_a", "a1", "new"))
    op = C._get("op_get1", "1bdb1503-2807-4771-b1b7-8689be8f8e0a")
    op["depends_on"] = ["conn", "p_a"]
    op["config"]["response_profile_id"] = "$ref:p_a"
    return [C._CONN, op, p_a, cache]


def run(binding, p_a_reused, xml, read_fails=False):
    request = C._route_request(components(binding, p_a_reused), IR)
    rows = [{"component_id": C._CACHE_ID, "name": C._STORED, "type": "documentcache", "folder_name": "f"}]
    if p_a_reused:
        rows.append({"component_id": P_STORED, "name": "p_a", "type": "profile.json", "folder_name": "f"})
    reads, writes = [], []

    def get_xml(_client, component_id, *a, **k):
        reads.append(component_id)
        if read_fails:
            raise RuntimeError("read failed")
        return {"xml": xml, "component_id": component_id, "version": 1}

    def execute(*args, **kwargs):
        comp = kwargs.get("comp") or (args[1] if len(args) > 1 else None)
        writes.append(getattr(comp, "key", None))
        target = kwargs.get("target_id")
        return {"_success": True, "component_id": target or "NEW-" + str(len(writes)),
                "status": "updated" if target else "created"}

    patches = lambda: (mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(r) for r in rows]),  # noqa: E731
                       mock.patch.object(integration_builder, "_execute_component", side_effect=execute),
                       mock.patch.object(_shared, "component_get_xml", side_effect=get_xml))
    out = {}
    a, b, c = patches()
    with a, b, c:
        planned = plan_authoring_request_v1(request, boomi_client=mock.MagicMock(), profile="qa", account_id="qa_account")[0]
    out["plan"] = sorted({(code, e.path) for e in planned.errors for code in e.cause_codes})
    out["plan_reads"] = len(reads)
    a, b, c = patches()
    with a, b, c:
        try:
            compiled = compile_authoring_request_v1(request, boomi_client=mock.MagicMock(), profile="qa", account_id="qa_account")[0]
            out["compile"] = sorted({(code, e.path) for e in compiled.errors for code in e.cause_codes})
        except Exception as exc:  # noqa: BLE001
            out["compile"] = "raised %s %s" % (type(exc).__name__, str(exc)[:200])
    del writes[:]
    a, b, c = patches()
    with a, b, c:
        try:
            result = integration_builder._apply_plan(mock.MagicMock(), "qa", {
                "dry_run": False, "conflict_policy": "reuse",
                "integration_spec": _normalize_intent(request).integration_spec.model_dump(mode="json")})
        except Exception:  # noqa: BLE001 - past the pre-write pass the offline harness cannot create a process
            result = {"_success": False, "error_code": None}
    out["apply"] = ("refused", result.get("error_code"), (result.get("error") or "")[:160]) if not writes else ("wrote", tuple(writes))
    return out


for binding in ("create_by_id", "reference_only", "by_name"):
    for label, p_a_reused, xml, fails in (
            ("stored=P_a(reused)", True, stored_xml(P_STORED), False),
            ("stored=other", True, stored_xml(P_OTHER), False),
            ("stored=other,p_a new", False, stored_xml(P_STORED), False),
            ("stored=none", True, stored_xml(None, "profile.none"), False),
            ("stored=missing attrs", True, stored_xml(None, None), False),
            ("read fails", True, None, True),
            ("malformed", True, "<bns:Component", False)):
        print(binding, "|", label, "|", run(binding, p_a_reused, xml, fails))
