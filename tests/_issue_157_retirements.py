"""Issue #157 (M12.19) — the legacy-baseline inertness proof behind each RETIRE.

Not a ``test_*`` module. It is the library ``tests/test_issue_157_intent_retirements.py``
imports and the CLI that freezes the retirement records:

    PYTHONPATH=src .venv/bin/python tests/_issue_157_retirements.py --freeze
    PYTHONPATH=src .venv/bin/python tests/_issue_157_retirements.py --check

**What a retirement record proves.** For one legacy metadata spelling, a PAIR of
archetype payloads differing ONLY in that field is run through the LEGACY chain
fully offline — the archetype contract, ``build_from_archetype_action``, the
component-plan lint (``build_integration_action(plan)``) and the real legacy
apply loop with the create boundary mocked to CAPTURE the exact bytes it would
send — and the record freezes, for base and varied: the sha256 of every
component's emitted XML (the process included), the plan verdict digest with
the spec echo stripped, and where the field surfaced in the spec echo. A
spelling earns ``RETIRE`` only when every byte and the verdict are equal AND a
non-vacuity control (a WIRED sibling field varied under the same harness) moves
a digest. The spec echo is expected to move — that is the legacy
``integration_spec`` intent's hash churn the issue anticipates — and is
recorded separately, never counted as inertness.

**Provenance.** ``--freeze`` runs the pairs from a pristine ``git archive`` of
the baseline (the same discipline as the flows corpus); ``--check`` and the test
replay every pair against HEAD's legacy producers, which must agree until #160
deletes them. Measurements were first taken by ten independent measurement
agents and re-run in the main thread; the harness here is the in-tree, reviewable
form of the one they converged on.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parent.parent
RETIREMENTS_DIR = ROOT / "tests" / "fixtures" / "governance" / "issue_157" / "retirements"
INDEX_PATH = RETIREMENTS_DIR / "INDEX.json"
BASELINE_SHA = "ba1be9f34317d720d3b012484622ab7e2742143b"

#: Every candidate the triage measured, with how its pair is built from the
#: committed `database_to_api_sync` example `minimal_manual_sync` (the base; a
#: `base_set` edit when the candidate needs a context first, e.g. a scheduled
#: trigger before `cron` can exist) and the single varied field.
#: `expected` is the classification the frozen record must carry.
CANDIDATES: Mapping[str, Mapping[str, Any]] = {
    "RET-157-01": {"spelling": "jdbc_options", "field_path": "source/binding/settings/jdbc_options",
                   "value": {"encrypt": "true", "trustServerCertificate": "true"}, "expected": "RETIRE",
                   "replacement": "none — connection creation settings are authored on the connection component; a reused connection carries binding keys only"},
    "RET-157-02": {"spelling": "fetch_size", "field_path": "source/read_operation/fetch_size", "value": 500, "expected": "RETIRE",
                   "replacement": "none — the DB Get operation builder exposes no fetch size"},
    "RET-157-03": {"spelling": "link_element", "field_path": "source/read_operation/link_element", "value": "Items/Item", "expected": "RETIRE",
                   "replacement": "none — no DB Get operation builder field"},
    "RET-157-04": {"spelling": "sql_type", "field_path": "source/read_operation/parameters/0/sql_type", "value": "TIMESTAMP",
                   "base_set": {"source/read_operation/parameters": [{"name": ":since"}]}, "expected": "RETIRE",
                   "replacement": "none — the read profile carries parameter names and mappability only"},
    "RET-157-05": {"spelling": "direction", "field_path": "source/read_operation/parameters/0/direction", "value": "out",
                   "base_set": {"source/read_operation/parameters": [{"name": ":since"}]}, "expected": "RETAIN",
                   "replacement": "retained — sets the read profile parameter's mappable flag and moves the emitted profile XML"},
    "RET-157-06": {"spelling": "cron", "field_path": "execution/trigger/schedule/cron", "value": "30 4 * * 1",
                   "base_set": {"execution/trigger": {"mode": "scheduled", "schedule": {"cron": "0 2 * * *"}}}, "expected": "RETIRE",
                   "replacement": "none — schedule activation is a future topology capability, deliberately unscheduled"},
    "RET-157-07": {"spelling": "run_metadata", "field_path": "execution/run_metadata",
                   "value": {"owner": "crm-team", "runbook": "https://runbooks.example.com/db-to-api"}, "expected": "RETIRE",
                   "replacement": "RuntimeHintDeclarationV1 (recorded_intents) for a deliberate, recorded note"},
    # The watermark-sourced query parameter is an ADDITIVE spelling: the pair is
    # "no query parameter" vs "one watermark-sourced parameter", and inertness
    # means the spelling emits nothing (the legacy chain recorded it as
    # `deferred_to #51` metadata). A literal-vs-watermark pair would measure the
    # LITERAL value leaving the bytes, which is not the question.
    "RET-157-08": {"spelling": "value_source=watermark", "field_path": "target/send_request/query_parameters",
                   "value": [{"name": "since", "value_source": "watermark"}],
                   "base_set": {"execution/watermark": {"field": "source_field_a", "kind": "timestamp", "persistence": "dpp"}},
                   "expected": "RETIRE",
                   "replacement": "WatermarkDeclarationV1.query_parameter_refs (recorded, not wired); the REST query-parameter binding half stays deferred past M12"},
    "RET-157-09": {"spelling": "runtime_hints", "field_path": "naming/runtime_hints",
                   "value": {"atom_pool": "primary", "environment_tag": "qa"}, "expected": "RETIRE",
                   "replacement": "RuntimeHintDeclarationV1 (recorded_intents)"},
    "RET-157-10": {"spelling": "watermark (legacy fragment: dpp_name / store_ref / persistence=dpp)", "field_path": "execution/watermark",
                   "value": {"field": "source_field_a", "kind": "timestamp", "persistence": "dpp"}, "expected": "SPLIT",
                   "replacement": "WatermarkDeclarationV1 on the per-root envelope (the sole accepted spelling) + SetDppNodeV1.persist for the executable persistence"},
}

ARCHETYPE = "database_to_api_sync"
EXAMPLE = "minimal_manual_sync"
CONTROL = {"field_path": "source/read_operation/batch_size", "value": 7}

#: The measurement program. Executed under the extraction's src for `--freeze`
#: and under HEAD's src for `--check` / the test; it imports only the legacy
#: modules it measures and returns one JSON report per candidate.
MEASURE_PROGRAM = r'''
import copy, hashlib, json, os, sys
os.environ.setdefault("BOOMI_LOCAL", "true")
from unittest.mock import MagicMock, patch
from boomi_mcp.patterns.registry import PatternRegistry
from boomi_mcp.patterns.base import PatternKind
from boomi_mcp.categories.integration_authoring import build_from_archetype_action
from boomi_mcp.categories import integration_builder as ib

spec_in = json.load(sys.stdin)
ARCHETYPE, EXAMPLE, CANDIDATES, CONTROL = spec_in["archetype"], spec_in["example"], spec_in["candidates"], spec_in["control"]


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def pointer_set(doc, pointer, value):
    parts = [p for p in pointer.split("/") if p]
    cursor = doc
    for part in parts[:-1]:
        if isinstance(cursor, list):
            cursor = cursor[int(part)]
        else:
            cursor = cursor.setdefault(part, {})
    if isinstance(cursor, list):
        cursor[int(parts[-1])] = value
    else:
        cursor[parts[-1]] = value


def flatten(obj, path=""):
    if isinstance(obj, dict):
        acc = {}
        for k, v in obj.items():
            acc.update(flatten(v, path + "/" + str(k)))
        return acc
    if isinstance(obj, list):
        acc = {}
        for i, v in enumerate(obj):
            acc.update(flatten(v, path + "/" + str(i)))
        return acc
    return {path: obj}


def make_client(store):
    client = MagicMock(name="offline-boomi-client")
    counter = {"n": 0}

    def create_component(xml):
        counter["n"] += 1
        cid = "00000000-0000-0000-0000-%012d" % counter["n"]
        store.append((cid, xml))
        return ('<bns:Component xmlns:bns="http://api.platform.boomi.com/" componentId="%s" '
                'name="placeholder" type="placeholder" version="1" folderName=""/>' % cid).encode("utf-8")

    client.component.create_component.side_effect = create_component
    return client


def assert_no_other_mutation(client):
    forbidden = ("update_component", "delete", "deploy", "execute", "clone")
    bad = [str(c) for c in client.mock_calls if any(f in str(c) for f in forbidden)]
    assert not bad, bad


def plan_offline(spec):
    client = make_client([])
    with patch.object(ib, "paginate_metadata", return_value=[]):
        plan = ib.build_integration_action(client, "renera", "plan", {"integration_spec": copy.deepcopy(spec)})
    assert_no_other_mutation(client)
    assert client.component.create_component.call_count == 0
    return plan


def apply_capture(spec):
    store = []
    client = make_client(store)
    with patch.object(ib, "paginate_metadata", return_value=[]):
        result = ib.build_integration_action(client, "renera", "apply", {"integration_spec": copy.deepcopy(spec), "dry_run": False})
    assert_no_other_mutation(client)
    assert result.get("_success"), canon({k: v for k, v in result.items() if k != "integration_spec"})[:1500]
    by_id = dict(store)
    digests = {}
    for key, rec in (result.get("results") or {}).items():
        assert rec.get("status") == "created" and rec.get("component_id") in by_id, (key, rec)
        digests[key] = sha(by_id[rec["component_id"]])
    return digests


def measure(base, varied):
    registry = PatternRegistry.from_package("boomi_mcp.patterns")
    cls = registry.get(ARCHETYPE, kind=PatternKind.ARCHETYPE)
    try:
        cls.validate_parameters(copy.deepcopy(varied))
    except Exception as exc:  # noqa: BLE001
        return {"accepted_by_contract": False, "refusal": str(exc)[:500]}
    rb = build_from_archetype_action(ARCHETYPE, copy.deepcopy(base))
    rv = build_from_archetype_action(ARCHETYPE, copy.deepcopy(varied))
    assert rb.get("_success") and rv.get("_success"), (rb.get("error"), rv.get("error"))
    sb, sv = rb["integration_spec"], rv["integration_spec"]
    fsb, fsv = flatten(sb), flatten(sv)
    echo = sorted(k for k in set(fsb) | set(fsv) if fsb.get(k) != fsv.get(k))
    pb, pv = plan_offline(sb), plan_offline(sv)
    strip = lambda p: {k: v for k, v in p.items() if k != "integration_spec"}
    db, dv = apply_capture(sb), apply_capture(sv)
    types = {c["key"]: c["type"] for c in sb["components"]}
    return {
        "accepted_by_contract": True,
        "spec_echo_paths": echo,
        "plan_verdict_digest": {"base": sha(canon(strip(pb))), "varied": sha(canon(strip(pv)))},
        "xml_digests": {k: {"base": db[k], "varied": dv.get(k)} for k in sorted(db)},
        "component_types": types,
        "xml_moved": sorted(k for k in db if dv.get(k) != db[k]),
        "spec_digest": {"base": sha(canon(sb)), "varied": sha(canon(sv))},
    }


registry = PatternRegistry.from_package("boomi_mcp.patterns")
cls = registry.get(ARCHETYPE, kind=PatternKind.ARCHETYPE)
example = copy.deepcopy(next(e.parameters for e in cls.examples if e.name == EXAMPLE))
out = {}
for rid, spec in CANDIDATES.items():
    base = copy.deepcopy(example)
    for pointer, value in (spec.get("base_set") or {}).items():
        pointer_set(base, pointer, copy.deepcopy(value))
    varied = copy.deepcopy(base)
    pointer_set(varied, spec["field_path"], copy.deepcopy(spec["value"]))
    fb, fv = flatten(base), flatten(varied)
    pair_diff = sorted(k for k in set(fb) | set(fv) if fb.get(k) != fv.get(k))
    assert all(k.startswith("/" + spec["field_path"]) for k in pair_diff), (rid, pair_diff)
    report = measure(base, varied)
    report.update({"pair_base": base, "pair_varied": varied, "pair_diff": pair_diff})
    out[rid] = report
# the non-vacuity control: a WIRED field moves a digest under the same harness
control_base = copy.deepcopy(example)
control_varied = copy.deepcopy(example)
pointer_set(control_varied, CONTROL["field_path"], CONTROL["value"])
control = measure(control_base, control_varied)
out["__control__"] = {"field_path": CONTROL["field_path"], "xml_moved": control["xml_moved"]}
json.dump(out, sys.stdout)
'''


def _run(src_root: Path) -> Dict[str, Any]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(src_root)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["BOOMI_LOCAL"] = "true"
    payload = json.dumps({"archetype": ARCHETYPE, "example": EXAMPLE, "candidates": CANDIDATES, "control": CONTROL})
    completed = subprocess.run(
        [sys.executable, "-c", MEASURE_PROGRAM], input=payload, text=True, capture_output=True,
        env=env, cwd=str(src_root.parent), check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr[-4000:])
    return json.loads(completed.stdout)


def classify(report: Mapping[str, Any], control_moved: Sequence[str]) -> str:
    if not report.get("accepted_by_contract"):
        return "NOT_ACCEPTED"
    inert = (
        not report["xml_moved"]
        and report["plan_verdict_digest"]["base"] == report["plan_verdict_digest"]["varied"]
    )
    if inert and control_moved:
        return "RETIRE"
    if inert:
        return "RETAIN"  # the harness proved nothing (vacuous control)
    return "SPLIT"


def _extract_baseline(sha: str, into: Path) -> str:
    tree = subprocess.run(["git", "rev-parse", sha + "^{tree}"], cwd=str(ROOT), text=True, capture_output=True, check=True).stdout.strip()
    archive = subprocess.run(["git", "archive", sha], cwd=str(ROOT), capture_output=True, check=True).stdout
    subprocess.run(["tar", "-x", "-C", str(into)], input=archive, check=True)
    return tree


def freeze() -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="issue157-retire-") as tmp:
        extraction = Path(tmp) / "tree"
        extraction.mkdir()
        tree_sha = _extract_baseline(BASELINE_SHA, extraction)
        reports = _run(extraction / "src")
    control_moved = reports.pop("__control__")["xml_moved"]
    RETIREMENTS_DIR.mkdir(parents=True, exist_ok=True)
    index: Dict[str, Any] = {}
    for rid, spec in CANDIDATES.items():
        report = reports[rid]
        measured = classify(report, control_moved)
        classification = spec["expected"]
        # a SPLIT decision may sit on an inert measurement (the watermark object
        # is inert AND retained as a typed declaration); every other expected
        # classification must equal the measured one.
        if classification == "SPLIT":
            # the watermark object is inert on the legacy chain AND kept as a
            # typed declaration: SPLIT is a design decision over an inert fact
            assert measured in ("RETIRE", "SPLIT"), (rid, measured)
        elif classification == "RETAIN":
            # `direction` moves emitted bytes: NOT inert, so it can never be
            # retired; the design decision is to keep the legacy field as is
            assert measured == "SPLIT", (rid, measured)
        else:
            assert measured == classification, (rid, measured, classification)
        record = {
            "id": rid,
            "spelling": spec["spelling"],
            "field_path": spec["field_path"],
            "classification": classification,
            "measured_classification": measured,
            "producer": "{0} example {1} (legacy chain at {2})".format(ARCHETYPE, EXAMPLE, BASELINE_SHA[:7]),
            "baseline_sha": BASELINE_SHA,
            "baseline_tree_sha": tree_sha,
            "pair_base": report.get("pair_base"),
            "pair_varied": report.get("pair_varied"),
            "pair_diff": report.get("pair_diff"),
            "observations": {
                "accepted_by_contract": report.get("accepted_by_contract"),
                "emitted_xml_digests": report.get("xml_digests"),
                "component_types": report.get("component_types"),
                "xml_moved": report.get("xml_moved"),
                "plan_verdict_digest": report.get("plan_verdict_digest"),
                "spec_echo_paths": report.get("spec_echo_paths"),
                "spec_digest": report.get("spec_digest"),
                "non_vacuity_control": {"field_path": CONTROL["field_path"], "xml_moved": control_moved},
            },
            "replacement": spec["replacement"],
            "refusal_code_on_typed_surface": "GOVERNANCE_RETIRED_SPELLING" if classification == "RETIRE" else None,
            "cutover_owner": "#160",
        }
        (RETIREMENTS_DIR / (rid + ".json")).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        index[rid] = {"spelling": spec["spelling"], "field_path": spec["field_path"], "classification": classification}
    INDEX_PATH.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return index


def load_records() -> Dict[str, Dict[str, Any]]:
    return {
        path.stem: json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(RETIREMENTS_DIR.glob("RET-*.json"))
    }


def load_index() -> Dict[str, Any]:
    return json.loads(INDEX_PATH.read_text(encoding="utf-8"))


def replay_at_head() -> Dict[str, Any]:
    """Re-measure every pair against HEAD's legacy producers (valid until #160)."""
    reports = _run(ROOT / "src")
    return reports


def check() -> List[str]:
    problems = []
    records = load_records()
    reports = replay_at_head()
    control_moved = reports.pop("__control__")["xml_moved"]
    if not control_moved:
        problems.append("non-vacuity control moved no digest at HEAD")
    for rid, record in records.items():
        report = reports.get(rid)
        if report is None:
            problems.append("no replay for " + rid)
            continue
        if classify(report, control_moved) != record["measured_classification"]:
            problems.append("classification drift at HEAD: " + rid)
        frozen = record["observations"]["emitted_xml_digests"]
        if report.get("xml_digests") != frozen:
            problems.append("emitted XML digests drift at HEAD: " + rid)
    return problems


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.freeze:
        index = freeze()
        print(json.dumps(index, indent=1))
        return 0
    if args.check:
        problems = check()
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
