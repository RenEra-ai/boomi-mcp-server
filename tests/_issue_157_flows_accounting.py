"""Issue #157 (M12.19) — the persistent, row-level served-flows baseline.

Not a ``test_*`` module, so pytest never collects it (precedent:
``tests/_m12_12_legacy_inventory.py``). It is BOTH the library
``tests/test_issue_157_flows_accounting.py`` imports and the CLI that freezes the
corpus:

    PYTHONPATH=src .venv/bin/python tests/_issue_157_flows_accounting.py \\
        --freeze --baseline-sha ba1be9f34317d720d3b012484622ab7e2742143b
    PYTHONPATH=src .venv/bin/python tests/_issue_157_flows_accounting.py --check

**What is frozen, and from where.** For every case in :data:`CASES` the LEGACY
producer is executed from a PRISTINE ``git archive <baseline-sha>`` extraction —
never from the working tree — and its complete ORDERED ``flows`` sequence and
its ordered ``endpoints`` + composition-provenance sequence are written under
``tests/fixtures/governance/issue_157/cases/<producer>/<case>/`` beside the
immutable legacy input that produced them. Each row is keyed
``(producer, case_id, key, kind)`` and keeps its ordinal; the payload is stored
both raw and in the ONE normalized representation
(:func:`boomi_mcp.authoring.derived_flows.normalize_flow_row`) with its digest.

**Why a persistent baseline and not recipe-level parity.** One (producer,
key/kind) legally produces DIFFERENT payloads across input cases — the
composition fan-out row carries ``legs``, the reliability row's ``target``
toggles on DLQ, the watermark row is conditional — so a single frozen payload
per (producer, key/kind) would either collide or silently stand for one
arbitrary variant. The case axis is what makes the accounting honest.

**Two-sided, ordered accounting** (:func:`discharge_case`). Every frozen row has
exactly ONE status in the case's ``discharges.json``: ``discharged`` (the
canonical projection of the case's ``input.canonical.json`` reproduces the
normalized payload EXACTLY), ``retired:<id>`` (a typed retirement record under
``retirements/`` proves the row inert), or ``pending:#159`` (owed by the
migration that discharges it). The derived sequence must then EQUAL, in order,
the frozen sequence filtered to discharged rows: an unexpected row, a
duplicate, or a reordering fails the case even when every discharged row is
matched. The pending count is pinned in the manifest so #159 cannot shrink the
denominator silently, and the manifest digests every file so an edited input
without a re-freeze, a modified baseline row, or a deleted one fails.

**Canonical-only survival.** The frozen sequences and the canonical input
fixtures are what remain after #160 deletes the legacy producers: replay is
canonical fixture → canonical projection → frozen sequence, and nothing parses a
legacy format. The legacy inputs stay as immutable historical data; a
``provenance.json`` beside a canonical fixture records how it was derived and is
never executed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parent.parent
CORPUS_DIR = ROOT / "tests" / "fixtures" / "governance" / "issue_157"
CASES_DIR = CORPUS_DIR / "cases"
RETIREMENTS_DIR = CORPUS_DIR / "retirements"
MANIFEST_PATH = CORPUS_DIR / "MANIFEST.json"

#: The step-0 baseline the corpus was frozen from. A re-freeze against another
#: SHA is a deliberate, reviewed change to this constant.
BASELINE_SHA = "ba1be9f34317d720d3b012484622ab7e2742143b"

#: The seven legacy producers of ``IntegrationSpecV1.flows`` (issue body S17),
#: short name -> module path. Frozen as DATA: after #160 these modules are
#: gone and the names live on only here, as provenance.
PRODUCER_MODULES: Mapping[str, str] = {
    "database_to_api_sync": "boomi_mcp.patterns.archetypes.database_to_api_sync",
    "api_to_api_sync": "boomi_mcp.patterns.archetypes.api_to_api_sync",
    "api_to_database_sync": "boomi_mcp.patterns.archetypes.api_to_database_sync",
    "http_listener_to_db": "boomi_mcp.patterns.archetypes.http_listener_to_db",
    "http_listener_to_rest": "boomi_mcp.patterns.archetypes.http_listener_to_rest",
    "stub_minimal": "boomi_mcp.patterns.archetypes.stub_minimal",
    "composition": "boomi_mcp.patterns.composition",
}

#: The archetype registry name each archetype producer answers to.
ARCHETYPE_NAMES: Mapping[str, str] = {
    "database_to_api_sync": "database_to_api_sync",
    "api_to_api_sync": "api_to_api_sync",
    "api_to_database_sync": "api_to_database_sync",
    "http_listener_to_db": "http_listener_to_db",
    "http_listener_to_rest": "http_listener_to_rest",
    "stub_minimal": "stub_minimal_integration",
}

#: Differential-pair classification (issue body, in-scope item 6): which frozen
#: row each varying parameter manifests in, and HOW. A presence assertion on a
#: persistent row can never pass, so the kind is decided here, before any pair
#: is written, and the accounting test asserts each pair's kind against the
#: frozen rows.
PAIR_CLASSIFICATION: Mapping[str, Mapping[str, str]] = {
    "watermark": {"kind": "presence", "row_key": "watermark", "producer": "database_to_api_sync"},
    "dlq": {"kind": "payload", "row_key": "reliability", "field": "target", "producer": "database_to_api_sync"},
    "leg_count": {"kind": "payload", "row_key": "fanout", "field": "legs", "producer": "composition"},
    "binding_mode": {"kind": "endpoint", "row_key": "", "producer": "database_to_api_sync"},
    # #159 S2 (L4): the linear presets' and the staged fan-out's own axes
    "binding_mode_api_to_db": {"kind": "endpoint", "row_key": "", "producer": "api_to_database_sync"},
    "publish_mode": {"kind": "endpoint", "row_key": "", "producer": "http_listener_to_db"},
    "staging_leg": {"kind": "payload", "row_key": "fanout", "field": "legs", "producer": "composition"},
}

#: The single-parameter differential PAIRS, each ``(parameter, base_case, varied_case)``.
DIFFERENTIAL_PAIRS: Tuple[Tuple[str, str, str], ...] = (
    ("watermark", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_on_dlq_off_create"),
    ("dlq", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_off_dlq_on_create"),
    ("leg_count", "composition/create_2legs_stream", "composition/create_3legs_stream"),
    ("binding_mode", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_off_dlq_off_target_reuse"),
    ("binding_mode", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_off_dlq_off_source_reuse"),
    ("binding_mode_api_to_db", "api_to_database_sync/create", "api_to_database_sync/source_reuse"),
    ("publish_mode", "http_listener_to_db/plain", "http_listener_to_db/advanced_runtime"),
    ("staging_leg", "composition/create_2legs_stream", "composition/create_2legs_staged"),
)

# ---------------------------------------------------------------------------
# the case corpus — defined as DATA derived from the archetypes' own committed
# examples, so every input is causally independent of #157's implementation
# ---------------------------------------------------------------------------

#: Every case: ``case_id`` -> how to build its legacy input from the committed
#: examples. ``example`` names an archetype example; ``derive`` is a list of
#: edits applied to a deep copy (each edit is ``(json_pointer, value)`` or
#: ``("<merge-from>", example, pointer)`` to copy a sub-payload from another
#: example). Composition cases assemble parts from the DB archetype examples.
#: The capture script below is the ONLY interpreter of this table, so the same
#: cases are frozen (from the extraction) and re-derived (from the tree).
CASES: Mapping[str, Mapping[str, Any]] = {
    # database_to_api_sync: the DLQ/watermark/binding matrix
    "database_to_api_sync/wm_off_dlq_off_create": {"example": "minimal_manual_sync"},
    "database_to_api_sync/wm_on_dlq_off_create": {
        "example": "minimal_manual_sync",
        "set": {"/execution/watermark": {"field": "source_field_a", "kind": "timestamp", "persistence": "dpp"}},
    },
    "database_to_api_sync/wm_off_dlq_on_create": {
        "example": "minimal_manual_sync",
        "copy": {"/reliability/dlq": ("scheduled_with_watermark", "/reliability/dlq")},
        "set": {"/reliability/retry": {"max_attempts": 1}},
    },
    "database_to_api_sync/wm_on_dlq_on_create": {
        "example": "minimal_manual_sync",
        "copy": {"/reliability/dlq": ("scheduled_with_watermark", "/reliability/dlq")},
        "set": {
            "/reliability/retry": {"max_attempts": 1},
            "/execution/watermark": {"field": "source_field_a", "kind": "timestamp", "persistence": "dpp"},
        },
    },
    "database_to_api_sync/wm_on_dlq_on_reuse": {"example": "scheduled_with_watermark"},
    "database_to_api_sync/wm_off_dlq_off_target_reuse": {
        "example": "minimal_manual_sync",
        "copy": {"/target/binding": ("scheduled_with_watermark", "/target/binding")},
    },
    "database_to_api_sync/wm_off_dlq_off_source_reuse": {
        "example": "minimal_manual_sync",
        "copy": {"/source/binding": ("scheduled_with_watermark", "/source/binding")},
    },
    # api_to_api_sync
    "api_to_api_sync/create": {"example": "minimal_rest_to_rest_sync"},
    "api_to_api_sync/reuse_with_function": {"example": "reuse_connections_with_function"},
    # api_to_database_sync
    "api_to_database_sync/create": {"example": "rest_to_database_dynamic_insert"},
    "api_to_database_sync/source_reuse": {
        "example": "rest_to_database_dynamic_insert",
        "set": {"/source/binding": {"mode": "reuse", "component_id": "<<existing REST connection id>>"}},
    },
    # listeners and the stub
    "http_listener_to_db/plain": {"example": "webhook_to_database_insert"},
    "http_listener_to_db/advanced_runtime": {"example": "webhook_to_database_insert_advanced_runtime"},
    "http_listener_to_rest/plain": {"example": "webhook_relay_to_rest"},
    "stub_minimal/default": {"example": "smoke_test_run"},
    # composition: leg count, handoff, binding mode
    "composition/create_2legs_stream": {"compose": {"base": "minimal_manual_sync", "legs": 2}},
    "composition/create_3legs_stream": {"compose": {"base": "minimal_manual_sync", "legs": 3}},
    "composition/create_2legs_staged": {"compose": {"base": "minimal_manual_sync", "legs": 2, "staged": ["billing"]}},
    "composition/reuse_2legs_stream": {"compose": {"base": "scheduled_with_watermark", "legs": 2, "strip_watermark_query": True}},
    "composition/reuse_3legs_staged_all": {
        "compose": {"base": "scheduled_with_watermark", "legs": 3, "staged": ["orders", "billing", "shipping"], "strip_watermark_query": True}
    },
    "composition/reuse_4legs_stream": {"compose": {"base": "scheduled_with_watermark", "legs": 4, "strip_watermark_query": True}},
}

PENDING = "pending:#159"
DISCHARGED = "discharged"
RETIRED_PREFIX = "retired:"

#: THE ONE-LINE SWITCH (#159 S2 -> the integrator's final replay). While False
#: the lanes land one by one: a case whose canonical replay reproduces its
#: COMPLETE frozen sequence is "completed" even while its rows still carry
#: ``pending:#159`` (reported as reproduced-pending, never as discharged), and
#: a case that does not yet reproduce it is held to the discharged rows alone.
#: Set it True once all 21 cases replay: then every case must be completed and
#: NO row may be pending — the pending-state witness in
#: ``test_issue_157_flows_accounting.py`` becomes the no-pending assertion.
FINAL_REPLAY = True


# ---------------------------------------------------------------------------
# the capture program (runs INSIDE the extraction, on its own interpreter)
# ---------------------------------------------------------------------------

#: Executed with ``PYTHONPATH=<extraction>/src`` so every producer it calls is
#: the BASELINE's. It reads the case table as JSON on stdin and writes one JSON
#: document per case into the directory named on argv[1]. It imports nothing
#: from this module — the extraction predates it.
CAPTURE_PROGRAM = r'''
import copy, json, os, sys
from boomi_mcp.patterns.registry import PatternRegistry
from boomi_mcp.patterns.base import PatternKind
from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action, compose_archetypes_action,
)

out_dir = sys.argv[1]
table = json.load(sys.stdin)
cases, names = table["cases"], table["archetype_names"]
registry = PatternRegistry.from_package("boomi_mcp.patterns")
examples = {}
for cls in registry.list_patterns(kind=PatternKind.ARCHETYPE):
    for example in cls.examples:
        examples[(cls.metadata.name, example.name)] = copy.deepcopy(example.parameters)


def pointer_set(doc, pointer, value):
    parts = [p for p in pointer.split("/") if p]
    cursor = doc
    for part in parts[:-1]:
        cursor = cursor.setdefault(part, {})
    cursor[parts[-1]] = value


def pointer_get(doc, pointer):
    cursor = doc
    for part in [p for p in pointer.split("/") if p]:
        cursor = cursor[part]
    return copy.deepcopy(cursor)


def compose_input(spec):
    base = copy.deepcopy(examples[("database_to_api_sync", spec["base"])])
    if spec.get("strip_watermark_query"):
        base["target"]["send_request"]["query_parameters"] = [
            qp for qp in base["target"]["send_request"].get("query_parameters", [])
            if qp.get("value_source") != "watermark"
        ]
    parts = [
        {"key": "db", "kind": "db_source", "parameters": copy.deepcopy(base["source"])},
        {"key": "shape", "kind": "transform", "parameters": copy.deepcopy(base["transform"])},
    ]
    links = [{"from_part": "db", "to_part": "shape"}]
    for leg in ["orders", "billing", "shipping", "audit"][: spec["legs"]]:
        parts.append({"key": leg, "kind": "rest_target", "label": leg.title(),
                      "parameters": copy.deepcopy(base["target"])})
        link = {"from_part": "shape", "to_part": leg}
        if leg in spec.get("staged", []):
            link["handoff"] = {"mode": "document_cache"}
        links.append(link)
    options = {"naming": {"integration_name": "compose-case", "component_prefix": "CMP"}, "links": links}
    return {"parts": parts, "options": options}


for case_id, spec in cases.items():
    producer = case_id.split("/")[0]
    if "compose" in spec:
        inputs = compose_input(spec["compose"])
        result = compose_archetypes_action(inputs["parts"], inputs["options"])
        entry = {"kind": "compose_archetypes", "legs": spec["compose"]["legs"],
                 "staged": spec["compose"].get("staged", [])}
    else:
        archetype = names[producer]
        params = copy.deepcopy(examples[(archetype, spec["example"])])
        for pointer, (source_example, source_pointer) in (spec.get("copy") or {}).items():
            pointer_set(params, pointer, pointer_get(examples[(archetype, source_example)], source_pointer))
        for pointer, value in (spec.get("set") or {}).items():
            pointer_set(params, pointer, copy.deepcopy(value))
        inputs = {"archetype": archetype, "parameters": params}
        result = build_from_archetype_action(archetype, params)
        entry = {"kind": "archetype_example", "example": spec["example"]}
    ok = bool(result.get("_success", result.get("success")))
    spec_out = result.get("integration_spec") or {}
    rules = spec_out.get("validation_rules") or {}
    record = {
        "case_id": case_id,
        "producer": producer,
        "entry": entry,
        "input": inputs,
        "success": ok,
        "flows": spec_out.get("flows"),
        "endpoints": spec_out.get("endpoints"),
        "composition": rules.get("composition"),
        "error": None if ok else {k: result.get(k) for k in ("error", "error_code")},
    }
    target = os.path.join(out_dir, case_id.replace("/", "__") + ".json")
    with open(target, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
'''


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _run_capture(src_root: Path, out_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Run the capture program under ``src_root`` and return its records."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(src_root)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    table = json.dumps({"cases": CASES, "archetype_names": ARCHETYPE_NAMES})
    completed = subprocess.run(
        [sys.executable, "-c", CAPTURE_PROGRAM, str(out_dir)],
        input=table, text=True, capture_output=True, env=env, cwd=str(src_root.parent),
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "capture failed under {0}:\n{1}".format(src_root, completed.stderr[-4000:])
        )
    records = {}
    for path in sorted(out_dir.glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        records[record["case_id"]] = record
    missing = set(CASES) - set(records)
    if missing:
        raise RuntimeError("capture produced no record for: {0}".format(sorted(missing)))
    failed = sorted(cid for cid, rec in records.items() if not rec["success"])
    if failed:
        raise RuntimeError("legacy producer refused: {0}".format(
            {cid: records[cid]["error"] for cid in failed}))
    return records


def _extract_baseline(sha: str, into: Path) -> str:
    """``git archive <sha> | tar -x`` into ``into``; returns the tree hash."""
    tree = subprocess.run(
        ["git", "rev-parse", sha + "^{tree}"], cwd=str(ROOT), text=True,
        capture_output=True, check=True,
    ).stdout.strip()
    archive = subprocess.run(
        ["git", "archive", sha], cwd=str(ROOT), capture_output=True, check=True
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(into)], input=archive, check=True)
    return tree


# ---------------------------------------------------------------------------
# rows
# ---------------------------------------------------------------------------


def _flow_rows(record: Mapping[str, Any]):
    from boomi_mcp.authoring.derived_flows import normalize_flow_row

    rows = []
    seen = set()
    for ordinal, raw in enumerate(record["flows"] or ()):
        identity = (record["producer"], record["case_id"], raw.get("key"), raw.get("operation"))
        if identity in seen:
            raise RuntimeError("duplicate row identity within a case: {0}".format(identity))
        seen.add(identity)
        normalized = normalize_flow_row(raw)
        rows.append(
            {
                "ordinal": ordinal,
                "producer": record["producer"],
                "case_id": record["case_id"],
                "key": raw.get("key"),
                "kind": raw.get("operation"),
                "payload_sha256": _digest(normalized),
                "payload": normalized,
                "raw": raw,
            }
        )
    return rows


def _endpoint_rows(record: Mapping[str, Any]):
    rows = []
    ordinal = 0
    for raw in record["endpoints"] or ():
        rows.append(
            {
                "ordinal": ordinal,
                "producer": record["producer"],
                "case_id": record["case_id"],
                "key": raw.get("key"),
                "kind": "endpoint",
                "payload_sha256": _digest(raw),
                "payload": raw,
            }
        )
        ordinal += 1
    if record.get("composition"):
        payload = record["composition"]
        rows.append(
            {
                "ordinal": ordinal,
                "producer": record["producer"],
                "case_id": record["case_id"],
                "key": "composition",
                "kind": "composition_provenance",
                "payload_sha256": _digest(payload),
                "payload": payload,
            }
        )
    return rows


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# freeze
# ---------------------------------------------------------------------------


def freeze(baseline_sha: str = BASELINE_SHA, out_dir: Path = CORPUS_DIR) -> Dict[str, Any]:
    """Freeze the corpus from a pristine extraction of ``baseline_sha``."""
    with tempfile.TemporaryDirectory(prefix="issue157-baseline-") as tmp:
        extraction = Path(tmp) / "tree"
        extraction.mkdir()
        tree_sha = _extract_baseline(baseline_sha, extraction)
        capture_dir = Path(tmp) / "capture"
        capture_dir.mkdir()
        records = _run_capture(extraction / "src", capture_dir)

    existing_discharges = {}
    for case_id in CASES:
        path = out_dir / "cases" / case_id / "discharges.json"
        if path.is_file():
            existing_discharges[case_id] = json.loads(path.read_text(encoding="utf-8"))

    (out_dir / "cases").mkdir(parents=True, exist_ok=True)
    (out_dir / "retirements").mkdir(parents=True, exist_ok=True)
    case_summaries = {}
    for case_id, record in sorted(records.items()):
        case_dir = out_dir / "cases" / case_id
        case_dir.mkdir(parents=True, exist_ok=True)
        (case_dir / "input.legacy.json").write_text(
            json.dumps(
                {"entry": record["entry"], "input": record["input"], "baseline_sha": baseline_sha},
                indent=2, sort_keys=True, ensure_ascii=True,
            ) + "\n",
            encoding="utf-8",
        )
        flow_rows = _flow_rows(record)
        endpoint_rows = _endpoint_rows(record)
        _write_jsonl(case_dir / "flows.baseline.jsonl", flow_rows)
        _write_jsonl(case_dir / "endpoints.baseline.jsonl", endpoint_rows)
        previous = existing_discharges.get(case_id, {})
        discharges = {
            "case_id": case_id,
            "flows": {
                str(row["ordinal"]): (previous.get("flows") or {}).get(str(row["ordinal"]), PENDING)
                for row in flow_rows
            },
            "endpoints": {
                str(row["ordinal"]): (previous.get("endpoints") or {}).get(str(row["ordinal"]), PENDING)
                for row in endpoint_rows
            },
        }
        (case_dir / "discharges.json").write_text(
            json.dumps(discharges, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        case_summaries[case_id] = {
            "producer": record["producer"],
            "flow_rows": len(flow_rows),
            "endpoint_rows": len(endpoint_rows),
            "flow_keys": [row["key"] for row in flow_rows],
        }
    manifest = _build_manifest(out_dir, baseline_sha, tree_sha, case_summaries)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def _build_manifest(out_dir: Path, baseline_sha: str, tree_sha: str, case_summaries) -> Dict[str, Any]:
    files = {}
    for path in sorted(out_dir.rglob("*")):
        if path.is_file() and path.name != "MANIFEST.json":
            files[str(path.relative_to(out_dir))] = _sha256_bytes(path.read_bytes())
    statuses = _status_counts(out_dir)
    return {
        "schema_version": 1,
        "issue": 157,
        "baseline_sha": baseline_sha,
        "baseline_tree_sha": tree_sha,
        "extraction": "git archive <baseline_sha> | tar -x, producers executed with PYTHONPATH=<extraction>/src",
        "producers": dict(PRODUCER_MODULES),
        "cases": case_summaries,
        "case_count": len(case_summaries),
        "frozen_flow_rows": sum(c["flow_rows"] for c in case_summaries.values()),
        "frozen_endpoint_rows": sum(c["endpoint_rows"] for c in case_summaries.values()),
        "status_counts": statuses,
        "files": files,
    }


def _status_counts(out_dir: Path) -> Dict[str, int]:
    counts = {"discharged": 0, "retired": 0, "pending": 0}
    for path in sorted((out_dir / "cases").rglob("discharges.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        for status in list((data.get("flows") or {}).values()) + list((data.get("endpoints") or {}).values()):
            if status == DISCHARGED:
                counts["discharged"] += 1
            elif status.startswith(RETIRED_PREFIX):
                counts["retired"] += 1
            else:
                counts["pending"] += 1
    return counts


def refresh_manifest(out_dir: Path = CORPUS_DIR) -> Dict[str, Any]:
    """Recompute digests and status counts after a discharge edit (no re-freeze)."""
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest = _build_manifest(out_dir, manifest["baseline_sha"], manifest["baseline_tree_sha"], manifest["cases"])
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


# ---------------------------------------------------------------------------
# load + verify
# ---------------------------------------------------------------------------


class Case:
    def __init__(self, case_id: str, directory: Path) -> None:
        self.case_id = case_id
        self.directory = directory
        self.producer = case_id.split("/")[0]
        self.input_legacy = json.loads((directory / "input.legacy.json").read_text(encoding="utf-8"))
        self.flow_rows = _read_jsonl(directory / "flows.baseline.jsonl")
        self.endpoint_rows = _read_jsonl(directory / "endpoints.baseline.jsonl")
        self.discharges = json.loads((directory / "discharges.json").read_text(encoding="utf-8"))
        canonical = directory / "input.canonical.json"
        self.input_canonical = (
            json.loads(canonical.read_text(encoding="utf-8")) if canonical.is_file() else None
        )
        provenance = directory / "provenance.json"
        self.provenance = (
            json.loads(provenance.read_text(encoding="utf-8")) if provenance.is_file() else None
        )

    def status(self, ordinal: int, *, side: str = "flows") -> str:
        return self.discharges[side][str(ordinal)]


def load_manifest() -> Dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def load_cases() -> Dict[str, Case]:
    cases = {}
    for case_id in CASES:
        directory = CASES_DIR / case_id
        if not directory.is_dir():
            raise FileNotFoundError("frozen case missing: {0}".format(case_id))
        cases[case_id] = Case(case_id, directory)
    return cases


def verify_manifest() -> List[str]:
    """Every listed file present with its digest, and no unlisted file. Problems, or []."""
    manifest = load_manifest()
    problems = []
    listed = manifest["files"]
    on_disk = {}
    for path in sorted(CORPUS_DIR.rglob("*")):
        if path.is_file() and path.name != "MANIFEST.json":
            on_disk[str(path.relative_to(CORPUS_DIR))] = _sha256_bytes(path.read_bytes())
    for rel, digest in listed.items():
        if rel not in on_disk:
            problems.append("listed-but-missing: {0}".format(rel))
        elif on_disk[rel] != digest:
            problems.append("digest-mismatch: {0}".format(rel))
    for rel in on_disk:
        if rel not in listed:
            problems.append("unlisted-file: {0}".format(rel))
    if manifest["baseline_sha"] != BASELINE_SHA:
        problems.append("baseline-sha-mismatch")
    counts = _status_counts(CORPUS_DIR)
    if counts != manifest["status_counts"]:
        problems.append("status-counts-mismatch: {0} != {1}".format(counts, manifest["status_counts"]))
    if manifest["frozen_flow_rows"] != sum(c["flow_rows"] for c in manifest["cases"].values()):
        problems.append("frozen-flow-row-count-mismatch")
    return problems


def retirement_ids() -> Dict[str, Dict[str, Any]]:
    """Every retirement record, keyed by id, with its own fields.

    The INDEX carries a summary; the applicability checks need the record's
    `classification` and the PRODUCER it was measured on, and reading them from
    the record itself is what makes them checkable at all — the index alone
    could only say the id exists, which is exactly the check that let an
    unrelated record retire a row.
    """
    index_path = RETIREMENTS_DIR / "INDEX.json"
    if not index_path.is_file():
        return {}
    index = json.loads(index_path.read_text(encoding="utf-8"))
    merged = {}
    for record_id, summary in index.items():
        record_path = RETIREMENTS_DIR / (record_id + ".json")
        record = json.loads(record_path.read_text(encoding="utf-8")) if record_path.is_file() else {}
        merged[record_id] = dict(summary, **{
            key: record[key]
            for key in ("classification", "field_path", "producer")
            if key in record
        })
    return merged


# ---------------------------------------------------------------------------
# derive + discharge
# ---------------------------------------------------------------------------


#: One public planner result per case input, reused by every derivation below
#: (the served rows, the normalized rows and the endpoint oracle all read the
#: SAME plan). Keyed by the canonical input's digest, so an edited fixture is
#: never answered from a stale plan.
_PLAN_CACHE: Dict[Tuple[str, str], Any] = {}

#: #159 S1 (ARCH-r1-12): the frozen reuse cases name their reused connection by
#: a PLACEHOLDER id, and a reused component is read back from the ACCOUNT. The
#: offline replay answers that read with a PLATFORM CAPTURE of a
#: proven-operable connection of the same family — SHA-pinned bytes under
#: ``tests/fixtures/boomi/replay_identity/`` whose ``records.json`` names the
#: placeholders each capture binds and the executions that prove it operable.
#: The case inputs are unchanged; only the network boundary is answered.
REPLAY_IDENTITY_DIR = ROOT / "tests" / "fixtures" / "boomi" / "replay_identity"


def _replay_identity_records() -> List[Tuple[Dict[str, Any], bytes]]:
    """Every replay-identity record with its capture's bytes, verified.

    A record's capture lives in its own scenario directory unless it names
    another one (``capture_scenario``): a NAME binding (#159 S2) answers with
    a capture another scenario already pins, never a copy of its bytes.
    """
    sums = {}
    for line in (REPLAY_IDENTITY_DIR / "SHA256SUMS").read_text().splitlines():
        if line.strip():
            digest, relative = line.split("  ", 1)
            sums[relative] = digest
    on_disk = {
        str(path.relative_to(REPLAY_IDENTITY_DIR))
        for path in REPLAY_IDENTITY_DIR.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }
    if on_disk != set(sums):
        raise RuntimeError("replay identity captures: file set differs from SHA256SUMS")
    for relative, digest in sums.items():
        if hashlib.sha256((REPLAY_IDENTITY_DIR / relative).read_bytes()).hexdigest() != digest:
            raise RuntimeError("replay identity captures: bytes differ from SHA256SUMS")
    out: List[Tuple[Dict[str, Any], bytes]] = []
    for records_path in sorted(REPLAY_IDENTITY_DIR.glob("*/records.json")):
        for record in json.loads(records_path.read_text()):
            directory = REPLAY_IDENTITY_DIR / record.get("capture_scenario", records_path.parent.name)
            data = (directory / (record["capture"] + ".xml")).read_bytes()
            if hashlib.sha256(data).hexdigest() != record["sha256_archived_bytes"]:
                raise RuntimeError("replay identity captures: a record's digest is stale")
            out.append((record, data))
    return out


def replay_identity_captures() -> Dict[str, Dict[str, Any]]:
    """``{component id: get-component answer}``, verified byte for byte.

    A placeholder ID is answered under itself; a placeholder NAME's record is
    answered under the capture's own component id, the id the name query
    (:func:`replay_identity_names`) resolves it to.
    """
    answers: Dict[str, Dict[str, Any]] = {}
    for record, data in _replay_identity_records():
        ids = list(record.get("binds_placeholder_ids") or ())
        if record.get("binds_placeholder_names"):
            ids.append(record["component_id"])
        for component_id in ids:
            answers[component_id] = {
                "component_id": component_id,
                "version": record["version_marker"],
                "xml": data.decode("utf-8"),
            }
    return answers


def replay_identity_names() -> Dict[str, Dict[str, Any]]:
    """``{placeholder name: component-metadata row}`` (#159 S2): the account's
    answer to a reuse-BY-NAME query, one current undeleted row per bound name."""
    rows: Dict[str, Dict[str, Any]] = {}
    for record, _data in _replay_identity_records():
        for name in record.get("binds_placeholder_names") or ():
            rows[name] = {
                "component_id": record["component_id"],
                "id": record["component_id"],
                "name": name,
                "folder_name": record.get("folder_name", ""),
                "type": record["metadata_type"],
                "version": record["version_marker"],
                "current_version": True,
                "deleted": False,
            }
    return rows


def replay_paginate_metadata(rows: Optional[Mapping[str, Dict[str, Any]]] = None):
    """A stand-in for ``integration_builder.paginate_metadata`` answering ONLY
    bound names, filtered by the query's component TYPE like the account."""
    bound = dict(replay_identity_names() if rows is None else rows)

    def paginate_metadata(_client, query_config, *_args, **_kwargs):
        try:
            wanted = set(query_config.query_filter.expression.argument or ())
        except AttributeError:
            wanted = set()
        return [dict(row) for row in bound.values() if row["type"] in wanted]

    return paginate_metadata


def replay_component_get_xml(answers: Optional[Mapping[str, Dict[str, Any]]] = None):
    """A stand-in for ``_shared.component_get_xml`` answering ONLY bound ids."""
    bound = dict(replay_identity_captures() if answers is None else answers)

    def component_get_xml(_client, component_id, *_args, **_kwargs):
        answer = bound.get(str(component_id))
        if answer is None:
            raise LookupError("no platform capture is bound to this component id")
        return dict(answer)

    return component_get_xml


_GET_XML = "boomi_mcp.categories.components._shared.component_get_xml"
_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


def case_plan_result(case: Case):
    """The public planning entry's result for the case's canonical fixture."""
    from unittest.mock import MagicMock

    from boomi_mcp.authoring.workflow import plan_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    if case.input_canonical is None:
        return None
    cache_key = (case.case_id, _digest(case.input_canonical))
    if cache_key not in _PLAN_CACHE:
        from unittest.mock import patch

        request = AuthoringRequestV1.model_validate(case.input_canonical)
        with patch(_GET_XML, replay_component_get_xml()):
            result, _internals = plan_authoring_request_v1(
                request, boomi_client=MagicMock(), profile="issue-157-replay"
            )
        _PLAN_CACHE[cache_key] = result
    return _PLAN_CACHE[cache_key]


_EXECUTABILITY_CACHE: Dict[Tuple[str, str], Any] = {}


def case_executability_plan(case: Case):
    """The public plan on the EXECUTABILITY boundary (#159 S2, item 5 (b)).

    The ROWS boundary (:func:`case_plan_result`) is the frozen, account-less
    one the rows were recorded on: a reuse BY NAME is unanswered there, so its
    plan blocks that one step, exactly as the legacy route's own plan did. The
    executability boundary adds the account's answer to the name query (the
    pinned ``binds_placeholder_names`` records), so the same request is planned
    as it would bind against the account. Rows come from the first, plan and
    compile validity from the second; the two never mix.
    """
    from unittest.mock import MagicMock, patch

    from boomi_mcp.authoring.workflow import plan_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    if case.input_canonical is None:
        return None
    cache_key = (case.case_id, _digest(case.input_canonical))
    if cache_key not in _EXECUTABILITY_CACHE:
        request = AuthoringRequestV1.model_validate(case.input_canonical)
        with patch(_GET_XML, replay_component_get_xml()), patch(_PAGINATE, replay_paginate_metadata()):
            result, _internals = plan_authoring_request_v1(
                request, boomi_client=MagicMock(), profile="issue-157-replay"
            )
        _EXECUTABILITY_CACHE[cache_key] = result
    return _EXECUTABILITY_CACHE[cache_key]


def _require_canonical_input(case: Case) -> None:
    """A case that OWES canonical rows must carry the canonical request.

    Owed means: the final replay (every case), or any row already dispositioned
    ``discharged``. Refused loudly rather than projected as an empty sequence,
    which the partial-mode accounting would otherwise read as "nothing yet".
    """
    if case.input_canonical is not None:
        return
    statuses = list((case.discharges.get("flows") or {}).values()) + list(
        (case.discharges.get("endpoints") or {}).values()
    )
    if FINAL_REPLAY or DISCHARGED in statuses:
        raise FileNotFoundError(
            "case {0} owes canonical rows but has no input.canonical.json".format(case.case_id)
        )


def derive_served_projection(case: Case) -> List[Dict[str, Any]]:
    """The case's SERVED rows, un-normalized. The name check reads these.

    `derive_case_projection` normalizes for the digest comparison, and R3 drops
    the profile name there by design — so the name has to be read from what the
    caller actually receives, or nothing checks it at all.
    """
    result = case_plan_result(case)
    if result is None:
        return []
    return [row.model_dump(mode="json", exclude_unset=True) for row in result.integration_spec_preview.flows]


def derive_case_projection(case: Case) -> List[Dict[str, Any]]:
    """The CANONICAL projection of the case's canonical fixture, normalized.

    Driven through the public planning entry (never the projector directly) so
    the replay exercises normalization and governance the way a caller would.
    Callers patch the live metadata boundary; this function is offline.
    """
    from boomi_mcp.authoring.derived_flows import normalize_flow_row

    _require_canonical_input(case)
    result = case_plan_result(case)
    if result is None:
        return []
    preview = result.integration_spec_preview
    # `exclude_unset`: a field the projection never SET is absent, exactly as
    # the legacy producers omitted absent keys, while a field set to None
    # (`target`, the JSON root's `data_type`) stays a meaningful null.
    return [
        normalize_flow_row(row.model_dump(mode="json", exclude_unset=True))
        for row in preview.flows
    ]


def derive_case_endpoints(case: Case) -> List[Dict[str, Any]]:
    """The case's served endpoint/provenance sequence, in the frozen oracle's order.

    The endpoint rows, then each composition summary as the
    ``composition_provenance`` row (``_endpoint_rows`` order). Dumped with
    ``exclude_unset`` so a key the projection never set is ABSENT — omission and
    null stay distinct, exactly as the legacy producers served them. The
    summary's process key is the map key, never part of the compared payload.
    """
    _require_canonical_input(case)
    result = case_plan_result(case)
    if result is None:
        return []
    preview = result.integration_spec_preview
    rows = [row.model_dump(mode="json", exclude_unset=True) for row in preview.endpoints]
    summaries = getattr(preview, "composition_summaries", None) or {}
    for _process_key, summary in sorted(summaries.items()):
        rows.append(summary.model_dump(mode="json", exclude_unset=True))
    return rows


class DischargeReport:
    def __init__(self) -> None:
        self.problems: List[str] = []
        self.discharged: int = 0
        self.pending: int = 0
        self.retired: int = 0
        #: Rows the canonical replay reproduces exactly, in order, whose
        #: disposition is still ``pending:#159`` (legal only while not
        #: ``FINAL_REPLAY``).
        self.reproduced_pending: int = 0
        #: The derived sequence EQUALS the full frozen sequence (retired rows
        #: removed) — completion is MEASURED, never declared.
        self.completed: bool = False

    @property
    def ok(self) -> bool:
        return not self.problems


def _retirement_problem(row_label, record_id, retirements, field_path=None, producer=None):
    """Is this row's cited retirement record one that RETIRES anything, and THIS?

    The status used to be believed on the strength of the id existing. A frozen
    row could therefore cite a record classified RETAIN or SPLIT — neither of
    which retires the row — or a record measured on an entirely unrelated
    producer, and the accounting reported it retired (architect evaluations 2
    and 3).

    ``producer`` is the applicability check the DATA actually supports for a
    flow or endpoint row: a row has no field pointer, but it belongs to a case,
    and a retirement record names the producer it was measured on. Requiring
    them to agree is what stops an API send row from citing a record measured on
    the database producer. ``field_path`` remains available for a caller that
    does have one.
    """
    record = retirements.get(record_id)
    if record is None:
        return "{0} names an unknown retirement record".format(row_label)
    classification = (record or {}).get("classification")
    if classification != "RETIRE":
        return "{0} cites {1}, classified {2}, which retires nothing".format(
            row_label, record_id, classification
        )
    if field_path is not None and record.get("field_path") != field_path:
        return "{0} cites {1}, whose measured field {2!r} is not this row's {3!r}".format(
            row_label, record_id, record.get("field_path"), field_path
        )
    if producer is not None:
        measured = str(record.get("producer") or "")
        if not measured or producer.split("/")[0] not in measured:
            return "{0} cites {1}, measured on {2!r}, which is not this row's producer {3!r}".format(
                row_label, record_id, record.get("producer"), producer
            )
    return None


def _cloned_name(component, authored):
    """The name a clone will carry, from the apply-side authority itself."""
    from boomi_mcp.categories.integration_builder import _apply_clone_suffix
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    try:
        spec = IntegrationComponentSpec(**component)
    except Exception:  # noqa: BLE001 - a fixture the model refuses clones nothing
        return authored
    cloned = _apply_clone_suffix(spec, dict(spec.config or {}))
    name = cloned.get("component_name") or cloned.get("name")
    return name.strip() if isinstance(name, str) and name.strip() else authored


def _profile_name_problems(case, served_rows):
    """Every served profile name must be one the case's own request authors.

    R3 drops `*_profile_generation.component_name` from the COMPARISON, because
    two legacy producers spell it differently — one passes None, the other a
    derived default — and the frozen payloads therefore carry no name to compare
    against. So the check is not a comparison: the SERVED rows are read
    un-normalized, and each name they carry has to be a name the canonical
    request actually authors. An unrelated profile name is then caught by the
    real replay, which is what the earlier version could not do — it compared a
    field both sides had already dropped (architect evaluation 3, e3-05).
    """
    # THE SAME NAMING AUTHORITY THE PREVIEW USES. The served name is the name
    # apply will give the component, which under `conflict_policy="clone"` is
    # the authored one plus a suffix — so an oracle that accepts only authored
    # names would call the correct served name wrong the moment a case clones
    # (live QA r14). Asked of `_apply_clone_suffix`, not modelled here.
    intent = (case.input_canonical or {}).get("intent") or {}
    conflict_policy = intent.get("conflict_policy") or "reuse"
    authored = set()
    # A RECIPE intent authors no component: its profile names come from the
    # frozen naming evidence its provenance recorded from the BASELINE producer
    # (#159 S1, C2) — never from the code under test.
    provenance = getattr(case, "provenance", None) or {}
    evidence = (provenance.get("naming_evidence") or {}).get("profile_names") or ()
    authored.update(name for name in evidence if isinstance(name, str) and name.strip())
    for component in intent.get("components") or ():
        if not isinstance(component, dict):
            continue
        names = [component.get("name"), (component.get("config") or {}).get("component_name")]
        for candidate in names:
            if isinstance(candidate, str) and candidate.strip():
                authored.add(candidate.strip())
                if conflict_policy == "clone":
                    authored.add(_cloned_name(component, candidate.strip()))
    problems = []
    for index, row in enumerate(served_rows):
        for side in ("source_profile_generation", "target_profile_generation"):
            summary = row.get(side)
            if not isinstance(summary, dict):
                continue
            name = summary.get("component_name")
            if name is None:
                continue
            if name not in authored:
                problems.append(
                    "served row {0} {1} names {2!r}, which this case authors nowhere".format(
                        index, side, name
                    )
                )
    return problems


def _derived_identity(side: str, payload: Mapping[str, Any]) -> Tuple[Any, Any]:
    """``(key, kind)`` of a DERIVED row — the identity a frozen row is keyed by.

    A flow row's kind is its ``operation``; on the endpoint side a row with a
    ``key`` is an ``endpoint`` and the composition summary (which has none) is
    the ``composition_provenance`` row.
    """
    if side == "flows":
        return payload.get("key"), payload.get("operation")
    if "key" in payload:
        return payload.get("key"), "endpoint"
    return "composition", "composition_provenance"


def _account_sequence(
    case: Case,
    side: str,
    derived: Sequence[Mapping[str, Any]],
    retirements: Mapping[str, Any],
    *,
    final: Optional[bool] = None,
) -> DischargeReport:
    """THE comparison path — flows and endpoints, partial and final replay alike.

    Explicit row accounting first: every frozen ordinal carries exactly one
    status, every frozen payload matches its own digest, every retirement cites
    a record that retires a row of THIS producer. Then ordered full-sequence
    equality, by full normalized payload digest (nested payload, null versus
    absent and ordinal all included), against the FULL frozen sequence with only
    individually retired rows removed. Equal means the case is COMPLETED. While
    not ``final`` a completed case's still-pending rows are counted as
    reproduced-pending, and a case that does not reproduce its full sequence is
    held to its discharged rows alone (the #157/S1 partial rule). ``final``
    (default: ``FINAL_REPLAY``) requires every case completed and no row
    pending.
    """
    final = FINAL_REPLAY if final is None else final
    report = DischargeReport()
    label = "row" if side == "flows" else "endpoint row"
    frozen = case.flow_rows if side == "flows" else case.endpoint_rows
    statuses = case.discharges.get(side) or {}
    if set(statuses) != {str(row["ordinal"]) for row in frozen}:
        report.problems.append(
            "discharges.json does not name every frozen {0}ordinal exactly once".format(
                "" if side == "flows" else "endpoint "
            )
        )
        return report
    if [row["ordinal"] for row in frozen] != list(range(len(frozen))):
        report.problems.append("frozen {0}s do not keep their ordinals in order".format(label))
    identities = [(row["key"], row["kind"]) for row in frozen]
    if len(set(identities)) != len(identities):
        report.problems.append("frozen {0} identities are not unique within the case".format(label))

    status_of: Dict[int, str] = {}
    for row in frozen:
        status = statuses[str(row["ordinal"])]
        status_of[row["ordinal"]] = status
        if _digest(row["payload"]) != row["payload_sha256"]:
            report.problems.append(
                "frozen {0} {1} payload does not match its own digest".format(label, row["ordinal"])
            )
        if status == DISCHARGED:
            report.discharged += 1
            if case.input_canonical is None:
                report.problems.append(
                    "{0} {1} discharged with no canonical fixture".format(label, row["ordinal"])
                )
        elif status.startswith(RETIRED_PREFIX):
            report.retired += 1
            problem = _retirement_problem(
                "{0} {1}".format(label, row["ordinal"]),
                status[len(RETIRED_PREFIX):],
                retirements,
                producer=case.producer,
            )
            if problem:
                report.problems.append(problem)
        elif status == PENDING:
            report.pending += 1
        else:
            report.problems.append(
                "{0} {1} carries an unknown status {2!r}".format(label, row["ordinal"], status)
            )

    owed = [row for row in frozen if not status_of[row["ordinal"]].startswith(RETIRED_PREFIX)]
    derived_digests = [_digest(row) for row in derived]
    if derived_digests == [row["payload_sha256"] for row in owed]:
        report.completed = True
        report.reproduced_pending = sum(1 for row in owed if status_of[row["ordinal"]] == PENDING)
        if final and report.reproduced_pending:
            report.problems.append(
                "{0} reproduced {1} still pending: the final replay accepts no pending row".format(
                    report.reproduced_pending, label + ("s are" if report.reproduced_pending > 1 else " is")
                )
            )
        return report

    # NOT completed. The diagnostics name the exact failure shape.
    target = owed if final else [row for row in owed if status_of[row["ordinal"]] == DISCHARGED]
    if final:
        report.problems.append(
            "case is not completed: the derived {0} sequence is not the full frozen sequence".format(side)
        )
    for row in target:
        if row["payload_sha256"] not in derived_digests:
            report.problems.append(
                "{0} {1} {2} ({3}/{4}) has no full-payload match in the derived output".format(
                    "discharged" if status_of[row["ordinal"]] == DISCHARGED else "owed",
                    label, row["ordinal"], row["key"], row["kind"],
                )
            )
    target_digests = [row["payload_sha256"] for row in target]
    if derived_digests != target_digests:
        not_owed = {
            row["payload_sha256"]: status_of[row["ordinal"]]
            for row in frozen
            if row["payload_sha256"] not in target_digests
        }
        for digest in derived_digests:
            if digest in not_owed:
                report.problems.append(
                    "derived output contains a row whose frozen status is {0} (unexpected row)".format(
                        not_owed[digest]
                    )
                )
        if len(set(derived_digests)) != len(derived_digests):
            report.problems.append("derived output contains a duplicate row")
        if sorted(derived_digests) == sorted(target_digests):
            report.problems.append("derived output is a reordering of the discharged sequence")
        elif set(derived_digests) - set(target_digests) - set(not_owed):
            unknown = [
                _derived_identity(side, row)
                for row in derived
                if _digest(row) not in target_digests and _digest(row) not in not_owed
            ]
            report.problems.append(
                "derived output contains a row the baseline never froze (unexpected row): {0}".format(
                    unknown
                )
            )
        report.problems.append("derived {0} sequence differs from the discharged sequence".format(side))
    return report


def discharge_case(
    case: Case,
    derived: Sequence[Mapping[str, Any]],
    retirements: Mapping[str, Any],
    served: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    final: Optional[bool] = None,
) -> DischargeReport:
    """Two-sided, ordered accounting of one case's flows sequence.

    :func:`_account_sequence` plus the names R3 removes from the DIGEST, checked
    against the SERVED rows instead (the frozen payloads carry none).
    """
    report = _account_sequence(case, "flows", derived, retirements, final=final)
    report.problems.extend(_profile_name_problems(case, served if served is not None else ()))
    return report


def discharge_endpoints(
    case: Case,
    derived: Sequence[Mapping[str, Any]],
    retirements: Mapping[str, Any],
    *,
    final: Optional[bool] = None,
) -> DischargeReport:
    """The SAME ordered two-sided accounting over the endpoint/provenance oracle
    (binding mode, ids/keys and ordinal all inside the compared payload)."""
    return _account_sequence(case, "endpoints", derived, retirements, final=final)


# ---------------------------------------------------------------------------
# the import-blocked batch replay (#159 S1)
# ---------------------------------------------------------------------------

#: The legacy producer modules #160 deletes. The batch child refuses every
#: import under these prefixes — even one a caller would catch — and filters the
#: SAME prefixes out of the discovery and registry inputs, so one tuple decides
#: both what is blocked and what is simulated as deregistered.
LEGACY_PRODUCER_PREFIXES: Tuple[str, ...] = (
    "boomi_mcp.patterns.archetypes",
    "boomi_mcp.patterns.composition",
    "boomi_mcp.patterns.recipe_bridge",
    "boomi_mcp.compiler.process_ir.legacy_adapters",
)

#: The ONE batch child. It installs the blocker BEFORE any ``boomi_mcp`` import,
#: then simulates the retirement at the discovery and registry INPUTS only —
#: the package scan no longer sees the blocked modules, the recipe layer's
#: source-module list drops them, and the obsolete registrations (the
#: ``boomi.adapter.*`` compatibility adapters and the fan-out version their
#: legacy surface targets) are filtered — while the real revision, planning and
#: projection code runs unchanged. It reads the case ids on argv and the
#: canonical inputs from the committed fixtures.
BATCH_CHILD_PROGRAM = r'''
import json, sys
BLOCKED = tuple(json.loads(sys.argv[1]))
attempts = []


def _blocked(name):
    return any(name == b or name.startswith(b + ".") for b in BLOCKED)


class _Blocker:
    def find_spec(self, name, path=None, target=None):
        if _blocked(name):
            import traceback
            origin = [
                "{0}:{1}".format(frame.filename.split("/src/")[-1], frame.lineno)
                for frame in traceback.extract_stack()
                if "/src/boomi_mcp/" in frame.filename
            ]
            attempts.append([name, origin[-1] if origin else ""])
            raise ImportError("blocked: " + name)
        return None


sys.meta_path.insert(0, _Blocker())  # BEFORE any boomi_mcp import
sys.path[:0] = [sys.argv[2], sys.argv[3]]

import importlib
import pkgutil


def _retired_walk(path=None, prefix="", onerror=None):
    """``pkgutil.walk_packages`` over a tree the retired modules are GONE from.

    Filtering the stock walker's output is not enough: it imports every
    subpackage to recurse into it, so the blocked package would be imported
    before the filter ever saw its name.
    """
    for info in pkgutil.iter_modules(path, prefix):
        if _blocked(info.name):
            continue
        yield info
        if not info.ispkg:
            continue
        try:
            module = importlib.import_module(info.name)
        except ImportError:
            if onerror is not None:
                onerror(info.name)
            continue
        except Exception:
            if onerror is not None:
                onerror(info.name)
                continue
            raise
        sub = getattr(module, "__path__", None)
        if sub:
            yield from _retired_walk(sub, info.name + ".", onerror)


pkgutil.walk_packages = _retired_walk  # discovery input: the modules are gone

from boomi_mcp.recipes import registry as registry_module
from boomi_mcp.recipes.builtins import catalog as catalog_module

registry_module.RECIPE_LAYER_MODULES = tuple(
    name for name in registry_module.RECIPE_LAYER_MODULES if not _blocked(name)
)
_OBSOLETE = {tuple(item) for item in json.loads(sys.argv[4])}
_ADAPTER_PREFIX = sys.argv[5]
catalog_module.PRODUCTION_REGISTRATIONS = tuple(
    reg for reg in catalog_module.PRODUCTION_REGISTRATIONS
    if not reg.recipe_id.startswith(_ADAPTER_PREFIX)
    and (reg.recipe_id, reg.recipe_version) not in _OBSOLETE
)
registry_module._PRODUCTION_REGISTRY = None

# #159 S2 (L4): the projector's LAZILY imported authorities, imported here under
# the blocker so a forbidden transitive import surfaces even before a case
# that reaches them (a listener, a DB write target, the API->API default) exists.
from boomi_mcp.authoring import derived_flows as _projector
_projector.rest_success_status_default()
from boomi_mcp.patterns.primitives.wss_listen import compute_wss_endpoint  # noqa: F401
from boomi_mcp.categories.components.builders._api_service_paths import (  # noqa: F401
    effective_api_service_route, wss_http_method,
)
from boomi_mcp.categories.components.builders.profile_builder import (  # noqa: F401
    DatabaseWriteProfileBuilder,
)
_authority_attempts = list(attempts)

from unittest.mock import MagicMock, patch
import _issue_157_flows_accounting as acct
from boomi_mcp.authoring.workflow import AuthoringWorkflowError, compile_authoring_request_v1
from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

APPLY_CASE = sys.argv[6]
cases = acct.load_cases()
out = {}
applied = {}
with patch("boomi_mcp.categories.integration_builder.paginate_metadata", lambda *a, **k: []), \
        patch(acct._GET_XML, acct.replay_component_get_xml()):
    for case_id in sys.argv[7:]:
        case = cases[case_id]
        # ROWS boundary: the frozen, account-less one the rows were recorded on
        # (metadata query -> [], reads by id -> the pinned captures).
        result = acct.case_plan_result(case)
        request = AuthoringRequestV1.model_validate(case.input_canonical)
        # EXECUTABILITY boundary (#159 S2, integrator decision on item 5): the
        # same, plus the account's NAME queries answered by the pinned name
        # records, so a reuse-by-name binds as it would against the account.
        executable = acct.case_executability_plan(case)
        with patch(acct._PAGINATE, acct.replay_paginate_metadata()):
            try:
                compiled, _internals = compile_authoring_request_v1(
                    request, boomi_client=MagicMock(), profile="issue-157-replay"
                )
                compile_valid, compile_error = bool(compiled.validation_report.is_valid), None
            except AuthoringWorkflowError as exc:
                compiled, compile_valid, compile_error = None, False, exc.code
        if case_id == APPLY_CASE and compiled is not None:
            # The full typed workflow, apply included, with ONLY the network
            # boundary faked (the #153 harness shape): every component write
            # returns a fresh id, the process write records its XML.
            from boomi_mcp.categories import integration_builder as builder
            payload = request.model_dump(mode="json")
            payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
            payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
            before = len(attempts)
            counter = {"n": 0}
            created = []

            def _component(*_a, **_k):
                counter["n"] += 1
                return {"_success": True, "component_id": "cid-%d" % counter["n"]}

            def _create(_c, _p, payload_in, **_k):
                created.append(payload_in["xml"])
                return {"_success": True, "component_id": "process-cid"}

            with patch.object(builder, "_execute_component", side_effect=_component), \
                    patch.object(builder, "create_component", side_effect=_create), \
                    patch.object(builder, "component_get_xml", side_effect=lambda _c, cid, *a, **k: {
                        "type": "process", "xml": created[-1] if created else "<bns:Component/>"}):
                response = builder.build_integration_action(
                    MagicMock(), "issue-157-replay", "apply",
                    config={"authoring_request": payload, "dry_run": False},
                )
            applied = {
                "case_id": case_id,
                "success": bool(response.get("_success")),
                "error_code": response.get("error_code"),
                "created_process": len(created) == 1 and "$ref:" not in created[0],
                "attempts_during_apply": [a[0] for a in attempts[before:]],
            }
        out[case_id] = {
            "compile_valid": compile_valid,
            "compile_error": compile_error,
            # The REAL component plan must succeed, not only the row comparison:
            # an unexecutable plan step is a served error on this route.
            # EXECUTABILITY boundary (name queries answered):
            "plan_valid": bool(executable.validation_report.is_valid),
            "plan_errors": sorted({d.code for d in executable.errors}),
            "component_dependencies": len(executable.component_dependencies),
            # ROWS boundary (account-less): what blocks there, and the rows.
            "rows_plan_blocked": sorted(
                [d.path, list(d.cause_codes)] for d in result.errors
            ),
            "flows": acct.derive_case_projection(case),
            "served": acct.derive_served_projection(case),
            "endpoints": acct.derive_case_endpoints(case),
        }
# ARCH-r1-13: the pinned flagship through the PUBLIC dispatch (plan -> compile)
# in this same blocked child, plus its process envelope from the workflow.
from _fanout_v2_support import flagship_v2, recipe_request
from boomi_mcp.categories import integration_builder as _builder
_flagship_request = recipe_request(*flagship_v2())
_flagship_payload = _flagship_request.model_dump(mode="json", exclude_none=True)
_flagship_before = len(attempts)
with patch("boomi_mcp.categories.integration_builder.paginate_metadata", lambda *a, **k: []):
    _plan_env = _builder.build_integration_action(
        MagicMock(), "issue-157-replay", "plan", {"authoring_request": _flagship_payload}
    )
    _compile_env = _builder.build_integration_action(
        MagicMock(), "issue-157-replay", "compile", {"authoring_request": _flagship_payload}
    )
    _compiled, _internals = compile_authoring_request_v1(
        _flagship_request, boomi_client=MagicMock(), profile="issue-157-replay"
    )
(_unit,) = _internals.normalized.integration_spec.processes
flagship = {
    "plan_success": bool(_plan_env.get("_success")),
    "compile_success": bool(_compile_env.get("_success")),
    "compile_valid": bool(_compiled.validation_report.is_valid),
    "components": [
        c for c in _compile_env.get("authoring_result", {}).get(
            "integration_spec_preview", {}).get("components", [])
        if c.get("type") != "process"
    ],
    "envelope": {
        "component_key": _unit.envelope.component_key,
        "name": _unit.envelope.name,
        "action": _unit.envelope.action,
        "depends_on": sorted(_unit.envelope.depends_on),
        "process_extensions": _unit.envelope.process_extensions.model_dump(
            mode="json", exclude_none=True
        ),
    },
    "attempts": [a[0] for a in attempts[_flagship_before:]],
    # ARCH-r1-13 (eval 2): the normalized root's CANONICAL topology and the FULL
    # envelope, both from the public-path compile in this blocked child.
    "root": __import__("boomi_mcp.models.process_ir", fromlist=["x"]).canonical_process_ir_json(
        _unit.process_ir
    ),
    "envelope_full": {
        name: (
            sorted(value) if name == "depends_on"
            else _unit.envelope.process_extensions.model_dump(mode="json", exclude_none=True)
            if name == "process_extensions"
            else value
        )
        for name, value in _unit.envelope.model_dump(mode="json").items()
    },
}
replay_attempts = list(attempts)
control = "not_refused"
try:
    import boomi_mcp.patterns.composition  # noqa: F401 — the deliberate control
except ImportError:
    control = "refused"
print(json.dumps({
    "attempts": replay_attempts,
    "authority_attempts": _authority_attempts,
    "control": control,
    "registrations": sorted(
        [reg.recipe_id, reg.recipe_version] for reg in catalog_module.PRODUCTION_REGISTRATIONS
    ),
    "layer_modules": list(registry_module.RECIPE_LAYER_MODULES),
    "cases": out,
    "apply": applied,
    "flagship": flagship,
}))
'''


def obsolete_registrations() -> Tuple[Tuple[str, str], ...]:
    """The non-adapter registrations S1's batch simulates as retired.

    Read from the catalog: an ``adapter_target`` that is SUPERSEDED — another
    version of the same recipe id is registered beside it — is the legacy
    surface's pinned target and retires with that surface (today: the fan-out
    version ``compose_archetypes`` targets, superseded by 2.0.0). A target with
    no successor (the sync recipes) stays.
    """
    from boomi_mcp.recipes.builtins import catalog

    registrations = catalog.PRODUCTION_REGISTRATIONS
    versions: Dict[str, set] = {}
    for reg in registrations:
        versions.setdefault(reg.recipe_id, set()).add(reg.recipe_version)
    targets = {
        (reg.adapter_target.recipe_id, reg.adapter_target.recipe_version)
        for reg in registrations
        if getattr(reg, "adapter_target", None) is not None
    }
    return tuple(
        sorted(
            (recipe_id, version)
            for recipe_id, version in targets
            if len(versions.get(recipe_id, ())) > 1
        )
    )


ADAPTER_ID_PREFIX = "boomi.adapter."

#: The executable recipe ids the planned retirement removes BEYOND the
#: superseded adapter targets (#159 S3): API->API retires with no successor,
#: so ``obsolete_registrations`` cannot find it. Named once, here; the child
#: filters every registered version of each, and the accounting test proves
#: each id is registered in the parent (the filter is not vacuous).
PLANNED_RETIREMENT_RECIPE_IDS: Tuple[str, ...] = ("boomi.archetype.api_to_api_sync",)


def simulated_retirements() -> Tuple[Tuple[str, str], ...]:
    """Every ``(recipe_id, version)`` the blocked child deregisters (adapters
    aside): the superseded adapter targets plus every registered version of a
    planned retirement."""
    from boomi_mcp.recipes.builtins import catalog

    planned = {
        (reg.recipe_id, reg.recipe_version)
        for reg in catalog.PRODUCTION_REGISTRATIONS
        if reg.recipe_id in PLANNED_RETIREMENT_RECIPE_IDS
    }
    return tuple(sorted(set(obsolete_registrations()) | planned))


def canonical_case_ids() -> Tuple[str, ...]:
    """Every case carrying a canonical fixture — the replayable subset."""
    return tuple(case_id for case_id, case in sorted(load_cases().items()) if case.input_canonical is not None)


#: The one case the batch child also APPLIES (typed, network faked): a staged
#: composition, so the generated pairs, the cache and the composed root all
#: materialize under the blocker.
BATCH_APPLY_CASE = "composition/create_2legs_staged"


def run_import_blocked_batch(
    case_ids: Sequence[str],
    *,
    blocked_prefixes: Sequence[str] = LEGACY_PRODUCER_PREFIXES,
    apply_case: str = BATCH_APPLY_CASE,
) -> Dict[str, Any]:
    """Replay ``case_ids`` in ONE fresh child with the legacy producers blocked:
    plan + compile every case, and apply ``apply_case`` through the typed tool
    route with only the network boundary faked."""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            BATCH_CHILD_PROGRAM,
            json.dumps(list(blocked_prefixes)),
            str(ROOT / "src"),
            str(ROOT / "tests"),
            json.dumps([list(item) for item in simulated_retirements()]),
            ADAPTER_ID_PREFIX,
            apply_case,
            *case_ids,
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
    )
    if completed.returncode != 0:
        raise RuntimeError("batch child failed:\n" + completed.stderr[-6000:])
    return json.loads(completed.stdout.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# check: the legacy producers at HEAD still reproduce the frozen rows
# ---------------------------------------------------------------------------


def check_against_tree() -> List[str]:
    """Re-derive every case from the CURRENT tree's legacy producers and diff.

    Meaningful until #160 deletes the producers; afterwards the corpus is
    canonical-only and this check is retired with them.
    """
    problems = []
    with tempfile.TemporaryDirectory(prefix="issue157-check-") as tmp:
        records = _run_capture(ROOT / "src", Path(tmp))
    cases = load_cases()
    for case_id, record in sorted(records.items()):
        frozen = cases[case_id]
        rows = _flow_rows(record)
        if [r["payload_sha256"] for r in rows] != [r["payload_sha256"] for r in frozen.flow_rows]:
            problems.append("flows drift at HEAD: {0}".format(case_id))
        if [r["payload_sha256"] for r in _endpoint_rows(record)] != [
            r["payload_sha256"] for r in frozen.endpoint_rows
        ]:
            problems.append("endpoint drift at HEAD: {0}".format(case_id))
    return problems


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--freeze", action="store_true", help="freeze from a pristine extraction")
    parser.add_argument("--baseline-sha", default=BASELINE_SHA)
    parser.add_argument("--check", action="store_true", help="re-derive at HEAD and diff")
    parser.add_argument("--refresh-manifest", action="store_true", help="recompute digests/counts")
    parser.add_argument("--source-tree", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.source_tree is not None:
        print("refused: the corpus is frozen from a git archive extraction, never from a working tree", file=sys.stderr)
        return 2
    if args.freeze:
        manifest = freeze(args.baseline_sha)
        print(json.dumps({k: manifest[k] for k in ("case_count", "frozen_flow_rows", "frozen_endpoint_rows", "status_counts")}))
        return 0
    if args.refresh_manifest:
        manifest = refresh_manifest()
        print(json.dumps(manifest["status_counts"]))
        return 0
    if args.check:
        problems = verify_manifest() + check_against_tree()
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
