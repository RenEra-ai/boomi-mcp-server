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
}

#: The single-parameter differential PAIRS, each ``(parameter, base_case, varied_case)``.
DIFFERENTIAL_PAIRS: Tuple[Tuple[str, str, str], ...] = (
    ("watermark", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_on_dlq_off_create"),
    ("dlq", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_off_dlq_on_create"),
    ("leg_count", "composition/create_2legs_stream", "composition/create_3legs_stream"),
    ("binding_mode", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_off_dlq_off_target_reuse"),
    ("binding_mode", "database_to_api_sync/wm_off_dlq_off_create", "database_to_api_sync/wm_off_dlq_off_source_reuse"),
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
    index_path = RETIREMENTS_DIR / "INDEX.json"
    if not index_path.is_file():
        return {}
    return json.loads(index_path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# derive + discharge
# ---------------------------------------------------------------------------


def derive_case_projection(case: Case) -> List[Dict[str, Any]]:
    """The CANONICAL projection of the case's canonical fixture, normalized.

    Driven through the public planning entry (never the projector directly) so
    the replay exercises normalization and governance the way a caller would.
    Callers patch the live metadata boundary; this function is offline.
    """
    from unittest.mock import MagicMock

    from boomi_mcp.authoring.derived_flows import normalize_flow_row
    from boomi_mcp.authoring.workflow import plan_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    if case.input_canonical is None:
        return []
    request = AuthoringRequestV1.model_validate(case.input_canonical)
    result, _internals = plan_authoring_request_v1(
        request, boomi_client=MagicMock(), profile="issue-157-replay"
    )
    preview = result.integration_spec_preview
    # `exclude_unset`: a field the projection never SET is absent, exactly as
    # the legacy producers omitted absent keys, while a field set to None
    # (`target`, the JSON root's `data_type`) stays a meaningful null.
    return [
        normalize_flow_row(row.model_dump(mode="json", exclude_unset=True))
        for row in preview.flows
    ]


class DischargeReport:
    def __init__(self) -> None:
        self.problems: List[str] = []
        self.discharged: int = 0
        self.pending: int = 0
        self.retired: int = 0

    @property
    def ok(self) -> bool:
        return not self.problems


def _retirement_problem(row_label, record_id, retirements, field_path=None):
    """Is this row's cited retirement record one that RETIRES anything, and this?

    The status used to be believed on the strength of the id existing. A frozen
    row could therefore cite a record classified RETAIN or SPLIT — neither of
    which retires the row — or a record about an entirely unrelated field, and
    the accounting reported it retired (architect evaluation 2, finding 4).
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
    return None


def _profile_name_problems(expected, derived):
    """Names must AGREE where both sides carry one.

    R3 drops `*_profile_generation.component_name` from the comparison because
    two legacy producers spell it differently — one passes None, the other a
    derived default. That reconciliation is between PRODUCERS; each replay here
    compares a case against its own baseline, where a wrong name is a wrong name.
    Dropping it unconditionally let an unrelated profile name be accepted.
    """
    problems = []
    for index, (frozen, actual) in enumerate(zip(expected, derived)):
        for side in ("source_profile_generation", "target_profile_generation"):
            a = (frozen.get("payload") or {}).get(side)
            b = actual.get(side)
            if not (isinstance(a, dict) and isinstance(b, dict)):
                continue
            want, got = a.get("component_name"), b.get("component_name")
            if want is None or got is None:
                continue
            if want != got:
                problems.append(
                    "row {0} {1} names {2!r}; the derived projection names {3!r}".format(
                        frozen.get("ordinal", index), side, want, got
                    )
                )
    return problems


def discharge_case(case: Case, derived: Sequence[Mapping[str, Any]], retirements: Mapping[str, Any]) -> DischargeReport:
    """Two-sided, ordered accounting of one case's flows sequence."""
    report = DischargeReport()
    statuses = case.discharges.get("flows") or {}
    if set(statuses) != {str(row["ordinal"]) for row in case.flow_rows}:
        report.problems.append("discharges.json does not name every frozen ordinal exactly once")
        return report
    expected: List[Mapping[str, Any]] = []
    for row in case.flow_rows:
        status = statuses[str(row["ordinal"])]
        if status == DISCHARGED:
            report.discharged += 1
            if case.input_canonical is None:
                report.problems.append("row {0} discharged with no canonical fixture".format(row["ordinal"]))
            expected.append(row)
        elif status.startswith(RETIRED_PREFIX):
            report.retired += 1
            problem = _retirement_problem(
                "row {0}".format(row["ordinal"]), status[len(RETIRED_PREFIX):], retirements
            )
            if problem:
                report.problems.append(problem)
        elif status == PENDING:
            report.pending += 1
        else:
            report.problems.append("row {0} carries an unknown status {1!r}".format(row["ordinal"], status))

    # side 1: every discharged row matched by FULL normalized payload
    derived_digests = [_digest(row) for row in derived]
    for row in expected:
        if row["payload_sha256"] not in derived_digests:
            report.problems.append(
                "discharged row {0} ({1}/{2}) has no full-payload match in the derived output".format(
                    row["ordinal"], row["key"], row["kind"]
                )
            )
        if _digest(row["payload"]) != row["payload_sha256"]:
            report.problems.append("frozen row {0} payload does not match its own digest".format(row["ordinal"]))

    # side 1b: the names R3 removes from the DIGEST are compared here instead
    report.problems.extend(_profile_name_problems(expected, derived))

    # side 2: the derived sequence EQUALS the discharged sequence, in order
    expected_digests = [row["payload_sha256"] for row in expected]
    if derived_digests != expected_digests:
        undischarged = {
            row["payload_sha256"]: statuses[str(row["ordinal"])]
            for row in case.flow_rows
            if statuses[str(row["ordinal"])] != DISCHARGED
        }
        for digest in derived_digests:
            if digest in undischarged:
                report.problems.append(
                    "derived output contains a row whose frozen status is {0} (unexpected row)".format(
                        undischarged[digest]
                    )
                )
        if len(set(derived_digests)) != len(derived_digests):
            report.problems.append("derived output contains a duplicate row")
        if sorted(derived_digests) == sorted(expected_digests) and derived_digests != expected_digests:
            report.problems.append("derived output is a reordering of the discharged sequence")
        elif set(derived_digests) - set(expected_digests) - set(undischarged):
            report.problems.append("derived output contains a row the baseline never froze (unexpected row)")
        if not report.problems:
            report.problems.append("derived sequence differs from the discharged sequence")
    return report


def discharge_endpoints(case: Case, derived: Sequence[Mapping[str, Any]], retirements: Mapping[str, Any]) -> DischargeReport:
    """The same ordered two-sided accounting over the endpoint/provenance oracle."""
    report = DischargeReport()
    statuses = case.discharges.get("endpoints") or {}
    if set(statuses) != {str(row["ordinal"]) for row in case.endpoint_rows}:
        report.problems.append("discharges.json does not name every frozen endpoint ordinal exactly once")
        return report
    expected = []
    for row in case.endpoint_rows:
        status = statuses[str(row["ordinal"])]
        if status == DISCHARGED:
            report.discharged += 1
            expected.append(row["payload_sha256"])
        elif status.startswith(RETIRED_PREFIX):
            report.retired += 1
            problem = _retirement_problem(
                "endpoint row {0}".format(row["ordinal"]), status[len(RETIRED_PREFIX):], retirements
            )
            if problem:
                report.problems.append(problem)
        elif status == PENDING:
            report.pending += 1
        else:
            report.problems.append("endpoint row {0} carries an unknown status".format(row["ordinal"]))
    derived_digests = [_digest(row) for row in derived]
    if derived_digests != expected:
        report.problems.append("derived endpoint sequence differs from the discharged sequence")
    return report


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
