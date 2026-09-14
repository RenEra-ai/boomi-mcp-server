"""Build the capture index behind #184's document-emission authority (amendment 3 §1).

Each measured cache rule the authority states is bound to the archived live evidence that
decides it: the capture, its verdict row, the row's recorded verdict and observation, the
runs and components that row cites, and the sha256 of every one of those files.

Only the RULE → ROW mapping below is authored. Everything else is read from the capture
MANIFESTs and the archive's `SHA256SUMS`: verdicts, observations, file lists, hashes. Each
binding also names the verdict the row must carry, so a mapping to the wrong row fails
here instead of silently pointing at unrelated evidence. A rule whose deciding row is OPEN
is recorded as OPEN; it is never admitted.

Run from the repository root:
    .venv/bin/python docs/architecture/evidence/issue-184/document_emission/build_capture_index.py
"""

from __future__ import annotations

import fnmatch
import json
import posixpath
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
ARCHIVE = ROOT / "docs/architecture/evidence/issue-184"
OUT = Path(__file__).resolve().parent / "CAPTURE_INDEX.json"

#: rule id -> (status, [(capture id, verdict row index, required recorded verdict), ...], controls)
RULES = {
    "cache_load.emits_zero_documents": ("measured", [
        ("cap184-cache-put-successor", 0, "REFUTE"),
        ("cap184-cache-put-successor", 1, "REFUTE"),
        ("cap184-cache-put-successor", 2, "REFUTE"),
        ("cap184-cache-put-successor", 3, "ADMIT"),
    ], []),
    "cache_load.terminal_executes": ("measured", [
        ("cap184-prefix-predecessors", 4, "ADMIT"),
        ("cap184-prefix-predecessors", 5, "ADMIT"),
        ("cap184-retrieve-ddp-replacement", 0, "ADMIT"),
    ], []),
    "cache_remove_all.emits_zero_documents": ("measured", [
        ("cap184-prefix-predecessors", 0, "REFUSE"),
        ("cap184-prefix-predecessors", 1, "REFUSE"),
        ("cap184-prefix-predecessors", 2, "REFUSE"),
        ("cap184-prefix-predecessors", 3, "REFUSE"),
        ("cap184-prefix-predecessors", 18, "REFUSE"),
        ("cap184-cache-remove-read", 0, "REFUTE"),
    ], ["cap184-prefix-predecessors/controls/legacy_builder_doccacheremove_emission_offline.txt"]),
    "cache_retrieve.requires_arriving_document": ("measured", [
        ("cap184-cache-put-successor", 0, "REFUTE"),
        ("cap184-cache-put-successor", 1, "REFUTE"),
        ("cap184-cache-remove-read", 0, "REFUTE"),
        ("cap184-shared-cache", 2, "ADMIT"),
    ], []),
    "cache_retrieve.empty_cache_emits_no_documents": ("measured", [
        ("cap184-shared-cache", 2, "ADMIT"),
    ], []),
    "cache_retrieve.replaces_payload_with_cached_documents": ("measured", [
        ("cap184-shared-cache", 0, "ADMIT"),
        ("cap184-shared-cache", 1, "ADMIT"),
        ("cap184-prefix-predecessors", 4, "ADMIT"),
        ("cap184-prefix-predecessors", 5, "ADMIT"),
        ("cap184-retrieve-ddp-replacement", 0, "ADMIT"),
    ], []),
    "cache_retrieve.ddp_overlay_one_current_one_cached": ("measured", [
        ("cap184-retrieve-ddp-replacement", 0, "ADMIT"),
        ("cap184-retrieve-ddp-replacement", 1, "ADMIT"),
        ("cap184-retrieve-ddp-replacement", 2, "ADMIT"),
        ("cap184-retrieve-ddp-replacement", 3, "ADMIT"),
        ("cap184-passthrough-ddp-handoff", 2, "REFUSE"),
    ], []),
    "cache_retrieve.ddp_overlay_many_current_many_cached": ("open", [
        ("cap184-retrieve-ddp-replacement", 4, "OPEN"),
    ], []),
    "cache_retrieve.after_non_producing_call": ("open", [
        ("cap184-send-then-read", 0, "OPEN"),
    ], []),
    "exception.parameter_source_none_refused_on_create": ("measured", [], [
        "cap184-cache-put-successor/controls/exception_binding_none_probe.json",
        "cap184-cache-put-successor/controls/exception_binding_none.submitted.xml",
    ]),
    "start.scheduled_supplies_one_empty_document": ("measured", [
        ("cap184-passthrough-standalone", 0, "ADMIT"),
        ("cap184-nodata-per-document", 0, "ADMIT"),
    ], []),
}


def _sums():
    sums = {}
    for line in (ARCHIVE / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, path = line.split("  ", 1)
        sums[path] = digest
    return sums


def _resolve(capture: str, pattern: str, sums) -> list:
    rel = posixpath.normpath(posixpath.join("captures", capture, pattern))
    matched = sorted(p for p in sums if fnmatch.fnmatchcase(p, rel))
    if not matched:
        raise SystemExit("{0}: evidence {1} matches no archived file".format(capture, pattern))
    return [{"file": p, "sha256": sums[p]} for p in matched]


def main() -> int:
    sums = _sums()
    manifests = {}
    index = []
    for rule, (status, bindings, controls) in RULES.items():
        rows = []
        for capture, row_index, required in bindings:
            if capture not in manifests:
                manifests[capture] = json.loads((ARCHIVE / "captures" / capture / "MANIFEST.json").read_text(encoding="utf-8"))
            row = manifests[capture]["verdict"]["rows"][row_index]
            if row["verdict"] != required:
                raise SystemExit("{0}: {1}[{2}] records {3}, not {4}".format(rule, capture, row_index, row["verdict"], required))
            files = []
            for pattern in row.get("evidence") or []:
                files += _resolve(capture, pattern, sums)
            files += [{"file": "captures/{0}/MANIFEST.json".format(capture), "sha256": sums["captures/{0}/MANIFEST.json".format(capture)]}]
            rows.append({
                "capture": capture,
                "row_index": row_index,
                "row": row["row"],
                "verdict": row["verdict"],
                "checks": row.get("checks") or {},
                "observation": row.get("observation"),
                "files": files,
            })
        control_files = []
        for control in controls:
            key = "captures/" + control
            if key not in sums:
                raise SystemExit("{0}: control {1} is not archived".format(rule, control))
            control_files.append({"file": key, "sha256": sums[key]})
        if status == "measured" and not any(r["verdict"] != "OPEN" for r in rows) and not control_files:
            raise SystemExit("{0} is marked measured with no deciding evidence".format(rule))
        if status == "open" and any(r["verdict"] != "OPEN" for r in rows):
            raise SystemExit("{0} is marked open but cites a decided row".format(rule))
        index.append({"rule": rule, "status": status, "rows": rows, "controls": control_files})
    OUT.write_text(json.dumps({
        "purpose": "#184 amendment 3 §1: measured cache rules bound to their deciding live evidence",
        "script": "docs/architecture/evidence/issue-184/document_emission/build_capture_index.py",
        "sums": "docs/architecture/evidence/issue-184/SHA256SUMS",
        "rules": index,
    }, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    for entry in index:
        print(entry["rule"], entry["status"], len(entry["rows"]), "rows", len(entry["controls"]), "controls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
