"""Issue #157 — derive the CANONICAL replay fixtures for the two rich flows forms.

Not a ``test_*`` module. This is the PROVENANCE artifact the issue requires: it
records HOW each ``input.canonical.json`` was derived from its immutable legacy
input, and it is auditable but never executed at replay time — the replay test
reads the committed canonical fixture and drives the canonical projection only.

    PYTHONPATH=src .venv/bin/python tests/_issue_157_canonical_fixtures.py --write
    PYTHONPATH=src .venv/bin/python tests/_issue_157_canonical_fixtures.py --compare

**Derivation.** For each rich case (the DB-source and API-source sync
producers) the BASELINE archetype is executed once more from a pristine
``git archive`` extraction, and its emitted SUPPORTING components — the
connections, operations, profiles and map the legacy process was assembled
over — become the canonical request's ``components`` verbatim. The legacy
``process`` component is dropped and the process is re-authored as ProcessIR:
``source(connection, operation) -> map_ref(map, label) -> target(connection,
operation) -> stop``. The map node's ``label`` carries the legacy row's display name,
which is content. Nothing else is invented: every profile field, mapping and
component name the canonical projection reads was emitted by the baseline
producer for that exact case.

**Why this is legitimate provenance and not a legacy parser at replay.** The
mapping's DOMAIN is the legacy archetype output, so running it at replay would
be the deleted producer one layer up (issue body, in-scope item 6). It runs once
here, its result is committed, and ``provenance.json`` pins the extraction tree,
the legacy input digest and this script's own digest so the derivation is
auditable after #160 deletes the producers.
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
from typing import Any, Dict, List, Mapping, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _issue_157_flows_accounting import (  # noqa: E402
    ARCHETYPE_NAMES,
    BASELINE_SHA,
    CASES,
    CASES_DIR,
    _extract_baseline,
    derive_case_projection,
    load_cases,
)

#: The cases whose rich `transform` row this issue discharges: every case of
#: the two rich producers.
RICH_PRODUCERS = ("database_to_api_sync", "api_to_api_sync")

COMPONENTS_PROGRAM = r'''
import json, sys
from boomi_mcp.categories.integration_authoring import build_from_archetype_action
payload = json.load(sys.stdin)
out = {}
for case_id, entry in payload.items():
    result = build_from_archetype_action(entry["archetype"], entry["parameters"])
    assert result.get("_success"), (case_id, result.get("error"))
    out[case_id] = result["integration_spec"]["components"]
json.dump(out, sys.stdout)
'''


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _legacy_components(extraction_src: Path, inputs: Mapping[str, Mapping[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(extraction_src)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", COMPONENTS_PROGRAM], input=json.dumps(inputs), text=True,
        capture_output=True, env=env, cwd=str(extraction_src.parent), check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr[-4000:])
    return json.loads(completed.stdout)


def _role(components: Sequence[Mapping[str, Any]], *, type_: str, key_contains: str) -> Dict[str, Any]:
    for component in components:
        if component["type"] == type_ and key_contains in component["key"]:
            return dict(component)
    raise KeyError((type_, key_contains))


def canonical_request(
    case_id: str,
    legacy_row_name: str,
    components: Sequence[Mapping[str, Any]],
    legacy_operations: Sequence[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    """The canonical ``AuthoringRequestV1`` payload for one rich case."""
    producer = case_id.split("/")[0]
    supporting = [dict(c) for c in components if c["type"] != "process"]
    if producer == "database_to_api_sync":
        source_conn = _role(components, type_="connector-settings", key_contains="source_db")
        source_op = _role(components, type_="connector-action", key_contains="source_db")
    else:
        source_conn = _role(components, type_="connector-settings", key_contains="source_rest")
        source_op = _role(components, type_="connector-action", key_contains="source_rest")
    target_conn = _role(components, type_="connector-settings", key_contains="target_rest")
    target_op = _role(components, type_="connector-action", key_contains="target_rest")
    map_component = _role(components, type_="transform.map", key_contains="transform")
    process = next(c for c in components if c["type"] == "process")
    # The caller's per-operation `documentation_hint` is CONTENT the legacy row
    # served but the emitted map component dropped; it is carried onto the
    # canonical mapping entry it annotates (matched by target path) so the
    # canonical projection serves the same note.
    hints = {
        op.get("target_path"): op["documentation_hint"]
        for op in (legacy_operations or ())
        if isinstance(op.get("documentation_hint"), str) and op.get("target_path")
    }
    if hints:
        config = dict(map_component.get("config") or {})
        for list_key in ("field_mappings", "function_mappings"):
            entries = []
            for entry in config.get(list_key) or ():
                entry = dict(entry)
                if entry.get("target_path") in hints:
                    entry["documentation_hint"] = hints[entry["target_path"]]
                entries.append(entry)
            if entries:
                config[list_key] = entries
        map_component["config"] = config
        supporting = [map_component if c["key"] == map_component["key"] else c for c in supporting]
    root = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    "connection_ref": "$ref:" + source_conn["key"],
                    "operation_ref": "$ref:" + source_op["key"],
                },
                {"kind": "map_ref", "map_ref": "$ref:" + map_component["key"], "label": legacy_row_name},
                {
                    "kind": "target",
                    "connection_ref": "$ref:" + target_conn["key"],
                    "operation_ref": "$ref:" + target_op["key"],
                },
                {"kind": "stop"},
            ],
        },
    }
    envelope = {
        "component_key": process["key"],
        "name": process.get("name") or process["key"],
        "action": "create",
        "depends_on": sorted(set(process.get("depends_on") or ()) | {map_component["key"]}),
    }
    folder = (process.get("config") or {}).get("folder_name")
    if folder:
        envelope["folder_name"] = folder
    return {
        "contract_version": "2",
        "intent": {
            "intent_kind": "process_ir",
            "integration_name": "issue-157 replay " + case_id,
            "units": [{"envelope": envelope, "process_ir": root}],
            "components": supporting,
        },
    }


def rich_cases() -> Dict[str, Dict[str, Any]]:
    return {
        case_id: spec for case_id, spec in CASES.items()
        if case_id.split("/")[0] in RICH_PRODUCERS
    }


def write_fixtures() -> None:
    cases = load_cases()
    inputs = {}
    for case_id in rich_cases():
        legacy = cases[case_id].input_legacy["input"]
        inputs[case_id] = {"archetype": legacy["archetype"], "parameters": legacy["parameters"]}
    with tempfile.TemporaryDirectory(prefix="issue157-canonical-") as tmp:
        extraction = Path(tmp) / "tree"
        extraction.mkdir()
        tree_sha = _extract_baseline(BASELINE_SHA, extraction)
        by_case = _legacy_components(extraction / "src", inputs)
    script_digest = _sha256(Path(__file__).read_bytes())
    for case_id, components in sorted(by_case.items()):
        case = cases[case_id]
        transform = next(row for row in case.flow_rows if row["key"] == "transform")
        request = canonical_request(
            case_id,
            transform["payload"]["name"],
            components,
            legacy_operations=case.input_legacy["input"]["parameters"].get("transform", {}).get("operations", ()),
        )
        target = CASES_DIR / case_id / "input.canonical.json"
        target.write_text(json.dumps(request, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
        provenance = {
            "case_id": case_id,
            "derived_by": "tests/_issue_157_canonical_fixtures.py",
            "derived_by_sha256": script_digest,
            "baseline_sha": BASELINE_SHA,
            "baseline_tree_sha": tree_sha,
            "legacy_input_sha256": _sha256((CASES_DIR / case_id / "input.legacy.json").read_bytes()),
            "method": (
                "supporting components = the baseline archetype's emitted component list "
                "(process entry dropped); process re-authored as ProcessIR source -> map_ref -> target; "
                "map label = the frozen transform row's display name"
            ),
            "executed_at_replay": False,
        }
        (CASES_DIR / case_id / "provenance.json").write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print("wrote", case_id)


def compare() -> int:
    from unittest.mock import patch

    cases = load_cases()
    failures = 0
    with patch("boomi_mcp.categories.integration_builder.paginate_metadata", lambda *a, **k: []):
        for case_id in sorted(rich_cases()):
            case = cases[case_id]
            if case.input_canonical is None:
                print(case_id, "NO CANONICAL FIXTURE"); failures += 1; continue
            derived = derive_case_projection(case)
            frozen = next(row for row in case.flow_rows if row["key"] == "transform")["payload"]
            if len(derived) != 1:
                print(case_id, "derived", len(derived), "rows"); failures += 1; continue
            if derived[0] == frozen:
                print(case_id, "MATCH")
            else:
                failures += 1
                print(case_id, "MISMATCH")
                _diff(frozen, derived[0], "")
    return failures


def _diff(a: Any, b: Any, path: str) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(set(a) | set(b)):
            if key not in a:
                print("   +", path + "/" + key, "derived only:", json.dumps(b[key])[:120])
            elif key not in b:
                print("   -", path + "/" + key, "frozen only:", json.dumps(a[key])[:120])
            else:
                _diff(a[key], b[key], path + "/" + key)
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for index, (x, y) in enumerate(zip(a, b)):
            _diff(x, y, path + "/" + str(index))
    elif a != b:
        print("   ~", path, "frozen:", json.dumps(a)[:120], "derived:", json.dumps(b)[:120])


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args(argv)
    if args.write:
        write_fixtures()
    if args.compare:
        return 1 if compare() else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
