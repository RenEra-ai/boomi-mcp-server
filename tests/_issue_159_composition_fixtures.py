"""Issue #159 S1 — derive the CANONICAL replay fixtures for the six composition cases.

Not a ``test_*`` module. Like ``tests/_issue_157_canonical_fixtures.py`` (which
it deliberately does NOT edit, so the nine direct fixtures and the digests their
``provenance.json`` files record stay byte-identical) this is the PROVENANCE
artifact: it records HOW each composition ``input.canonical.json`` was derived
from the case's immutable ``input.legacy.json``, and it never runs at replay.

    PYTHONPATH=src .venv/bin/python tests/_issue_159_composition_fixtures.py --write
    PYTHONPATH=src .venv/bin/python tests/_issue_159_composition_fixtures.py --compare

**Derivation.** Each frozen composition case's legacy ``compose_archetypes``
request is re-spelled into ONE pinned fan-out 2.0.0 invocation by the same
mechanical re-spelling the flagship witness uses
(``_fanout_v2_support.v2_from_compose_request``): secret-free identities and
classifications into ``raw_input``, connection settings / SQL / paths / mapping
into the closed ``component_material``. The request carries ZERO prebuilt
connection or operation pairs, so the replay exercises the v2 preparer's pair
generation. The one added value is the map label, which is the frozen
transform row's display name (content the legacy row served; the same precedent
the nine direct fixtures follow). The one re-spelled value is a create-mode
database ``credential_ref``: fan-out 2.0.0's credential boundary admits only the
opaque ``credential://`` form (CDX-r1-02), so the legacy reference is carried
verbatim under that scheme (``_fanout_v2_support.v2_credential_ref``). No served
flow or endpoint row reads it.

**Naming evidence.** The served-name check needs names the request did not get
from the code under test. A recipe intent authors no component, so the profile
names the BASELINE composition emitted for this exact case are captured once
from a pristine ``git archive`` extraction and recorded in ``provenance.json``
as ``naming_evidence``; a projector that invents a profile name fails against it.
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

from _fanout_v2_support import RECIPE_ID, V2, v2_from_compose_request  # noqa: E402
from _issue_157_flows_accounting import (  # noqa: E402
    BASELINE_SHA,
    CASES,
    CASES_DIR,
    _extract_baseline,
    load_cases,
)

COMPOSITION_PRODUCER = "composition"

#: Runs INSIDE the baseline extraction: the baseline composition's emitted
#: profile component names, per case. Imports nothing from this module.
NAMING_PROGRAM = r'''
import json, sys
from boomi_mcp.categories.integration_authoring import compose_archetypes_action
payload = json.load(sys.stdin)
out = {}
for case_id, entry in payload.items():
    result = compose_archetypes_action(entry["parts"], entry["options"])
    assert result.get("_success", result.get("success")), (case_id, result.get("error"))
    out[case_id] = sorted(
        c["name"] for c in result["integration_spec"]["components"]
        if c["type"] in ("profile.db", "profile.json")
    )
json.dump(out, sys.stdout)
'''


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def composition_cases() -> List[str]:
    return sorted(case_id for case_id in CASES if case_id.split("/")[0] == COMPOSITION_PRODUCER)


def canonical_request(case_id: str, legacy_input: Mapping[str, Any], map_label: str) -> Dict[str, Any]:
    """The canonical ``AuthoringRequestV1`` payload for one composition case."""
    raw_input, material = v2_from_compose_request(dict(legacy_input))
    raw_input["transform"]["map_label"] = map_label
    return {
        "contract_version": "2",
        "intent": {
            "intent_kind": "recipe",
            "integration_name": "issue-157 replay " + case_id,
            "invocations": [
                {
                    "recipe_id": RECIPE_ID,
                    "recipe_version": V2,
                    "invocation_id": "fanout",
                    "raw_input": raw_input,
                    "component_material": material,
                }
            ],
        },
    }


def _baseline_profile_names(inputs: Mapping[str, Mapping[str, Any]]) -> Dict[str, List[str]]:
    with tempfile.TemporaryDirectory(prefix="issue159-composition-") as tmp:
        extraction = Path(tmp) / "tree"
        extraction.mkdir()
        tree_sha = _extract_baseline(BASELINE_SHA, extraction)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(extraction / "src")
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        completed = subprocess.run(
            [sys.executable, "-c", NAMING_PROGRAM],
            input=json.dumps(inputs), text=True, capture_output=True, env=env,
            cwd=str(extraction), check=False,
        )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr[-4000:])
    return {"tree_sha": tree_sha, "names": json.loads(completed.stdout)}


def write_fixtures() -> None:
    cases = load_cases()
    inputs = {case_id: cases[case_id].input_legacy["input"] for case_id in composition_cases()}
    evidence = _baseline_profile_names(inputs)
    script_digest = _sha256(Path(__file__).read_bytes())
    for case_id in composition_cases():
        case = cases[case_id]
        transform = next(row for row in case.flow_rows if row["key"] == "transform")
        request = canonical_request(case_id, inputs[case_id], transform["payload"]["name"])
        directory = CASES_DIR / case_id
        (directory / "input.canonical.json").write_text(
            json.dumps(request, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8"
        )
        provenance = {
            "case_id": case_id,
            "derived_by": "tests/_issue_159_composition_fixtures.py",
            "derived_by_sha256": script_digest,
            "baseline_sha": BASELINE_SHA,
            "baseline_tree_sha": evidence["tree_sha"],
            "legacy_input_sha256": _sha256((directory / "input.legacy.json").read_bytes()),
            "method": (
                "legacy compose_archetypes request re-spelled into one pinned fan-out "
                "2.0.0 invocation (raw_input + component_material) by "
                "_fanout_v2_support.v2_from_compose_request; zero prebuilt pairs; "
                "map label = the frozen transform row's display name"
            ),
            "naming_evidence": {
                "source": "baseline compose_archetypes_action, git archive extraction",
                "profile_names": evidence["names"][case_id],
            },
            "executed_at_replay": False,
        }
        (directory / "provenance.json").write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print("wrote", case_id)


def compare() -> int:
    """Derived vs frozen, both oracles, every composition case. Diagnostic only."""
    from unittest.mock import patch

    from _issue_157_flows_accounting import (
        _digest,
        derive_case_endpoints,
        derive_case_projection,
    )

    cases = load_cases()
    failures = 0
    with patch("boomi_mcp.categories.integration_builder.paginate_metadata", lambda *a, **k: []):
        for case_id in composition_cases():
            case = cases[case_id]
            for side, derive, frozen in (
                ("flows", derive_case_projection, case.flow_rows),
                ("endpoints", derive_case_endpoints, case.endpoint_rows),
            ):
                derived = derive(case)
                expected = [row["payload"] for row in frozen]
                if [_digest(r) for r in derived] == [_digest(r) for r in expected]:
                    print(case_id, side, "MATCH")
                    continue
                failures += 1
                print(case_id, side, "MISMATCH", len(derived), "derived vs", len(expected), "frozen")
                for index, (a, b) in enumerate(zip(expected, derived)):
                    _diff(a, b, "/{0}".format(index))
    return failures


def _diff(a: Any, b: Any, path: str) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(set(a) | set(b)):
            if key not in a:
                print("   +", path + "/" + key, "derived only:", json.dumps(b[key])[:160])
            elif key not in b:
                print("   -", path + "/" + key, "frozen only:", json.dumps(a[key])[:160])
            else:
                _diff(a[key], b[key], path + "/" + key)
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for index, (x, y) in enumerate(zip(a, b)):
            _diff(x, y, path + "/" + str(index))
    elif a != b:
        print("   ~", path, "frozen:", json.dumps(a)[:160], "derived:", json.dumps(b)[:160])


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
