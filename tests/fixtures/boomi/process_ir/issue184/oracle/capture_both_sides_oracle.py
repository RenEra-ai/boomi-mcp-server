#!/usr/bin/env python3
"""Capture the legacy both-sides dynamic-path oracle (#184 A8, EVAL-155-02).

Issue #184 A8 defines the mapped both-sides form by the LEGACY renderer's spine,
not by prose: `ProcessFlowBuilder.build()` on a `database_to_api_sync` config whose
REST `source` and REST `target` both carry `dynamic_path` emits

    documentproperties -> connectoraction(GET, Path) -> map
        -> documentproperties -> connectoraction(PATCH, Path)

and requires that oracle to be frozen BEFORE the issue's step-0 baseline, with its
provenance recorded. None of the six existing dynamic-path goldens binds more than
one connector, so none of them is an oracle for this form.

Run it against a PRISTINE extraction of the branch point, never the working tree:

    git archive <branch-point> | tar -x -C <oracle>
    printf '%s\\n' <branch-point> > <oracle>/.baseline-sha
    ln -s <repo>/.venv <oracle>/.venv
    <oracle>/.venv/bin/python capture_both_sides_oracle.py <oracle> <out-dir>

The config is the existing #155 source-role corpus config
(`dynpath_source_role_config`) with a second, target-side profile-bearing
`dynamic_path` added to its REST PATCH target. The target path reads a DIFFERENT
request profile (`MAPPED-PROFILE-UUID`) from the source path (`PROFILE-UUID`),
because the canonical spelling this oracle anchors places the target-side writer
AFTER the map, reading the map's OUTPUT profile.

The capture refuses unless: the legacy validator accepts the config (with the REST
source admitted, exactly as the production route admits it); two independent builds
agree byte-for-byte; and the emitted spine is exactly the one A8 names, with a
non-empty dynamic `Path` property on BOTH connector shapes. Bytes are written as
produced, with no reserialization.
"""

import copy
import hashlib
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

COMPONENT_NAME = "Dynamic Path Both Sides Golden"
FOLDER_NAME = "Golden/Fixtures"

EXPECTED_SPINE = (
    ("start", None),
    ("documentproperties", None),
    ("connectoraction", "GET"),
    ("map", None),
    ("documentproperties", None),
    ("connectoraction", "PATCH"),
    ("stop", None),
)


def both_sides_config(corpus):
    """The attested architect design's frozen config (`.codex/plans/issue-184.md` §3).

    The SOURCE path composes from a run-supplied dynamic PROCESS property, the
    live-attested source-role driver (`cap155-e1-source-dynamic-path`); a profile
    read there would read the scheduled start document. The TARGET path reads a
    profile element from the MAPPED document, so its request profile
    (`PROFILE-REQUEST`) is the map's output profile, distinct from the GET's.
    """
    del corpus
    return {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "rest",
            "action_type": "GET",
            "connection_id": "RCONN",
            "operation_id": "ROP",
            "dynamic_path": {
                "ddp_name": "DDP_PATH_SOURCE",
                "request_profile_id": None,
                "profile_type": None,
                "segments": [
                    {"type": "static", "value": "/v1/customers/"},
                    {"type": "dpp", "property_name": "seed_id"},
                ],
            },
        },
        "transform": {"mode": "map_ref", "map_ref": "MAP-UUID"},
        "target": {
            "connector_type": "rest",
            "action_type": "PATCH",
            "connection_id": "CONN-UUID",
            "operation_id": "OP-UUID",
            "dynamic_path": {
                "ddp_name": "DDP_PATH_TARGET",
                "request_profile_id": "PROFILE-REQUEST",
                "profile_type": "profile.json",
                "segments": [
                    {"type": "static", "value": "/v1/requests/"},
                    {
                        "type": "profile",
                        "element_id": 3,
                        "element_name": "requestId (Root/Object/requestId)",
                    },
                ],
            },
        },
    }


def spine_of(xml_text):
    root = ET.fromstring(xml_text)
    shapes = [el for el in root.iter() if el.tag.endswith("shape")]
    by_name = {shape.get("name"): shape for shape in shapes}
    spine = []
    cursor = shapes[0] if shapes else None
    seen = set()
    while cursor is not None and cursor.get("name") not in seen:
        seen.add(cursor.get("name"))
        actions = [el.get("actionType") for el in cursor.iter() if el.get("actionType")]
        dynamic = sum(
            len(list(el)) for el in cursor.iter() if el.tag.endswith("dynamicProperties")
        )
        spine.append((cursor.get("shapetype"), actions[0] if actions else None, dynamic))
        targets = [d.get("toShape") for d in cursor.iter() if d.tag.endswith("dragpoint")]
        if len(targets) > 1:
            raise SystemExit("refusing: shape {0} fans out".format(cursor.get("name")))
        cursor = by_name.get(targets[0]) if targets else None
    if len(spine) != len(shapes):
        raise SystemExit("refusing: the wired spine does not reach every shape")
    return spine


def main(oracle_root, out_dir):
    oracle = Path(oracle_root).resolve()
    out = Path(out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    for candidate in (oracle / "src", oracle, oracle / "tests"):
        sys.path.insert(0, str(candidate))

    import _wave_gate_golden_corpus as corpus
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        ProcessFlowBuilder,
    )

    loaded = Path(sys.modules["boomi_mcp"].__file__).resolve()
    if oracle not in loaded.parents:
        raise SystemExit("refusing: boomi_mcp was imported from outside the oracle tree")

    config = both_sides_config(corpus)
    error = ProcessFlowBuilder.validate_config(
        copy.deepcopy(config), depends_on=[], allow_rest_source=True
    )
    if error is not None:
        raise SystemExit("refusing: the legacy validator rejects the config: {0}".format(error))

    first = ProcessFlowBuilder.build(
        copy.deepcopy(config), name=COMPONENT_NAME, folder_name=FOLDER_NAME
    )
    second = ProcessFlowBuilder.build(
        copy.deepcopy(config), name=COMPONENT_NAME, folder_name=FOLDER_NAME
    )
    if first != second:
        raise SystemExit("refusing: two builds of the same config disagree")

    spine = spine_of(first)
    if tuple((kind, action) for kind, action, _dynamic in spine) != EXPECTED_SPINE:
        raise SystemExit("refusing: unexpected spine {0}".format(spine))
    dynamic_by_kind = [dynamic for kind, _action, dynamic in spine if kind == "connectoraction"]
    if dynamic_by_kind != [1, 1]:
        raise SystemExit(
            "refusing: each connector must carry one dynamic Path property, got {0}".format(
                dynamic_by_kind
            )
        )

    data = first.encode("utf-8")
    xml_name = "dynamic_path_both_sides_profile.xml"
    (out / xml_name).write_bytes(data)
    input_name = "dynamic_path_both_sides_profile.input.json"
    (out / input_name).write_text(
        json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    manifest = {
        "issue": 184,
        "kind": "pre-baseline legacy-oracle capture (EVAL-155-02 mapped both-sides spine)",
        "provenance": (
            "legacy renderer ProcessFlowBuilder.build at the branch point, executed "
            "from a pristine `git archive` extraction of that commit (never the "
            "working tree), before any #184 source change"
        ),
        "branch_point_sha": (oracle / ".baseline-sha").read_text().strip(),
        "python": sys.version.split()[0],
        "invocation": "capture_both_sides_oracle.py <oracle-extraction> <out-dir>",
        "validator": "ProcessFlowBuilder.validate_config(allow_rest_source=True) -> None",
        "spine": [list(item) for item in spine],
        "artifacts": [
            {
                "component_name": COMPONENT_NAME,
                "folder_name": FOLDER_NAME,
                "input": input_name,
                "output": xml_name,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                "builds_agree": 2,
            }
        ],
    }
    (out / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: capture_both_sides_oracle.py <oracle-root> <out-dir>")
    sys.exit(main(sys.argv[1], sys.argv[2]))
