#!/usr/bin/env python3
"""Capture the two SOAP-target WSS listener anchors from the LEGACY renderer (#158).

Issue #158 requires these bytes to be captured BEFORE either routing gate flips:
once `_sync_pipeline_is_canonical` admits listener chains and `adapt_sync_pipeline`
stops refusing them, the legacy renderer is no longer reached for these inputs and
this evidence can no longer be produced independently of the implementation under
test (CLAUDE.md, Stage 1 step 2: legacy-oracle parity captures).

Run it against a PRISTINE extraction of the step-0 baseline tree, never the working
tree, and pass that extraction as ORACLE_ROOT:

    git archive <baseline> | tar -x -C <oracle>
    ln -s <repo>/.venv <oracle>/.venv
    <oracle>/.venv/bin/python capture_preflip_anchors.py <oracle> <out-dir>

The two chains are the existing listener corpus with its target swapped for a SOAP
Client send, built from the same corpus helpers the four existing
`sync_pipeline_listener_*.xml` anchors were built from. Each form is rendered twice,
through `SyncPipelineBuilder.build` (the production entry, which routes a listener
chain to the legacy arm at the baseline) and through `ProcessFlowBuilder.build` on the
lowered config (the legacy renderer called directly). Both must agree byte-for-byte,
or the capture refuses. Bytes are written as produced, with no reserialization.
"""

import copy
import hashlib
import json
import sys
from pathlib import Path


def main(oracle_root: str, out_dir: str) -> int:
    oracle = Path(oracle_root).resolve()
    out = Path(out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    for candidate in (oracle / "src", oracle, oracle / "tests", oracle / "tests" / "patterns"):
        sys.path.insert(0, str(candidate))

    import _wave_gate_golden_corpus as corpus
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        ProcessFlowBuilder,
        SyncPipelineBuilder,
    )

    def soap_send(key="t", **cfg):
        return corpus.chain_stage(
            key, "send", {"primitive": "soap_send", **corpus.CHAIN_TGT}, cfg
        )

    chains = {
        "listener_soap_send": [corpus.chain_listen(), soap_send()],
        "listener_map_soap_send": [corpus.chain_listen(), corpus.chain_map(), soap_send()],
    }

    baseline = (oracle / ".baseline-sha").read_text().strip()
    artifacts = []
    for chain, stages in chains.items():
        raw = corpus.listener_pipeline(copy.deepcopy(stages))
        name = corpus.listener_chain_golden_name(chain)
        via_sync = SyncPipelineBuilder.build(
            copy.deepcopy(raw), name=name, folder_name="Golden/Fixtures"
        )
        via_legacy = ProcessFlowBuilder.build(
            SyncPipelineBuilder.lower_config(copy.deepcopy(raw)),
            name=name,
            folder_name="Golden/Fixtures",
        )
        if via_sync != via_legacy:
            raise SystemExit(
                "refusing: SyncPipelineBuilder.build and the legacy renderer disagree "
                "for " + chain
            )
        data = via_legacy.encode("utf-8")
        xml_name = "sync_pipeline_" + chain + ".xml"
        (out / xml_name).write_bytes(data)
        input_name = chain + ".input.json"
        (out / input_name).write_text(
            json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        artifacts.append({
            "chain": chain,
            "component_name": name,
            "folder_name": "Golden/Fixtures",
            "input": input_name,
            "output": xml_name,
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "producers_agree": ["SyncPipelineBuilder.build", "ProcessFlowBuilder.build(lower_config)"],
        })

    manifest = {
        "issue": 158,
        "kind": "pre-flip legacy-oracle listener anchor capture",
        "provenance": (
            "legacy renderer at the step-0 baseline, executed from a pristine "
            "`git archive` extraction of that commit (never the working tree)"
        ),
        "baseline_sha": baseline,
        "python": sys.version.split()[0],
        "invocation": "capture_preflip_anchors.py <oracle-extraction> <out-dir>",
        "artifacts": artifacts,
    }
    (out / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: capture_preflip_anchors.py <oracle-root> <out-dir>")
    sys.exit(main(sys.argv[1], sys.argv[2]))
