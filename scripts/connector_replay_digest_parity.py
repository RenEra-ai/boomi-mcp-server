#!/usr/bin/env python3
"""Recompute the connector-replay digests and compare them to the host fixture.

Run on the host it regenerates nothing; run INSIDE a built image it answers the
question the image-parity workflow exists to ask. Loading the packaged registry
proves the file shipped, which is necessary and not sufficient: the image must also
compute the SAME identities from the same bytes. A digest that differs between host
and image means evidence captured on one cannot be matched on the other, and the
registry's whole purpose is that matching.

Exit 0 on parity, 1 on any mismatch. Prints what differed rather than only that
something did.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

FIXTURE = "tests/fixtures/connector_replay/digest_parity.json"
CAPTURES = "docs/architecture/evidence/issue-155/captures"
CONNECTION = "cap155-e1-conn-readback/rest-conn-c4281346.xml"


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / FIXTURE
    if not fixture_path.is_file():
        print(f"FAIL: {FIXTURE} is missing; there is nothing to compare against",
              file=sys.stderr)
        return 1
    expected = json.loads(fixture_path.read_text())

    from boomi_mcp.connector_replay.digests import (
        component_config_digest_v1,
        route_digest_v1,
    )
    from boomi_mcp.connector_replay.registry import load_registry

    captures = root / CAPTURES
    conn = (captures / CONNECTION).read_text()

    problems: list[str] = []

    actual_conn = component_config_digest_v1(conn, "connection")
    if actual_conn != expected["connection_config_digest"]:
        problems.append(
            f"connection config digest: host {expected['connection_config_digest']} "
            f"!= here {actual_conn}")

    registry = load_registry()
    if len(registry.vocabulary) != expected["vocabulary_size"]:
        problems.append(
            f"vocabulary size: host {expected['vocabulary_size']} "
            f"!= here {len(registry.vocabulary)}")

    by_scenario = {row["scenario"]: row for row in expected["operations"]}
    seen = set()
    for op in sorted(captures.rglob("operation_component.xml")):
        scenario = op.parent.name
        seen.add(scenario)
        row = by_scenario.get(scenario)
        if row is None:
            problems.append(f"{scenario}: present here but absent from the host fixture")
            continue
        xml = op.read_text()
        for label, actual in (("route_digest", route_digest_v1(conn, xml)),
                              ("config_digest", component_config_digest_v1(xml, "operation"))):
            if actual != row[label]:
                problems.append(f"{scenario} {label}: host {row[label]} != here {actual}")

    missing = sorted(set(by_scenario) - seen)
    if missing:
        problems.append(f"present in the host fixture but absent here: {missing}")

    if problems:
        print("DIGEST PARITY FAILED:", file=sys.stderr)
        for line in problems:
            print("  " + line, file=sys.stderr)
        return 1

    print(f"digest parity OK: {len(seen)} operations, "
          f"{len(registry.vocabulary)} vocabulary entries, connection digest matches")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
