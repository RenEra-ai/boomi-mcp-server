#!/usr/bin/env python3
"""Mint the packaged connector-replay registry from the capture archive (#155 F).

The mechanism landed dark in slice B; this is the script that runs it for real.
Everything served here is DERIVED from captures whose bytes are checksummed
against the archive manifest — no row is written from a claim in this file.

The one thing this file DOES assert is which capture exercised which verb, and
that assertion is not taken on trust: `ingest()` reconciles it against the
capture's own observed components and its counterparty log, and refuses the
capture when they disagree. A directory name is not evidence.

Run with `--write` to update the packaged registry; without it, the script
reports what it would write and changes nothing.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from boomi_mcp.connector_replay.ingest import ingest, IngestRefused  # noqa: E402

ARCHIVE = ROOT / "docs/architecture/evidence/issue-155"
CAPTURES = ARCHIVE / "captures"
REGISTRY = ROOT / "src/boomi_mcp/connector_replay/registry_v1.json"

#: Capture directory -> the verb it exercised. RECONCILED by `ingest()` against
#: what the capture observed; a wrong entry here is refused, not recorded.
#:
#: Only ATTESTED captures appear. The `cap155-e2-*` set ran two executions but
#: archived no counterparty log, and the classifier holds — correctly — that an
#: execution status cannot stand in for an outcome, because the platform reports
#: COMPLETE for a request the counterparty refused. Measured: every one of them
#: classifies UNKNOWN.
ACTIONS = {
    "cap155-e5-delete-attested": "DELETE",
    "cap155-e5-patch-attested": "PATCH",
    "cap155-e4-head-status": "HEAD",
    "cap155-e4-options-status": "OPTIONS",
    "cap155-e4-trace-status": "TRACE",
    "cap155-e6-post-attested": "POST",
    "cap155-e6-put-attested": "PUT",
}

#: Captures that minted an ACCOUNT-SCOPED operation contract record. Separate
#: from the class-level set above because a record's meaning is bounded by the
#: account it was observed on, which the class-level rows are deliberately not.
OPERATION_RECORD_CAPTURES = ("cap155-e7-patch-operation-record",)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true",
                        help="update the packaged registry; otherwise report only")
    args = parser.parse_args()

    present = {name: verb for name, verb in ACTIONS.items()
               if (CAPTURES / name).is_dir()}
    absent = sorted(set(ACTIONS) - set(present))
    for name in absent:
        print(f"absent, no row minted: {name} ({ACTIONS[name]})")

    try:
        rows = ingest(ARCHIVE, [CAPTURES / n for n in present],
                      family="rest", actions=present)
    except IngestRefused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2

    for row in rows:
        print(f"{row.family}/{row.action}: side_effect={row.side_effect.value} "
              f"retry_safety={row.retry_safety.value} "
              f"digest={row.capture_digest[:16]}")

    payload = json.loads(REGISTRY.read_text(encoding="utf-8"))

    # OPERATION RECORDS and the semantics they cite, read from the capture that
    # minted them. A record is account-scoped, so unlike the class-level evidence
    # rows it is only meaningful on the account it was minted against — and the
    # registry refuses any record whose semantics definition it cannot resolve,
    # so the two must be published together or neither loads.
    definitions, records = [], []
    for name in sorted(OPERATION_RECORD_CAPTURES):
        directory = CAPTURES / name
        if not directory.is_dir():
            print(f"absent, no operation record minted: {name}")
            continue
        record = json.loads((directory / "operation_record.json").read_text("utf-8"))
        derivation = json.loads((directory / "record_derivation.json").read_text("utf-8"))
        definition = derivation.get("semantics_definition")
        if definition is None:
            raise SystemExit(
                f"{name}: the capture carries an operation record but no semantics "
                "definition, and the registry refuses a record it cannot resolve"
            )
        # Only the fields the registry's model defines; the derivation carries its
        # own provenance alongside them and that stays in the archive.
        definitions.append({
            k: definition[k] for k in
            ("semantics_id", "revision", "mechanism", "key_scope", "duplicate_guarantee")
        })
        records.append(record)
        print(f"operation record: {record['family']}/{record['action']} "
              f"{record['contract_ref']} scope={record['account_scope_hash'][:16]}")

    seen, unique = set(), []
    for definition in definitions:
        key = (definition["semantics_id"], definition["revision"])
        if key not in seen:
            seen.add(key)
            unique.append(definition)
    payload["semantics_definitions"] = sorted(
        unique, key=lambda d: (d["semantics_id"], d["revision"]))
    payload["operation_records"] = sorted(
        records, key=lambda r: (r["family"], r["action"], r["contract_ref"]))

    payload["evidence_records"] = [
        json.loads(r.model_dump_json(exclude_none=True))
        for r in sorted(rows, key=lambda r: (r.family, r.action))
    ]
    rendered = json.dumps(payload, indent=1, sort_keys=False) + "\n"

    if not args.write:
        print(f"\n(dry run) {len(rows)} evidence record(s) would be written to "
              f"{REGISTRY.relative_to(ROOT)}")
        return 0
    REGISTRY.write_text(rendered, encoding="utf-8")
    print(f"\nwrote {len(rows)} evidence record(s) to {REGISTRY.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
