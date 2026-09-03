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
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from boomi_mcp.connector_replay.digests import (  # noqa: E402
    operation_record_digest_v1,
)
from boomi_mcp.connector_replay.ids import authored_contract_ref  # noqa: E402
from boomi_mcp.connector_replay.models import (  # noqa: E402
    OperationContractRecordV1,
)
from boomi_mcp.connector_replay.ingest import (  # noqa: E402
    IngestRefused,
    ingest,
    verify_archive,
)

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
# `e8` SUPERSEDES `e7`, which stays archived rather than being overwritten: the
# evidence archive is append-only, and a capture is the primary record of what
# the account actually did at a moment. `e7` was taken when the connection was
# at version 1; the drift cells that had to run in the same QA round advanced it
# to version 3, and a component version is monotonic — so the record `e7` mints
# can no longer be placed against the live component. The two differ in exactly
# five values, and BOTH configuration digests are among the ones that did not
# move: that is the credential-only version advance this issue documents,
# observed rather than argued.
def _grammar_safe_identifier(raw: str) -> str:
    """Fold one raw vocabulary value onto the contract grammar's alphabet.

    Deliberately narrow: lower-case, and hyphens and dots to underscores. It is
    NOT asked to be injective — nothing about a fold can promise that — which is
    why every caller records its output and lets the registry models refuse a
    collision. A fold that silently merged two raw values would give two
    contracts one name, and the failure would surface as a reference resolving to
    the wrong evidence rather than as an error here.
    """
    return re.sub(r"[^a-z0-9_]", "_", raw.lower())


OPERATION_RECORD_CAPTURES = ("cap155-e9-patch-operation-record",)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true",
                        help="update the packaged registry; otherwise report only")
    args = parser.parse_args()

    absent = sorted(name for name in ACTIONS if not (CAPTURES / name).is_dir())
    if absent:
        # REFUSED, not skipped. Filtering a missing capture out let `--write`
        # exit zero and replace the packaged registry with a SMALLER evidence
        # set — which could leave a capability served as supported with no
        # production record behind it. The ingester's own rule is that an
        # unverifiable candidate is refused; an absent one is the same case.
        print("refusing to publish: configured captures are missing: "
              + ", ".join(absent), file=sys.stderr)
        return 2
    present = dict(ACTIONS)

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

    # THE GRAMMAR-SAFE ACTION IDENTIFIERS, DERIVED HERE AND RECORDED THERE.
    # The registry's raw vocabulary is the platform's, and the platform's is not
    # grammar-safe: verbs are upper case and a connector type carries hyphens and
    # an account segment. A contract reference needs a closed lowercase alphabet,
    # so something must fold — and the choice is whether the fold happens at mint,
    # once, where a collision can be refused while the whole vocabulary is in
    # view, or at every point of use, where two raw values quietly becoming one
    # identifier is invisible. It happens here. The model then re-checks totality
    # and injectivity on load, so the recorded map cannot be edited into a lie.
    for entry in payload.get("vocabulary", []):
        entry["action_ids"] = sorted(
            [action, _grammar_safe_identifier(action)]
            for action in entry.get("recognised_actions", [])
        )

    # OPERATION RECORDS and the semantics they cite, read from the capture that
    # minted them. A record is account-scoped, so unlike the class-level evidence
    # rows it is only meaningful on the account it was minted against — and the
    # registry refuses any record whose semantics definition it cannot resolve,
    # so the two must be published together or neither loads.
    definitions, records = [], []
    for name in sorted(OPERATION_RECORD_CAPTURES):
        directory = CAPTURES / name
        if not directory.is_dir():
            print(f"refusing to publish: operation-record capture {name} is missing",
                  file=sys.stderr)
            return 2
        # VERIFIED against the archive manifest, exactly as the class-level
        # captures are. These two files carry the account, the component
        # identities and the semantics this registry serves as authority, and
        # they were parsed directly — so an altered file beside a stale manifest
        # could have been published, because the registry validates SHAPE and
        # not archive bytes.
        verify_archive(ARCHIVE, directory)
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
        # THE REFERENCE IS DERIVED, NOT COPIED. The capture carries a
        # `contract_ref` its author typed, and that author was me — the same hand
        # the record is meant to be independent of. Rebuilding it from the
        # record's own family, action, semantics and revision means the published
        # name follows from the published facts, and the registry re-derives it on
        # load, so a name and the record it names cannot drift apart. The capture
        # file is left exactly as archived: it is evidence, and evidence is not
        # edited to agree with a later decision about naming.
        record = dict(record, contract_ref=authored_contract_ref(
            record["family"],
            _grammar_safe_identifier(record["action"]),
            record["semantics_id"],
            record["semantics_revision"],
        ))
        # AND THE DIGEST FOLLOWS THE CONTENT. The digest is the evidence
        # IDENTITY — grants carry it, the apply recheck matches on it, the durable
        # attestation records it — so deriving a new reference without recomputing
        # it would publish a record whose identity described an earlier version of
        # itself. The loader recomputes and refuses a mismatch, which is how this
        # was caught rather than shipped.
        record["record_digest"] = operation_record_digest_v1(
            OperationContractRecordV1.model_validate(
                {k: v for k, v in record.items() if k != "record_digest"}
                | {"record_digest": "0" * 64}
            )
        )
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
