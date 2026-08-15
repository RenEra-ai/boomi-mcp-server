# Issue #152 (M12.13) — durable review-evidence archive

Byte-for-byte copies of the collector-written review artifacts for issue #152, salvaged from
the operator machine's session-lifetime `/tmp` run directories on 2026-08-15 (source tip
`adfd8b53f5a38fbebc86eac646c4917c1ae9e016`). Every file was byte-compared against its source
at copy time; `SHA256SUMS` (path-sorted, C collation) covers every archived file except
itself.

## Authority order

1. **Immutable copied files and their hashes** prove provenance. Nothing here is authored;
   `index.jsonl` and this README are the only generated files.
2. **`index.jsonl`** is the machine-readable archive contract (schema_version 1): one header
   row, then one row per archived run with collector, source/durable paths, loop attribution,
   status, scope SHAs, verdict + verdict source, per-file hashes, and explicit limitations.
3. **`../../ISSUE_152_AUDIT_LEDGER.md`** governs reconciliation, severity, deferral, and
   closure. Where a run's verdict text was not persisted by its collector, the ledger row (or
   the raw report, below) is the named verdict source.
4. **Operator hooks** guard live completion claims but do not replace this in-tree record.

## Contents

- `review-report.raw.md` — the raw, in-progress working report for the slice (170,631 bytes).
  **Historical**: its rollout tail is stale; `ISSUE_152_AUDIT_LEDGER.md` from its "Rollout
  evidence (post-landing)" section onward is the final closure authority. It is preserved as
  the only durable home of the pre-terminal per-finding narrative.
- `plans/issue-152.md`, `plans/issue-152.claude.md` — the attested architect input plan and
  the implementation plan, hash-pinned by the gate attestations (`inputPlan.sha256`).
- `architect-reviews/cdx-gate-review.<suffix>/` — 17 gate-attest run dirs: `start.json`,
  `attestation.json`, `review.md`, plus `prompts/` (the attested external prompt directory's
  `brief`/`prompt`/`retry` files). `TnpZpj` is a refused start (only `start.json` +
  `refusal.json`); it is not an evaluation.
- `commit-reviews/cdx-review.<suffix>/` — 70 commit-review-collect run dirs, allowlisted
  sidecars only (`baseline`, `cwd`, `dirty`, `scope`, `start-head`, `last-reviewed-sha`,
  `t0`, `teardown`, `start.json`, `review.json`, `phase` where present). Mutable coordination
  files (locks, sockets, pids, cache paths, probe counters) were deliberately not copied;
  `start.json` preserves the original process/socket metadata.

## Known limitations (recorded, not inferred away)

- Commit-review `review.json` still reads `"running"` after collection and contains no
  verdict body; verdicts come from collector trailers, the raw report, or reconciled ledger
  rows — never from that field.
- Commit-review and architect-review runs have **different attestation schemas**; sidecar
  rules for one must not be applied to the other.
- Historical dirty-tree reviews carry no exact uncommitted-worktree fingerprint.
- `cdx-review.kXfU2v` failed and was replaced by `cdx-review.Kkf8n6` over the same scope.
- `cdx-review.rnxOao` was an auto-scope dirty-tree review at `b2b608b`; it does not attest
  the later `adfd8b5` fixes (verdict provenance: ledger rows 322–332).
- `cdx-review.UBGkaP` covers `27eda5e..b2b608b`; its retained transcript is truncated to the
  final finding and trailers.
- Architect rows for §6 evaluations 12–16 carry `baseline`/`reviewed_sha` values
  **reconstructed** from prompt bases, adjacent commit-review sidecars, and timestamps — not
  collector-attested (`scope_provenance: "reconstructed"`).
- Raw reviewer round labels and policy evaluation numbers are different namespaces; `ordinal`
  is populated only where the ledger itself assigns it, else `null`.
