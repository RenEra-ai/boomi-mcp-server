# Evidence archive — issue #171

Durable evidence for the #171 slice (scratch-branch verification gate). Created in the
Stage-1.5 baseline commit, alongside `../../ISSUE_171_AUDIT_LEDGER.md`, because the
scanner requires every instantiated ledger to own its archive unconditionally — a
citation-syntax detector cannot be total, so the archive's existence is not allowed to
depend on one.

## Authority order

1. **`index.jsonl`** — one header line, then one row per COLLECTED review round. The
   header's `source_tip` is the archive's provenance anchor: a full 40-character commit
   that is an ancestor of `HEAD` (here, the slice's step-0 baseline, which satisfies that
   permanently). Zero run rows is a legal state; it means no round has been collected
   yet, not that a round was lost.
2. **The archived run directories themselves** — the collector's own artifacts, byte for
   byte, with sidecars restricted to that collector type's allowlist. These outrank any
   claim made about them: an `index.jsonl` row is a claim, the sidecars are the evidence.
3. **`SHA256SUMS`** — covers every file in this tree except itself. The scanner asserts
   set equality with what is on disk, so it is regenerated as the LAST action of any
   commit that adds a file here.
4. **`../../ISSUE_171_AUDIT_LEDGER.md`** — the human-facing record. A run the ledger cites
   must exist here, spelled with its complete run-directory name.

## Limitations, stated rather than implied

**GitHub Actions evidence is hash-covered but deliberately NOT indexed.** Whatever
exists under `actions/` and `no-pr/` — run metadata, raw logs, quoted gate stderr, and the
empty pull-request queries — is covered by `SHA256SUMS` and gets no `index.jsonl` row.

*This paragraph deliberately describes the RULE, not the current contents.* Two earlier
revisions stated the contents instead ("archived" before capture, then "not present yet"
after capture), and each went stale the moment the next step ran — twice, in opposite
directions. **Which criteria have been executed is recorded in exactly one place: the
criteria matrix in `../../ISSUE_171_AUDIT_LEDGER.md`.** Read it there; do not infer it
from this file, and do not restate it here. The verifier accepts only the `commit-review-collect` and `gate-attest` collector
schemas; a row shaped like a collector row with no collector behind it is exactly the
fabrication this archive exists to make impossible. The ledger and this README are where
Actions evidence is accounted for.

**The final green run cannot be archived here.** The last preflight run attests the final
tree, so writing its URL into that tree would change the thing it attests. Its SHA and
conclusion are recorded in the slice's final report and on the issue; the earlier runs,
which attest earlier SHAs, are archived normally.
