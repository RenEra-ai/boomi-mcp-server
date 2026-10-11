# Completion-workflow rules that outlive a single checkout

`CLAUDE.md` and `AGENTS.md` are **gitignored** (`.gitignore:100-101`). They are the files an agent
reads, but they are per-checkout: a rule written only there reaches nobody else, and a fresh clone
gets none of it. Anything meant to govern FUTURE slices therefore lives here, in version control,
and the local instruction files cite it.

This file holds standing amendments to the completion workflow. It does not restate the workflow —
`CLAUDE.md` does that — only the rules that were adopted later and must survive a clone.

**How a fresh clone reaches this file, and the limit of that.** The tracked `README.md` names it
under *Contributing: the completion workflow*, and
`tests/test_completion_workflow_rules_are_discoverable.py` asserts three things: this document
exists and still carries its rules, the README still points at it, and any local `CLAUDE.md` /
`AGENTS.md` that is PRESENT cites it. What no tracked artifact can do is make an ignored file appear
in a clone — so an agent that is handed a `CLAUDE.md` without the citation is caught by the test,
while an agent working in a clone with no instruction file at all has the README as its entry point.
That is the honest ceiling of this arrangement while the instruction files stay gitignored.

---

## The architect implementation review is capped at THREE evaluations

*Owner rule, 2026-08-22, adopted during #177.*

The architect implementation review (the `/codex-claude:codex-issue` §6 gate) is the one roster gate
with a FIXED window rather than a severity-aware checkpoint. On its third evaluation the loop ENDS:

1. apply that round's batched correction;
2. run the Stage-2 commit review over the correction delta until it returns **CLEAN**;
3. close on that clean commit review.

There is no fourth architect evaluation, and "the architect would probably still find something" is
not a reason to withhold closure — the clean commit review IS the closing gate.

### What the cap does NOT relax

A cap on one gate is not a cap on correctness:

- every applied correction still gets its owed validation **unconditionally**, cap or no cap;
- the composite **wave gate** must still be current on the FINAL tree;
- a **CRITICAL** finding is still undeferrable — the cap ends the architect loop, not the
  critical-residue rules, and a critical finding at evaluation 3 goes to `ESCALATE-OPEN`;
- residue left behind is recorded like any other deferral: enumerated, reason-classed, and filed
  against an already-planned issue.

### Why the cap exists

Recorded from the run that produced it (#177), because the reasoning is the part that transfers.

The architect gate reviews the GUARDS as well as the code, and a guard is an artifact a reviewer can
always harden further. Across three architect evaluations and twenty-two commit-review rounds, every
round found something real — and almost every one of them was in the checking machinery rather than
in the served behaviour it protects. That is not the reviewer being wrong: the findings were correct
and worth fixing. It is a loop with no natural fixed point, because *"is this guard evadable"* has no
closing case set the way *"is this code correct"* does.

Concretely, in #177 one property — binding a "frozen fixture" provenance claim to reality — was
narrowed five times, each version defeated by a witness that met the letter of the claim:
file-was-opened → document-was-parsed → compiled-in-two-helpers → set-equality → the shared compile
core. Each fix was right. None of them was the last one until the binding moved to the single
function every path funnels through.

Three evaluations buys the substantive findings. The rounds after that are increasingly narrow
properties of the checker. The commit review closes instead because it judges the **delta**, which is
finite.

### The general lesson, for gates other than this one

When a checker's findings stop being about the subject and start being about the checker, the
convergent move is to shrink what the checker MODELS — instrument the one authority every path goes
through, or refuse an unreadable form outright — rather than to teach it one more case. A reader over
an open-ended space (Python's call and binding syntax, English prose, Markdown's rendering rules)
cannot make the coverage claim the structural-fix rule requires, and each additional case it learns
disguises that a little longer.

---

## Development-only revision-freshness deferral

*Owner proposal, 2026-10-08; design reviewed with Codex (adopt with changes) and adopted in #134.*

Every correction batch on a compiler slice used to rebuild the revision artifacts — the packaged
behaviour corpus, the authoring contract snapshots and the #149 reachability inventory — because
the suite fails the moment one of them is stale. Those artifacts are a guarantee to CALLERS, and
callers only see what lands on `dev`; rebuilding them for every intermediate branch commit buys
nothing. Measured on 2026-10-10: the corpus harvest alone took 12.5 minutes on `dev`'s 372 KB
corpus and more than 22 minutes on a slice-sized 719 KB one; the inventory rebaseline is 33 seconds.

### The rule

A local pytest correction run MAY set `BOOMI_DEFER_REVISION_GATES=1`. No other value is accepted —
empty, `0`, `true` and padded spellings are usage errors, never a quiet "off". Under it:

- every test marked `revision_freshness(artifact=…)` — a comparison of a committed derived artifact
  against the current source — is skipped;
- an input absent from the packaged corpus is recorded as pending debt instead of failing its test.

Nothing else changes. The corpus plugin stays installed; unreadable corpora, unrecordable inputs and
funnel probes reached around the entry points stay fatal; the revision-movement sweeps and every
behavioural assertion keep running.

Such a run is **PARTIAL VALIDATION**. It prints `PARTIAL VALIDATION — NOT COMPLETION` with the
deferred tests and pending inputs, its audit record names the tree it tested and the obligations
it deferred, and it never establishes a final revision identity or completion.

**Refused wherever debt is paid or a tree is certified**, whatever the value: every `wave_gate.py`
subcommand (`REVISION_DEFERRAL_PRESENT`), pytest under CI, the corpus producer and its check, the
inventory producer and the rebaseline script. Child runs never inherit it.

**Publication.** Publish or deploy only the exact source tree of a commit that passed
`Python 3.11 non-KB` on `dev` or a scratch preflight — never a working tree with outstanding
revision-freshness debt. A branch name, or a SHA label on uploaded files, is not that evidence.

### What closure still owes

Before final wave closure: unset the variable; refresh the affected derived artifacts
(`tests/_revision_corpus.py --write`, `scripts/rebaseline_m12_12_inventory.py`, the contract
snapshots); inspect their semantic diffs — the corpus as decoded inputs, not as "binary changed";
run the full non-KB suite with every guard active; replay the affected live revision/binding QA
(fresh bindings compile and apply, pre-refresh bindings are refused before any write); and obtain
the required fix-only review over the complete unreviewed delta. Every required gate covers the
final tree.

The refresh and any correction it forces belong to the existing rostered wave loop and inherit its
history; regeneration never mints a new loop. Every applied correction receives its owed validation
unconditionally. Deferring during development does not defer validated findings, waive severity
rules, or authorize regenerating an artifact to erase evidence — and "one rebuild" is an
expectation, not a stop condition.
