# Completion-workflow rules that outlive a single checkout

`CLAUDE.md` and `AGENTS.md` are **gitignored** (`.gitignore:100-101`). They are the files an agent
reads, but they are per-checkout: a rule written only there reaches nobody else, and a fresh clone
gets none of it. Anything meant to govern FUTURE slices therefore lives here, in version control,
and the local instruction files cite it.

This file holds standing amendments to the completion workflow. It does not restate the workflow —
`CLAUDE.md` does that — only the rules that were adopted later and must survive a clone.

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
