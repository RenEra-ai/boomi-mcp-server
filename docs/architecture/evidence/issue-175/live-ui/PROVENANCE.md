# Live UI canvas check — #175 acceptance criterion 4 (UI half)

Measured 2026-08-20 on `traininghlibbochkarov-JKIY2X` against the fixed tip, through the
Boomi platform Build canvas in Chrome. This is the check that three earlier attempts
recorded as blocked by an unavailable browser extension (ledger rows L1 2/2, L1 3/3 and
L3R-03). The extension became available; the check is now **measured**, and the rows that
said otherwise carry dated revisions.

## Why a control was built

"UI shows no orphaned shapes" is only evidence if the orphan was there before. A post-fix
canvas alone cannot distinguish "the fix works" from "this shape never orphaned anything".
So the pre-fix form was planted and photographed alongside it.

The control could not be authored through the MCP tool boundary — the fixed boundary
refuses it, which is the fix — so the pre-fix golden bytes went in through the direct
Boomi REST API. That is the correct instrument here: the question is what the PLATFORM
does with those bytes, not what our tool would emit.

## Result

| | stored shapes | stored dragpoints | rendered edges | orphan |
| --- | --- | --- | --- | --- |
| PRE-FIX `UICHECK175_PREFIX_88622e` | `start`, `processcall`, `stop` | `shape2`, **`shape3`** | 1 (Start→Call) | **yes** |
| POST-FIX `UICHECK175_9510fe` | `start`, `processcall` | `shape2` | 1 (Start→Call) | none |

The control reproduces the reported defect exactly: Boomi **stores** the
`shape2 → shape3` dragpoint and then **declines to draw it**, leaving the Stop as a free
floating icon on the canvas. The post-fix component emits no such shape at all — the
Process Call is the path terminal — so there is nothing left to orphan.

## What this settles, and what it does not

**Settles §6 probe 1** (strip vs store), which the bug report left open and made the P0
escalation conditional on: the answer is **(b) stored but not rendered**. Submitted bytes
and stored bytes agree, so there is no attestation drift of the kind #153's digest design
would have to absorb, and the severity stays at the filed P1.

**Settles the UI leg of ADR-001's §9 exception.** That amendment previously rested on the
runtime measurement alone and recorded the canvas behaviour as REPORTED / INCONCLUSIVE,
because QA could not reproduce it. It is now measured in both directions and the ADR says
so. The runtime measurement remains the stronger of the two and still carries the
argument; this removes an "inconclusive" that is no longer true.

**Does not settle probe 2** (a UI-built parent calling a *returning* child, to confirm the
`childShapeName` ↔ `dragpoint/@identifier` pairing beyond the single in-tree sample). That
belongs to #176, which owns emitting the binding, and it is the measurement that would
decide whether the reverted identifier-correspondence rule (row L3R-02) should be
reinstated.

## Measurement notes — two ways this check reads false

Both were hit and corrected here; they are recorded because either one would have produced
a confident wrong answer.

1. **The shape palette answers to the same names.** An element query for `Start` / `Stop` /
   `Process Call` returns the left-hand palette icons, which exist on every canvas whatever
   the process contains. All of them sit at 0×0 inside `.shape_palette_widget`. Canvas
   shapes are `.shapeLabel` nodes with real geometry.
2. **A URL-hash navigation does not re-render.** The Boomi SPA keeps open component tabs
   mounted; navigating the hash to another component left the previous canvas on screen, so
   a post-fix measurement returned the pre-fix component's three shapes. Every reading here
   is attributed to the component name visible in the editor header at the time it was
   taken, after a hard reload.

Screenshots are deliberately not the evidence of record: the canvas renders at
`devicePixelRatio` 2.5, so the third shape fell outside the captured viewport at the
window sizes available. `dom-measurements.json` carries the geometry the conclusions rest
on.

## Files

- `dom-measurements.json` — shape/edge geometry per component, with the method and both
  measurement traps recorded.
- `prefix-control.stored.xml` — the pre-fix control read back from the platform after save
  (this is the artifact proving store-not-strip).
- `postfix.stored.xml` — the post-fix component read back from the platform.

## Cleanup

Both components are disposable QA fixtures in the demo account and are deleted at slice
close; the readback bytes above are the durable record.
