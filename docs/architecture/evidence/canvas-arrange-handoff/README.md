# Boomi Arrange emulator — integration package for the Boomi MCP server

**Audience:** the developer agent that maintains the Boomi management MCP server.
**Goal:** every process component the server creates or changes is saved with the exact coordinates Boomi's canvas
**Arrange** (Auto Format) button would give it, so no one has to open the editor and click Arrange, and the server can
report the real canvas size at build time.

## 1. What is in this package

| Path | What it is |
|---|---|
| `boomi_autoformat/` | The emulator. Pure Python 3.10+, standard library only, ~350 lines. Drop it into the server unchanged. |
| `boomi_autoformat/README.md` | Library usage, accepted input, exit codes, known limit. |
| `docs/arrange_rules.md` | The algorithm, transcribed from the Boomi web client's own code, and its evidence. |
| `docs/arrange_observations.md` | How Arrange behaves in the UI (flow, prompts, crashes) — needed to re-verify after Boomi releases. |
| `tests/test_autoformat.py` | 400+ checks: recorded Boomi results, byte preservation, idempotence, 30 edge cases. |
| `lab/before/*.saved.xml`, `lab/after/*.saved.xml` | 35 processes (5–146 shapes) recorded in the disposable demo account (see `PROVENANCE.md`): the body before and after Arrange + Save in the editor. |
| `lab/evaluate.py` | Scores the emulator against every recorded pair; exit 1 on any difference. |
| `lab/page_helper.js`, `lab/generate*.py` | The kit used to record new before/after pairs (section 7). |

Run: `python -m pytest tests -q` and `python lab/evaluate.py`.

## 2. Evidence that it is exact

- 35 lab processes (1,535 shapes, 1,611 dragpoints incl. 36 unconnected) arranged in the Boomi UI and read back: every
  shape and dragpoint coordinate equal.
- Blind round: 7 processes predicted before Arrange ran in the UI — 296 shapes, 309 dragpoints, 0 mismatches.
- Two independent code reviews (Codex, Grok); all confirmed findings fixed and covered by tests.

## 3. API

```python
from boomi_autoformat import autoformat_xml, compare, UnarrangeableProcess
from boomi_autoformat import layout, model

new_xml = autoformat_xml(component_xml)         # str -> str, only shape/dragpoint x/y change
cells = layout.grid(model.parse(component_xml))  # {shape name: (column, row)} — for previews / canvas size
diffs = compare(actual_xml, expected_xml)        # [] when both canvases are identical (used by tests)
```

| Input | Result |
|---|---|
| Full component XML (`bns:Component/bns:object/process/shapes`) or a bare `<process>` / `<shapes>` | Arranged XML; every other byte (declaration, configuration, labels, comments, CDATA, quoting, line ends) unchanged. |
| No Start shape | Returned unchanged (Boomi's Arrange does nothing either). |
| A loop that Start cannot reach | `UnarrangeableProcess` (subclass of `ValueError`) — Boomi's Arrange crashes on it (`ConcurrentModificationException`). |
| A one-output shape whose `toShape` names no shape | `UnarrangeableProcess` — Boomi's Arrange crashes on it (undefined node). |
| A DOCTYPE, duplicate shape names, malformed XML | `ValueError` / `xml.etree.ElementTree.ParseError` (a `SyntaxError`). |

Speed: ~4 ms for a 146-shape, 56 KB process. Linear in input size; no recursion limits (explicit stacks).

## 4. Where to hook it into the server

Apply it in one helper, called right before the server sends a **process** component body to the Boomi Component API
(create or update), after all other edits to the body are final:

| Tool / action | When to arrange |
|---|---|
| `manage_component` `create` (type `process`) | Always (unless `layout="preserve"`). |
| `manage_component` `update` with full XML (type `process`) | When the process topology changed against the current live body (see below), or `layout="arrange"`. |
| `build_integration` `apply` (every process component it writes) | Always for created processes; updated ones as for `update`. |
| `prepare_component_edit` / `apply_component_edit` on a process | When the patch changes topology; compute in `prepare` so the diff and token cover the final coordinates, and recompute identically in `apply`. |
| `build_from_archetype`, `build_integration` `plan` (no write) | Do not write; return the layout preview (section 5) in the plan. |

**Topology** = the ordered list of shapes (name, shapetype) and, per shape, the ordered dragpoints with `toShape` and
`identifier`. Arrange only depends on that, so: unchanged topology → keep the live coordinates (a person may have
moved shapes by hand); changed topology → arrange. Suggested parameter on the write actions:
`layout: "auto" | "arrange" | "preserve"` (default `"auto"`: create → arrange; update → arrange iff topology changed).

**Envelope:** add a `layout` object to the result so callers see what happened, e.g.

```json
"layout": {"applied": true, "mode": "auto", "reason": "topology changed",
           "columns": 9, "rows": 6, "estimated_extent": {"width": 1792, "height": 918}}
```

**Errors:** map `UnarrangeableProcess` to a validation error **before** any write (the message names the shapes). A
process Boomi cannot arrange is almost always a generator bug (unreachable loop, dangling `toShape`). With
`layout="preserve"` the server may still write it, but should warn.

**Do not** reorder shapes or dragpoints, rename dragpoints, or re-serialize the XML — the emulator keeps the text exact
and the safe-edit diff must show coordinate changes only.

## 5. Read-only layout preview (recommended new action)

Expose the grid without writing, e.g. `query_components(action="layout_preview", component_id=…)` or a field in
`build_integration(action="plan")`:

- per shape: `name`, `column`, `row`, `x`, `y`;
- `columns = max column + 1`, `rows = max row + 1`;
- estimated extent: width ≈ `48 + 192·(columns − 1) + 208` (208 = widest caption box), height ≈
  `48 + 160·(rows − 1) + 70` (70 = tallest caption: icon plus up to four caption lines). Label these **estimates**: the
  true rendered size depends on captions;
- `fits_1366x768`: compare with the client's review viewport (the skill's one-screen rule). This turns the skill's
  "provisional spacing aids" into exact placement.

## 6. Rules the emulator implements (summary — full text in `docs/arrange_rules.md`)

1. One global row counter from 0. `place(shape, col)`: skip if placed; put it at (col, counter); if it has more than one
   dragpoint, every dragpoint after the first first advances the counter (even unset or already placed ones), then
   recurse into its target at col + 1; with one dragpoint recurse on the same row.
2. Start = the last `shapetype="start"` shape in XML order. Then the unreachable roots (not pointed at by another
   unplaced shape), each on a new row from column 0.
3. `x = 48 + 192·col`, `y = 48 + 160·row` (Start: 46). Connected dragpoint: `(target.x − 16, target.y + 8)`.
   Unconnected dragpoint k of n: `x = shape.x + 176`, `y = round16(old shape y) + 10 + 40·(k − (n−1)/2)`.
4. Old coordinates, XML shape order, labels and shape types never change the grid. Branch path numbers, True/False and
   Route keys do not either: the dragpoint order in the XML does.

## 7. Keeping it correct across Boomi releases

Arrange is client-side code in the Boomi web client; a release can change it. Once per few Boomi releases:

1. Generate a few random processes (`lab/generate.py --blind 3 --seed <new> --prefix chk`), create them in a sandbox
   account, open each in the Build editor, wait until every shape is rendered, click **Arrange**, answer
   **Don't Save** to the unsaved-changes prompt, then **Save** (`lab/page_helper.js` automates this through
   chrome-devtools-mcp; see `docs/arrange_observations.md`).
2. Read the saved bodies back and run the emulator against them (`python -m boomi_autoformat before.xml --check after.xml`).
3. Any difference: read the current routine from the page (GWT iframe globals; in the 2026-09-30 build `Ike`, `Hke`,
   `Jke`, `Lke`, constants 48/192/160 — names change per build) and update `layout.py`.

## 8. Known limit

Boomi offsets a shape's y by `−trunc((rendered height mod 16) / 2)`; only the browser knows the rendered height. The
emulator uses −2 for Start and 0 otherwise, which held for every shape type recorded here (message, notify, set properties,
decision, branch, try/catch, flow control, process call, stop, exception, return documents).
Connector, map, data process, document cache and route shapes (and connector/listener Start
variants) are NOT covered by the reproducible corpus in this package: record them before relying on the offset for them. A shape type that renders at another height would be placed 1–7 units low.

## 9. Acceptance checklist for the server change

- [ ] Package vendored unchanged; its tests run in the server's CI.
- [ ] One `arrange_if_needed(xml, live_xml, mode)` helper used by every process write path in section 4.
- [ ] Topology comparison unit-tested (unchanged → no coordinate change; one new shape → arranged).
- [ ] `UnarrangeableProcess` becomes a pre-write validation error with the shape names.
- [ ] Safe-edit diff shows only coordinate changes for an arrange-only change.
- [ ] Layout object in results; optional preview action.
- [ ] Live check: create one generated process through the server, open it in the editor, click Arrange — nothing moves.
