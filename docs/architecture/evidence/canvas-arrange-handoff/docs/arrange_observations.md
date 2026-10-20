# Recording Boomi's Arrange (Auto Format) in the UI — observations

Account: the disposable demo account (id in [`PROVENANCE.md`](../PROVENANCE.md)), folder `AutoFormat Lab`.
Browser: the user's signed-in Chrome via `chrome-devtools-mcp@1.10.1 --autoConnect`, in a page opened by Claude.
Date: 2026-09-30. Helper: [`lab/page_helper.js`](../lab/page_helper.js).

## The UI flow

1. Open the process: `https://platform.boomi.com/AtomSphere.html#build;accountId=<account>;components=<componentId>`.
   Changing only the hash inside an open Build page also opens the component in a new editor tab.
   - First load of the day shows an announcement dialog ("Tech Preview Connectors renamed to EA"), dismissed with `okay`.
   - Switching the hash while another component has unsaved changes shows "Are you sure you want to navigate away
     from the build screen? … Click Yes to continue". After Save & Close this still appeared once for a component
     that had in fact been saved (the server copy was the new version).
2. The canvas toolbar's **Arrange** button is an anchor `a[data-locator="link-arrange"]` holding an inline SVG titled
   "Arrange" (the accessibility tree shows it as link "Arrange").
3. Clicking it on a process written through the API asks: *"There are unsaved changes to this process. What would you
   like to do before arranging it?"* — `Save` / `Don't Save` / `Cancel`. The process had no real edits; `Don't Save`
   arranges without an extra revision. Processes that had just been opened sometimes did not ask.
4. A modal *"Calculating step positions"* is shown while the layout is computed: ~1 s for 5–70 shapes,
   3–10 s for 60–150 shapes.
5. **Save** (or **Save & Close**) writes a new revision; the page shows "<name> saved successfully." Every lab
   process went v1 → v2 (p01 went to v3 because it was saved once from the editor as well).

## What changes in the saved XML

Only coordinates, plus one renaming by the editor:

- every shape `x`/`y`;
- every dragpoint `x`/`y` (connected ones and unset ones);
- when saving, the editor renames dragpoints to `<shape>.dragpoint<N>` by their position (seen after Branch
  reorders). Shape order, dragpoint order, `toShape`, identifiers and configuration are untouched.

Arrange never reads the old coordinates for shapes: the same topology arranged from two different random placements,
and with the shapes listed in a shuffled XML order, came out identical (`p06_merge2`, `_coordsB`, `_shuffled`).
The single place the old layout leaks through is the height of unset dragpoints (see the rules).

## Crashes — Arrange must run on a fully rendered canvas

Clicking Arrange 1.5 s after the button appeared crashed it on big processes (r01, r04, r09): the console showed
`SEVERE: (TypeError) : Cannot read properties of undefined (reading 'c')` inside the recursive placement function, and
the "Calculating step positions" modal never closed. The routine builds its shape map from the rendered shape widgets,
so a canvas still being drawn leaves targets undefined. Waiting until all shape labels are rendered (`.shapeLabel`
count = shape count) plus 5 s fixed it: r04 (63 shapes, walk depth 37) then arranged in 9 s.
A `RangeError: Maximum call stack size exceeded` is logged on every page load of the Build screen and is unrelated.

The same code would also fail (undefined node) for a single-output shape whose `toShape` names a shape that does not
exist; generated XML must never contain such a dangling reference.

## The routine itself

The page exposes the arrange code as globals of the GWT iframe (`document.querySelectorAll('iframe')[1].contentWindow`),
functions `Ike`, `Hke`, `Jke`, `Lke`, `Kke`, constants `Dke`=48, `Eke`=192, `Fke`=160 in this build. Their obfuscated
names change with every Boomi release; the logic is transcribed in [arrange_rules.md](arrange_rules.md).
