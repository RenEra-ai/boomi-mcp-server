# How Boomi's Arrange places shapes

Sources, strongest first:

1. **The routine in the Boomi web client** (build of 2026-09-30), read from the page: `Ike` (entry), `Hke` (recursive
   placement), `Jke` (shape position), `Lke` (unconnected dragpoint), `Kke` (graph built from rendered widgets).
2. **Recorded results**: 35 lab processes in the demo account (probes p01–p19, randoms r01–r10 of 57–146
   shapes, blind b01–b03) arranged in the UI and read back — 1,535 shapes, 1,611 dragpoints (36 unset). The emulator reproduces all of them exactly.
   Blind check: p15, p16, p18, p19 and b01–b03 were predicted (`lab/predictions/`) before Arrange ran —
   296 shapes and 309 dragpoints, 0 mismatches.

## Algorithm (as coded in the client)

```
counter = 0                                   # one global row counter
place(node, col):
    if node placed: return
    node.col, node.row = col, counter
    if node has more than one dragpoint:
        for i, dp in enumerate(dragpoints in XML order):
            if i > 0: counter += 1            # every later outcome gets a new row, even if unset
            if dp has a target: place(target, col + 1)   # placed targets are skipped
            else: mark dp unconnected at (col + 1, counter)
    elif the one dragpoint has a target: place(target, col + 1)

start = the LAST shapetype="start" shape in XML order     # no Start: Arrange returns, nothing moves
place(start, 0)
roots = unplaced shapes that no other unplaced shape points to      (XML order)
for r in roots: counter += 1; place(r, 0)
if shapes are still unplaced (a loop that Start cannot reach):
    counter += 1
    for s in live_set_of_unplaced: counter += 1; place(s, 0)   # removes from the set it iterates
    -> java.util.ConcurrentModificationException: Arrange fails (recorded on p17_orphan_cycle)
```

Consequences:

| Rule | Evidence |
|---|---|
| Walk order = dragpoint order in the XML. Branch numbers, True/False, Try/Catch and Route keys play no role. | code; `p02_…_dpswap`, `p06_merge2_dpswap` |
| A merge target stays where it was first reached; later predecessors point left or down to it. | code; `p06`, `p07` |
| Loops: the back edge is ignored (target already placed) but still takes a row if it is not the first outcome. | code; `p13_cycle`, `p18` |
| Unset outcomes: a non-first unset outcome still advances the row counter. | code; `p15`, `r05` |
| Unreachable shapes: roots first (not XML order), from column 0, each on a new row below everything else. | code; `p08`, `p12`, `p16` |
| A loop that Start cannot reach makes Boomi's Arrange fail; the emulator raises `UnarrangeableProcess`. | code; `p17` |
| A single-output shape whose `toShape` names a missing shape makes Arrange fail (undefined node); the emulator raises `UnarrangeableProcess`. With several outputs such a dragpoint is treated as unconnected. | code |
| No Start shape: Arrange returns without moving anything; the emulator returns the XML unchanged. | code (`Ike`) |
| Several Start shapes: the graph builder (`Kke`) overwrites its start reference for each one, so the last in XML order is used; the others are unreachable roots. | code only, not seen in the UI |
| Old coordinates, XML shape order, labels and shape type never change the grid. | `p06_…_coordsB/_shuffled`, `p11_labels` |

## Coordinates

- Shape: `x = 48 + 192·col`, `y = 48 + 160·row − trunc((h mod 16) / 2)` where `h` is the rendered widget height
  (`offsetHeight`). In practice: **Start y offset −2** (y = 46 on row 0), **0 for every other shape type seen**
  (message, notify, set properties, decision, branch, try/catch, flow control, process call, stop, exception,
  return documents). Connector, map, data process, document cache and route shapes (and connector/listener Start
  variants) are NOT covered by the reproducible corpus in this package. **Limit:** the emulator cannot
  measure widget heights, so a shape type not yet recorded whose widget height is not ≡ 0 or 1 (mod 16) would be
  placed 1–7 units too low. Record one such process and add it to `START_DY`-style per-type offsets if it happens.
- Connected dragpoint (saved value): `(target.x − 16, target.y + 8)`, also for merges and back edges.
- Unset dragpoint (saved value), for dragpoint `k` of `n` on shape `S`:
  `x = S.x_new + 176`, `y = round16(S.y_old) + 10 + 40·(k − (n−1)/2)`. The routine only moves it sideways; its
  height is where the editor drew the dangling arrow before Arrange. This is the only input-dependent value.
