"""Emulation of the Boomi process canvas Arrange (Auto Format) placement.

Mirrors the Boomi web client's own routine (GWT ProcessDisplay code, functions Ike/Hke/Jke/Lke
in the 2026-09-30 build; see docs/arrange_rules.md):

  * One global row counter, starting at 0.
  * place(shape, col): skip if already placed; put the shape at (col, counter).
    - Several dragpoints: for each dragpoint in XML order, every one except the first first
      advances the counter (even when its target is unset or already placed); then recurse into
      the target at col + 1 (a placed target is skipped).
    - One dragpoint: recurse into its target at col + 1 on the same row.
  * No Start shape: nothing moves. Several: the last one in XML order is used (the graph builder
    overwrites its start reference for each start shape; read from code, not seen in the UI).
  * A one-output shape whose toShape names no shape makes Arrange fail (undefined node); with
    several outputs such a dragpoint is treated as unconnected. The emulator raises
    UnarrangeableProcess for the failing case.
  * place(Start, 0). Then, among the shapes still unplaced, the "roots" (not the target of another
    unplaced shape), in XML order: advance the counter, place(root, 0). If shapes remain (a cycle
    that Start cannot reach), Boomi iterates its set of unplaced shapes while placing removes from it
    and Arrange fails with ConcurrentModificationException (recorded on p17_orphan_cycle); the
    emulator raises UnarrangeableProcess for the same input.
  * Grid: x = 48 + 192*col, y = 48 + 160*row - trunc((h % 16) / 2) with h the rendered height of
    the shape widget; that is 2 for the Start shape (y 46) and 0 for every other shape seen.
  * Saved dragpoints: a connected one sits at (target.x - 16, target.y + 8). An unconnected one of
    shape S (index k of n dragpoints) moves to x = S.x + 176 but keeps the height S had before
    Arrange: round16(old y) + 10 + 40*(k - (n-1)/2).
"""
import math

from .model import Shape

X0, Y0, COL, ROW = 48.0, 48.0, 192.0, 160.0
START_DY = -2.0
DP_DX, DP_DY = -16.0, 8.0
UNSET_DX, UNSET_DY, UNSET_STEP, SNAP = 176.0, 10.0, 40.0, 16.0


class UnarrangeableProcess(ValueError):
    """Boomi's own Arrange fails on this process."""


def grid(shapes: list[Shape]) -> dict[str, tuple[int, int]]:
    """{shape name: (column, row)} exactly as Arrange assigns them."""
    by_name = {s.name: s for s in shapes}
    cell: dict[str, tuple[int, int]] = {}
    counter = 0

    def visit(s: Shape, c: int):
        nonlocal counter
        cell[s.name] = (c, counter)
        targets = [by_name.get(d.to) if d.to else None for d in s.dragpoints]
        if len(targets) > 1:
            for i, t in enumerate(targets):
                if i:
                    counter += 1
                if t is not None:  # unset or naming no shape: Boomi marks it unconnected (Lke)
                    yield t, c + 1
        elif targets:
            if targets[0] is None and s.dragpoints[0].to:
                raise UnarrangeableProcess(
                    f"shape {s.name!r} points at missing shape {s.dragpoints[0].to!r} "
                    "(Boomi's Arrange fails on an undefined node here)")
            if targets[0] is not None:
                yield targets[0], c + 1

    def place(root: Shape, col: int):
        if root.name in cell:
            return
        stack = [visit(root, col)]  # explicit stack: Boomi recurses, Python would hit its limit
        while stack:
            nxt = next(stack[-1], None)
            if nxt is None:
                stack.pop()
            elif nxt[0].name not in cell:
                stack.append(visit(*nxt))

    starts = [s for s in shapes if s.shapetype == "start"]
    if not starts:
        return cell  # Boomi's Arrange returns without moving anything
    place(starts[-1], 0)  # the graph builder keeps the last start shape it meets
    remaining = [s for s in shapes if s.name not in cell]
    if remaining:
        pointed = {d.to for s in remaining for d in s.dragpoints if d.to in by_name}
        for s in [s for s in remaining if s.name not in pointed]:
            counter += 1
            place(s, 0)
        remaining = [s for s in remaining if s.name not in cell]
        if remaining:
            raise UnarrangeableProcess(
                "shapes in a loop that Start cannot reach: " + ", ".join(s.name for s in remaining)
                + " (Boomi's Arrange fails with ConcurrentModificationException here)")
    return cell


def arrange(shapes: list[Shape]):
    """Return ({shape name: (x, y)}, {(shape name, dragpoint index): (x, y)}).

    Both are empty when the process has no Start shape: Boomi leaves such a canvas as it is.
    """
    cell = grid(shapes)
    if not cell:
        return {}, {}
    pos = {}
    for s in shapes:
        c, r = cell[s.name]
        pos[s.name] = (X0 + COL * c, Y0 + ROW * r + (START_DY if s.shapetype == "start" else 0.0))
    dps = {}
    for s in shapes:
        sx, _ = pos[s.name]
        for k, dp in enumerate(s.dragpoints):
            if dp.to and dp.to in pos:
                tx, ty = pos[dp.to]
                dps[(s.name, k)] = (tx + DP_DX, ty + DP_DY)
            else:
                old_y = SNAP * math.floor(s.y / SNAP + 0.5)
                spread = UNSET_STEP * (k - (len(s.dragpoints) - 1) / 2)
                dps[(s.name, k)] = (sx + UNSET_DX, old_y + UNSET_DY + spread)
    return pos, dps
