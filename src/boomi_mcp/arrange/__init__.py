"""Offline emulator of the Boomi process canvas Arrange (Auto Format) button."""
from . import layout, model, xmlpatch
from .layout import UnarrangeableProcess

__all__ = ["autoformat_xml", "autoformat_shapes", "compare", "UnarrangeableProcess"]


def autoformat_shapes(shapes):
    """Positions for parsed shapes: ({shape: (x, y)}, {(shape, dragpoint index): (x, y)})."""
    return layout.arrange(shapes)


def autoformat_xml(xml_text: str) -> str:
    """Return the process XML with every shape and dragpoint placed as Arrange would place it."""
    shape_pos, dp_pos = layout.arrange(model.parse(xml_text))
    return xmlpatch.apply(xml_text, shape_pos, dp_pos)


def compare(actual_xml: str, expected_xml: str, include_unset: bool = True):
    """List (kind, where, actual, expected) for every difference between two process canvases.

    kinds: missing / extra (shape only on one side), topology (dragpoint count or toShape differs),
    shape, dragpoint, unset (coordinates). Dragpoints are matched by their position in the shape:
    Arrange never reorders them, while the editor renames them on save.
    """
    a, e = model.parse(actual_xml), model.parse(expected_xml)
    out = []
    ea = {s.name: s for s in e}
    aa = {s.name: s for s in a}
    out += [("missing", t.name, None, None) for t in e if t.name not in aa]
    out += [("extra", s.name, None, None) for s in a if s.name not in ea]
    for s in a:
        t = ea.get(s.name)
        if t is None:
            continue
        if (s.x, s.y) != (t.x, t.y):
            out.append(("shape", s.name, (s.x, s.y), (t.x, t.y)))
        if len(s.dragpoints) != len(t.dragpoints):
            out.append(("topology", f"{s.name} dragpoints", len(s.dragpoints), len(t.dragpoints)))
        for k, (d, u) in enumerate(zip(s.dragpoints, t.dragpoints)):
            where = f"{s.name}[{k}]"
            if d.to != u.to:
                out.append(("topology", where, d.to, u.to))
                continue
            if (d.x, d.y) != (u.x, u.y) and (include_unset or d.to is not None):
                out.append(("dragpoint" if d.to else "unset", where, (d.x, d.y), (u.x, u.y)))
    return out
