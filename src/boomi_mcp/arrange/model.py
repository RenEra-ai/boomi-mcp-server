"""Read a Boomi process component (or bare <process>) into shapes and ordered dragpoints."""
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field


@dataclass
class Dragpoint:
    name: str
    to: str | None          # None when toShape is "unset", empty or missing
    identifier: str | None
    x: float
    y: float


@dataclass
class Shape:
    name: str
    shapetype: str
    label: str
    x: float
    y: float
    dragpoints: list[Dragpoint] = field(default_factory=list)


def local(tag: str) -> str:
    """Element name without its namespace ({uri}name or prefix:name)."""
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _children(el, name):
    return [c for c in el if isinstance(c.tag, str) and local(c.tag) == name]


def _canvas(root):
    """The first <shapes> element in document order that is the root or a direct child of a
    <process>; xmlpatch.apply rewrites exactly this element's shapes."""
    if local(root.tag) == "shapes":
        return root
    stack = [(root, None)]  # pre-order walk = document order of start tags
    while stack:
        el, parent = stack.pop()
        if parent is not None and local(el.tag) == "shapes" and local(parent.tag) == "process":
            return el
        stack.extend((c, el) for c in reversed(el) if isinstance(c.tag, str))
    return None


def _reject_doctype(text: str):
    """Boomi component XML never has a DOCTYPE; refusing it rules out entity tricks."""
    i, n = (1 if text.startswith(chr(0xFEFF)) else 0), len(text)
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        for opener, closer in (("<?", "?>"), ("<!--", "-->")):
            if text.startswith(opener, i):
                end = text.find(closer, i + len(opener))
                i = n if end < 0 else end + len(closer)
                break
        else:
            if text.startswith("<!", i) and text[i + 2:i + 9].upper() == "DOCTYPE":
                raise ValueError("DOCTYPE declarations are not accepted")
            return  # first element reached: no DOCTYPE in the prolog


def parse(xml_text: str) -> list[Shape]:
    if isinstance(xml_text, bytes):
        xml_text = xml_text.decode("utf-8")
    _reject_doctype(xml_text)
    root = ET.fromstring(xml_text.encode("utf-8"))
    shapes_el = _canvas(root)
    if shapes_el is None:
        raise ValueError("no <process><shapes> element found")
    shapes, seen = [], set()
    for s in _children(shapes_el, "shape"):
        name = s.get("name")
        if not name:
            raise ValueError("a <shape> has no name")
        if name in seen:
            raise ValueError(f"duplicate shape name {name!r}")
        seen.add(name)
        dps = []
        for dp_el in _children(s, "dragpoints"):
            for d in _children(dp_el, "dragpoint"):
                to = d.get("toShape")
                dps.append(Dragpoint(d.get("name") or "", None if to in (None, "", "unset") else to,
                                     d.get("identifier"), float(d.get("x", 0)), float(d.get("y", 0))))
        shapes.append(Shape(name, s.get("shapetype") or "", s.get("userlabel") or "",
                            float(s.get("x", 0)), float(s.get("y", 0)), dps))
    return shapes
