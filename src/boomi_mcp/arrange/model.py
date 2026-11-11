"""Read a Boomi process component (or bare <process> / <shapes>) into shapes and ordered dragpoints.

Vendored from the canvas-Arrange handoff (#188). Changes from the handoff:

* The canvas is selected by PATH (``Component/object/process/shapes``, a bare ``process/shapes`` or
  a bare ``shapes``), never as "the first ``process/shapes`` in document order"; an ambiguous or
  unsupported wrapper raises :class:`CanvasUnsupported` instead of choosing a descendant.
* Every modeled coordinate must be a finite number (:class:`CoordinateError`), including the ones
  Arrange overwrites.
"""
import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional, Tuple


class CanvasUnsupported(ValueError):
    """The document holds process markup, but not in one canvas this module can select."""


class CoordinateError(ValueError):
    """A shape or dragpoint coordinate is not a finite number."""


@dataclass
class Dragpoint:
    name: str
    to: Optional[str]          # None when toShape is "unset", empty or missing
    identifier: Optional[str]
    x: float
    y: float


@dataclass
class Shape:
    name: str
    shapetype: str
    label: str
    x: float
    y: float
    dragpoints: List["Dragpoint"] = field(default_factory=list)


#: What :func:`read` found. ``kind`` is ``"canvas"`` (``shapes`` holds the canvas), ``"empty_canvas"``
#: (a process with no ``<shapes>`` element) or ``"not_process"`` (no process body at all).
@dataclass
class Canvas:
    kind: str
    path: Tuple[Tuple[str, str], ...]
    shapes: List[Shape]


#: The Boomi Component API namespace: the only one whose ``Component``/``object`` is a component.
BNS = "http://api.platform.boomi.com/"


def local(tag: str) -> str:
    """Element name without its namespace ({uri}name or prefix:name)."""
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def qname(tag: str) -> Tuple[str, str]:
    """``(namespace uri, local name)`` of an ElementTree tag; ``""`` for no namespace."""
    if tag.startswith("{"):
        uri, name = tag[1:].split("}", 1)
        return uri, name
    return "", tag


def _children_q(el, uri: str, name: str):
    return [c for c in el if isinstance(c.tag, str) and qname(c.tag) == (uri, name)]


def _children(el, name):
    """Children with this local name IN THE PARENT'S OWN NAMESPACE (canvas internals)."""
    return _children_q(el, qname(el.tag)[0], name)


def _contains_process(el) -> bool:
    return any(isinstance(d.tag, str) and local(d.tag) == "process" for d in el.iter())


def _process_canvas(process_el, prefix: Tuple[Tuple[str, str], ...]):
    pq = qname(process_el.tag)
    shapes = _children_q(process_el, pq[0], "shapes")
    if not shapes:
        return None, "empty_canvas", prefix
    if len(shapes) > 1:
        raise CanvasUnsupported("the process holds more than one <shapes> element")
    return shapes[0], "canvas", prefix + ((pq[0], "shapes"),)


def select_canvas(root):
    """(shapes element or None, kind, path of ``(namespace, local name)`` from the root to the canvas).

    | root                       | selection                                                     |
    |----------------------------|---------------------------------------------------------------|
    | Component (Boomi API ns)   | its one ``object`` (API ns) → one ``process`` (no ns or API ns) → one ``shapes`` (the process's ns) |
    | process                    | its one ``shapes`` (same namespace)                           |
    | shapes                     | itself                                                        |
    | anything                   | not a process, unless it holds ``process`` markup (then unsupported) |

    Elements in any other namespace are not part of the component (a foreign ``x:object`` or
    ``x:process`` is neither counted nor arranged). A Component with no ``object/process`` is not a
    process, unless it declares ``type="process"`` (then an empty canvas: nothing to arrange). The
    declared type never hides a body: ``object/process`` is arranged whatever ``type`` says.
    """
    rq = qname(root.tag)
    if rq[1] == "shapes":
        return root, "canvas", (rq,)
    if rq[1] == "process":
        return _process_canvas(root, (rq,))
    if rq == (BNS, "Component"):
        objects = _children_q(root, BNS, "object")
        if len(objects) > 1:
            raise CanvasUnsupported("the component holds more than one <object> element")
        processes = [c for o in objects for uri in ("", BNS) for c in _children_q(o, uri, "process")]
        if len(processes) > 1:
            raise CanvasUnsupported("the component object holds more than one <process> element")
        if not processes:
            # A declared type="process" with no body has nothing to arrange; it is passed
            # through as an empty canvas. A body under ANY declared type is found and arranged.
            if root.get("type") == "process":
                return None, "empty_canvas", (rq, (BNS, "object"))
            return None, "not_process", ()
        return _process_canvas(processes[0], (rq, (BNS, "object"), qname(processes[0].tag)))
    if _contains_process(root):
        raise CanvasUnsupported(f"process markup inside an unsupported <{rq[1]}> wrapper")
    return None, "not_process", ()


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


def _coordinate(el, attr: str, what: str) -> float:
    raw = el.get(attr)
    if raw is None:
        return 0.0  # absent coordinates count as 0 (the emulator writes them)
    try:
        value = float(raw)
    except ValueError:
        raise CoordinateError(f"{what} has a non-numeric {attr}={raw!r}") from None
    if not math.isfinite(value):
        raise CoordinateError(f"{what} has a non-finite {attr}={raw!r}")
    return value


def read(xml_text) -> Canvas:
    """Parse and select the canvas. Raises ``ValueError`` (incl. :class:`CanvasUnsupported`,
    :class:`CoordinateError`) or ``xml.etree.ElementTree.ParseError`` (a ``SyntaxError``)."""
    if isinstance(xml_text, bytes):
        xml_text = xml_text.decode("utf-8")
    _reject_doctype(xml_text)
    # Parsed AS A STRING: the text is already decoded, so re-encoding it under its own
    # declaration (encoding="ISO-8859-1", "UTF-16", ...) would decode it a second time.
    root = ET.fromstring(xml_text)
    shapes_el, kind, path = select_canvas(root)
    if shapes_el is None:
        return Canvas(kind, path, [])
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
                where = f"dragpoint {len(dps)} of shape {name!r}"
                dps.append(Dragpoint(d.get("name") or "", None if to in (None, "", "unset") else to,
                                     d.get("identifier"), _coordinate(d, "x", where),
                                     _coordinate(d, "y", where)))
        shapes.append(Shape(name, s.get("shapetype") or "", s.get("userlabel") or "",
                            _coordinate(s, "x", f"shape {name!r}"),
                            _coordinate(s, "y", f"shape {name!r}"), dps))
    return Canvas(kind, path, shapes)


def parse(xml_text) -> List[Shape]:
    """The canvas shapes. Raises ``ValueError`` when the document is not a process."""
    canvas = read(xml_text)
    if canvas.kind == "not_process":
        raise ValueError("no process canvas found")
    return canvas.shapes
