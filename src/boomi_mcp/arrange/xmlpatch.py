"""Rewrite only the x/y attributes of process shapes and dragpoints, keeping every other byte.

A small tokenizer walks the text: comments, CDATA, processing instructions and declarations
are copied untouched; start and end tags are read with quote-aware attribute parsing and an
element stack, so only the canvas shapes and their dragpoints change (see apply()).

Vendored from the canvas-Arrange handoff (#188). Changes from the handoff: the canvas is the
element at the PATH :func:`model.read` selected (not the first ``process/shapes``); attribute
values are decoded by XML rules (not ``html.unescape``); and every target in the position maps
must be patched exactly once, or :class:`PatchCoverageError` is raised before any text is returned.
"""
import re
from typing import Tuple

_SKIP = (("<!--", "-->"), ("<![CDATA[", "]]>"), ("<?", "?>"))
_NAME = re.compile(r"[^\s/>=]+")
_ATTR = re.compile(r"""(\s+)([^\s/>=]+)(\s*=\s*)("[^"]*"|'[^']*')""")
_REF = re.compile(r"&(#[xX][0-9a-fA-F]+|#[0-9]+|[^;&\s]*);?")
_PREDEFINED = {"lt": "<", "gt": ">", "amp": "&", "quot": '"', "apos": "'"}


class PatchCoverageError(ValueError):
    """A modeled shape or dragpoint was not patched exactly once."""


def _local(name: str) -> str:
    return name.rsplit(":", 1)[-1]


def _fmt(v: float) -> str:
    return f"{v:.1f}"


def _scan_tag(text: str, i: int):
    """Parse the tag at text[i] == '<': (end, name, attrs, closing, self-closing, index of '>' or '/>')."""
    closing = text.startswith("</", i)
    j = i + (2 if closing else 1)
    m = _NAME.match(text, j)
    if not m:
        raise ValueError(f"malformed tag at offset {i}")
    name, j = m.group(0), m.end()
    attrs = []  # (value start, value end, attribute name, quote)
    while True:
        a = _ATTR.match(text, j)
        if not a:
            break
        vstart = a.start(4)
        attrs.append((vstart + 1, a.end(4) - 1, a.group(2), text[vstart]))
        j = a.end()
    k = j
    while k < len(text) and text[k].isspace():
        k += 1
    if text.startswith("/>", k):
        return k + 2, name, attrs, closing, True, k
    if k < len(text) and text[k] == ">":
        return k + 1, name, attrs, closing, False, k
    raise ValueError(f"malformed tag {name!r} at offset {i}")


def apply(xml_text: str, shape_pos: dict, dp_pos: dict, canvas_path: Tuple[str, ...]) -> str:
    """shape_pos: {shape name: (x, y)}; dp_pos: {(shape name, dragpoint index): (x, y)}.

    Only the canvas is touched: the element whose path of local names from the document root is
    ``canvas_path`` (the path :func:`model.read` selected) and, inside it, its direct <shape>
    children and their direct <dragpoints>/<dragpoint> children.
    """
    out, i, n = [], 0, len(xml_text)
    stack: list = []
    canvas = None         # stack depth of the canvas <shapes> once open; "done" after it closes
    shape = None          # name of the canvas shape whose tag is open
    dp_index = 0
    patched_shapes, patched_dps = set(), set()
    while i < n:
        lt = xml_text.find("<", i)
        if lt < 0:
            out.append(xml_text[i:])
            break
        out.append(xml_text[i:lt])
        for opener, closer in _SKIP:
            if xml_text.startswith(opener, lt):
                end = xml_text.find(closer, lt + len(opener))
                end = n if end < 0 else end + len(closer)
                out.append(xml_text[lt:end])
                i = end
                break
        else:
            if xml_text.startswith("<!", lt):  # declarations (model.read rejects DOCTYPE)
                end = xml_text.find(">", lt)
                end = n if end < 0 else end + 1
                out.append(xml_text[lt:end])
                i = end
                continue
            end, name, attrs, closing, empty, close_at = _scan_tag(xml_text, lt)
            tag = xml_text[lt:end]
            local = _local(name)
            depth = len(stack)
            if closing:
                if stack and stack[-1] == local:
                    stack.pop()
                if isinstance(canvas, int):
                    if len(stack) == canvas - 1:
                        canvas = "done"
                    elif local == "shape" and len(stack) == canvas:
                        shape = None
                out.append(tag)
                i = end
                continue
            target = None
            if canvas is None and tuple(stack) + (local,) == tuple(canvas_path):
                canvas = "done" if empty else depth + 1
            elif isinstance(canvas, int):
                if local == "shape" and depth == canvas:
                    shape = _attr_value(xml_text, attrs, "name")
                    dp_index = 0
                    if shape in shape_pos:
                        if shape in patched_shapes:
                            raise PatchCoverageError(f"shape {shape!r} appears twice on the canvas")
                        patched_shapes.add(shape)
                        target = shape_pos[shape]
                    if empty:
                        shape = None
                elif (local == "dragpoint" and shape is not None and depth == canvas + 2
                      and stack[-1] == "dragpoints"):
                    key = (shape, dp_index)
                    dp_index += 1
                    if key in dp_pos:
                        patched_dps.add(key)
                        target = dp_pos[key]
            if target is not None:
                tag = _rewrite(xml_text, lt, end, attrs, close_at, target)
            out.append(tag)
            if not empty:
                stack.append(local)
            i = end
    missing_shapes = set(shape_pos) - patched_shapes
    missing_dps = set(dp_pos) - patched_dps
    if missing_shapes or missing_dps:
        raise PatchCoverageError(
            "modeled targets were not found in the text: "
            f"shapes {sorted(missing_shapes)!r}, dragpoints {sorted(missing_dps)!r}")
    return "".join(out)


def _decode_reference(match) -> str:
    body = match.group(1)
    if not match.group(0).endswith(";"):
        raise ValueError(f"unterminated reference {match.group(0)!r} in an attribute value")
    if body.startswith(("#x", "#X")):
        return chr(int(body[2:], 16))
    if body.startswith("#"):
        return chr(int(body[1:]))
    if body in _PREDEFINED:
        return _PREDEFINED[body]
    raise ValueError(f"undeclared entity &{body}; in an attribute value")


def _attr_value(text, attrs, wanted):
    """Attribute value decoded as an XML parser does: line ends and literal tabs/newlines become
    spaces (attribute-value normalization), then character references and the five predefined
    entities are expanded (XML rules, not HTML: ``&#x80;`` is U+0080)."""
    for vs, ve, name, _ in attrs:
        if name == wanted:
            raw = text[vs:ve].replace("\r\n", "\n").replace("\r", "\n")
            return _REF.sub(_decode_reference, raw.replace("\t", " ").replace("\n", " "))
    return None


def _rewrite(text, start, end, attrs, close_at, xy):
    values = {"x": _fmt(xy[0]), "y": _fmt(xy[1])}
    pieces, pos = [], start
    for vs, ve, name, _ in attrs:
        if name in values:
            pieces.append(text[pos:vs])
            pieces.append(values.pop(name))
            pos = ve
    pieces.append(text[pos:close_at])
    for name in ("x", "y"):  # attributes the tag did not have
        if name in values:
            pieces.append(f' {name}="{values[name]}"')
    pieces.append(text[close_at:end])
    return "".join(pieces)
