"""Server-side canvas Arrange (#188): Boomi's own Auto Format layout, applied at the write boundary.

The emulator (``model``, ``layout``, ``xmlpatch``) is vendored from the canvas-Arrange handoff; it
reproduces every before/after pair recorded in the Boomi UI (``tests/fixtures/arrange/``).

The server's entry point is :func:`arrange_process_body`. Every route that writes a process body
calls it EXACTLY ONCE, after the body is final (placement included) and before any digest, pre-push
callback or transport call. Layout is a write-time control, not part of a plan or compile:

* ``mode="arrange"`` (the default) rewrites only shape and dragpoint ``x``/``y`` attributes;
* ``mode="preserve"`` validates the body and returns its bytes unchanged.

Callers choose the mode through a root ``layout`` control key (consumed by
:func:`resolve_layout_control` before builder dispatch) or, for internal callers, through an
explicit ``layout_mode`` argument on every hook.
"""
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Tuple

from .. import errors as _errors
from . import layout, model, xmlpatch
from .layout import UnarrangeableProcess
from .model import CanvasUnsupported, CoordinateError
from .xmlpatch import PatchCoverageError

__all__ = [
    "LAYOUT_MODES",
    "DEFAULT_LAYOUT_MODE",
    "LAYOUT_CONTROL_KEY",
    "ProcessLayoutError",
    "UnarrangeableProcess",
    "arrange_process_body",
    "arrange_for_submission",
    "consume_receipt",
    "resolve_layout_control",
    "layout_refusal",
    "layout_report_for",
    "autoformat_xml",
    "autoformat_shapes",
    "compare",
]

LAYOUT_MODES = ("arrange", "preserve")
DEFAULT_LAYOUT_MODE = "arrange"
LAYOUT_CONTROL_KEY = "layout"

#: Widest caption box and tallest rendered shape (icon + four caption lines) the handoff measured;
#: the extent is an ESTIMATE — the true rendered size depends on captions.
_CAPTION_WIDTH, _CAPTION_HEIGHT = 208.0, 70.0


class ProcessLayoutError(Exception):
    """A pre-submission layout refusal. ``error_code`` is a ``PROCESS_LAYOUT_*`` taxonomy code."""

    def __init__(self, error_code: str, detail: str):
        self.error_code = error_code
        self.detail = detail
        super().__init__(f"{_errors.ERROR_TAXONOMY[error_code].summary} {detail}")


def _report(applied, mode, reason, columns=None, rows=None, extent=None, warnings=()):
    return {
        "applied": applied,
        "mode": mode,
        "reason": reason,
        "columns": columns,
        "rows": rows,
        "estimated_extent": extent,
        "warnings": list(warnings),
    }


def _check_mode(mode) -> str:
    if mode not in LAYOUT_MODES:
        raise ProcessLayoutError(
            _errors.PROCESS_LAYOUT_MODE_INVALID,
            f"layout must be one of {list(LAYOUT_MODES)}; got {mode!r}.")
    return mode


def _read(xml: str) -> model.Canvas:
    try:
        return model.read(xml)
    except CanvasUnsupported as exc:
        raise ProcessLayoutError(_errors.PROCESS_LAYOUT_CANVAS_UNSUPPORTED, str(exc)) from exc
    except (ValueError, SyntaxError, UnicodeError) as exc:  # ET.ParseError is a SyntaxError
        raise ProcessLayoutError(_errors.PROCESS_LAYOUT_XML_INVALID, str(exc)) from exc


def _grid_report(mode, cells):
    columns = max(c for c, _ in cells.values()) + 1
    rows = max(r for _, r in cells.values()) + 1
    extent = {
        "width": layout.X0 + layout.COL * (columns - 1) + _CAPTION_WIDTH,
        "height": layout.Y0 + layout.ROW * (rows - 1) + _CAPTION_HEIGHT,
    }
    return _report(True, mode, "arranged", columns, rows, extent)


def arrange_process_body(xml: str, mode: str = DEFAULT_LAYOUT_MODE) -> Tuple[str, Dict[str, Any]]:
    """Return ``(xml, layout_report)`` for one component body about to be written.

    * Not a process (any other component type): the bytes, unchanged, with ``reason="not_process"``.
    * ``"arrange"``: every canvas shape and dragpoint at the position Boomi's Arrange gives it;
      every other byte unchanged. A process with no canvas (``empty_canvas``) or no Start shape
      (``no_start``, Boomi's Arrange does nothing either) is returned unchanged.
    * ``"preserve"``: the bytes unchanged after the same validation; an unsupported canvas or an
      unarrangeable topology is reported in ``warnings`` instead of refused.

    Raises :class:`ProcessLayoutError` before anything is returned: ``PROCESS_LAYOUT_XML_INVALID``
    (unparseable XML, a DOCTYPE, duplicate or unnamed shapes, a non-finite coordinate — in both
    modes), ``PROCESS_LAYOUT_CANVAS_UNSUPPORTED`` / ``PROCESS_LAYOUT_UNARRANGEABLE`` (arrange mode
    only), ``PROCESS_LAYOUT_INTERNAL_ERROR`` (a modeled target the patcher could not find) and
    ``PROCESS_LAYOUT_MODE_INVALID``.
    """
    mode = _check_mode(mode)
    if isinstance(xml, (bytes, bytearray)):
        xml = bytes(xml).decode("utf-8")
    if mode == "preserve":
        try:
            canvas = _read(xml)
        except ProcessLayoutError as exc:
            if exc.error_code != _errors.PROCESS_LAYOUT_CANVAS_UNSUPPORTED:
                raise
            return xml, _report(False, mode, "preserved", warnings=[exc.error_code])
        if canvas.kind == "not_process":
            return xml, _report(False, mode, "not_process")
        warnings = []
        try:
            layout.grid(canvas.shapes)
        except UnarrangeableProcess:
            warnings.append(_errors.PROCESS_LAYOUT_UNARRANGEABLE)
        return xml, _report(False, mode, "preserved", warnings=warnings)

    canvas = _read(xml)
    if canvas.kind == "not_process":
        return xml, _report(False, mode, "not_process")
    if canvas.kind == "empty_canvas" or not canvas.shapes:
        return xml, _report(False, mode, "empty_canvas", 0, 0, None)
    try:
        cells = layout.grid(canvas.shapes)
        shape_pos, dp_pos = layout.arrange(canvas.shapes)
    except UnarrangeableProcess as exc:
        raise ProcessLayoutError(_errors.PROCESS_LAYOUT_UNARRANGEABLE, str(exc)) from exc
    if not shape_pos:
        return xml, _report(False, mode, "no_start", 0, 0, None)
    try:
        arranged = xmlpatch.apply(xml, shape_pos, dp_pos, canvas.path)
    except PatchCoverageError as exc:
        raise ProcessLayoutError(_errors.PROCESS_LAYOUT_INTERNAL_ERROR, str(exc)) from exc
    except ValueError as exc:  # the tokenizer met text the parser accepted: never expected
        raise ProcessLayoutError(_errors.PROCESS_LAYOUT_INTERNAL_ERROR, str(exc)) from exc
    return arranged, _grid_report(mode, cells)


# --------------------------------------------------------------------------- exactly-once receipt


@dataclass(frozen=True)
class _LayoutReceipt:
    """Proof, inside one call chain, that ``xml`` is the final arranged body for ``mode``.

    Private and in-process only: it is passed as a Python keyword argument from a layer that has
    already arranged (and digested) the body to the transport layer below it, so the bytes are
    never arranged twice. It is never a config key, a JSON field or an XML attribute.
    """

    xml: str
    mode: str
    report: Mapping[str, Any]


def arrange_for_submission(xml: str, mode: str = DEFAULT_LAYOUT_MODE) -> _LayoutReceipt:
    arranged, report = arrange_process_body(xml, mode)
    return _LayoutReceipt(arranged, mode, MappingProxyType(dict(report)))


def consume_receipt(receipt: Any, xml: str, mode: str) -> Dict[str, Any]:
    """The receipt's report, if it describes exactly these bytes in this mode; else refuse."""
    if (not isinstance(receipt, _LayoutReceipt) or receipt.xml != xml or receipt.mode != mode):
        raise ProcessLayoutError(
            _errors.PROCESS_LAYOUT_INTERNAL_ERROR,
            "the body changed after it was arranged; it is refused rather than arranged twice.")
    return dict(receipt.report)


# --------------------------------------------------------------------------- control key


def resolve_layout_control(container: Optional[Mapping[str, Any]], *,
                           explicit: Optional[str] = None) -> Tuple[Dict[str, Any], str]:
    """``(copy of container without the layout key, effective mode)``. Never mutates its input.

    The root ``layout`` control selects the mode; an internal caller may pass ``explicit``
    instead. Both present and different, or any value outside :data:`LAYOUT_MODES`, raise
    ``PROCESS_LAYOUT_MODE_INVALID``. Neither present: ``"arrange"``.
    """
    stripped = dict(container or {})
    present = LAYOUT_CONTROL_KEY in stripped
    control = stripped.pop(LAYOUT_CONTROL_KEY, None)
    if explicit is not None:
        _check_mode(explicit)
    if present:
        if not isinstance(control, str):
            raise ProcessLayoutError(
                _errors.PROCESS_LAYOUT_MODE_INVALID,
                f"layout must be one of {list(LAYOUT_MODES)}; got {control!r}.")
        _check_mode(control)
        if explicit is not None and explicit != control:
            raise ProcessLayoutError(
                _errors.PROCESS_LAYOUT_MODE_INVALID,
                f"layout {control!r} conflicts with the caller's explicit layout {explicit!r}.")
        return stripped, control
    return stripped, explicit if explicit is not None else DEFAULT_LAYOUT_MODE


def layout_refusal(exc: ProcessLayoutError) -> Dict[str, Any]:
    """The served refusal envelope: stamped as written-nothing (the refusal precedes submission)."""
    return {
        "_success": False,
        "error_code": exc.error_code,
        "error": str(exc),
        "field": LAYOUT_CONTROL_KEY,
        "write_attempted": False,
        "layout": None,
    }


def layout_report_for(xml: str, reason: str = "metadata_only") -> Optional[Dict[str, Any]]:
    """A no-op report for a route that re-submits a body without arranging it (metadata
    operations), or ``None`` when the body is not a process. Never raises."""
    try:
        canvas = model.read(xml)
    except Exception:
        return None
    if canvas.kind == "not_process":
        return None
    return _report(False, None, reason)


# --------------------------------------------------------------------------- emulator API (CLI, tests)


def autoformat_shapes(shapes):
    """Positions for parsed shapes: ({shape: (x, y)}, {(shape, dragpoint index): (x, y)})."""
    return layout.arrange(shapes)


def autoformat_xml(xml_text: str) -> str:
    """Return the process XML with every shape and dragpoint placed as Arrange would place it."""
    canvas = model.read(xml_text)
    if canvas.kind == "not_process":
        raise ValueError("no process canvas found")
    shape_pos, dp_pos = layout.arrange(canvas.shapes)
    return xmlpatch.apply(xml_text, shape_pos, dp_pos, canvas.path)


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
