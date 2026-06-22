"""Report-only process-graph integrity verifier (issue #80, M9.4).

Parses an emitted or live Boomi *process* Component XML and reports structural
graph problems WITHOUT modifying, merging, normalizing, rewriting, deleting, or
reordering the XML. This is the verification half of the Companion canvas
arranger contract, run server-side during ``build_integration(action="verify")``.

Design constraints (see issue #80):
  * Pure read. Stdlib ``xml.etree.ElementTree`` only — no dependencies.
  * Report-only — never auto-fix, delete, or rewire shapes. Remediation text
    names the offending shape.
  * Namespace-tolerant by local element name: the inner ``<process>`` carries an
    empty default namespace (``xmlns=""``) while the outer Component envelope is
    in the ``bns:`` namespace, so we match elements by their local name.
  * Does NOT un-gate gated shapes (Branch / standalone Process Call / retries):
    verifying XML that contains them reports graph integrity only.
  * Does NOT introduce its own XML merge/normalization (#50 owns preservation).

Contract::

    verify_process_graph(process_xml: str) -> {
        "errors":   [issue_dict, ...],   # non-empty -> verification fails
        "warnings": [issue_dict, ...],   # GUI/runtime lints — never fail
        "shapes_checked": int,
    }

Each ``issue_dict`` is ``{code, shape, shape_type, message, remediation}``.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional


# Shape types that legitimately have no outbound edge (process sinks).
# ``doccacheload`` is a terminal DLQ sink emitted by today's builder for
# ``dlq.mode="document_cache_ref"`` catch legs without notify — it ends the leg
# with an empty ``<dragpoints/>`` and must not be flagged as a dead end.
_TERMINAL_SHAPE_TYPES = frozenset({"stop", "returndocuments", "doccacheload"})

# Shape types whose outputs are explicit branch outputs that must be wired.
_BRANCHING_SHAPE_TYPES = frozenset({"branch", "decision", "route"})

# Display attributes whose absence renders as "null" in the GUI canvas.
# ``userlabel`` is intentionally excluded — today's Stop shapes omit it by
# design and must stay clean.
_DISPLAY_ATTRS = ("image", "x", "y")


def _local(tag: str) -> str:
    """Strip an XML namespace prefix (``{uri}name`` -> ``name``)."""
    if isinstance(tag, str) and "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _direct_children(elem: Optional[ET.Element], name: str) -> List[ET.Element]:
    """Direct children of ``elem`` whose local name is ``name`` (order preserved)."""
    if elem is None:
        return []
    return [c for c in list(elem) if _local(c.tag) == name]


def _first_direct(elem: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    """First direct child of ``elem`` whose local name is ``name``, else None."""
    if elem is None:
        return None
    for c in list(elem):
        if _local(c.tag) == name:
            return c
    return None


def _find_process(root: ET.Element) -> Optional[ET.Element]:
    """Locate the ``<process>`` element by local name (root or any descendant).

    Covers both the normal envelope (``<bns:Component><bns:object><process>``)
    and the raw escape hatch where the root element is itself ``<process>``.
    """
    for elem in root.iter():
        if _local(elem.tag) == "process":
            return elem
    return None


def _issue(code: str, shape: str, shape_type: str, message: str, remediation: str) -> Dict[str, str]:
    return {
        "code": code,
        "shape": shape,
        "shape_type": shape_type,
        "message": message,
        "remediation": remediation,
    }


def _shape_type(shape: ET.Element) -> str:
    return (shape.get("shapetype") or "").strip().lower()


def _is_terminal(shape: ET.Element, shape_type: str) -> bool:
    """A shape that legitimately needs no outbound edge.

    Terminals: ``stop``, ``returndocuments``, ``doccacheload``, and a
    ``processcall`` whose ``configuration/processcall/returnpaths`` is absent or
    has no child elements (i.e. it does not return to a downstream path).
    """
    if shape_type in _TERMINAL_SHAPE_TYPES:
        return True
    if shape_type == "processcall":
        config = _first_direct(shape, "configuration")
        processcall = _first_direct(config, "processcall")
        returnpaths = _first_direct(processcall, "returnpaths")
        if returnpaths is None:
            return True
        return len(list(returnpaths)) == 0
    return False


def verify_process_graph(process_xml: str) -> Dict[str, Any]:
    """Verify the wiring of a process graph. See module docstring for the contract."""
    errors: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []

    if not isinstance(process_xml, str) or not process_xml.strip():
        errors.append(
            _issue(
                "PROCESS_XML_EMPTY",
                "",
                "",
                "Process XML was empty or not a string.",
                "Ensure the component GET returned the process Component XML before verifying.",
            )
        )
        return {"errors": errors, "warnings": warnings, "shapes_checked": 0}

    try:
        root = ET.fromstring(process_xml)
    except ET.ParseError as exc:
        errors.append(
            _issue(
                "PROCESS_XML_PARSE_FAILED",
                "",
                "",
                f"Process XML did not parse: {exc}",
                "Inspect the emitted process Component XML for malformed markup.",
            )
        )
        return {"errors": errors, "warnings": warnings, "shapes_checked": 0}

    process = _find_process(root)
    if process is None:
        errors.append(
            _issue(
                "PROCESS_GRAPH_NOT_FOUND",
                "",
                "",
                "No <process> element was found in the component XML.",
                "Confirm the component is a process and its XML contains a <process> graph.",
            )
        )
        return {"errors": errors, "warnings": warnings, "shapes_checked": 0}

    shapes_elem = _first_direct(process, "shapes")
    shape_elems = _direct_children(shapes_elem, "shape")
    shapes_checked = len(shape_elems)

    # Index shapes by their ``name`` attribute (the canvas shape id). Shapes
    # without a name cannot be referenced by a dragpoint.
    shapes_by_id: Dict[str, ET.Element] = {}
    for shape in shape_elems:
        name = shape.get("name")
        if name:
            shapes_by_id[name] = shape

    # ------------------------------------------------------------------
    # Pass 1 — edges, dangling/unset dragpoints, per-shape attribute lints.
    # ------------------------------------------------------------------
    edges: Dict[str, List[str]] = {name: [] for name in shapes_by_id}
    for shape in shape_elems:
        name = shape.get("name") or ""
        stype = _shape_type(shape)
        dragpoints_elem = _first_direct(shape, "dragpoints")
        dp_children = _direct_children(dragpoints_elem, "dragpoint")

        for dp in dp_children:
            to_shape = dp.get("toShape")
            normalized = (to_shape or "").strip()
            if normalized == "" or normalized == "unset":
                if stype in _BRANCHING_SHAPE_TYPES:
                    errors.append(
                        _issue(
                            "BRANCH_OUTPUT_UNSET",
                            name,
                            stype,
                            f"Branching shape '{name}' ({stype}) has an output left "
                            f"toShape=\"{to_shape if to_shape is not None else ''}\".",
                            f"Wire every output of branching shape '{name}' to a target shape.",
                        )
                    )
                continue
            if normalized not in shapes_by_id:
                errors.append(
                    _issue(
                        "DRAGPOINT_TO_SHAPE_UNRESOLVED",
                        name,
                        stype,
                        f"Shape '{name}' has a dragpoint whose toShape=\"{normalized}\" "
                        "references a shape that does not exist.",
                        f"Point the dragpoint of '{name}' at an existing shape, or add the missing shape.",
                    )
                )
                continue
            if name in edges:
                edges[name].append(normalized)

        # --- Attribute lints (warnings only) ---
        if dragpoints_elem is None:
            warnings.append(
                _issue(
                    "DRAGPOINTS_ELEMENT_MISSING",
                    name,
                    stype,
                    f"Shape '{name}' ({stype or 'unknown'}) has no <dragpoints> element.",
                    "Every shape should carry a <dragpoints> element (empty for terminal shapes).",
                )
            )

        missing_display = [a for a in _DISPLAY_ATTRS if not (shape.get(a) or "").strip()]
        if missing_display:
            warnings.append(
                _issue(
                    "DISPLAY_ATTRIBUTE_MISSING",
                    name,
                    stype,
                    f"Shape '{name}' is missing display attribute(s) {missing_display}; "
                    "they render as \"null\" in the GUI.",
                    f"Set {missing_display} on shape '{name}' so it renders correctly on the canvas.",
                )
            )

        if stype == "stop":
            config = _first_direct(shape, "configuration")
            stop_cfg = _first_direct(config, "stop")
            if stop_cfg is None or stop_cfg.get("continue") is None:
                warnings.append(
                    _issue(
                        "STOP_CONTINUE_MISSING",
                        name,
                        stype,
                        f"Stop shape '{name}' is missing the 'continue' attribute.",
                        f"Add continue=\"true\" or continue=\"false\" to the <stop> configuration of '{name}'.",
                    )
                )

        if stype == "branch":
            config = _first_direct(shape, "configuration")
            branch_cfg = _first_direct(config, "branch")
            if branch_cfg is not None and branch_cfg.get("numBranches") is not None:
                try:
                    declared = int(branch_cfg.get("numBranches"))
                except (TypeError, ValueError):
                    declared = None
                if declared is not None and declared != len(dp_children):
                    warnings.append(
                        _issue(
                            "BRANCH_NUM_BRANCHES_MISMATCH",
                            name,
                            stype,
                            f"Branch shape '{name}' declares numBranches={declared} but has "
                            f"{len(dp_children)} dragpoint(s).",
                            f"Align numBranches on '{name}' with its dragpoint count.",
                        )
                    )

    # ------------------------------------------------------------------
    # Pass 2 — non-terminal dead ends.
    # ------------------------------------------------------------------
    for shape in shape_elems:
        name = shape.get("name") or ""
        stype = _shape_type(shape)
        if _is_terminal(shape, stype):
            continue
        if not edges.get(name):
            errors.append(
                _issue(
                    "NON_TERMINAL_SHAPE_DEAD_END",
                    name,
                    stype,
                    f"Non-terminal shape '{name}' ({stype or 'unknown'}) has no outbound path.",
                    f"Wire '{name}' to a next shape, or make it a terminal shape (stop/returndocuments).",
                )
            )

    # ------------------------------------------------------------------
    # Pass 3 — reachability from the start shape.
    # ------------------------------------------------------------------
    start_name: Optional[str] = None
    for shape in shape_elems:
        if _shape_type(shape) == "start":
            start_name = shape.get("name")
            break

    if not start_name:
        errors.append(
            _issue(
                "PROCESS_START_MISSING",
                "",
                "start",
                "Process graph has no start shape (shapetype=\"start\").",
                "Add a start shape; reachability cannot be determined without one.",
            )
        )
    else:
        visited: set = set()
        queue: List[str] = [start_name]
        while queue:
            cur = queue.pop()
            if cur in visited:
                continue
            visited.add(cur)
            for nxt in edges.get(cur, []):
                if nxt not in visited:
                    queue.append(nxt)
        for shape in shape_elems:
            name = shape.get("name")
            if name and name not in visited:
                errors.append(
                    _issue(
                        "SHAPE_UNREACHABLE",
                        name,
                        _shape_type(shape),
                        f"Shape '{name}' is not reachable from the start shape.",
                        f"Wire a path from the start shape to '{name}', or remove it.",
                    )
                )

    return {"errors": errors, "warnings": warnings, "shapes_checked": shapes_checked}
