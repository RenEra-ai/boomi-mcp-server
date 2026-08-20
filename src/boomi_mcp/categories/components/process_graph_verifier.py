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
# ``exception`` (throw a user-defined error) terminates document/process
# execution on unhappy paths and is always authored with an empty
# ``<dragpoints/>`` (see boomi_companion .../steps/exception_step.md: "Exception
# is a terminal shape"); it is common in escape-hatch process XML, which is
# exactly what this pass verifies.
_TERMINAL_SHAPE_TYPES = frozenset({"stop", "returndocuments", "doccacheload", "exception"})

# Shape types that ALWAYS end the path — an outbound dragpoint to a real shape
# is malformed (documents would flow past a terminal). Narrower than
# ``_TERMINAL_SHAPE_TYPES``: ``doccacheload`` is excluded because a Document
# Cache load can legitimately continue downstream in hand-authored/live XML,
# and ``processcall`` is conditionally terminal (handled by ``_is_terminal``).
# ``processcall`` stays out of this set because its continuation is legal when
# the child declares return paths; the illegal half — no declared return path
# yet an outgoing dragpoint — is Pass 2a′, which reports its own code.
_ALWAYS_TERMINAL_SHAPE_TYPES = frozenset({"stop", "returndocuments", "exception"})

# Shape types whose outputs are explicit branch outputs that must be wired.
_BRANCHING_SHAPE_TYPES = frozenset({"branch", "decision", "route"})

# Control shapes that split documents into rejected/unmatched/error branches
# (issue #102 C2). A bare Stop wired directly off one of these branches drops
# the rejected documents with no Message/Notify/Return-Documents/Document-Cache
# trail, so they become untraceable. ``catcherrors`` is Boomi's Try/Catch shape;
# the builder itself never wires a catcherrors/route/decision branch straight to
# a Stop (its catch legs route through notify/doccacheload first), so this lint
# does not fire on builder output.
_CONTROL_BRANCH_SHAPE_TYPES = frozenset({"route", "decision", "catcherrors"})

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


def _processcall_declares_return_paths(shape: ET.Element) -> bool:
    """Whether a ``processcall`` declares a BINDABLE return path from its child.

    The called process's Return Documents shapes are the ONLY authority on
    whether a Process Call continues: ``configuration/processcall/returnpaths``
    names them by ``childShapeName``, and a call whose child returns nothing
    carries the element empty (or not at all). This is the single definition of
    that question — ``_is_terminal`` and the continuation invariant below both
    read it, so the two can never drift into disagreeing about the same shape.

    "Bindable" is the load-bearing word, and counting child elements was not
    enough. A hand-authored or escape-hatch document can carry
    ``<returnpaths><returnpaths/></returnpaths>``, an entry with an empty
    ``childShapeName``, or an unrelated child — all of which named no shape in
    the called process, yet all of which counted as "declares return paths" and
    so SUPPRESSED the continuation error for a path the platform cannot bind.
    The check therefore asks what the live UI-built capture shows a real entry
    to be: a ``returnpaths`` child carrying a non-empty ``childShapeName``
    (``tests/fixtures/live_xml/m11/…``, shape10 — ``childShapeName="shape233"``,
    with ``returnLabel`` legitimately empty, which is why only the former is
    required).

    Failing toward "declares nothing" is the safe direction: it reports the
    continuation rather than certifying it.
    """
    config = _first_direct(shape, "configuration")
    processcall = _first_direct(config, "processcall")
    returnpaths = _first_direct(processcall, "returnpaths")
    if returnpaths is None:
        return False
    return bool(_processcall_return_path_keys(shape))


def _processcall_return_path_keys(shape: ET.Element) -> set:
    """The child shape names this ``processcall`` declares as return paths.

    These are the keys an outgoing connection must be attributed to: the live
    capture pairs ``returnpaths/@childShapeName`` with the outgoing
    ``dragpoint/@identifier`` carrying the SAME value, which is how the platform
    knows which return branch an edge belongs to.
    """
    config = _first_direct(shape, "configuration")
    processcall = _first_direct(config, "processcall")
    returnpaths = _first_direct(processcall, "returnpaths")
    if returnpaths is None:
        return set()
    return {
        (entry.get("childShapeName") or "").strip()
        for entry in _direct_children(returnpaths, "returnpaths")
        if (entry.get("childShapeName") or "").strip()
    }


def _is_terminal(shape: ET.Element, shape_type: str) -> bool:
    """A shape that legitimately needs no outbound edge.

    Terminals: ``stop``, ``returndocuments``, ``doccacheload``, and a
    ``processcall`` whose ``configuration/processcall/returnpaths`` is absent or
    has no child elements (i.e. it does not return to a downstream path).
    """
    if shape_type in _TERMINAL_SHAPE_TYPES:
        return True
    if shape_type == "processcall":
        return not _processcall_declares_return_paths(shape)
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
    # Validate shape ids first. A shape with no ``name`` cannot be referenced by
    # a dragpoint (so it is unreachable and unwireable), and a duplicate name
    # makes the graph ambiguous — the index would silently collapse the two and
    # mask reachability/dead-end problems. Both are graph-integrity errors, not
    # lints: without this, malformed escape-hatch XML verifies false-clean.
    shapes_by_id: Dict[str, ET.Element] = {}
    seen_names: set = set()
    flagged_duplicates: set = set()
    for shape in shape_elems:
        name = shape.get("name")
        stype = _shape_type(shape)
        if not name:
            errors.append(
                _issue(
                    "SHAPE_NAME_MISSING",
                    "",
                    stype,
                    f"A shape ({stype or 'unknown'}) is missing its required 'name' attribute.",
                    "Give every shape a unique non-empty name so it can be wired and reached.",
                )
            )
            continue
        if name in seen_names:
            if name not in flagged_duplicates:
                flagged_duplicates.add(name)
                errors.append(
                    _issue(
                        "DUPLICATE_SHAPE_NAME",
                        name,
                        stype,
                        f"Shape name '{name}' is used by more than one shape.",
                        f"Make shape names unique; rename the duplicate '{name}'.",
                    )
                )
        seen_names.add(name)
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
                # Issue #102 C1: a bare <stop/> with no continue= is a runtime NPE
                # and a GUI stack overflow — promoted from a warning to a hard
                # error so it blocks emission/verification rather than merely
                # advising.
                errors.append(
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
    # Start-shape discovery (shared by the Pass 2c inbound-edge guard and the
    # Pass 3 reachability BFS). ``start_name`` keeps the existing first-start
    # behavior; ``start_names`` collects every named start so an inbound edge
    # into ANY start (e.g. a second one) is caught without adding a
    # multiple-start validator.
    # ------------------------------------------------------------------
    start_name: Optional[str] = None
    start_names: set = set()
    first_start_seen = False
    for shape in shape_elems:
        if _shape_type(shape) != "start":
            continue
        name = shape.get("name")
        if not first_start_seen:
            start_name = name
            first_start_seen = True
        if name:
            start_names.add(name)

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
    # Pass 2a — always-terminal shapes must not carry an outbound edge.
    # ------------------------------------------------------------------
    # ``stop`` / ``returndocuments`` / ``exception`` end the path. An outbound
    # dragpoint to a real shape means documents flow past a terminal, which is
    # malformed even when no downstream Stop is reached (so the #102 C2a
    # reachability check below stays silent). The builder always emits these
    # with an empty ``<dragpoints/>``, so this never fires on builder output.
    for shape in shape_elems:
        name = shape.get("name") or ""
        stype = _shape_type(shape)
        outbound = edges.get(name)
        if stype in _ALWAYS_TERMINAL_SHAPE_TYPES and outbound:
            errors.append(
                _issue(
                    "TERMINAL_SHAPE_HAS_OUTBOUND",
                    name,
                    stype,
                    f"Terminal shape '{name}' ({stype}) has an outbound edge to "
                    f"{', '.join(outbound)}; terminal shapes end the path.",
                    f"Remove the outbound dragpoint from '{name}', or replace "
                    f"'{name}' with a non-terminal shape if the path must continue.",
                )
            )

    # ------------------------------------------------------------------
    # Pass 2a′ — a Process Call may not continue without a declared return path.
    # ------------------------------------------------------------------
    # The inverse of the ``_is_terminal`` rule above, and the reason this pass
    # exists at all (issue #175): that rule PERMITS a call with no declared
    # return path to end the path, but nothing rejected the contradictory
    # pairing — an empty/absent ``returnpaths`` together with an outgoing
    # dragpoint. Boomi keys the outbound connection on the return paths, not on
    # the dragpoint, so the platform simply does not draw the edge and whatever
    # the dragpoint pointed at is left orphaned on the canvas.
    #
    # Keyed on the RAW dragpoint children rather than ``edges[name]``, which
    # holds only resolved targets: a dragpoint whose ``toShape`` is missing,
    # empty or unresolved is still a continuation the author asked for, and it
    # must be reported here as well as by the generic edge diagnostics.
    for shape in shape_elems:
        name = shape.get("name") or ""
        stype = _shape_type(shape)
        if stype != "processcall":
            continue
        dragpoints = _direct_children(_first_direct(shape, "dragpoints"), "dragpoint")
        if not dragpoints:
            continue
        # ONE invariant: a Process Call carrying an outgoing connection must
        # DECLARE at least one bindable return path. Stated as a property of the
        # declaration alone — not of the edges — so there is no per-edge case to
        # get progressively weaker.
        #
        # Three review rounds each found a weaker form of an EARLIER, wider rule
        # — "has any child element", then "some edge binds", then "every edge
        # binds" — and each was a correct defect. That is the instance-patch
        # pattern. The answer was not a fourth condition: the wider rule rested
        # on a single live sample and was withdrawn to #176 (see below), leaving
        # the narrow property above, which four captures and one runtime
        # measurement support.
        # SCOPE, deliberately narrow (#175; the wider rule was reverted here and
        # belongs to #176).
        #
        # This fires only when the call declares NO bindable return path at all —
        # the case the platform's own captures evidence four times over, and the
        # one live QA measured at runtime: with a child proven to return nothing,
        # the shape downstream of such a call does not execute.
        #
        # It does NOT judge a POPULATED declaration. An earlier revision also
        # required every outgoing dragpoint's `identifier` to match a declared
        # `childShapeName`, because the one connected call in the live corpus
        # pairs them. One sample is not a platform rule: if any valid platform
        # form omits the identifier, that check would reject a customer's
        # legitimate process through `build_integration(action="verify")` — a
        # false positive on real data, inferred from a single observation. Two
        # internal review rounds asked for the wider rule and both reasoned from
        # that same sample, which is agreement, not evidence.
        #
        # #176 owns the binding contract and will establish the wire rule from a
        # UI-built returning parent. Until then this stays with what is
        # measured, and a populated declaration is left alone.
        declared_keys = _processcall_return_path_keys(shape)
        if declared_keys:
            continue
        errors.append(
            _issue(
                "PROCESS_CALL_ORPHAN_CONTINUATION",
                name,
                stype,
                # States ONLY what this check establishes. An earlier revision
                # ended "...and the shapes it points at are left unreachable",
                # which this check cannot know: a target reached by a sibling
                # branch leg is perfectly reachable, and the verifier emits no
                # unreachability error for it — so one payload certified the
                # shape reachable and told the caller it was not. That wording
                # was fixed once and came back with the #175 scope revert; it is
                # the same defect class, so the claim is now bounded to the
                # connection rather than extended to the graph.
                f"Process Call '{name}' declares no return path from the called "
                "process but carries an outgoing connection. The called process's "
                "Return Documents shapes are what make a forward connection "
                "valid, so the platform does not bind this connection and it "
                "does not exist in the emitted graph.",
                f"Remove the outgoing connection from '{name}' — a call whose "
                "child returns no documents ends its path — or, if the child "
                "does return documents, declare its Return Documents shapes as "
                f"the return paths of '{name}'.",
            )
        )

    # ------------------------------------------------------------------
    # Pass 2b — terminal-shape exclusivity / untraceable rejected docs (#102 C2).
    # ------------------------------------------------------------------
    for shape in shape_elems:
        name = shape.get("name") or ""
        stype = _shape_type(shape)
        # C2a — Return Documents and Stop are mutually exclusive path terminals.
        # A Return-Documents shape is itself terminal, so ANY downstream Stop
        # reachable from it (directly OR via intervening shapes, e.g.
        # returndocuments -> message -> stop) means one path uses BOTH terminal
        # mechanisms and the returned documents never reach the caller (Codex
        # review — walk reachability, not just the direct edge).
        if stype == "returndocuments":
            visited: set = set()
            queue: List[str] = list(edges.get(name, []))
            reached_stop: Optional[str] = None
            while queue:
                cur = queue.pop()
                if cur in visited:
                    continue
                visited.add(cur)
                cur_shape = shapes_by_id.get(cur)
                if cur_shape is not None and _shape_type(cur_shape) == "stop":
                    reached_stop = cur
                    break
                queue.extend(edges.get(cur, []))
            if reached_stop is not None:
                errors.append(
                    _issue(
                        "RETURN_DOCS_STOP_EXCLUSIVE",
                        name,
                        stype,
                        f"Return Documents shape '{name}' reaches Stop shape "
                        f"'{reached_stop}' downstream; Return Documents and Stop "
                        "are mutually exclusive path terminals.",
                        f"End the path at either '{name}' (Return Documents) or a Stop, "
                        "not both — remove the downstream path into the Stop.",
                    )
                )
        # C2b — a bare Stop wired straight off a Route/Decision/Try-Catch reject
        # branch drops those documents with no trace (warning: an intentional
        # drop is legal, but it should log/notify/return first).
        if stype in _CONTROL_BRANCH_SHAPE_TYPES:
            for target_name in edges.get(name, []):
                target = shapes_by_id.get(target_name)
                if target is not None and _shape_type(target) == "stop":
                    warnings.append(
                        _issue(
                            "CONTROL_BRANCH_BARE_STOP",
                            name,
                            stype,
                            f"{stype.capitalize()} shape '{name}' routes a branch "
                            f"directly into Stop shape '{target_name}'; rejected "
                            "documents are dropped with no trace.",
                            "Route the rejected branch through a Message, Notify, "
                            "Return Documents, or Document Cache before the Stop so "
                            "the documents remain traceable.",
                        )
                    )

    # ------------------------------------------------------------------
    # Pass 2c — start shapes must not be the target of any inbound edge.
    # ------------------------------------------------------------------
    # The start is the sole entry point; any resolved edge that lands on it is
    # malformed wiring — the exact mirror of the Pass 2a terminal-outbound
    # guard. This runs on the resolved ``edges`` map, so a self-edge
    # (start -> start) is flagged, while legal builder loop-backs (which target
    # a downstream shape, never the start) do not trigger it. Offenders are
    # sorted for deterministic output.
    for inbound_start in sorted(start_names):
        offenders = sorted(src for src, targets in edges.items() if inbound_start in targets)
        if offenders:
            errors.append(
                _issue(
                    "START_SHAPE_HAS_INBOUND",
                    inbound_start,
                    "start",
                    f"Start shape '{inbound_start}' is the target of inbound edge(s) from "
                    f"{', '.join(offenders)}; the start is the sole entry point and must "
                    "have no inbound edge.",
                    f"Re-point the dragpoint(s) from {', '.join(offenders)} at a non-start "
                    "shape (loop-backs must target a downstream shape, never the start).",
                )
            )

    # ------------------------------------------------------------------
    # Pass 3 — reachability from the start shape.
    # ------------------------------------------------------------------
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
