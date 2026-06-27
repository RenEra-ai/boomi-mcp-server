"""Component XML update preservation (issue #45).

Provides read-merge-write semantics for ``build_integration`` "update" steps
on builder-generated components. Without this module, structured builder
updates would call Boomi's Component PUT endpoint with freshly built XML,
which is a full XML replacement — any nodes the builder doesn't emit are
deleted from the live component. That breaks ``bns:encryptedValues``,
``bns:processOverrides``, unknown root attributes, unknown children inside
``bns:object``, and any future Boomi schema additions.

``merge_for_update`` parses both XMLs, validates the component type/subType
matches the builder's declared policy, then for each owned subtree in the
policy replaces (or key-merges) the current XML's element with the desired
one. Everything else — including unknown attributes, unknown siblings inside
``bns:Component``, and unknown children inside ``bns:object`` — is preserved
from the current live XML. ``bns:encryptedValues`` entries are merged by
``@path`` so existing isSet=true secrets survive while desired-side new
entries are added.

Errors are raised as :class:`BuilderValidationError` with one of the six
``UPDATE_PRESERVATION_*`` codes documented in the issue #45 plan.
"""

from __future__ import annotations

import copy
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

from ._shared import set_description_element
from .builders._preservation_policy import (
    OwnedPath,
    PreservationPolicy,
)
from .builders.connector_builder import BuilderValidationError


_BNS_URI = "http://api.platform.boomi.com/"
_BNS_NSMAP = {"bns": _BNS_URI}


def merge_for_update(
    current_xml: str,
    desired_xml: str,
    policy: PreservationPolicy,
) -> str:
    """Merge builder-owned subtrees from ``desired_xml`` into ``current_xml``.

    Returns the merged XML as a unicode string. Raises
    :class:`BuilderValidationError` with one of:

    - ``UPDATE_PRESERVATION_XML_PARSE_FAILED`` — either XML is malformed.
    - ``UPDATE_PRESERVATION_TYPE_MISMATCH`` — type/subType do not align.
    - ``UPDATE_PRESERVATION_OBJECT_MISSING`` — an owned subtree is absent.
    - ``UPDATE_PRESERVATION_POLICY_UNSUPPORTED`` — policy is missing.
    - ``UPDATE_PRESERVATION_MERGE_FAILED`` — an owned subtree can't be
      merged with the requested mode.
    """
    if policy is None:
        raise BuilderValidationError(
            "Structured update requires a preservation policy.",
            error_code="UPDATE_PRESERVATION_POLICY_UNSUPPORTED",
            field="policy",
            hint=(
                "This builder route does not declare a PRESERVATION_POLICY. "
                "Use the raw-XML escape hatch (config.xml) to update via "
                "full XML replacement, or open an issue to add a policy for "
                "the missing route."
            ),
        )

    current_root = _parse(current_xml, side="current")
    desired_root = _parse(desired_xml, side="desired")

    _validate_type(current_root, desired_root, policy)
    _validate_subtype_marker(current_root, policy)
    _replace_owned_root_attrs(current_root, desired_root, policy)
    _maybe_replace_description(current_root, desired_root)
    _merge_encrypted_values(current_root, desired_root, policy)

    for owned in policy.owned_paths:
        _apply_owned_path(current_root, desired_root, owned)

    return ET.tostring(current_root, encoding="unicode")


def _parse(xml_text: str, *, side: str) -> ET.Element:
    if not isinstance(xml_text, str) or not xml_text.strip():
        raise BuilderValidationError(
            f"{side} XML is empty.",
            error_code="UPDATE_PRESERVATION_XML_PARSE_FAILED",
            field=f"{side}_xml",
            hint=(
                f"Re-fetch the {side} component via query_components action='get' "
                "to inspect its XML, or report this as an SDK bug if the live "
                "component is empty."
            ),
        )
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise BuilderValidationError(
            f"{side} XML cannot be parsed: {exc}",
            error_code="UPDATE_PRESERVATION_XML_PARSE_FAILED",
            field=f"{side}_xml",
            hint=(
                f"Re-fetch the {side} component via query_components action='get' "
                "to inspect its XML; if it's malformed in Boomi, edit/repair it "
                "in the UI before retrying the structured update."
            ),
        ) from exc


def _validate_type(
    current_root: ET.Element,
    desired_root: ET.Element,
    policy: PreservationPolicy,
) -> None:
    current_type = current_root.attrib.get("type", "")
    desired_type = desired_root.attrib.get("type", "")
    if current_type != policy.component_type or desired_type != policy.component_type:
        raise BuilderValidationError(
            (
                f"Component type mismatch — policy expects "
                f"{policy.component_type!r}, current is {current_type!r}, "
                f"desired is {desired_type!r}."
            ),
            error_code="UPDATE_PRESERVATION_TYPE_MISMATCH",
            field="type",
            hint=(
                "Confirm the target component_id points at a "
                f"{policy.component_type!r} component, not a "
                f"{current_type!r} one. If you intended to migrate types, "
                "delete the existing component and recreate."
            ),
            details={
                "policy_type": policy.component_type,
                "current_type": current_type,
                "desired_type": desired_type,
            },
        )
    if policy.subtype is not None:
        current_sub = current_root.attrib.get("subType", "")
        desired_sub = desired_root.attrib.get("subType", "")
        if current_sub != policy.subtype or desired_sub != policy.subtype:
            raise BuilderValidationError(
                (
                    f"Component subType mismatch — policy expects "
                    f"{policy.subtype!r}, current is {current_sub!r}, "
                    f"desired is {desired_sub!r}."
                ),
                error_code="UPDATE_PRESERVATION_TYPE_MISMATCH",
                field="subType",
                hint=(
                    "Confirm the target component_id matches the "
                    f"builder's subType {policy.subtype!r}. Cross-subType "
                    "migrations (e.g. database↔rest) require a new component."
                ),
                details={
                    "policy_subtype": policy.subtype,
                    "current_subtype": current_sub,
                    "desired_subtype": desired_sub,
                },
            )


def _validate_subtype_marker(
    current_root: ET.Element,
    policy: PreservationPolicy,
) -> None:
    """Codex r8 P2 narrow-risk guard. When the policy declares a
    subtype-marker xpath, look up the element in current XML and check
    its attribute matches the expected value. Catches cases where the
    root type/subType pass but an internal discriminator (e.g.
    DatabaseGeneralInfo @executionType) doesn't match the builder's
    actual shape — would otherwise produce a hybrid invalid component.
    """
    if not (
        policy.subtype_marker_xpath
        and policy.subtype_marker_attr
        and policy.subtype_marker_expected is not None
    ):
        return
    _, child = _walk(current_root, policy.subtype_marker_xpath)
    if child is None:
        raise BuilderValidationError(
            (
                f"Subtype-marker path "
                f"{policy.subtype_marker_xpath!r} missing in current XML."
            ),
            error_code="UPDATE_PRESERVATION_TYPE_MISMATCH",
            field="subtype_marker",
            hint=(
                "The live component does not match the builder's expected "
                "shape. Confirm the component_id targets the right kind of "
                "component (e.g. profile.db database.read, not "
                "database.write)."
            ),
            details={
                "subtype_marker_xpath": policy.subtype_marker_xpath,
                "side": "current",
            },
        )
    actual = child.attrib.get(policy.subtype_marker_attr, "")
    if actual != policy.subtype_marker_expected:
        raise BuilderValidationError(
            (
                f"Subtype-marker mismatch — "
                f"{policy.subtype_marker_xpath}/"
                f"@{policy.subtype_marker_attr} expects "
                f"{policy.subtype_marker_expected!r}, got {actual!r}."
            ),
            error_code="UPDATE_PRESERVATION_TYPE_MISMATCH",
            field="subtype_marker",
            hint=(
                "The live component is a different builder shape than the "
                "structured update assumes. Confirm component_id and "
                "profile_type / operation_mode match the live component."
            ),
            details={
                "subtype_marker_xpath": policy.subtype_marker_xpath,
                "subtype_marker_attr": policy.subtype_marker_attr,
                "expected": policy.subtype_marker_expected,
                "actual": actual,
            },
        )


def _replace_owned_root_attrs(
    current_root: ET.Element,
    desired_root: ET.Element,
    policy: PreservationPolicy,
) -> None:
    for attr in policy.owned_root_attrs:
        if attr in desired_root.attrib:
            current_root.set(attr, desired_root.attrib[attr])


def _maybe_replace_description(
    current_root: ET.Element,
    desired_root: ET.Element,
) -> None:
    desired_desc = desired_root.find("bns:description", _BNS_NSMAP)
    if desired_desc is None:
        return
    desired_text = (desired_desc.text or "").strip()
    if not desired_text:
        # Empty desired description = builder didn't author one; preserve.
        # The empty placeholder builders emit (``<bns:description></bns:description>``)
        # would otherwise clobber the live description on EVERY structured
        # update because builders unconditionally emit the element.
        #
        # Codex r3 P2 known limitation: this also means a structured
        # update cannot CLEAR an existing description by passing
        # ``description=""`` — the empty desired text is indistinguishable
        # from "user omitted description, builder defaulted to empty".
        # Workaround: use ``manage_component action='update'`` with
        # ``config={"description": ""}`` to clear via smart-merge, or
        # supply a single-space placeholder. Fixing properly requires
        # every structured builder to conditionally emit the
        # ``<bns:description>`` element only when the caller supplied
        # a description, which is a wider refactor tracked as future
        # work.
        return
    set_description_element(current_root, desired_desc.text or "")


def _merge_encrypted_values(
    current_root: ET.Element,
    desired_root: ET.Element,
    policy: PreservationPolicy,
) -> None:
    ev_tag = f"{{{_BNS_URI}}}encryptedValues"
    entry_tag = f"{{{_BNS_URI}}}encryptedValue"
    current_ev = current_root.find(ev_tag)
    desired_ev = desired_root.find(ev_tag)
    owned_paths_set = set(policy.owned_encrypted_paths)
    desired_paths: set = set()
    desired_entries: List[ET.Element] = []
    if desired_ev is not None:
        for entry in desired_ev.findall(entry_tag):
            path = entry.attrib.get("path")
            if path:
                desired_paths.add(path)
                desired_entries.append(entry)

    # Codex r7 P2: prune builder-owned encrypted paths that are absent
    # from desired (e.g. auth-mode change from BASIC to NONE clears the
    # password slot). Unknown paths (not in owned_encrypted_paths)
    # always survive.
    if current_ev is not None and owned_paths_set:
        for entry in list(current_ev.findall(entry_tag)):
            path = entry.attrib.get("path")
            if path in owned_paths_set and path not in desired_paths:
                current_ev.remove(entry)
        # If pruning emptied the container, leave it as an empty
        # element rather than removing — Boomi's exports always carry
        # the placeholder, and the parent component layout stays stable.

    if not desired_entries:
        # Builder emitted no entries; nothing more to merge in.
        return
    if current_ev is None:
        # Insert a fresh container at the top of <bns:Component>.
        current_ev = ET.Element(ev_tag)
        current_root.insert(0, current_ev)
    existing_paths = {
        entry.attrib.get("path")
        for entry in current_ev.findall(entry_tag)
        if entry.attrib.get("path") is not None
    }
    for desired_entry in desired_entries:
        path = desired_entry.attrib.get("path")
        if path and path in existing_paths:
            # Existing entry wins — never clobber an isSet=true secret slot.
            continue
        current_ev.append(copy.deepcopy(desired_entry))


def _expand_segment(segment: str) -> str:
    if segment.startswith("bns:"):
        return f"{{{_BNS_URI}}}{segment[4:]}"
    return segment


def _walk(root: ET.Element, path: str) -> Tuple[Optional[ET.Element], Optional[ET.Element]]:
    """Walk ``path`` from ``root``. Returns (parent, child) or (None, None)."""
    segments = path.split("/")
    if not segments:
        return None, None
    parent = root
    for seg in segments[:-1]:
        tag = _expand_segment(seg)
        nxt = parent.find(tag)
        if nxt is None:
            return None, None
        parent = nxt
    last_tag = _expand_segment(segments[-1])
    child = parent.find(last_tag)
    return parent, child


def _apply_owned_path(
    current_root: ET.Element,
    desired_root: ET.Element,
    owned: OwnedPath,
) -> None:
    cur_parent, cur_child = _walk(current_root, owned.path)
    _, des_child = _walk(desired_root, owned.path)
    if cur_child is None:
        raise BuilderValidationError(
            f"Owned path {owned.path!r} missing in current XML.",
            error_code="UPDATE_PRESERVATION_OBJECT_MISSING",
            field="owned_path",
            hint=(
                f"The live component is missing the {owned.path!r} subtree "
                "this builder owns. Re-fetch and inspect the component XML "
                "via query_components action='get' to verify it matches the "
                "expected builder schema."
            ),
            details={"path": owned.path, "side": "current"},
        )
    if des_child is None:
        raise BuilderValidationError(
            f"Owned path {owned.path!r} missing in desired XML.",
            error_code="UPDATE_PRESERVATION_OBJECT_MISSING",
            field="owned_path",
            hint=(
                f"The builder did not emit the {owned.path!r} subtree it "
                "claims to own. This is a builder bug — please file an issue."
            ),
            details={"path": owned.path, "side": "desired"},
        )

    if owned.mode == "replace":
        index = list(cur_parent).index(cur_child)
        cur_parent.remove(cur_child)
        cur_parent.insert(index, copy.deepcopy(des_child))
        return

    if owned.mode == "key_merge":
        if not owned.key_attr:
            raise BuilderValidationError(
                f"OwnedPath mode 'key_merge' requires key_attr (path={owned.path!r}).",
                error_code="UPDATE_PRESERVATION_MERGE_FAILED",
                field="owned_path",
                hint=(
                    "This is a builder policy bug — set key_attr= on the "
                    "OwnedPath when declaring mode='key_merge'. File an issue."
                ),
                details={"path": owned.path},
            )
        # Element attribute handling on the keyed path. Two policies:
        # - owned_attrs SET: the owned attribute set is CLOSED. For each
        #   attr in owned_attrs, copy from desired if present, otherwise
        #   REMOVE from current (builder cleared that attr). Unknown
        #   current attrs (not in owned_attrs) survive. Use this for
        #   shapes like <GenericOperationConfig requestProfile="..."
        #   ...> where switching off the profile means desired omits
        #   requestProfile and current's stale value must drop.
        # - owned_attrs UNSET: additive merge — overwrite attrs desired
        #   provides, never remove anything. Matches earlier behavior
        #   for simpler shapes like <GenericConnectionConfig> (which
        #   has no attrs of its own anyway).
        if owned.owned_attrs is not None:
            for attr_name in owned.owned_attrs:
                if attr_name in des_child.attrib:
                    cur_child.set(attr_name, des_child.attrib[attr_name])
                elif attr_name in cur_child.attrib:
                    del cur_child.attrib[attr_name]
        else:
            for attr_name, attr_value in des_child.attrib.items():
                cur_child.set(attr_name, attr_value)
        # Codex r17 P2: owned_attrs_additive runs after the closed-set
        # owned_attrs pass and overwrites without removing. Use for
        # builder-conditional attrs (e.g. REST ``requestProfile`` /
        # ``requestProfileType`` / ``responseProfile`` /
        # ``responseProfileType`` after #50) where the builder emits the
        # attr only when the caller supplies it — current's live value
        # should win when desired omits the attr.
        if owned.owned_attrs_additive is not None:
            for attr_name in owned.owned_attrs_additive:
                if attr_name in des_child.attrib:
                    cur_child.set(attr_name, des_child.attrib[attr_name])
        # Coupled attribute groups: a dependent attr is applied from
        # desired only when its trigger attr is present in desired;
        # otherwise current's value is preserved. For a builder that
        # unconditionally emits a default for an attr meaningful only
        # alongside another, this keeps the live value when the trigger
        # is absent. (No active policy uses it after #50 — the REST
        # profile-type attrs that motivated it became conditionally
        # emitted and moved to owned_attrs_additive — but the engine
        # retains the feature for any future such case.)
        if owned.coupled_attr_groups is not None:
            for trigger_attr, dependent_attrs in owned.coupled_attr_groups:
                if trigger_attr in des_child.attrib:
                    for dep_attr in dependent_attrs:
                        if dep_attr in des_child.attrib:
                            cur_child.set(dep_attr, des_child.attrib[dep_attr])
        _merge_keyed_children(
            cur_child,
            des_child,
            owned.key_attr,
            owned.owned_keys or (),
            owned.preserve_keys or (),
            owned.preserve_when_desired_empty or (),
        )
        return

    if owned.mode == "attrs_only":
        # Overwrite only the listed owned_attrs from desired; preserve
        # everything else on the element (children + other attrs).
        if not owned.owned_attrs:
            raise BuilderValidationError(
                f"OwnedPath mode 'attrs_only' requires owned_attrs (path={owned.path!r}).",
                error_code="UPDATE_PRESERVATION_MERGE_FAILED",
                field="owned_path",
                hint=(
                    "This is a builder policy bug — set owned_attrs= on the "
                    "OwnedPath when declaring mode='attrs_only'. File an issue."
                ),
                details={"path": owned.path},
            )
        for attr_name in owned.owned_attrs:
            if attr_name in des_child.attrib:
                cur_child.set(attr_name, des_child.attrib[attr_name])
        return

    if owned.mode == "subtree_merge":
        # Overwrite owned attrs additively (preserve unknown attrs) +
        # replace owned named child blocks (preserve unknown children).
        # Granular alternative to wholesale `replace` for connector
        # bodies where Boomi/UI may add unknown attrs or children.
        if not owned.owned_attrs and not owned.owned_child_tags:
            raise BuilderValidationError(
                (
                    f"OwnedPath mode 'subtree_merge' requires owned_attrs "
                    f"and/or owned_child_tags (path={owned.path!r})."
                ),
                error_code="UPDATE_PRESERVATION_MERGE_FAILED",
                field="owned_path",
                hint=(
                    "This is a builder policy bug — declare the owned attrs "
                    "and child tags for mode='subtree_merge'. File an issue."
                ),
                details={"path": owned.path},
            )
        for attr_name in owned.owned_attrs or ():
            if attr_name in des_child.attrib:
                cur_child.set(attr_name, des_child.attrib[attr_name])
        if owned.owned_child_tags:
            _replace_children_by_tag(
                cur_child, des_child, owned.owned_child_tags
            )
        return

    raise BuilderValidationError(
        f"Unsupported OwnedPath mode {owned.mode!r}.",
        error_code="UPDATE_PRESERVATION_MERGE_FAILED",
        field="owned_path",
        hint=(
            "Supported OwnedPath modes are 'replace', 'key_merge', "
            "'attrs_only', and 'subtree_merge'. This is a builder policy "
            "bug — file an issue."
        ),
        details={"path": owned.path, "mode": owned.mode},
    )


def _replace_children_by_tag(
    cur_elem: ET.Element,
    des_elem: ET.Element,
    owned_tags: Tuple[str, ...],
) -> None:
    """Replace ``cur_elem``'s owned-tag children with ``des_elem``'s, and
    preserve children of unknown tags.

    Desired is authoritative for the owned-tag children's content *and*
    their relative order: they are emitted as a contiguous block in
    desired document order, spliced in at the position of the first
    owned-tag child currently present (or appended if current has none).
    This keeps the merged output canonical even when current is missing
    an owned block or carries the owned blocks in a non-canonical order
    (e.g. a raw-XML-created component) — appending missing blocks at the
    end would otherwise emit them out of schema order.

    Unknown-tag children are preserved in their current relative order.
    A surviving unknown child that sat *between* two owned blocks lands
    after the regrouped owned block; this is acceptable because the merge
    necessarily rewrites the owned blocks and no canonical Boomi export
    interleaves unknown children among these connector-body blocks.

    Tag matching is namespace-aware: ``owned_tags`` entries are bare
    local names (builders emit these children with ``xmlns=""``), and
    are expanded to the empty-namespace tag for comparison.
    """
    owned_tag_set = {_expand_segment(t) for t in owned_tags}
    # Desired's owned-tag children, in desired document order — authoritative
    # for both content and ordering of the spliced-in owned block.
    desired_owned: List[ET.Element] = [
        copy.deepcopy(child)
        for child in list(des_elem)
        if child.tag in owned_tag_set
    ]

    new_children: List[ET.Element] = []
    owned_block_emitted = False
    for cur_child in list(cur_elem):
        if cur_child.tag in owned_tag_set:
            # Splice the full owned block (desired order) at the first owned
            # position; drop the remaining current owned children.
            if not owned_block_emitted:
                new_children.extend(desired_owned)
                owned_block_emitted = True
        else:
            new_children.append(cur_child)
    if not owned_block_emitted:
        # Current had no owned children at all — append the owned block.
        new_children.extend(desired_owned)

    for c in list(cur_elem):
        cur_elem.remove(c)
    for c in new_children:
        cur_elem.append(c)


def _element_has_meaningful_content(elem: ET.Element) -> bool:
    """True iff ``elem`` carries any descendant data (attribute or text).

    Used by ``preserve_when_desired_empty``: a builder-emitted
    placeholder like ``<field id="queryParameters"
    type="customproperties"><customProperties/></field>`` has no
    descendants with text or attributes — treat as "empty" so the
    merge preserves current. A populated customProperties has
    ``<properties key="..." value="..."/>`` children whose attributes
    flag this as meaningful content.

    The element's OWN attributes are excluded (they encode the key
    and type, not the data). Only descendants count.
    """
    for descendant in elem.iter():
        if descendant is elem:
            continue
        if descendant.attrib:
            return True
        if (descendant.text or "").strip():
            return True
    return False


def _merge_keyed_children(
    cur_elem: ET.Element,
    des_elem: ET.Element,
    key_attr: str,
    owned_keys: Tuple[str, ...] = (),
    preserve_keys: Tuple[str, ...] = (),
    preserve_when_desired_empty: Tuple[str, ...] = (),
) -> None:
    """In-place key-merge of ``des_elem`` children into ``cur_elem``.

    Behaviour by case:
    - Keyed desired child whose key is in ``preserve_keys``: current
      wins. The builder emits a placeholder/skeleton for this id but
      Boomi populates live state at runtime (e.g. OAuth2
      ``oauthContext`` accessToken cache), so the desired entry is
      ignored when current already has one. If current lacks the key,
      desired's value is added (initial setup).
    - Keyed desired child whose key is in
      ``preserve_when_desired_empty`` AND desired's element has no
      meaningful content: current wins (interpreted as "caller didn't
      supply, builder emitted an empty placeholder"). When desired IS
      populated, normal same-key replacement applies. Use for fields
      like REST ``queryParameters``/``requestHeaders`` so a path-only
      update doesn't wipe UI-added custom properties.
    - Keyed desired child whose ``@key_attr`` matches a keyed current
      child (and key not in either preserve set): replace current's
      element in place.
    - Keyed desired child whose key is not in current: append at end.
    - Keyed current child whose key is in ``owned_keys`` (builder
      enumerates the ids it owns) but absent from desired: REMOVE
      (builder explicitly cleared that key).
    - Keyed current child whose key is NOT in ``owned_keys`` and NOT
      in desired: PRESERVE (truly unknown to this builder).
    - Unkeyed children (no ``@key_attr``): replace by tag name. The
      first unkeyed current child matching each desired unkeyed
      child's tag is replaced in place; if none match, append. Avoids
      duplicating builder-emitted placeholders like ``<Options/>``.
    """
    desired_keyed: List[Tuple[str, ET.Element]] = []
    desired_unkeyed: List[ET.Element] = []
    for child in list(des_elem):
        key = child.attrib.get(key_attr)
        if key is None:
            desired_unkeyed.append(child)
        else:
            desired_keyed.append((key, child))
    desired_keys = {k for k, _ in desired_keyed}
    owned_key_set = set(owned_keys)
    preserve_key_set = set(preserve_keys)
    preserve_empty_key_set = set(preserve_when_desired_empty)
    # Map key → desired's element so we can check the "desired empty"
    # predicate without re-scanning.
    desired_keyed_by_key: Dict[str, ET.Element] = {k: dc for k, dc in desired_keyed}

    # Track desired unkeyed children consumed by tag-name match so each
    # is used at most once.
    desired_unkeyed_remaining: List[ET.Element] = list(desired_unkeyed)

    new_children: List[ET.Element] = []
    seen_keys: set = set()
    for cur_child in list(cur_elem):
        key = cur_child.attrib.get(key_attr)
        if key is not None:
            if key in preserve_key_set:
                # preserve-only key: current wins, ignore desired's
                # placeholder/skeleton emission.
                new_children.append(cur_child)
                seen_keys.add(key)
            elif (
                key in preserve_empty_key_set
                and key in desired_keyed_by_key
                and not _element_has_meaningful_content(desired_keyed_by_key[key])
            ):
                # preserve-when-desired-empty key: builder emitted an
                # empty placeholder (caller didn't supply this field) —
                # keep current's populated value.
                new_children.append(cur_child)
                seen_keys.add(key)
            elif key in desired_keys and key not in seen_keys:
                # Same-keyed replacement.
                for k, dc in desired_keyed:
                    if k == key:
                        new_children.append(copy.deepcopy(dc))
                        seen_keys.add(key)
                        break
            elif key in owned_key_set:
                # Builder owns this id but desired omitted it — DROP it.
                continue
            else:
                # Truly unknown id — preserve untouched.
                new_children.append(cur_child)
        else:
            # Unkeyed current child — replace with same-tag desired
            # unkeyed child if available, else preserve untouched.
            replacement_idx = None
            for i, dc in enumerate(desired_unkeyed_remaining):
                if dc.tag == cur_child.tag:
                    replacement_idx = i
                    break
            if replacement_idx is not None:
                new_children.append(
                    copy.deepcopy(desired_unkeyed_remaining.pop(replacement_idx))
                )
            else:
                new_children.append(cur_child)

    # Desired entries whose key wasn't in current — insert before the
    # first unkeyed child to preserve the canonical builder/live order
    # (keyed fields precede unkeyed placeholders like <Options/>).
    # Codex r11 P2: appending at the end placed a newly-added field
    # like followRedirects after <Options/> on a PATCH→GET update,
    # producing non-canonical XML.
    insert_idx = next(
        (
            i for i, ch in enumerate(new_children)
            if ch.attrib.get(key_attr) is None
        ),
        len(new_children),
    )
    for k, dc in desired_keyed:
        if k not in seen_keys:
            new_children.insert(insert_idx, copy.deepcopy(dc))
            insert_idx += 1
            seen_keys.add(k)
    # Remaining desired unkeyed children (no current sibling shared their
    # tag) — append. Rare; typically reserved for builder-emitted
    # placeholders not already present in current.
    for dc in desired_unkeyed_remaining:
        new_children.append(copy.deepcopy(dc))

    for c in list(cur_elem):
        cur_elem.remove(c)
    for c in new_children:
        cur_elem.append(c)


__all__ = [
    "OwnedPath",
    "PreservationPolicy",
    "merge_for_update",
]
