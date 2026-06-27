"""Preservation policy data structures (issue #45).

Lives in a tiny standalone module so the structured XML builders can declare
``PRESERVATION_POLICY = PreservationPolicy(...)`` without pulling in the
heavier ``component_update_preservation`` module — which itself imports
``BuilderValidationError`` from ``connector_builder`` and would otherwise
introduce a circular import.

See ``component_update_preservation.merge_for_update`` for the merge engine
that consumes these policies during ``build_integration`` "update" steps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class OwnedPath:
    """A single subtree the builder fully owns.

    ``path`` is a slash-separated ElementTree path relative to the
    ``<bns:Component>`` root. Use the ``bns:`` prefix for elements in
    the Boomi namespace; bare names match the empty namespace (builders
    emit children of ``<bns:object>`` with ``xmlns=""``).

    ``mode`` is one of:
      - ``"replace"`` (default) — replace the entire subtree at ``path``
        with the desired element of the same path. Use for shapes where
        the builder owns every attribute and child of the element
        (e.g., ``<DatabaseConnectionSettings>`` or ``<MappingScript>``).
      - ``"key_merge"`` — direct children of ``path`` are merged keyed
        by ``key_attr``; desired children replace current children with
        the same key in place. Children of ``cur_elem`` whose key is in
        ``owned_keys`` but NOT present in desired are REMOVED (builder
        cleared that key). Children whose key isn't in ``owned_keys``
        are preserved as truly unknown. Unkeyed children matching by
        tag name are replaced in place from desired (no duplicates).
        Additionally, the element's own attributes are overwritten from
        desired (preserving unknown attributes desired doesn't supply).
      - ``"attrs_only"`` — only the listed ``owned_attrs`` on the
        element at ``path`` are overwritten from desired. Other
        attributes and all children are preserved. Use for shapes like
        REST Client's ``<Operation returnApplicationErrors="..."
        trackResponse="...">`` where the builder owns specific envelope
        attributes but not the children.
      - ``"subtree_merge"`` — overwrite the listed ``owned_attrs`` from
        desired (additive: set those desired provides, never touch
        unknown current attrs) AND replace owned child blocks named in
        ``owned_child_tags`` (replace by tag; add if current lacks
        them). Children of unknown tags are preserved in place. Use for
        owned elements that carry a fixed builder-owned attr/child set
        but may accrue unknown/future Boomi attrs or children, e.g.
        ``<DatabaseConnectionSettings host=... ><WriteOptions/>
        <AdapterPoolInfo/></DatabaseConnectionSettings>``. This is the
        granular alternative to wholesale ``replace`` for connector
        bodies where Boomi/UI may add fields the builder doesn't own.
    """

    path: str
    mode: str = "replace"
    key_attr: Optional[str] = None
    # For ``mode="key_merge"``: tuple of key values the builder owns.
    # Current children with key_attr value in this set but absent from
    # desired are removed (interpreted as "builder cleared this key").
    # When None, no removal happens — all current children survive
    # unless explicitly replaced by a same-keyed desired child.
    owned_keys: Optional[Tuple[str, ...]] = None
    # For ``mode="attrs_only"`` (and as a forward-compat hint for
    # ``mode="key_merge"``): tuple of attribute names the builder owns
    # on the element at ``path``. Only these attrs are overwritten;
    # everything else on the element is preserved.
    owned_attrs: Optional[Tuple[str, ...]] = None
    # For ``mode="key_merge"``: tuple of attribute names the builder
    # emits CONDITIONALLY (only when the caller supplies them). When
    # present in desired, overwrite current; when absent, preserve
    # current. Unlike ``owned_attrs`` (closed set / remove-when-missing),
    # this is "additive" so a path-only update doesn't clear live
    # values the builder defaults out. Use for attrs like REST
    # ``requestProfile`` / ``responseProfile`` that the builder
    # only emits when the caller passes the corresponding profile ref
    # (Codex r17 P2 follow-up).
    owned_attrs_additive: Optional[Tuple[str, ...]] = None
    # For ``mode="key_merge"``: tuple of key values the builder emits a
    # placeholder for but does NOT actually own. When current has the
    # key, current wins — desired's emission is ignored. When current
    # lacks the key, desired's value is added (initial setup). Use for
    # fields whose live value is populated post-authorization (e.g.
    # REST Client OAuth2 ``oauthContext`` which Boomi fills with
    # ``accessToken``/``accessTokenKey`` at handshake time but the
    # builder unconditionally emits as a token-not-set skeleton).
    preserve_keys: Optional[Tuple[str, ...]] = None
    # For ``mode="key_merge"``: tuple of key values that participate
    # in conditional preservation. Current wins ONLY when desired's
    # element has no meaningful content (no descendant attributes or
    # text), interpreted as "caller didn't supply a value, builder
    # emitted a placeholder." When desired IS populated, normal
    # same-key replacement applies. Use for fields the builder emits
    # an empty placeholder for when the caller omits them (e.g. REST
    # operation ``queryParameters`` / ``requestHeaders``
    # customProperties slots — Codex r8 P2: a path-only structured
    # update would otherwise wipe UI-added live custom properties).
    preserve_when_desired_empty: Optional[Tuple[str, ...]] = None
    # For ``mode="subtree_merge"``: tuple of child element tag names the
    # builder owns. Current children with these tags are replaced by
    # desired's same-tag children (added if current lacks them);
    # children of any other tag are preserved in place.
    owned_child_tags: Optional[Tuple[str, ...]] = None
    # For ``mode="key_merge"``: coupled attribute groups
    # ``((trigger_attr, (dependent_attr, ...)), ...)``. Each dependent
    # attr is applied from desired ONLY when its trigger attr is
    # present in desired; otherwise current's value is preserved. Use
    # when a builder unconditionally emits a default for one attr that
    # is only meaningful alongside another, so the default must not
    # clobber the live value when the trigger attr is absent. (Historic
    # example: REST ``requestProfileType`` was coupled to
    # ``requestProfile`` until #50 made both attrs conditionally emitted,
    # at which point the REST policy moved them to plain
    # ``owned_attrs_additive`` and dropped this coupling. The merge
    # engine still supports the feature for any future such case.)
    coupled_attr_groups: Optional[Tuple[Tuple[str, Tuple[str, ...]], ...]] = None


@dataclass(frozen=True)
class PreservationPolicy:
    """A builder's declaration of which XML it owns vs. preserves.

    ``component_type`` and (optional) ``subtype`` must match the live
    component's ``type``/``subType`` root attributes; otherwise the
    merge raises ``UPDATE_PRESERVATION_TYPE_MISMATCH``.

    ``owned_root_attrs`` lists root-element attributes the builder may
    overwrite. Default is just ``("name",)`` — folder attributes are
    intentionally NOT in the default set because builders emit
    ``folderName="Home"`` whenever the caller omits ``folder_name``,
    which would silently move components to Home on every structured
    update (Codex r2 P2). Folder moves via build_integration structured
    updates aren't supported in M2.6d; use ``manage_component`` /
    ``manage_connector`` smart-merge metadata updates to move
    components instead. Attributes not listed are kept verbatim from
    current.

    ``owned_paths`` lists owned subtrees. Everything else inside the
    component (including unknown ``<bns:Component>`` children and
    unknown siblings inside ``<bns:object>``) is preserved automatically.
    """

    component_type: str
    subtype: Optional[str] = None
    owned_root_attrs: Tuple[str, ...] = ("name",)
    owned_paths: Tuple[OwnedPath, ...] = ()
    # Codex r8 P2 narrow-risk guard: live components whose
    # ``type``/``subType`` match the policy but whose internal shape is
    # incompatible can slip past the type check. Example: profile.db
    # DB write profile + read-builder config — same type="profile.db"
    # passes the root check, but ``DatabaseGeneralInfo
    # executionType="dbwrite"`` doesn't match the read builder's
    # expected ``dbread`` and the merge produces a hybrid component.
    # When non-empty, the merge engine looks up the element at
    # ``subtype_marker_xpath``, reads its ``@subtype_marker_attr``
    # value, and fails with UPDATE_PRESERVATION_TYPE_MISMATCH unless
    # it matches ``subtype_marker_expected``.
    subtype_marker_xpath: Optional[str] = None
    subtype_marker_attr: Optional[str] = None
    subtype_marker_expected: Optional[str] = None
    # Codex r7 P2: ``bns:encryptedValue`` ``@path`` values the builder
    # may emit. Auth-mode changes (e.g. BASIC → NONE) cause the builder
    # to emit an empty ``<bns:encryptedValues/>``; without this set the
    # stale password / client-secret slot would survive in current XML,
    # leaving stored credentials on a connection that no longer uses
    # them. Entries in this set whose ``@path`` is absent from desired
    # are PRUNED from current. Entries whose ``@path`` is present in
    # both keep current's ``@isSet`` value (preserves the live secret).
    # Unknown ``@path`` values (not in this set) are always preserved.
    owned_encrypted_paths: Tuple[str, ...] = ()


__all__ = ["OwnedPath", "PreservationPolicy"]
