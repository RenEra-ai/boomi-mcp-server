"""Served ``account_governance`` knowledge surface (issue #93, epic #85 / M4.5.8).

A read-only catalog of account / workspace *governance* decisions — where a
component is placed, what it is named, and who may edit it. This is real
authoring-decision knowledge the LLM owns, but it is **not** integration-runtime
shape (that is ``design_doctrine`` #86) and **not** GUI click-path mechanics
(builders / docs KB). It is served through the existing ``get_schema_template``
+ ``list_capabilities`` machinery (see ``categories/meta_tools.py``), a peer of
the inline ``operating_doctrine`` and the ``design_doctrine`` surface.

Honest capability split (per the issue's adversarial review): name-encoding on
component create is a candidate builder behavior (``emittable_today``, enforced
by the ``build_integration`` plan-time name-governance lint), while folder
placement, role/write-restriction gating, and component locking are genuinely
GUI-only (``gated`` / ``na``). Every ``gated`` entry carries a
``gui_only_boundary`` naming what the agent PROPOSES and the user APPLIES.

Provenance discipline: entries derived from the Boomi architect course (a
labeled third-party training source) are ``course_unverified`` and never
silently adopted. Two genuinely platform-behavioral claims — component locking
and folder write-restrictions via assigned roles — are corroborated against the
official KB and carry ``docs_corroborated`` with a ``docs_page_key`` citation.

This module is intentionally stdlib-only (``copy`` is the sole import) so it is
safe to import on every server start — it must never pull in the heavy docs-KB
ML stack that lives in ``boomi_mcp.kb.service``.

Catalog size: **19 entries** = 3 ``emittable_today`` (naming) + 10 ``gated``
(folder/role) + 5 ``guidance_only`` + 1 ``na``. (The issue body's "~18" is
approximate; the enumerated capability split is 3+10+5+1 = 19.)
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Schema / vocabularies
# ---------------------------------------------------------------------------

#: Every catalog entry MUST carry these 11 fields (enforced by tests). The
#: ``governance_decision`` field replaces design_doctrine's ``boomi_shape_mapping``
#: — a governance decision (where/what-name/who) is not a runtime shape mapping.
ACCOUNT_GOVERNANCE_REQUIRED_FIELDS = (
    "name",
    "problem",
    "governance_decision",
    "when_to_use",
    "when_not_to_use",
    "verification_status",
    "capability_status",
    "category",
    "mutual_exclusion",
    "cross_refs",
    "provenance",
)

#: Whether the MCP's typed builders can honor the governance pattern today.
#: Name-encoding-on-create is ``emittable_today`` (the build_integration name
#: lint); folder/role/locking are ``gated`` or ``na``.
CAPABILITY_STATUSES = frozenset(
    {"emittable_today", "gated", "guidance_only", "na"}
)

#: #76 verification vocabulary, extended with ``course_unverified`` for
#: third-party-training-only claims (shared with design_doctrine #86).
VERIFICATION_STATUSES = frozenset(
    {
        "live_verified",
        "docs_corroborated",
        "companion_unverified",
        "course_unverified",
        "disputed",
    }
)

#: Provenance shares the verification vocabulary — it labels where a claim came
#: from. ``course_unverified`` marks the Boomi architect course (a labeled
#: third-party training hypothesis source, not official docs nor the Companion).
PROVENANCE_LABELS = VERIFICATION_STATUSES

#: Faceting category for the single served surface.
CATEGORIES = frozenset(
    {
        "naming",
        "folders",
        "roles",
        "copy_versioning",
        "locking",
        "process",
    }
)

#: JSON-schema-shaped description of one entry, returned alongside the catalog so
#: callers (and tests) share one schema source. ``gui_only_boundary`` and
#: ``docs_page_key`` are OPTIONAL (gated entries carry the former; the two
#: docs_corroborated entries carry the latter).
ACCOUNT_GOVERNANCE_ENTRY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "problem": {"type": "string"},
        "governance_decision": {
            "type": "string",
            "description": "The account/workspace governance decision — where a "
            "component goes, what it is named, or who may edit it. Conceptual; "
            "never GUI click-paths or folder-API mechanics.",
        },
        "when_to_use": {"type": "string"},
        "when_not_to_use": {"type": "string"},
        "verification_status": {"enum": sorted(VERIFICATION_STATUSES)},
        "capability_status": {"enum": sorted(CAPABILITY_STATUSES)},
        "category": {"enum": sorted(CATEGORIES)},
        "mutual_exclusion": {
            "type": "array",
            "items": {"type": "string"},
            "description": "First-class cross-entry tradeoffs the architect "
            "must weigh.",
        },
        "cross_refs": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Related entry names — a concept is defined once and "
            "referenced, not duplicated.",
        },
        "provenance": {"enum": sorted(PROVENANCE_LABELS)},
        "gui_only_boundary": {
            "type": "string",
            "description": "Required on every ``gated`` entry: what the agent "
            "PROPOSES vs what the user APPLIES in the GUI.",
        },
        "docs_page_key": {
            "type": "string",
            "description": "help.boomi.com citation for a docs_corroborated "
            "claim.",
        },
    },
    "required": list(ACCOUNT_GOVERNANCE_REQUIRED_FIELDS),
}


# ---------------------------------------------------------------------------
# The catalog — 19 entries. Defined as an ordered list; indexed by name below.
# =====================================================================
# 3 emittable_today (naming) + 10 gated (folder/role) + 5 guidance_only + 1 na.
# ---------------------------------------------------------------------------

_ENTRIES: List[Dict[str, Any]] = [
    # =====================================================================
    # emittable_today (3) — name-encoding on component create. Honored by the
    # build_integration plan-time name-governance lint (names only, not folders).
    # =====================================================================
    {
        "name": "descriptive_unique_component_names",
        "problem": (
            "Components left at Integration's default names (a new map, a new "
            "profile) or carrying copy-induced numeric suffixes are "
            "indistinguishable in the Component Explorer and break reuse — two "
            "default-named maps cannot be told apart, and a suffixed clone "
            "hides which component is canonical."
        ),
        "governance_decision": (
            "Every authored component carries a descriptive, account-unique "
            "display name — never an Integration default and never a "
            "copy-suffix artifact. The build_integration plan-time name lint "
            "enforces this on create: it rejects missing names, the platform "
            "default names, copy-induced numeric suffixes, and duplicate names "
            "across the spec; it never silently rewrites."
        ),
        "when_to_use": (
            "On every component create authored through build_integration or "
            "emitted by an archetype."
        ),
        "when_not_to_use": (
            "A reference-only reuse of an existing component inherits its "
            "established name — the lint skips it rather than renaming it."
        ),
        "verification_status": "course_unverified",
        "capability_status": "emittable_today",
        "category": "naming",
        "mutual_exclusion": [],
        "cross_refs": [
            "encode_metadata_in_component_names",
            "selective_step_display_naming",
            "naming_convention_governance_mandate",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "encode_metadata_in_component_names",
        "problem": (
            "A merely descriptive name still forces a reader to open a "
            "component to learn its reference identifier, direction, execution "
            "style, business object, and action; names that omit this metadata "
            "make an estate hard to scan and audit at a glance."
        ),
        "governance_decision": (
            "Encode the stable identifying metadata into the display name — a "
            "reference identifier, the source-to-target direction, the "
            "execution style, the business object, and the action — so the "
            "name is self-documenting. When a caller supplies a naming "
            "convention pattern on the spec, the plan lint checks each created "
            "name against it."
        ),
        "when_to_use": (
            "At scale, where an estate has many components and an at-a-glance "
            "scan must reveal each component's role without opening it."
        ),
        "when_not_to_use": (
            "A throwaway proof-of-concept where encoding rigor is not yet "
            "warranted — scale the rigor to the organization "
            "(agile_vs_formal_naming_tradeoff)."
        ),
        "verification_status": "course_unverified",
        "capability_status": "emittable_today",
        "category": "naming",
        "mutual_exclusion": [],
        "cross_refs": [
            "descriptive_unique_component_names",
            "naming_convention_governance_mandate",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "selective_step_display_naming",
        "problem": (
            "A process canvas where every step keeps its default label is "
            "unreadable, but labeling every step is noise; the steps whose "
            "underlying component is not shown on the canvas are the ones a "
            "reader cannot otherwise identify."
        ),
        "governance_decision": (
            "Label the steps whose underlying component is not visible on the "
            "canvas — property-setting, data-processing, and decision steps — "
            "and leave self-evident steps (map, start, stop) at their "
            "defaults, so the canvas reads as documentation."
        ),
        "when_to_use": (
            "Authoring any non-trivial process whose canvas mixes "
            "visible-component steps with opaque ones."
        ),
        "when_not_to_use": (
            "A trivial linear flow where every step's role is already obvious "
            "from its shape."
        ),
        "verification_status": "course_unverified",
        "capability_status": "emittable_today",
        "category": "naming",
        "mutual_exclusion": [],
        "cross_refs": ["descriptive_unique_component_names"],
        "provenance": "course_unverified",
    },
    # =====================================================================
    # gated (10) — folder placement, role/write-restriction gating. GUI-only:
    # the agent PROPOSES, the user APPLIES. Every entry carries gui_only_boundary.
    # =====================================================================
    {
        "name": "canonical_folder_taxonomy",
        "problem": (
            "An account whose components accrete in an ad-hoc folder layout "
            "becomes impossible to navigate, secure, or promote; where a "
            "component lives is a governance decision, not an afterthought."
        ),
        "governance_decision": (
            "Adopt a canonical top-level folder taxonomy — a shared common "
            "area, a development area, per-business-unit areas, a sandbox "
            "area, a support area, and a tools-and-utilities area — with a "
            "framework subtree for cross-cutting reusable services. The agent "
            "proposes the placement; the user creates and arranges the folders."
        ),
        "when_to_use": (
            "Standing up a new account, or rationalizing an existing estate "
            "that has grown without a layout."
        ),
        "when_not_to_use": (
            "A single-purpose throwaway account with a handful of components "
            "where a full taxonomy is overhead."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "create_component_in_final_folder",
            "common_folder_certs_connections_libs",
            "multi_org_single_account_folder_hierarchy",
            "folder_structure_review_checklist",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "The typed builders expose no folder-placement API; folder "
            "creation and arrangement are applied by the user in the Component "
            "Explorer. The agent proposes the taxonomy only."
        ),
    },
    {
        "name": "create_component_in_final_folder",
        "problem": (
            "Components built in a scratch location and relocated later confuse "
            "reviewers and risk being deployed from the wrong place."
        ),
        "governance_decision": (
            "Create each component directly in its final governed folder "
            "rather than building it elsewhere and moving it afterward. The "
            "agent proposes the destination folder; the user creates the "
            "component there."
        ),
        "when_to_use": (
            "Whenever a canonical folder taxonomy is in place and a new "
            "component has a clear governed home."
        ),
        "when_not_to_use": (
            "Exploratory work in a personal sandbox, which is graduated into "
            "the governed taxonomy by rebuilding (per_user_sandbox_for_poc)."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "folder_write_restrictions_custom_roles",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Folder placement is GUI-applied; the agent proposes the final "
            "folder, the user creates the component in it."
        ),
    },
    {
        "name": "folder_write_restrictions_custom_roles",
        "problem": (
            "Without write restrictions, any developer can edit any component, "
            "so a finance integration can be changed by a marketing developer "
            "and edits collide and overwrite one another."
        ),
        "governance_decision": (
            "Restrict write access per folder by assigning custom, "
            "domain-scoped developer roles — for example finance, HR, and "
            "marketing development roles — so only the owning team can modify a "
            "folder's components; a newly created folder inherits its parent's "
            "permissions. The agent proposes the role-to-folder mapping; an "
            "administrator applies it."
        ),
        "when_to_use": (
            "A multi-team account where folders must be owned by, and writable "
            "only by, the responsible domain team."
        ),
        "when_not_to_use": (
            "A small single-team account where per-folder role gating is "
            "ceremony without benefit."
        ),
        "verification_status": "docs_corroborated",
        "capability_status": "gated",
        "category": "roles",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "common_folder_certs_connections_libs",
            "create_component_in_final_folder",
        ],
        "provenance": "docs_corroborated",
        "gui_only_boundary": (
            "Role assignment and folder permissions are set by an "
            "administrator in the Folder Permissions dialog; the typed builders "
            "expose no role API. The agent proposes the mapping only."
        ),
        "docs_page_key": (
            "https://help.boomi.com/docs/Atomsphere/Integration/Process%20building/"
            "r-atm-Folder_Permissions_dialog_82322fd5-6b62-41c7-a906-621afb7e906a"
        ),
    },
    {
        "name": "common_folder_certs_connections_libs",
        "problem": (
            "Duplicating connections, certificates, and shared libraries "
            "across teams causes drift and multiplies licensing cost; these "
            "assets belong in one shared place."
        ),
        "governance_decision": (
            "Keep one-per-endpoint connections, certificates, and shared "
            "libraries in the common shared folder, write-restricted so most "
            "teams consume them read-only and a single owner maintains them. "
            "The agent proposes the shared assets; the user places and "
            "permissions them."
        ),
        "when_to_use": (
            "Any account where endpoints, certs, or libraries are reused across "
            "more than one integration or team."
        ),
        "when_not_to_use": (
            "A genuinely single-consumer asset that no other integration will "
            "ever reference."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "folder_write_restrictions_custom_roles",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Shared-asset placement and read-only permissioning are GUI-"
            "applied; the agent proposes the consolidation only."
        ),
    },
    {
        "name": "per_user_sandbox_for_poc",
        "problem": (
            "Proof-of-concept work built in shared folders pollutes the "
            "governed estate and risks accidental promotion."
        ),
        "governance_decision": (
            "Give each developer a personal sandbox folder for proof-of-concept "
            "work, kept out of the promotable taxonomy; graduate proven work "
            "into the governed folders by rebuilding it there, not by moving "
            "the scratch component. The agent proposes the sandbox boundary; "
            "the user creates it."
        ),
        "when_to_use": (
            "Any account with active experimentation that must be isolated from "
            "the promotable estate."
        ),
        "when_not_to_use": (
            "A locked-down account where no ad-hoc experimentation is "
            "permitted."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "avoid_test_copy_processes_use_versioning",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Sandbox folder creation and scoping are GUI-applied; the agent "
            "proposes the boundary only."
        ),
    },
    {
        "name": "support_folder_isolation_and_privacy",
        "problem": (
            "Customer-support reproductions often contain sensitive data and "
            "ad-hoc components that must not leak into the production estate or "
            "be visible to unauthorized staff."
        ),
        "governance_decision": (
            "Isolate support work in a dedicated support area with a per-case "
            "folder, scoped access, and sensitive steps scrubbed before the "
            "work is shared. The agent proposes the isolation boundary; the "
            "user applies the folder scoping and access."
        ),
        "when_to_use": (
            "Any account that reproduces customer issues or handles sensitive "
            "data during support."
        ),
        "when_not_to_use": (
            "An account with no support reproduction workflow."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "folder_write_restrictions_custom_roles",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Per-case folder isolation, scoped access, and step scrubbing are "
            "GUI-applied; the agent proposes the privacy boundary only."
        ),
    },
    {
        "name": "multi_org_single_account_folder_hierarchy",
        "problem": (
            "Several business units or locations sharing one account collide in "
            "a flat layout, with no clear ownership or blast-radius boundary."
        ),
        "governance_decision": (
            "Model a multi-org single account as a hierarchy — business unit, "
            "location, or domain at the top, then sub-unit, then use-case — so "
            "ownership and permissions follow the org structure. The agent "
            "proposes the hierarchy; the user builds it."
        ),
        "when_to_use": (
            "One account shared by multiple business units, locations, or "
            "domains that each own distinct integrations."
        ),
        "when_not_to_use": (
            "A single-org account where one taxonomy level already captures "
            "ownership."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "folder_write_restrictions_custom_roles",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "The org hierarchy of folders is GUI-applied; the agent proposes "
            "the structure only."
        ),
    },
    {
        "name": "tools_utilities_folder_non_integration",
        "problem": (
            "Utility and helper components that are not integrations themselves "
            "clutter the integration folders and confuse readers about what is "
            "deployable."
        ),
        "governance_decision": (
            "Keep non-integration tools and utilities in a dedicated "
            "tools-and-utilities folder, separate from the integration "
            "taxonomy, so the integration folders hold only deployable work. "
            "The agent proposes the separation; the user places the components."
        ),
        "when_to_use": (
            "Any account that accumulates helper or one-off utility components "
            "alongside real integrations."
        ),
        "when_not_to_use": (
            "An account with no non-integration helper components."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": ["canonical_folder_taxonomy"],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Tools-and-utilities folder placement is GUI-applied; the agent "
            "proposes the separation only."
        ),
    },
    {
        "name": "tracked_field_naming_generic_and_unique",
        "problem": (
            "Tracked fields are account-level, so a field named for one "
            "integration's local concept collides with another's and becomes "
            "ambiguous across the whole account."
        ),
        "governance_decision": (
            "Name account-level tracked fields by the generic business concept "
            "they capture, kept unique across the account, never by a single "
            "integration's local label. The agent proposes the tracked-field "
            "names; the user defines them in account-level configuration."
        ),
        "when_to_use": (
            "Defining tracked fields that more than one integration will read "
            "through Process Reporting."
        ),
        "when_not_to_use": (
            "There is no exception — account-level scope makes generic, unique "
            "naming mandatory whenever a tracked field is defined."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "naming",
        "mutual_exclusion": [],
        "cross_refs": [
            "descriptive_unique_component_names",
            "naming_convention_governance_mandate",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Tracked-field definitions are account-level GUI settings; the "
            "typed builders do not create them. The agent proposes the names "
            "only."
        ),
    },
    {
        "name": "folder_structure_review_checklist",
        "problem": (
            "A folder taxonomy that is never reviewed drifts back into ad-hoc "
            "sprawl as teams add components under deadline."
        ),
        "governance_decision": (
            "Periodically review the folder structure against a checklist — "
            "taxonomy adherence, final-folder placement, write-restriction "
            "coverage, shared-asset consolidation, and sandbox/support "
            "isolation — and correct drift. The agent can produce the "
            "checklist and flag drift; the user reorganizes."
        ),
        "when_to_use": (
            "On a recurring cadence for any account large enough for its "
            "taxonomy to drift."
        ),
        "when_not_to_use": (
            "A tiny account whose entire layout is visible at once and needs no "
            "formal review."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "folders",
        "mutual_exclusion": [],
        "cross_refs": [
            "canonical_folder_taxonomy",
            "create_component_in_final_folder",
        ],
        "provenance": "course_unverified",
        "gui_only_boundary": (
            "Folder reorganization is GUI-applied; the agent proposes the "
            "checklist and flags drift only."
        ),
    },
    # =====================================================================
    # guidance_only (5) — organizational policy / discipline. No emit, no GUI
    # gate the agent applies; advice the architect weighs.
    # =====================================================================
    {
        "name": "naming_convention_governance_mandate",
        "problem": (
            "Without an account-wide naming convention, every developer names "
            "components differently and the estate becomes unsearchable; a "
            "convention only helps if it is mandated and enforced."
        ),
        "governance_decision": (
            "Establish and mandate a single account-wide naming convention, "
            "documented and enforced through review, so names are predictable "
            "and searchable. This is organizational policy the agent "
            "recommends; the build_integration lint enforces only the "
            "mechanical floor (no defaults, no copy-suffixes, uniqueness, and "
            "an optional supplied pattern)."
        ),
        "when_to_use": (
            "Any account expected to grow beyond a handful of components or "
            "involve more than one author."
        ),
        "when_not_to_use": (
            "A tiny short-lived account may scale the rigor down rather than "
            "mandate a full convention (agile_vs_formal_naming_tradeoff)."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "naming",
        "mutual_exclusion": [],
        "cross_refs": [
            "descriptive_unique_component_names",
            "encode_metadata_in_component_names",
            "agile_vs_formal_naming_tradeoff",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "agile_vs_formal_naming_tradeoff",
        "problem": (
            "A heavyweight naming convention slows a small team, while no "
            "convention fails a large enterprise; the right rigor depends on "
            "the organization's size and pace."
        ),
        "governance_decision": (
            "Scale naming rigor to the organization — a lightweight agile "
            "convention for a small fast-moving team, a formal enterprise "
            "convention for a large multi-team account — as a deliberate "
            "tradeoff, not a default."
        ),
        "when_to_use": (
            "When setting the naming-convention policy and deciding how much "
            "rigor the team can sustain."
        ),
        "when_not_to_use": (
            "Once the policy is set, individual authors follow it rather than "
            "re-litigating the tradeoff per component."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "naming",
        "mutual_exclusion": [
            "A lightweight agile naming scheme optimizes for speed on a small "
            "team; a formal enterprise convention optimizes for consistency at "
            "scale — a given account picks one, not both."
        ],
        "cross_refs": ["naming_convention_governance_mandate"],
        "provenance": "course_unverified",
    },
    {
        "name": "avoid_test_copy_processes_use_versioning",
        "problem": (
            "Copying a process to make a test or experimental variant loses the "
            "original's revision history and leaves orphaned near-duplicates "
            "that drift apart."
        ),
        "governance_decision": (
            "Edit and promote a single component through its revision history "
            "rather than cloning test or experimental copies; copies lose "
            "lineage and multiply maintenance. Try changes via revisions and "
            "environment promotion, not copies."
        ),
        "when_to_use": (
            "Whenever a change must be tried or staged on an existing process."
        ),
        "when_not_to_use": (
            "A genuinely new variant that must diverge permanently is a new "
            "component, not a throwaway copy — and then copy depth matters "
            "(shallow_vs_deep_copy_dependents)."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "copy_versioning",
        "mutual_exclusion": [],
        "cross_refs": [
            "shallow_vs_deep_copy_dependents",
            "per_user_sandbox_for_poc",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "shallow_vs_deep_copy_dependents",
        "problem": (
            "When a copy is genuinely needed, copying without understanding "
            "dependent handling either shares dependents — so edits leak back "
            "to the original — or clones them — independent but duplicative; "
            "the wrong choice corrupts the original or bloats the estate."
        ),
        "governance_decision": (
            "Choose copy depth deliberately: a shallow copy shares the "
            "original's dependent components, a deep copy clones them; decide "
            "based on whether the copy must evolve independently of the "
            "original."
        ),
        "when_to_use": (
            "When a permanent divergent variant is being created and its "
            "dependents must be reasoned about explicitly."
        ),
        "when_not_to_use": (
            "When the goal is merely to try a change — use revisions instead of "
            "any copy (avoid_test_copy_processes_use_versioning)."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "copy_versioning",
        "mutual_exclusion": [
            "A shallow copy shares dependents (changes propagate back to the "
            "original); a deep copy clones them (independent but duplicative) — "
            "the two are mutually exclusive for a given copy."
        ],
        "cross_refs": ["avoid_test_copy_processes_use_versioning"],
        "provenance": "course_unverified",
    },
    {
        "name": "pattern_templates_speed_new_projects",
        "problem": (
            "Starting each new project from a blank canvas repeats setup and "
            "loses consistency across the estate."
        ),
        "governance_decision": (
            "Maintain a small library of project pattern templates organized by "
            "invocation style — scheduled, asynchronous, synchronous, and "
            "fire-and-forget — so a new project starts from a governed skeleton "
            "rather than a blank canvas."
        ),
        "when_to_use": (
            "An account that builds many projects falling into a few "
            "recurring invocation shapes."
        ),
        "when_not_to_use": (
            "A one-off integration that no future project will resemble."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "process",
        "mutual_exclusion": [],
        "cross_refs": ["canonical_folder_taxonomy"],
        "provenance": "course_unverified",
    },
    # =====================================================================
    # na (1) — a concept the builder cannot emit; an account-level safeguard.
    # =====================================================================
    {
        "name": "component_locking_prevents_overwrite",
        "problem": (
            "When component locking is off, two users editing the same "
            "component independently overwrite each other — the last to save "
            "wins and the other user's changes are silently lost."
        ),
        "governance_decision": (
            "Treat component locking as the account-level safeguard against "
            "concurrent-edit data loss: with it enabled, a component opens "
            "read-only until a user explicitly locks it for editing, so "
            "concurrent edits are serialized. It is off by default and is an "
            "account setting an administrator toggles — nothing an integration "
            "build emits, hence not emittable."
        ),
        "when_to_use": (
            "Any account with multiple authors who could open the same "
            "component at once."
        ),
        "when_not_to_use": (
            "A single-author account where concurrent edits cannot occur, "
            "though enabling it is still harmless insurance."
        ),
        "verification_status": "docs_corroborated",
        "capability_status": "na",
        "category": "locking",
        "mutual_exclusion": [],
        "cross_refs": [
            "avoid_test_copy_processes_use_versioning",
            "folder_write_restrictions_custom_roles",
        ],
        "provenance": "docs_corroborated",
        "docs_page_key": (
            "https://help.boomi.com/docs/Atomsphere/Integration/Process%20building/"
            "c-atm-Component_locking_9c951ff5-186e-46eb-908e-bf32b55e87b2"
        ),
    },
]


# Index by name (insertion order preserved). Built at import so lookups are O(1)
# and a duplicate name fails loudly at import rather than silently shadowing.
def _build_index(entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        name = entry["name"]
        if name in index:
            raise ValueError(f"Duplicate account_governance entry name: {name!r}")
        index[name] = entry
    return index


ACCOUNT_GOVERNANCE_ENTRIES: Dict[str, Dict[str, Any]] = _build_index(_ENTRIES)

#: Stable published catalog size — see module docstring for the derivation.
ACCOUNT_GOVERNANCE_ENTRY_COUNT = len(ACCOUNT_GOVERNANCE_ENTRIES)


# ---------------------------------------------------------------------------
# Public accessors — every accessor returns a deepcopy so per-call mutation by
# a caller never corrupts the shared module state (same discipline as
# design_doctrine / meta_tools._authoring_workflow_sequences()).
# ---------------------------------------------------------------------------


def get_account_governance_catalog() -> Dict[str, Any]:
    """Full catalog payload: all entries, the count, and the entry schema."""
    return {
        "entry_count": ACCOUNT_GOVERNANCE_ENTRY_COUNT,
        "entries": copy.deepcopy(list(ACCOUNT_GOVERNANCE_ENTRIES.values())),
        "entry_schema": copy.deepcopy(ACCOUNT_GOVERNANCE_ENTRY_SCHEMA),
    }


def get_governance_pattern(name: str) -> Optional[Dict[str, Any]]:
    """One entry by name, or ``None`` if unknown."""
    entry = ACCOUNT_GOVERNANCE_ENTRIES.get(name)
    return copy.deepcopy(entry) if entry is not None else None


def list_account_governance_index() -> List[Dict[str, str]]:
    """Compact index rows for ``list_capabilities`` — no prose, just the
    name / category / capability_status triple per entry."""
    return [
        {
            "name": entry["name"],
            "category": entry["category"],
            "capability_status": entry["capability_status"],
        }
        for entry in ACCOUNT_GOVERNANCE_ENTRIES.values()
    ]


def valid_governance_pattern_names() -> List[str]:
    """Sorted entry names — used by ``_valid_schema_names`` and the
    unknown-pattern error envelope."""
    return sorted(ACCOUNT_GOVERNANCE_ENTRIES)
