"""Tests for the served ``account_governance`` knowledge surface (issue #93).

Pure-unit against boomi_mcp — no server import, no SDK calls. Covers the
catalog/entry schema, the capability split, the get_schema_template dispatch +
error envelope, the list_capabilities compact index, the workflow routing, the
anti-template lint over served prose, the docs-corroborated citations, and the
build_integration plan-time name-governance lint (names only; folder/role
governance stays GUI-only and is never linted).
"""

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
    list_capabilities_action,
)
from boomi_mcp.errors import SCHEMA_NAME_UNSUPPORTED
from boomi_mcp.kb.account_governance import (
    ACCOUNT_GOVERNANCE_ENTRIES,
    ACCOUNT_GOVERNANCE_ENTRY_COUNT,
    ACCOUNT_GOVERNANCE_REQUIRED_FIELDS,
    CAPABILITY_STATUSES,
    CATEGORIES,
    PROVENANCE_LABELS,
    VERIFICATION_STATUSES,
    get_account_governance_catalog,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec
from boomi_mcp.categories.integration_builder import (
    _build_plan,
    _apply_plan,
    _lint_component_names,
)


# Authoritative capability-split partition (issue #93 body).
EMITTABLE_3 = {
    "descriptive_unique_component_names",
    "encode_metadata_in_component_names",
    "selective_step_display_naming",
}
GATED_10 = {
    "canonical_folder_taxonomy",
    "create_component_in_final_folder",
    "folder_write_restrictions_custom_roles",
    "common_folder_certs_connections_libs",
    "per_user_sandbox_for_poc",
    "support_folder_isolation_and_privacy",
    "multi_org_single_account_folder_hierarchy",
    "tools_utilities_folder_non_integration",
    "tracked_field_naming_generic_and_unique",
    "folder_structure_review_checklist",
}
GUIDANCE_5 = {
    "naming_convention_governance_mandate",
    "agile_vs_formal_naming_tradeoff",
    "avoid_test_copy_processes_use_versioning",
    "shallow_vs_deep_copy_dependents",
    "pattern_templates_speed_new_projects",
}
NA_1 = {"component_locking_prevents_overwrite"}


_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_RAW_XML_RE = re.compile(r"<\?xml|</?[a-zA-Z][\w:-]*>")
_SQL_CRUD_RE = re.compile(
    r"(?i)\b(select\s+.+\s+from|insert\s+into|update\s+\w+\s+set|delete\s+from)\b"
)
# Classic canned-template artifacts that must never leak into governance prose.
_CANNED_RE = re.compile(r"(?i)\b(lorem ipsum|acme|foo\s?bar|john doe)\b")


def _flatten_strings(node):
    """Yield every string anywhere in a nested dict/list structure.

    Skips ``docs_page_key`` values: those are help.boomi.com citation URLs that
    legitimately contain a UUID slug, so they are not governance prose and must
    not trip the anti-template UUID guard.
    """
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            if key == "docs_page_key":
                continue
            yield from _flatten_strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _flatten_strings(value)


def _served_prose_blob():
    return "\n".join(_flatten_strings(get_account_governance_catalog()))


# ---------------------------------------------------------------------------
# Catalog retrieval + entry schema
# ---------------------------------------------------------------------------


def test_catalog_returns_19_entries_with_safety_flags():
    result = get_schema_template_action(schema_name="account_governance")
    assert result["_success"] is True
    assert result["schema_name"] == "account_governance"
    assert result["entry_count"] == 19
    assert len(result["entries"]) == 19
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    assert result["read_only"] is True
    assert "entry_schema" in result
    # #93 keeps the surface to the 19-entry catalog — no design_doctrine
    # corroboration_backlog key here.
    assert "corroboration_backlog" not in result


def test_entry_count_partition_3_10_5_1():
    names = set(ACCOUNT_GOVERNANCE_ENTRIES)
    assert ACCOUNT_GOVERNANCE_ENTRY_COUNT == 19
    assert len(names) == 19
    assert EMITTABLE_3 <= names
    assert GATED_10 <= names
    assert GUIDANCE_5 <= names
    assert NA_1 <= names
    # The four groups are disjoint and together account for all 19.
    assert EMITTABLE_3 | GATED_10 | GUIDANCE_5 | NA_1 == names
    assert not (EMITTABLE_3 & GATED_10)
    assert not (GATED_10 & GUIDANCE_5)
    assert not (GUIDANCE_5 & NA_1)
    assert not (EMITTABLE_3 & NA_1)


def test_capability_split_matches_acceptance_criteria():
    by_cap = {}
    for entry in ACCOUNT_GOVERNANCE_ENTRIES.values():
        by_cap.setdefault(entry["capability_status"], set()).add(entry["name"])
    assert by_cap["emittable_today"] == EMITTABLE_3
    assert by_cap["gated"] == GATED_10
    assert by_cap["guidance_only"] == GUIDANCE_5
    assert by_cap["na"] == NA_1


def test_every_entry_has_required_fields_and_valid_vocabularies():
    for name, entry in ACCOUNT_GOVERNANCE_ENTRIES.items():
        for field in ACCOUNT_GOVERNANCE_REQUIRED_FIELDS:
            assert field in entry, f"{name} missing field {field!r}"
        assert entry["name"] == name
        for prose in ("problem", "governance_decision", "when_to_use", "when_not_to_use"):
            assert isinstance(entry[prose], str) and entry[prose].strip(), (
                f"{name}.{prose} must be non-empty prose"
            )
        assert entry["capability_status"] in CAPABILITY_STATUSES, name
        assert entry["verification_status"] in VERIFICATION_STATUSES, name
        assert entry["provenance"] in PROVENANCE_LABELS, name
        assert entry["category"] in CATEGORIES, name
        assert isinstance(entry["mutual_exclusion"], list), name
        assert isinstance(entry["cross_refs"], list), name


def test_cross_refs_resolve_to_real_entries():
    for name, entry in ACCOUNT_GOVERNANCE_ENTRIES.items():
        for ref in entry["cross_refs"]:
            assert ref in ACCOUNT_GOVERNANCE_ENTRIES, (
                f"{name} cross_refs unknown entry {ref!r}"
            )
            assert ref != name, f"{name} cross-references itself"


def test_gated_entries_have_gui_only_boundary():
    for name, entry in ACCOUNT_GOVERNANCE_ENTRIES.items():
        if entry["capability_status"] == "gated":
            assert isinstance(entry.get("gui_only_boundary"), str), name
            assert entry["gui_only_boundary"].strip(), (
                f"{name} (gated) must carry a non-empty gui_only_boundary"
            )
        else:
            # Non-gated entries do not need the boundary (only gated entries
            # have a GUI-apply handoff to describe).
            assert entry["capability_status"] != "gated" or "gui_only_boundary" in entry


def test_component_locking_is_na():
    entry = ACCOUNT_GOVERNANCE_ENTRIES["component_locking_prevents_overwrite"]
    assert entry["capability_status"] == "na"
    assert entry["category"] == "locking"


def test_docs_corroborated_entries_cite_a_help_boomi_page():
    corroborated = [
        e for e in ACCOUNT_GOVERNANCE_ENTRIES.values()
        if e["verification_status"] == "docs_corroborated"
    ]
    # The two genuinely platform-behavioral claims (component locking, folder
    # write-restrictions via roles) are corroborated and cite a docs page.
    names = {e["name"] for e in corroborated}
    assert names == {
        "component_locking_prevents_overwrite",
        "folder_write_restrictions_custom_roles",
    }
    for entry in corroborated:
        assert entry["provenance"] == "docs_corroborated", entry["name"]
        assert entry.get("docs_page_key", "").startswith("https://help.boomi.com"), (
            entry["name"]
        )


def test_course_unverified_is_the_default_provenance():
    # The governance opinions (taxonomy, naming conventions, copy discipline)
    # are course-derived and honestly labeled course_unverified.
    course = [
        e for e in ACCOUNT_GOVERNANCE_ENTRIES.values()
        if e["provenance"] == "course_unverified"
    ]
    assert len(course) == 17  # 19 total − 2 docs_corroborated
    for entry in course:
        assert entry["verification_status"] == "course_unverified", entry["name"]


# ---------------------------------------------------------------------------
# Single-entry lookup + error envelope
# ---------------------------------------------------------------------------


def test_single_entry_lookup():
    result = get_schema_template_action(
        schema_name="governance_pattern:canonical_folder_taxonomy"
    )
    assert result["_success"] is True
    assert result["read_only"] is True
    assert result["governance_pattern"]["name"] == "canonical_folder_taxonomy"
    assert result["governance_pattern"]["capability_status"] == "gated"


def test_unknown_governance_pattern_returns_schema_name_unsupported():
    result = get_schema_template_action(schema_name="governance_pattern:__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_NAME_UNSUPPORTED
    assert "valid_governance_patterns" in result
    assert "canonical_folder_taxonomy" in result["valid_governance_patterns"]
    # The error envelope must NOT dump the full catalog.
    assert "entries" not in result


def test_valid_schema_names_includes_account_governance_and_patterns():
    names = _valid_schema_names()
    assert "account_governance" in names
    pattern_names = [n for n in names if n.startswith("governance_pattern:")]
    assert len(pattern_names) == 19
    assert "governance_pattern:component_locking_prevents_overwrite" in names


# ---------------------------------------------------------------------------
# Abstraction filter: anti-template lint over served prose
# ---------------------------------------------------------------------------


def test_anti_template_no_raw_artifacts():
    blob = _served_prose_blob()
    assert not _RAW_XML_RE.search(blob), "raw XML must not appear in governance prose"
    assert "```" not in blob, "code fences must not appear in governance prose"
    assert "<<" not in blob and ">>" not in blob, "placeholder markers must not appear"
    assert not _UUID_RE.search(blob), "component UUIDs must not appear in governance prose"
    sql = _SQL_CRUD_RE.search(blob)
    assert sql is None, f"reusable SQL must not appear in governance prose: {sql!r}"
    canned = _CANNED_RE.search(blob)
    assert canned is None, f"canned template examples must not appear: {canned!r}"


# ---------------------------------------------------------------------------
# list_capabilities compact index
# ---------------------------------------------------------------------------


def test_list_capabilities_compact_index():
    catalog = list_capabilities_action()
    assert "account_governance" in catalog
    ag = catalog["account_governance"]
    assert ag["entry_count"] == 19
    assert "get_schema_template" in ag["surface"]
    assert "governance_pattern:<name>" in ag["pattern_surface"]
    assert len(ag["index"]) == 19
    # Index rows are compact — name/category/capability_status only, no prose.
    for row in ag["index"]:
        assert set(row.keys()) == {"name", "category", "capability_status"}


def test_account_governance_index_survives_available_tools_filtering():
    catalog = list_capabilities_action(available_tools={"build_integration"})
    assert "account_governance" in catalog
    assert catalog["account_governance"]["entry_count"] == 19


def test_workflow_routes_through_account_governance_with_design_doctrine_before_archetypes():
    wf = list_capabilities_action()["workflows"]["build_integration_from_description"]
    steps = wf["steps"]
    joined = "\n".join(steps)
    assert "account_governance" in joined, "workflow must reference account_governance"
    assert "design_doctrine" in joined, "workflow must still reference design_doctrine"

    def _first_index(needle):
        return next(i for i, s in enumerate(steps) if needle in s)

    # Both doctrine consults live in the same pre-archetype step and precede
    # archetype discovery (design_doctrine's ordering invariant is preserved).
    dd_idx = _first_index("design_doctrine")
    ag_idx = _first_index("account_governance")
    arch_idx = _first_index("list_integration_archetypes(")
    assert dd_idx < arch_idx
    assert ag_idx < arch_idx
    assert _first_index("list_boomi_profiles(") < ag_idx


# ---------------------------------------------------------------------------
# build_integration plan-time name-governance lint (names only)
# ---------------------------------------------------------------------------

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"


def _dc(key, name, **config_extra):
    """A documentcache create component — reaches planned_action='create'
    cleanly (no connector/profile body validation), so the name lint is the
    only gate exercised."""
    config = {"name": name, **config_extra}
    return IntegrationComponentSpec(
        key=key, type="documentcache", action="create", name=name, config=config
    )


def _plan_cfg(comps, naming=None):
    spec = {
        "version": "1.0",
        "name": "demo",
        "components": [c.model_dump() for c in comps],
    }
    if naming is not None:
        spec["naming"] = naming
    return {"conflict_policy": "reuse", "integration_spec": spec}


def _step(plan, key):
    return next(s for s in plan["steps"] if s["key"] == key)


@patch(_PAGINATE, return_value=[])
def test_build_plan_rejects_missing_component_name(_mock_pag):
    comp = IntegrationComponentSpec(
        key="c", type="documentcache", action="create", name=None, config={}
    )
    plan = _build_plan(MagicMock(), _plan_cfg([comp]))
    step = _step(plan, "c")
    assert step["planned_action"] == "error_name_governance"
    assert step["validation_error"]["error_code"] == "COMPONENT_NAME_REQUIRED"


@patch(_PAGINATE, return_value=[])
def test_build_plan_rejects_boomi_default_component_name(_mock_pag):
    plan = _build_plan(MagicMock(), _plan_cfg([_dc("c", "New Map")]))
    step = _step(plan, "c")
    assert step["planned_action"] == "error_name_governance"
    assert step["validation_error"]["error_code"] == "COMPONENT_NAME_BOOMI_DEFAULT"


@patch(_PAGINATE, return_value=[])
def test_build_plan_rejects_default_name_in_config_component_name(_mock_pag):
    # Codex P2: a descriptive top-level name must not mask a default carried in
    # config.component_name (the field profile/map/trading-partner builders
    # actually emit). The lint checks ALL candidate name fields.
    comp = IntegrationComponentSpec(
        key="c", type="documentcache", action="create", name="Good Cache",
        config={"component_name": "New Map"},
    )
    plan = _build_plan(MagicMock(), _plan_cfg([comp]))
    step = _step(plan, "c")
    assert step["planned_action"] == "error_name_governance"
    assert step["validation_error"]["error_code"] == "COMPONENT_NAME_BOOMI_DEFAULT"
    assert "New Map" in step["validation_error"]["error"]


def test_lint_skips_raw_xml_create():
    # Codex P2: a raw-XML create carries its name inside author-controlled XML
    # the lint does not parse — it must NOT be rejected as missing.
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    comp = IntegrationComponentSpec(
        key="c", type="documentcache", action="create", name=None,
        config={"xml": "<DocumentCache name='Real Cache Name'/>"},
    )
    spec = IntegrationSpecV1(**_plan_cfg([comp])["integration_spec"])
    out = _lint_component_names(spec)
    assert out["errors"] == {}


def test_lint_process_ignores_unused_component_name_for_duplicate():
    # Codex round 2 P2: a process emits config.name, NOT config.component_name.
    # Two processes with the same config.name but distinct component_name emit
    # duplicate Boomi names — the lint must catch that (component_name must not
    # mask the emitted name).
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    p1 = IntegrationComponentSpec(
        key="p1", type="process", action="create", name=None,
        config={"name": "Dup Process", "component_name": "A",
                "process_kind": "database_to_api_sync"},
    )
    p2 = IntegrationComponentSpec(
        key="p2", type="process", action="create", name=None,
        config={"name": "Dup Process", "component_name": "B",
                "process_kind": "database_to_api_sync"},
    )
    spec = IntegrationSpecV1(**_plan_cfg([p1, p2])["integration_spec"])
    out = _lint_component_names(spec)
    assert "p1" not in out["errors"]
    assert out["errors"]["p2"]["error_code"] == "COMPONENT_NAME_NOT_UNIQUE"


def test_lint_process_component_name_default_not_flagged():
    # The flip side: a process whose emitted name (config.name) is descriptive
    # must NOT be flagged just because an UNUSED config.component_name happens to
    # be a Boomi default — the process never emits component_name.
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    p = IntegrationComponentSpec(
        key="p", type="process", action="create", name=None,
        config={"name": "Good Process", "component_name": "New Map",
                "process_kind": "database_to_api_sync"},
    )
    spec = IntegrationSpecV1(**_plan_cfg([p])["integration_spec"])
    out = _lint_component_names(spec)
    assert out["errors"] == {}


def test_lint_connector_emits_component_name_not_config_name():
    # Codex round 3 P2: connector creates REQUIRE and emit config.component_name
    # (create_connector), not config.name. A default in component_name must be
    # caught even when config.name is descriptive.
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    c = IntegrationComponentSpec(
        key="c", type="connector-settings", action="create", name="Good",
        config={"name": "Good", "component_name": "New Map",
                "connector_type": "database"},
    )
    spec = IntegrationSpecV1(**_plan_cfg([c])["integration_spec"])
    out = _lint_component_names(spec)
    assert out["errors"]["c"]["error_code"] == "COMPONENT_NAME_BOOMI_DEFAULT"


def test_lint_connector_duplicate_on_component_name_not_config_name():
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    # Same component_name (the emitted field), distinct config.name → real
    # duplicate, must be caught.
    dup = [
        IntegrationComponentSpec(
            key="a", type="connector-settings", action="create", name="A name",
            config={"name": "A name", "component_name": "Shared Conn",
                    "connector_type": "database"},
        ),
        IntegrationComponentSpec(
            key="b", type="connector-settings", action="create", name="B name",
            config={"name": "B name", "component_name": "Shared Conn",
                    "connector_type": "database"},
        ),
    ]
    out = _lint_component_names(
        IntegrationSpecV1(**_plan_cfg(dup)["integration_spec"])
    )
    assert "a" not in out["errors"]
    assert out["errors"]["b"]["error_code"] == "COMPONENT_NAME_NOT_UNIQUE"

    # Distinct component_name (the emitted field), same config.name → NOT a
    # duplicate (config.name is ignored for connectors).
    distinct = [
        IntegrationComponentSpec(
            key="a", type="connector-settings", action="create", name="Same",
            config={"name": "Same", "component_name": "Conn A",
                    "connector_type": "database"},
        ),
        IntegrationComponentSpec(
            key="b", type="connector-settings", action="create", name="Same",
            config={"name": "Same", "component_name": "Conn B",
                    "connector_type": "database"},
        ),
    ]
    out2 = _lint_component_names(
        IntegrationSpecV1(**_plan_cfg(distinct)["integration_spec"])
    )
    assert out2["errors"] == {}


def test_lint_generated_types_emit_component_name_not_config_name():
    # Codex round 4 P2: builder-dispatched generated types (profile.db,
    # transform.map, script.mapping, transform.function, trading_partner) emit
    # config.component_name (with comp.name setdefaulted in) and IGNORE
    # config.name. The lint must mirror the emitted field.
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    for ctype in ("profile.db", "transform.map", "script.mapping",
                  "transform.function", "trading_partner"):
        # comp.name (the emitted fallback) is a default; config.name is
        # descriptive but ignored by the builder → MUST be flagged.
        bad = IntegrationComponentSpec(
            key="x", type=ctype, action="create", name="New Map",
            config={"name": "Good Descriptive Name"},
        )
        out = _lint_component_names(
            IntegrationSpecV1(**_plan_cfg([bad])["integration_spec"])
        )
        assert out["errors"].get("x", {}).get("error_code") == "COMPONENT_NAME_BOOMI_DEFAULT", ctype

        # Inverse: descriptive emitted name (comp.name), a default sitting only
        # in the ignored config.name → MUST NOT be flagged.
        ok = IntegrationComponentSpec(
            key="y", type=ctype, action="create", name="Good Descriptive Name",
            config={"name": "New Map"},
        )
        out2 = _lint_component_names(
            IntegrationSpecV1(**_plan_cfg([ok])["integration_spec"])
        )
        assert out2["errors"] == {}, ctype


@patch(_PAGINATE, return_value=[])
def test_build_plan_rejects_duplicate_component_names(_mock_pag):
    # Case-insensitive / trimmed duplicate across the create set: second flagged.
    plan = _build_plan(
        MagicMock(), _plan_cfg([_dc("a", "Shared Cache"), _dc("b", "shared cache")])
    )
    assert _step(plan, "a")["planned_action"] == "create"
    step_b = _step(plan, "b")
    assert step_b["planned_action"] == "error_name_governance"
    assert step_b["validation_error"]["error_code"] == "COMPONENT_NAME_NOT_UNIQUE"


@patch(_PAGINATE, return_value=[])
def test_build_plan_checks_supplied_component_name_pattern(_mock_pag):
    # Opt-in pattern: a non-matching name is a hard error.
    plan = _build_plan(
        MagicMock(),
        _plan_cfg([_dc("c", "lowercase name")], naming={"component_name_pattern": r"^[A-Z]"}),
    )
    step = _step(plan, "c")
    assert step["planned_action"] == "error_name_governance"
    assert step["validation_error"]["error_code"] == "COMPONENT_NAME_PATTERN_MISMATCH"


@patch(_PAGINATE, return_value=[])
def test_build_plan_invalid_pattern_warns_does_not_error(_mock_pag):
    plan = _build_plan(
        MagicMock(),
        _plan_cfg([_dc("c", "Good Name")], naming={"component_name_pattern": "["}),
    )
    step = _step(plan, "c")
    assert step["planned_action"] == "create"  # not blocked
    joined = "\n".join(plan["warnings"] or [])
    assert "component_name_pattern is not a valid" in joined


@patch(_PAGINATE, return_value=[])
def test_build_plan_copy_suffix_is_warning_not_error(_mock_pag):
    # The copy-suffix signal is ambiguous (it cannot be told apart from
    # legitimate enumeration like an emitted "Map Script 1"), so it is a soft
    # flag, never a hard gate — the archetype-first path must not be blocked.
    plan = _build_plan(MagicMock(), _plan_cfg([_dc("c", "Staging Cache 1")]))
    step = _step(plan, "c")
    assert step["planned_action"] == "create"
    joined = "\n".join(plan["warnings"] or [])
    assert "resembles a copy artifact" in joined


@patch(_PAGINATE, return_value=[])
def test_build_plan_allows_clean_descriptive_name(_mock_pag):
    plan = _build_plan(MagicMock(), _plan_cfg([_dc("c", "Order Staging Cache")]))
    step = _step(plan, "c")
    assert step["planned_action"] == "create"
    assert step.get("validation_error") is None


@patch(_PAGINATE, return_value=[])
def test_build_plan_skips_reference_only_and_update(_mock_pag):
    # A reference_only create resolves on the reuse path (not name-governance),
    # and an update component is out of the lint's create-only scope.
    ref = IntegrationComponentSpec(
        key="r", type="documentcache", action="create", name=None,
        config={"reference_only": True, "component_id": "00000000-0000-0000-0000-stub00000001"},
    )
    upd = IntegrationComponentSpec(
        key="u", type="documentcache", action="update",
        component_id="00000000-0000-0000-0000-stub00000002", name="New Map",
        config={"name": "New Map"},
    )
    out = _lint_component_names(
        __import__("boomi_mcp.models.integration_models", fromlist=["IntegrationSpecV1"])
        .IntegrationSpecV1(**_plan_cfg([ref, upd])["integration_spec"])
    )
    # Neither create-scoped: no name-governance errors raised.
    assert out["errors"] == {}


@patch(_PAGINATE, return_value=[])
def test_apply_rejects_name_governance_plan_before_execute(_mock_pag):
    result = _apply_plan(
        MagicMock(), "p", {**_plan_cfg([_dc("c", "New Profile")]), "dry_run": False}
    )
    assert result["_success"] is False
    actions = [u["planned_action"] for u in result.get("unresolvable_steps", [])]
    assert "error_name_governance" in actions
    joined = "\n".join(result.get("details", []))
    assert "name governance" in joined
    assert "COMPONENT_NAME_BOOMI_DEFAULT" in joined


@patch(_PAGINATE, return_value=[])
def test_build_plan_allows_database_to_api_archetype_names(_mock_pag):
    # Acceptance criterion: archetype-emitted names pass the lint without caller
    # effort. Build a real database_to_api_sync spec via the archetype's own
    # emit_spec (the same emit logic build_from_archetype uses) and route it
    # through the plan; no create step may be converted to error_name_governance.
    # We call emit_spec directly rather than build_from_archetype_action to keep
    # this test independent of package-wide PatternRegistry discovery (which is
    # fragile under a stale editable-install .pth) — the emitted names are
    # identical either way.
    from boomi_mcp.patterns.archetypes.database_to_api_sync import (
        DatabaseToApiSyncArchetype,
        DatabaseToApiSyncParameters,
    )

    # Smallest executable database_to_api_sync payload (mirrors the e2e suite's
    # _minimal()): create DB + create REST + a single direct transform.
    payload = {
        "naming": {"integration_name": "demo-sync", "component_prefix": "DEMO"},
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "driver": "microsoft_jdbc",
                    "auth_mode": "username_password",
                    "host": "db.internal",
                    "database": "AppDB",
                    "username": "svc_sync",
                    "credential_ref": "secrets/db/svc_sync",
                },
            },
            "read_operation": {
                "sql": "<<user-authored DB read statement>>",
                "result_schema": {
                    "fields": [{"name": "source_a", "data_type": "character"}]
                },
            },
        },
        "target": {
            "binding": {
                "mode": "create",
                "settings": {"base_url": "https://api.example.com", "auth_mode": "none"},
            },
            "send_request": {"method": "POST", "path": "/v1/items"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "target_a", "kind": "simple", "data_type": "character"}
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_field": "source_a",
                    "target_path": "Root/target_a",
                }
            ]
        },
        "execution": {"trigger": {"mode": "manual"}},
        "reliability": {
            "retry": {"max_attempts": 1},
            "dlq": {"enabled": False},
            "error_classifier": {},
        },
    }
    spec = DatabaseToApiSyncArchetype.emit_spec(
        DatabaseToApiSyncParameters(**payload)
    )
    plan = _build_plan(
        MagicMock(),
        {"conflict_policy": "reuse", "integration_spec": spec.model_dump()},
    )
    assert plan["_success"] is True
    offenders = [
        s["key"] for s in plan["steps"]
        if s["planned_action"] == "error_name_governance"
    ]
    assert offenders == [], f"archetype names tripped the name lint: {offenders}"
