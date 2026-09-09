"""Issue #157 (M12.19) — QA-157-r1-01..04: placement, claim scope, served text.

Stage-1 live QA measured four defects in the governance surface and this module
pins the corrections, each with the control that would have caught it:

* **QA-157-r1-01** — the one-knob folder fan-out placed NOTHING. Every builder
  emits ``folderName``/``folderFullPath``, which this platform ignores on
  create; ``folderId`` is honoured (proven live). Placement is now injected at
  the single raw-create boundary, resolved live at apply, and every created
  component carries the root's own placement attestation and warning.
* **QA-157-r1-02** — an ``action="update"`` binding of an EXISTING component was
  renamed by inheritance. Governance now claims a name and a folder only for
  what apply will CREATE.
* **QA-157-r1-03** — the coverage diagnostic promised a count and paths it never
  served.
* **QA-157-r1-04** — the derived-``flows`` refusal located itself at the intent
  rather than at the key.

Round 2 discharged all four and found three more, fixed here too:

* **QA-157-r2-01** — removing the derived name left an ``action="update"``
  binding with nothing to update, and the smart merge discovered that INSIDE
  the mutation loop, leaving a partial apply behind.
* **QA-157-r2-02** — the fan-out claimed the root's folder NAME for a component
  that declared its own ``folder_id``, so the served row named one folder,
  carried another folder's id, and reported the placement verified.
* **QA-157-r2-03** — the coverage remediation claimed the envelope never echoes
  the unbound paths; the derived-flows preview echoes the caller's own paths.
"""

from __future__ import annotations

import copy
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP  # noqa: E402
from boomi_mcp.authoring.governance import (  # noqa: E402
    GovernanceRefusal,
    owned_supporting_keys,
    resolve_governance,
)
from boomi_mcp.authoring.workflow import compile_authoring_request_v1  # noqa: E402
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.components._shared import with_folder_id  # noqa: E402
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    ProcessIRAuthoringIntentV1,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitAuthoredV1,
    ProcessComponentEnvelopeAuthoredV1,
    ProcessComponentEnvelopeV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_PROFILE = "issue-157"
_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_FOLDERS = "boomi_mcp.categories.folders._query_all_folders"
_GET_XML = "boomi_mcp.categories.integration_builder.component_get_xml"
_FOLDER = {"id": "folder-QA157", "name": "QA157 Governance", "deleted": False}


# ---------------------------------------------------------------------------
# the transport seam (QA-157-r1-01)
# ---------------------------------------------------------------------------


_COMPONENT = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="process" name="P" folderName="Requested"><bns:object/></bns:Component>'
)


def test_the_transport_places_the_document_and_leaves_it_alone_when_it_cannot():
    placed = with_folder_id(_COMPONENT, "folder-1")
    assert 'folderId="folder-1"' in placed
    # the ignored spelling is NOT removed: the builders own it, and this layer
    # only adds the one the platform reads
    assert 'folderName="Requested"' in placed
    # no id -> the caller's bytes, unchanged
    assert with_folder_id(_COMPONENT, None) == _COMPONENT
    assert with_folder_id(_COMPONENT, "  ") == _COMPONENT
    # idempotent: a second placement REPLACES, never duplicates (two folderId
    # attributes on one element is not parseable XML)
    assert with_folder_id(placed, "folder-2").count("folderId=") == 1
    assert 'folderId="folder-2"' in with_folder_id(placed, "folder-2")


def test_the_transport_fails_closed_rather_than_corrupting_a_document():
    """The mutation witness for the verification branch.

    A raw ``>`` inside an attribute value is legal XML and ends the start-tag
    match early, so the naive edit would insert the attribute into the middle of
    the document. The check parses both sides and keeps the caller's bytes.
    """
    hostile = _COMPONENT.replace('name="P"', 'name="a > b"')
    assert with_folder_id(hostile, "folder-1") == hostile
    # ...and the control: the same document without the trap IS placed.
    assert 'folderId="folder-1"' in with_folder_id(
        _COMPONENT.replace('name="P"', 'name="a b"'), "folder-1"
    )
    # unparseable bytes travel untouched
    assert with_folder_id("not xml at all", "folder-1") == "not xml at all"


def _sdk_client(captured):
    client = MagicMock()

    def _create(xml):
        captured.append(xml)
        root = re.search(r'name="([^"]*)"', xml)
        return (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'componentId="cid-{0}" name="{1}" type="process" version="1"/>'
        ).format(len(captured), root.group(1) if root else "x").encode("utf-8")

    client.component.create_component.side_effect = _create
    return client


def test_the_raw_create_boundary_is_where_placement_reaches_the_wire():
    """Every builder path funnels here, so none of them has to know."""
    from boomi_mcp.categories.components.manage_component import create_component

    captured: list = []
    client = _sdk_client(captured)
    result = create_component(
        client, _PROFILE, {"xml": _COMPONENT}, folder_id="folder-1"
    )
    assert result["_success"] is True
    assert 'folderId="folder-1"' in captured[0]

    # ...and `config['folder_id']` — the OTHER spelling FOLDER_REQUIRED_ON_CREATE
    # already accepted as proof of placement — now places too.
    captured.clear()
    create_component(client, _PROFILE, {"xml": _COMPONENT, "folder_id": "folder-2"})
    assert 'folderId="folder-2"' in captured[0]

    # control: no placement declared anywhere -> the bytes are the builder's
    captured.clear()
    create_component(client, _PROFILE, {"xml": _COMPONENT})
    assert "folderId" not in captured[0]


def test_every_create_path_reaches_the_platform_through_the_one_placed_boundary():
    """The coverage claim behind the structural fix, derived from the authority.

    Placement is injected at `_create_component_raw` rather than in the ten
    builders, and that is only sound if no create reaches the platform any other
    way. The SDK's own create call is the authority: it appears in exactly one
    module, so every builder path — present and future — is placed by
    construction rather than by remembering.
    """
    root = Path(__file__).resolve().parent.parent
    callers = sorted(
        path.relative_to(root).as_posix()
        for path in (root / "src").rglob("*.py")
        if "component.create_component(" in path.read_text(encoding="utf-8")
    )
    assert callers == ["src/boomi_mcp/categories/components/_shared.py"], callers
    # ...and that module applies the placement on the way through.
    boundary = (
        root / "src/boomi_mcp/categories/components/_shared.py"
    ).read_text(encoding="utf-8")
    assert "xml = with_folder_id(xml, folder_id)" in boundary
    # THE EXCEPTION IS NAMED, because the claim was too wide without it
    # (QA-157-r4-01). One create in this repository posts a TYPED MODEL through
    # its own SDK entry point instead of a component document, so it never
    # reaches the boundary above and cannot be placed. The claim is therefore
    # "every create that submits a component document is placed", and the one
    # that does not declares its own limit at its own arm.
    other_creates = sorted(
        path.relative_to(root).as_posix()
        for path in (root / "src").rglob("*.py")
        if "create_trading_partner_component_json(" in path.read_text(encoding="utf-8")
    )
    assert other_creates == [
        "src/boomi_mcp/categories/components/trading_partners.py"
    ], other_creates
    assert (
        "placement_unsupported=True"
        in (root / "src/boomi_mcp/categories/integration_builder.py").read_text(
            encoding="utf-8"
        )
    )


# ---------------------------------------------------------------------------
# the governed apply, end to end and offline (QA-157-r1-01)
# ---------------------------------------------------------------------------


def _unit(**envelope):
    kwargs = {
        "component_key": "proc",
        "action": "create",
        "depends_on": ("conn", "op"),
    }
    kwargs.update(envelope)
    return ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(**kwargs),
        process_ir=parse_process_ir_v1(
            {
                "version": "1",
                "body": {
                    "kind": "sequence",
                    "steps": [
                        {
                            "kind": "source",
                            "connection_ref": "$ref:conn",
                            "operation_ref": "$ref:op",
                        },
                        {"kind": "return_documents"},
                    ],
                },
            }
        ),
    )


def _unnamed(spec):
    spec = copy.deepcopy(spec)
    spec.pop("name", None)
    spec["config"] = {
        key: value
        for key, value in spec["config"].items()
        if key != "component_name"
    }
    return spec


def _request(units, components):
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="Issue 157 placement",
            units=tuple(units),
            components=tuple(components),
        )
    )


def _apply(request, *, folders=(_FOLDER,), readback=None):
    """plan -> compile -> apply through the PUBLIC action, offline.

    Only the SDK's own create call and the folder/readback reads are faked, so
    the governance resolution, the apply-time placement resolution, the legacy
    dispatcher, every builder and the raw-create boundary all run for real —
    which is the whole chain QA-157-r1-01 measured as broken.
    """
    from boomi_mcp.categories.integration_builder import build_integration_action

    captured: list = []
    client = _sdk_client(captured)
    with patch(_PAGINATE, lambda *a, **k: []):
        compiled, _ = compile_authoring_request_v1(
            request, boomi_client=client, profile=_PROFILE
        )
    payload = request.model_dump(mode="json")
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    payload["expected_capability_revision"] = (
        compiled.revision_binding.capability_revision
    )

    def _live(_client, component_id, *_a, **_k):
        return {"xml": (readback or _readback_for)(component_id, captured)}

    with patch(_PAGINATE, lambda *a, **k: []), patch(
        _FOLDERS, return_value=list(folders)
    ), patch(_GET_XML, side_effect=_live):
        result = build_integration_action(
            client,
            _PROFILE,
            "apply",
            {"authoring_request": payload, "dry_run": False},
        )
    return result, captured


def _readback_for(component_id, captured):
    """THIS platform, modelled from what QA measured on it.

    ``folderId`` is honoured — the readback then reports the folder's full path
    — and ``folderName`` is IGNORED on create, so a document that carried only
    the name comes back with no placement at all. Echoing the submitted name
    back would make every unplaced component look placed, which is precisely
    the illusion QA-157-r1-01 had to see through.
    """
    index = (
        int(component_id.rsplit("-", 1)[-1]) - 1
        if str(component_id).startswith("cid-")
        else -1
    )
    if not captured or index >= len(captured):
        return ""
    xml = captured[index]
    if 'folderId="{0}"'.format(_FOLDER["id"]) in xml:
        return xml.replace(
            "<bns:Component ",
            '<bns:Component folderFullPath="Acct/{0}" '.format(_FOLDER["name"]),
            1,
        )
    return re.sub(r'\s+folderName="[^"]*"', "", xml)


def test_the_one_knob_places_the_root_and_every_component_it_owns():
    request = _request(
        [_unit(component_prefix="QA157", folder_name=_FOLDER["name"])],
        [_unnamed(APPLIABLE_CONN), _unnamed(APPLIABLE_OP)],
    )
    result, captured = _apply(request)
    assert result["_success"] is True, result.get("error")
    # THE FINDING: every submitted document carries the resolved folder id.
    assert len(captured) == 3, captured
    assert all('folderId="{0}"'.format(_FOLDER["id"]) in xml for xml in captured)
    # ...including the canonical ROOT, whose builder emits folderName alone.
    assert any('type="process"' in xml for xml in captured)
    # every created component reports its own placement, as the root always did
    for key in ("conn", "op"):
        row = result["results"][key]
        assert row["requested_folder_name"] == _FOLDER["name"]
        assert row["resolved_folder_id"] == _FOLDER["id"]
        assert row["placement_verified"] is True
    assert result["results"]["proc"]["placement_verified"] is True
    assert not [
        warning
        for warning in (result.get("warnings") or [])
        if "NOT placed" in warning
    ]


def test_an_unplaced_component_is_reported_rather_than_silently_rooted():
    """The mutation witness for the component attestation.

    A folder name that matches no account folder leaves the component where it
    would have landed anyway — and says so, on the component's own row and in
    the warnings. Before this, the components the fan-out placed carried no
    placement field at all while the root beside them said `false`.

    The canonical ROOT refuses an unresolvable folder before any write, so the
    unresolvable name is carried by a component's own explicit placement, which
    outranks the inherited one.
    """
    conn = _unnamed(APPLIABLE_CONN)
    conn["config"]["folder_name"] = "Missing Folder"
    request = _request(
        [_unit(component_prefix="QA157", folder_name=_FOLDER["name"])],
        [conn, _unnamed(APPLIABLE_OP)],
    )
    result, _captured = _apply(request)
    assert result["_success"] is True, result.get("error")
    row = result["results"]["conn"]
    assert row["requested_folder_name"] == "Missing Folder"
    assert "resolved_folder_id" not in row
    assert row["placement_verified"] is False
    warning = [
        text
        for text in (result.get("warnings") or [])
        if text.startswith("Component 'conn'")
    ]
    assert warning, result.get("warnings")
    assert "no folder id could be submitted" in warning[0]
    # ...and the mechanism is DERIVED: the op, whose inherited folder DID
    # resolve, is placed and warns about nothing.
    assert result["results"]["op"]["placement_verified"] is True


def test_a_create_with_no_placement_submits_none_and_claims_none():
    """Non-vacuity: the injection is driven by the request, not unconditional."""
    request = _request(
        [_unit(component_prefix="QA157")],
        [_unnamed(APPLIABLE_CONN), _unnamed(APPLIABLE_OP)],
    )
    result, captured = _apply(request)
    assert result["_success"] is True, result.get("error")
    assert captured and not any("folderId" in xml for xml in captured)
    assert "placement_verified" not in result["results"]["conn"]
    assert not [
        text
        for text in (result.get("warnings") or [])
        if "requested folder" in text
    ]
    # ...and the folderless-create lint still fires, as it always has: nothing
    # was placed and nothing claims otherwise.
    assert [
        text
        for text in (result.get("warnings") or [])
        if "FOLDER_REQUIRED_ON_CREATE" in text
    ]


def test_a_legacy_route_apply_resolves_no_folder_name_and_reads_no_account():
    """The scope boundary, with the failure that forced it.

    A folder NAME is resolved only where #157 governs placement — an apply
    carrying canonical process roots. The legacy `integration_spec` route keeps
    what it shipped with, the same boundary decision D3 drew for the coverage
    gate, and it also never pays for an account-wide folder listing.

    That is not only a scope preference. Resolving names on every route made
    every legacy apply call the folder pager, whose loop follows whatever page
    token the counterparty returns — so a client that never clears the token
    spins forever while the call recorder grows without bound. The whole
    `test_integration_builder.py` file went from half a second to a multi-GB
    hang, which is how this was found.
    """
    resolve = integration_builder._resolve_component_placement

    def _explode():  # a loader the legacy route must never reach
        raise AssertionError("the legacy route read the account's folders")

    assert resolve({"folder_name": "Anywhere"}, None) == (None, None, "Anywhere")
    assert resolve({"folder_name": "Anywhere"}) == (None, None, "Anywhere")
    # ...while a literal id places on every route, with no account read at all.
    assert resolve({"folder_id": "folder-1"}, None) == ("folder-1", None, None)
    assert resolve({"folder_id": "folder-1"}, _explode) == ("folder-1", None, None)
    # ...and a governed apply DOES resolve the name against the snapshot.
    assert resolve({"folder_name": _FOLDER["name"]}, lambda: [_FOLDER]) == (
        _FOLDER["id"],
        _FOLDER["name"],
        _FOLDER["name"],
    )
    # zero and several matches both leave it unplaced rather than refusing —
    # and still report the name that was asked for, because the name is what
    # drove the (failed) resolution.
    assert resolve({"folder_name": "Missing"}, lambda: [_FOLDER]) == (None, "Missing", "Missing")
    twin = dict(_FOLDER, id="folder-twin")
    assert resolve({"folder_name": _FOLDER["name"]}, lambda: [_FOLDER, twin]) == (
        None,
        _FOLDER["name"],
        _FOLDER["name"],
    )


def test_a_component_declaring_both_spellings_reports_only_the_one_that_won():
    """QA-157-r3-01: the row named the folder the request LOST.

    An authored `folder_id` wins the placement, so an authored `folder_name`
    beside it drove nothing. Reporting it as the requested folder put the losing
    name next to the winning id under `placement_verified: true` — a row
    describing a placement the component does not have. Both directions are
    measured, because the defect is symmetric.
    """
    resolve = integration_builder._resolve_component_placement
    other = dict(_FOLDER, id="folder-C", name="Folder C")
    for config in (
        {"folder_name": _FOLDER["name"], "folder_id": other["id"]},
        {"folder_name": other["name"], "folder_id": _FOLDER["id"]},
    ):
        placement = resolve(config, lambda: [_FOLDER, other])
        assert placement.folder_id == config["folder_id"]
        assert placement.requested_name is None, "the losing spelling must not be reported"
        # ...but the declared name is CARRIED, because a route that submits
        # folders by name places with it (QA-157-r6-01).
        assert placement.declared_name == config["folder_name"]


# ---------------------------------------------------------------------------
# governance claims only what apply CREATES (QA-157-r1-02)
# ---------------------------------------------------------------------------


def _bound_update(spec, component_id="existing-conn-id"):
    """An existing component bound by action=update, with no authored name."""
    spec = _unnamed(spec)
    spec["action"] = "update"
    spec["component_id"] = component_id
    return spec


def _bare_unit(**envelope):
    """A root that DEPENDS on the connection without calling it.

    An `action="update"` connection beside a created operation is refused by
    the legacy REST lint — pre-existing, measured on this tree and on the
    baseline — so ownership is exercised through `depends_on` alone, exactly as
    the live probe did.
    """
    kwargs = {"component_key": "proc", "action": "create", "depends_on": ("conn",)}
    kwargs.update(envelope)
    return ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(**kwargs),
        process_ir=parse_process_ir_v1(
            {
                "version": "1",
                "body": {
                    "kind": "sequence",
                    "steps": [
                        {
                            "kind": "branch",
                            "label": "qa157",
                            "legs": [
                                {
                                    "steps": [
                                        {
                                            "kind": "set_dpp",
                                            "name": "DPP_QA157",
                                            "source_values": [
                                                {"value_type": "static", "value": "1"}
                                            ],
                                        }
                                    ],
                                    "terminal": {"kind": "stop"},
                                },
                                {
                                    "steps": [{"kind": "message", "text": "hello"}],
                                    "terminal": {"kind": "stop"},
                                },
                            ],
                        }
                    ],
                },
            }
        ),
    )


def _governed(units, components):
    """The governance authority alone, over specs the legacy lint would refuse.

    An `action="update"` REST connection is refused by the pre-existing
    component-plan lint (`error_rest_validation`) and its live readback is
    unavailable offline — both unrelated to what is under test here, and both
    measured as pre-existing on the baseline. The claim being pinned is
    governance's own: what it claims, and for which materialization modes.
    """
    specs = tuple(IntegrationComponentSpec(**component) for component in components)
    resolution = resolve_governance(tuple(units), specs)
    return {component.key: component for component in resolution.components}


def test_an_update_bound_component_is_never_renamed_or_moved_by_inheritance():
    """QA-157-r1-02, measured live: the fan-out renamed a shared connection.

    `action="update"` + `component_id` names something the account already
    holds. The ownership walk skipped only `reference_only`, so this third mode
    was treated as an unnamed owned component, given `"<prefix> <key>"`, and the
    legacy update pushed that name.
    """
    by_key = _governed(
        [_bare_unit(component_prefix="QA157", folder_name=_FOLDER["name"])],
        [_bound_update(APPLIABLE_CONN)],
    )
    assert by_key["conn"].name is None
    assert "folder_name" not in (by_key["conn"].config or {})
    # ...and the control, one field apart: the SAME component as a create is
    # claimed, so the rule is the materialization mode and nothing else.
    by_key = _governed(
        [_bare_unit(component_prefix="QA157", folder_name=_FOLDER["name"])],
        [_unnamed(APPLIABLE_CONN)],
    )
    assert by_key["conn"].name == "QA157 conn"
    assert by_key["conn"].config["folder_name"] == _FOLDER["name"]


def test_ownership_excludes_every_component_apply_will_not_create():
    envelope = ProcessComponentEnvelopeV1(
        component_key="proc", name="P", action="create", depends_on=("conn", "op")
    )
    ir = _unit(name="P").process_ir
    for spec, owned in (
        (_unnamed(APPLIABLE_CONN), ("conn", "op")),
        (_bound_update(APPLIABLE_CONN), ("op",)),
        (
            {
                "key": "conn",
                "type": "connector-settings",
                "action": "create",
                "config": {
                    "reference_only": True,
                    "component_id": "existing",
                    "connector_type": "rest",
                },
            },
            ("op",),
        ),
    ):
        components = [
            IntegrationComponentSpec(**spec),
            IntegrationComponentSpec(**_unnamed(APPLIABLE_OP)),
        ]
        assert (
            owned_supporting_keys(
                envelope, ir, {c.key: c for c in components}, {"proc"}
            )
            == owned
        )


def test_two_roots_over_an_update_bound_component_raise_no_ambiguity():
    """The refusal follows the claim: nothing claimed, nothing to disagree about."""
    root_a = _bare_unit(component_key="a", component_prefix="A", folder_name=_FOLDER["name"])
    root_b = _bare_unit(component_key="b", component_prefix="B", folder_name="Other Folder")
    by_key = _governed([root_a, root_b], [_bound_update(APPLIABLE_CONN)])
    assert by_key["conn"].name is None
    # control: as a create, the same two roots ARE the ambiguity refusal
    with pytest.raises(GovernanceRefusal):
        _governed([root_a, root_b], [_unnamed(APPLIABLE_CONN)])


def _trading_partner_arm(placement, config=None):
    """Run the ONE create arm that submits a typed model, not a document.

    The config carries whatever the request declared, exactly as the apply
    splats it into the typed model — including a `folder_name`, which is the
    field this route places with.
    """
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    declared = {"component_name": "TP"}
    if placement is not None and placement.declared_name:
        declared["folder_name"] = placement.declared_name
    declared.update(config or {})
    comp = IntegrationComponentSpec(
        key="tp", type="trading_partner", action="create", config=declared
    )
    def _route(_client, _profile, request_data):
        # The stub answers the way the real route does — its own return block,
        # with its own `Home` default — because a stub that answers differently
        # is how the round-6 test passed over the defect it was guarding
        # (QA-157-r7-03: the route's default was reported nowhere).
        return {
            "_success": True,
            "component_id": "tp-1",
            "trading_partner": {
                "component_id": "tp-1",
                "name": request_data.get("component_name"),
                "folder_name": request_data.get("folder_name", "Home"),
            },
        }

    with patch.object(integration_builder, "create_trading_partner", _route):
        return integration_builder._execute_component(
            MagicMock(), _PROFILE, comp, dict(comp.config or {}), placement=placement
        )


def test_the_name_submitting_route_reports_what_it_submitted():
    """QA-157-r7-03: the report is READ from the route, not re-derived.

    Five corrections modelled this route from outside; the fifth said it
    reports its submission and then rebuilt the value from the resolved
    placement — stripped on one branch, ranked on the other — while the route
    had already returned verbatim what it sent, including its own `Home`
    default when the request named no folder at all.
    """
    placement = integration_builder._ComponentPlacement
    # whatever the request declares, the row carries the bytes the ROUTE sent
    for value, config, expected in (
        (placement("folder-A", "Folder A", "Folder A"), None, "Folder A"),
        (placement(None, "Folder A", "Folder A"), None, "Folder A"),
        (placement("folder-C", None, "Folder A"), None, "Folder A"),
        # padding is NOT normalised away: the route submits what it was given
        (placement("folder-A", None, "Folder A"), {"folder_name": "  Folder A  "}, "  Folder A  "),
    ):
        outcome = _trading_partner_arm(value, config)
        assert outcome.get("placement_submitted_name") == expected, (value, config)
        assert "placement_unsupported" not in outcome

    # ASKED BY ID: this route sends its own default instead, so the request was
    # NOT honoured however the component landed — and the row says both, the
    # name that went and the id that could not (QA-157-r7-01/-02).
    by_id = _trading_partner_arm(placement("folder-A", None, None))
    assert by_id.get("placement_unsupported") is True
    assert by_id.get("placement_submitted_name") == "Home"

    # ASKED FOR NOTHING: a route default is not a request and earns no claim.
    nothing = _trading_partner_arm(placement(None, None, None))
    assert "placement_submitted_name" not in nothing
    assert "placement_unsupported" not in nothing
    assert "placement_submitted_name" not in _trading_partner_arm(None)


def test_the_id_limit_row_is_built_end_to_end_and_names_its_own_id():
    """QA-157-r8-01: the guard that records the id was unreachable.

    The id was cleared before the branch that would have written it, and both
    facts came from the same arm, so the row served "requested None" on exactly
    the case the branch existed for. This drives the real apply — the row is
    built by the loop, not by hand, which is what the hand-built version of this
    test could not see.
    """
    tp = {
        "key": "tp",
        "type": "trading_partner",
        "action": "create",
        "config": {"component_name": "TP", "folder_id": "folder-OTHER"},
    }

    def _route(_client, _profile, request_data):
        return {
            "_success": True,
            "component_id": "tp-1",
            "trading_partner": {
                "component_id": "tp-1",
                "name": "TP",
                "folder_name": request_data.get("folder_name", "Home"),
            },
        }

    request = _request(
        [
            _unit(
                component_prefix="QA157",
                folder_name=_FOLDER["name"],
                depends_on=("conn", "op", "tp"),
            )
        ],
        [_unnamed(APPLIABLE_CONN), _unnamed(APPLIABLE_OP), tp],
    )
    with patch.object(integration_builder, "create_trading_partner", _route):
        result, _captured = _apply(request)
    assert result["_success"] is True, result.get("error")
    row = result["results"]["tp"]
    assert row["unsubmitted_folder_id"] == "folder-OTHER"
    assert row["submitted_folder_name"] == "Home"
    assert row["placement_verified"] is False
    notice = [
        text for text in (result.get("warnings") or []) if text.startswith("Component 'tp'")
    ]
    assert notice, result.get("warnings")
    assert "folder id 'folder-OTHER'" in notice[0]
    assert "requested None" not in notice[0]
    assert "Author folder_name instead" in notice[0]


def test_the_four_placement_mechanisms_are_each_reachable_and_distinct():
    """One sentence per way a placement fails, and none of them empty.

    Five now: an update that preserves, an id submitted and unconfirmed, a name
    that resolved to no single folder, a name this route submitted and could not
    confirm, and a request by id on a route that submits names only. Each is
    asserted on the row shape the apply actually builds for it.

    The fourth was missing (QA-157-r4-01) and, once added, the row it fires on
    had no request left to name — the notice read "requested folder None"
    (QA-157-r5-02). Each mechanism is asserted on the row shape the apply
    actually builds for it.
    """
    warn = integration_builder._placement_warning
    unsupported = warn(
        "Component",
        "tp",
        {
            "status": "created",
            "unsubmitted_folder_id": "folder-A",
            "submitted_folder_name": "Home",
            "observed_folder": None,
        },
    )
    assert "submits a folder by NAME only" in unsupported
    assert "folder id 'folder-A'" in unsupported, unsupported
    assert "requested folder None" not in unsupported
    # ...and the remedy is one that WORKS on this route: the platform refuses to
    # move a trading partner through the folder tool (QA-157-r7-01).
    assert "Author folder_name instead" in unsupported
    assert "Move it via manage_folders or the UI" not in unsupported

    # a NAME this route submitted, unconfirmed: nothing about ids applies
    by_name = warn(
        "Component",
        "tp",
        {
            "status": "created",
            "submitted_folder_name": "Folder A",
            "observed_folder": None,
        },
    )
    assert "submitted the folder by name" in by_name
    assert "folder id" not in by_name
    assert "was submitted on the create" not in by_name

    submitted = warn(
        "Component",
        "c",
        {
            "status": "created",
            "requested_folder_name": "Folder A",
            "resolved_folder_id": "folder-A",
            "observed_folder": None,
        },
    )
    assert "was submitted on the create" in submitted
    assert "'Folder A'" in submitted

    unresolved = warn(
        "Component",
        "c",
        {"status": "created", "requested_folder_name": "Ghost", "observed_folder": None},
    )
    assert "no folder id could be submitted" in unresolved

    updated = warn(
        "Process",
        "p",
        {"status": "updated", "requested_folder_name": "Folder A", "observed_folder": None},
    )
    assert "update preservation" in updated
    # ...and the four are genuinely different sentences.
    assert len({unsupported, by_name, submitted, unresolved, updated}) == 5


# ---------------------------------------------------------------------------
# a bind is not a write (QA-157-r2-01)
# ---------------------------------------------------------------------------


def _legacy_update(config, *, execute):
    """A legacy apply of ONE connector bound by action="update"."""
    from boomi_mcp.categories.integration_builder import build_integration_action

    spec = {
        "integration_name": "qa157",
        "components": [
            {
                "key": "conn",
                "type": "connector-settings",
                "action": "update",
                "component_id": "existing-conn-id",
                "config": config,
            }
        ],
    }
    with patch(_PAGINATE, lambda *a, **k: []), patch.object(
        integration_builder, "_execute_component"
    ) as dispatch:
        dispatch.return_value = {
            "_success": True,
            "component_id": "existing-conn-id",
        }
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply", {"integration_spec": spec, "dry_run": False}
        )
    execute.append(dispatch.call_count)
    return result


def test_an_update_that_would_write_nothing_is_bound_before_the_loop_can_start():
    """QA-157-r2-01: the answer is fixed by the request, so it precedes the writes.

    Removing the inherited name (QA-157-r1-02) left this binding with none of
    the smart-merge fields, and the merge refused mid-apply — after earlier
    components had been created, with a partial result and no error code. The
    step is now recorded as `reused`, which is what actually happens: the
    component is bound, not written.
    """
    dispatches: list = []
    result = _legacy_update({"connector_type": "rest"}, execute=dispatches)
    assert result["_success"] is True, result.get("error")
    assert result["results"]["conn"]["status"] == "reused"
    assert result["results"]["conn"]["component_id"] == "existing-conn-id"
    assert dispatches == [0], "a bind must not reach the write dispatcher"


@pytest.mark.parametrize(
    "authored",
    [{"description": "d"}, {"component_name": "N"}, {"name": "N"}, {"folder_name": "F"}],
)
def test_an_update_authoring_any_smart_merge_field_still_dispatches(authored):
    """The control, one field apart: something to write is still written."""
    dispatches: list = []
    result = _legacy_update(dict({"connector_type": "rest"}, **authored), execute=dispatches)
    assert result["_success"] is True, result.get("error")
    assert result["results"]["conn"]["status"] == "updated"
    assert dispatches == [1]


def test_the_smart_merge_field_list_and_the_two_merge_sites_agree_both_ways():
    """The predicate is pinned to the sites it models, in both directions.

    A field added to a merge site without being added here would make the
    pre-write check call a real update a no-op — the silent-skip failure this
    check exists to prevent — so every field is exercised against both sites,
    and the empty config is exercised against all three.
    """
    from boomi_mcp.categories.components._shared import (
        SMART_MERGE_UPDATE_FIELDS,
        smart_merge_would_change,
    )
    from boomi_mcp.categories.components.connectors import update_connector
    from boomi_mcp.categories.components.manage_component import update_component

    live = {
        "name": "Existing",
        "type": "connector-settings",
        "xml": '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'name="Existing" type="connector-settings"><bns:description/>'
        "</bns:Component>",
    }
    for field in sorted(SMART_MERGE_UPDATE_FIELDS):
        config = {field: "v"}
        assert smart_merge_would_change(config) is True, field
        for site in (update_connector, update_component):
            with patch(
                "boomi_mcp.categories.components.connectors.component_get_xml",
                return_value=live,
            ), patch(
                "boomi_mcp.categories.components.manage_component.component_get_xml",
                return_value=live,
            ):
                out = site(MagicMock(), _PROFILE, "cid", config)
            assert out["_success"] is True, (field, site.__name__, out)
    # ...and with none of them, the predicate and both sites all say "nothing".
    assert smart_merge_would_change({"connector_type": "rest"}) is False
    for site in (update_connector, update_component):
        with patch(
            "boomi_mcp.categories.components.connectors.component_get_xml",
            return_value=live,
        ), patch(
            "boomi_mcp.categories.components.manage_component.component_get_xml",
            return_value=live,
        ):
            out = site(MagicMock(), _PROFILE, "cid", {"connector_type": "rest"})
        assert out["_success"] is False, site.__name__
        assert "No updatable fields" in out["error"], site.__name__


# ---------------------------------------------------------------------------
# one placement per component (QA-157-r2-02)
# ---------------------------------------------------------------------------


def test_an_explicit_folder_id_stops_the_fan_out_exactly_as_a_name_does():
    """QA-157-r2-02: the served row named one folder and carried another's id.

    Both spellings are placements. Claiming the root's NAME for a component that
    declares an `folder_id` left the row assembled from two authorities: the
    requested name from the fan-out, the resolved id from the component, and a
    `placement_verified: true` that described neither.
    """
    conn = _unnamed(APPLIABLE_CONN)
    conn["config"]["folder_id"] = "folder-OTHER"
    by_key = _governed(
        [_unit(component_prefix="QA157", folder_name=_FOLDER["name"])],
        [conn, _unnamed(APPLIABLE_OP)],
    )
    assert "folder_name" not in by_key["conn"].config
    assert by_key["conn"].config["folder_id"] == "folder-OTHER"
    # the inherited name is untouched: only the PLACEMENT is already declared
    assert by_key["conn"].name == "QA157 conn"
    # ...and the sibling with no placement of its own still inherits
    assert by_key["op"].config["folder_name"] == _FOLDER["name"]


def test_a_component_declaring_a_folder_id_reports_that_placement_and_no_other():
    """End to end: one authority per row, so the row cannot contradict itself."""
    conn = _unnamed(APPLIABLE_CONN)
    conn["config"]["folder_id"] = "folder-OTHER"
    request = _request(
        [_unit(component_prefix="QA157", folder_name=_FOLDER["name"])],
        [conn, _unnamed(APPLIABLE_OP)],
    )
    result, captured = _apply(request)
    assert result["_success"] is True, result.get("error")
    row = result["results"]["conn"]
    # ONE authority per row: the id it declared is what was submitted, what is
    # attested, and what the verification compared — and no folder NAME is
    # reported, because none drove the placement.
    assert "requested_folder_name" not in row
    assert row["resolved_folder_id"] == "folder-OTHER"
    assert row["placement_verified"] is True
    assert any('folderId="folder-OTHER"' in xml for xml in captured)


_RAW_DOC = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="profile.xml" name="RawProfile"{0}><bns:object/></bns:Component>'
)


def test_only_a_document_carried_folder_id_stops_the_fan_out():
    """The non-vacuity witness for the raw-XML rule, in both directions.

    A caller who authored a `folderId` in their own component document authored
    the placement, and the fan-out must leave it alone. A document carrying only
    `folderName` has authored NOTHING this platform honours, so the fan-out must
    still claim it — accepting that spelling suppressed the fan-out for a
    document that places nothing, which is the finding this slice opened on,
    reinstated on the escape hatch the rule was written for.
    """
    from boomi_mcp.authoring.governance import _declares_its_own_placement
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def _spec(document):
        return IntegrationComponentSpec(
            key="raw", type="profile.xml", action="create", config={"xml": document}
        )

    assert _declares_its_own_placement(_spec(_RAW_DOC.format(' folderId="own"'))) == "own"
    assert _declares_its_own_placement(_spec(_RAW_DOC.format(' folderName="Home"'))) is None
    assert _declares_its_own_placement(_spec(_RAW_DOC.format(""))) is None

    # ...and through the fan-out: the id-bearing document is left alone, the
    # name-bearing one is claimed like any other unplaced component.
    for document, claimed in (
        (_RAW_DOC.format(' folderId="own"'), False),
        (_RAW_DOC.format(' folderName="Elsewhere"'), True),
        (_RAW_DOC.format(""), True),
    ):
        raw = {"key": "raw", "type": "profile.xml", "action": "create",
               "config": {"xml": document}}
        by_key = _governed(
            [
                _bare_unit(
                    component_prefix="QA157",
                    folder_name=_FOLDER["name"],
                    depends_on=("conn", "raw"),
                )
            ],
            [_unnamed(APPLIABLE_CONN), raw],
        )
        got = (by_key["raw"].config or {}).get("folder_name")
        assert (got == _FOLDER["name"]) is claimed, (document[:80], got)


def test_a_component_document_that_declares_an_entity_is_not_read():
    """#157 put CALLER bytes in front of the placement reader.

    That reader used to see only bytes this server built or the platform
    returned. An expat parser expands internal entities — measured, a four-level
    declaration turns a few hundred bytes into ten thousand — so a document that
    declares one is refused rather than parsed, and an unreadable document
    simply carries no placement.
    """
    from boomi_mcp.categories.components.canonical_process_apply import (
        applied_placement,
    )

    bomb = (
        '<?xml version="1.0"?><!DOCTYPE lolz ['
        '<!ENTITY a "aaaaaaaaaa">'
        '<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">'
        ']><bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'name="&b;" folderId="X"/>'
    )
    assert applied_placement(bomb) == {"folder_name": None, "folder_id": None}
    # the control: the same placement WITHOUT a declaration is still read
    assert applied_placement(_RAW_DOC.format(' folderId="X"'))["folder_id"] == "X"


def test_every_closed_key_builder_accepts_the_key_the_fan_out_writes():
    """QA-157-r8-03 and QA-157-r9-01, closed at the place the fact belongs.

    The fan-out writes `folder_name` onto every component a root owns. Builders
    that enforce a closed top-level key set refused it, which made their
    component types unbuildable under a governed root that declares a folder.

    The first correction asked each builder whether it accepted the key and
    skipped placing the ones that did not — the seventh instance of this
    slice's recurring class, because a builder's config vocabulary has nothing
    to do with whether a component can be placed: placement is an argument to
    the create, injected as a folder id at the one raw-create boundary, and
    these builders never see it. So the builders accept the key as metadata and
    emit nothing for it, and the fan-out is unconditional again.

    The closed-key builders are DERIVED from the package, not listed: a hand
    list is how a sixth such builder would reproduce the defect undetected
    (QA-157-r9-03).
    """
    import importlib
    import pkgutil

    from boomi_mcp.categories.components import builders as package

    # RECURSIVE: `iter_modules` stops at the top level, so a closed-key builder
    # in a sub-package would pass unseen (QA-157-r10-05). Not reachable in this
    # tree today, which is exactly when a walk gets written too narrow.
    closed = {}
    for info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module = importlib.import_module(info.name)
        allowed = getattr(module, "_ALLOWED_TOP_LEVEL_KEYS", None)
        if allowed is not None:
            closed[info.name] = allowed
    assert len(closed) >= 5, closed
    # BOTH spellings: the fan-out writes the name, and `folder_id` is the one
    # that actually places — a lint telling a caller to set one must not be
    # refused by the builder that receives it (QA-157-r10-03).
    for spelling in ("folder_name", "folder_id"):
        refusing = sorted(name for name, keys in closed.items() if spelling not in keys)
        assert refusing == [], (
            "these builders refuse %r, so a governed root that declares a folder "
            "cannot build their component types: %r" % (spelling, refusing)
        )
    # ...and accepting it changes no emitted byte: the key is metadata, and this
    # platform ignores every folder spelling in a component document anyway.
    from boomi_mcp.categories.components.builders.process_property_builder import (
        get_process_property_builder,
    )

    builder = get_process_property_builder("processproperty")()
    config = {
        "component_name": "P",
        "properties": [
            {
                "key": "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
                "name": "A",
                "type": "string",
                "default_value": "1",
            }
        ],
    }
    assert builder.build(**config) == builder.build(
        **dict(config, folder_name="F", folder_id="folder-1")
    )

    # the fan-out reaches such a component again, and claims its placement
    prop = {
        "key": "prop",
        "type": "processproperty",
        "action": "create",
        "config": {},
    }
    by_key = _governed(
        [
            _bare_unit(
                component_prefix="QA157",
                folder_name=_FOLDER["name"],
                depends_on=("conn", "prop"),
            )
        ],
        [_unnamed(APPLIABLE_CONN), prop],
    )
    assert by_key["prop"].config["folder_name"] == _FOLDER["name"]
    assert by_key["prop"].name == "QA157 prop"


def test_every_served_create_template_offers_the_spelling_that_places():
    """QA-157-r10-02: the templates carried a second copy of the builders' keys.

    The correction taught the builders to accept both placement spellings; the
    served create templates, hand-written beside them, did not follow — and the
    only folder key several offered was the one measured never to place. A
    caller reading the template authored the spelling that does nothing.

    The graded set is DERIVED from the templates' own declaration — every
    create template that mentions a folder at all — rather than from a hand
    list or a hand-typed floor, because a floor is what let this be graded on
    four templates while ten others were wrong.
    """
    from boomi_mcp.categories import meta_tools

    graded, offenders = [], []
    for attr in dir(meta_tools):
        template = getattr(meta_tools, attr)
        if not (isinstance(template, dict) and isinstance(template.get("template"), dict)):
            continue
        if template.get("operation") != "create":
            continue
        body = template["template"]
        if not any(key.startswith("folder") for key in body):
            continue
        # Organizations are not components and never reach the component
        # create boundary, so no component placement applies to them.
        if attr == "_ORGANIZATION_CREATE":
            continue
        graded.append(attr)
        if "folder_id" not in body:
            offenders.append(attr)
        # The completeness check applies only where the template DECLARES a
        # complete surface (an `optional` list beside `required`); several
        # templates list requirements alone by long-standing convention, and
        # inventing a declaration for them is not this test's business.
        if "optional" in template:
            declared = (
                set(template.get("required", ()))
                | set(template["optional"])
                | set(template.get("defaults", {}))
            )
            if set(body) - declared:
                offenders.append((attr, sorted(set(body) - declared)))
    assert len(graded) >= 15, graded
    assert offenders == [], offenders


def test_no_builder_refuses_the_spelling_that_places():
    """Every closed enumeration in the builder layer accepts a placement id.

    Derived by BEHAVIOUR, not by an attribute name: each closed key set is found
    by walking the package, and the one enumeration that lived under a different
    name — the process builder's — is included by asking it directly. It had
    excluded the key on a rationale the transport correction made stale: that a
    create carrying it "would still land in the account root". It no longer
    does, which is the whole point of the boundary.
    """
    import importlib
    import pkgutil

    from boomi_mcp.categories.components import builders as package
    from boomi_mcp.categories.components.builders import process_flow_builder

    closed = {}
    for info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module = importlib.import_module(info.name)
        keys = getattr(module, "_ALLOWED_TOP_LEVEL_KEYS", None)
        if keys is not None:
            closed[info.name] = set(keys)
    closed["sync_pipeline"] = set(process_flow_builder._SYNC_PIPELINE_ALLOWED_TOP_LEVEL)
    assert len(closed) >= 6, sorted(closed)
    for spelling in ("folder_name", "folder_id"):
        refusing = sorted(name for name, keys in closed.items() if spelling not in keys)
        assert refusing == [], (spelling, refusing)


# ---------------------------------------------------------------------------
# served text (QA-157-r1-03, QA-157-r1-04)
# ---------------------------------------------------------------------------


def test_a_caller_authored_flows_key_is_located_at_the_key():
    from boomi_mcp.categories.integration_builder import build_integration_action

    for intent in (
        {
            "intent_kind": "process_ir",
            "integration_name": "x",
            "units": [],
            "components": [],
            "flows": [],
        },
        {
            "intent_kind": "recipe",
            "integration_name": "x",
            "invocations": [],
            "flows": [],
        },
    ):
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "plan",
            {"authoring_request": {"contract_version": "2", "intent": intent}},
        )
        assert result["_success"] is False
        assert result["error_code"] == "GOVERNANCE_FLOWS_OUTPUT_ONLY"
        located = [
            entry["path"]
            for entry in result["validation_errors"]
            if entry["type"] == "governance_flows_output_only"
        ]
        assert located and all(path.endswith(".flows") for path in located), located


def test_the_located_key_map_is_pinned_to_the_named_code_map():
    """A leaf may only be declared for a refusal the served-code map knows."""
    assert set(integration_builder._NAMED_VALIDATION_PATH_LEAVES) <= set(
        integration_builder._NAMED_VALIDATION_CODES
    )
    # non-vacuous: an ordinary schema failure keeps pydantic's own location
    assert (
        integration_builder._located_validation_path("intent.units.0", "missing")
        == "intent.units.0"
    )
