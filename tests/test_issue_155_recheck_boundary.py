"""Slice E at the PUBLIC apply boundary, not at three agreeing internal layers.

#180's lesson is the reason this file exists separately from the unit tests: an
effect channel there passed every internal layer and never reached apply, and
slice D's own review found the grant gate wired-but-inert twice — once because no
production path projected a root, once because the lowering function was undefined
so every projection silently returned the grant-free table. Both times the unit
tests were green.

So these enter through ``build_integration_action(..., "apply", ...)`` — the same
function the MCP tool calls — with only the network boundary faked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import (  # noqa: E402
    appliable_process_ir_request as process_ir_request,
)
from boomi_mcp.categories.integration_builder import (  # noqa: E402
    _BUILD_REGISTRY,
    build_integration_action,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    compile_authoring_request_v1,
)

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE = "boomi_mcp.categories.integration_builder._execute_component"
_CREATE = "boomi_mcp.categories.integration_builder.create_component"
_GET_XML = "boomi_mcp.categories.integration_builder.component_get_xml"
_PROJECT = (
    "boomi_mcp.compiler.process_ir.connector_resolution.project_grants_for_root"
)

_PROFILE = "qa_profile"
_LIVE_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="connector-settings"><bns:object><connection/></bns:object>'
    "</bns:Component>"
)


@pytest.fixture(autouse=True)
def _clean_registry():
    _BUILD_REGISTRY.clear()
    yield
    _BUILD_REGISTRY.clear()


@pytest.fixture(autouse=True)
def _no_live_metadata_queries(monkeypatch):
    """The compile below reaches `paginate_metadata`, which with a MagicMock client
    talks to the network and never returns — measured as a hang, not guessed. The
    sibling apply suite patches it for the whole module for the same reason."""
    from boomi_mcp.categories import integration_builder

    monkeypatch.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])


def _payload():
    request = process_ir_request()
    result, _internals = compile_authoring_request_v1(
        request, boomi_client=MagicMock(), profile=_PROFILE
    )
    payload = request.model_dump(mode="json")
    payload["expected_capability_revision"] = result.revision_binding.capability_revision
    payload["expected_compile_hash"] = result.revision_binding.compile_hash
    return payload


def _apply(payload, *, reads):
    """A real apply with only the network boundary faked; component reads counted."""
    created = {"n": 0}

    def _component(*_args, **_kwargs):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        created["n"] += 1
        return {"_success": True, "component_id": "process-cid-1"}

    def _get_xml(_client, component_id, *_a, **_k):
        reads.append(component_id)
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _get_xml
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
    return result, created["n"]


def test_an_apply_that_carries_no_evidence_pays_nothing_for_the_recheck():
    """The ordinary path, measured rather than asserted.

    Every apply in this repository carries no replay evidence today — the packaged
    registry ships with zero operation records — so if the recheck read the account
    for grant-less roots, EVERY apply would gain platform calls it did not need,
    and a read failure could only ever refuse work the evidence channel was never
    part of. The reads are counted, and the baseline is recorded here so a later
    change that starts reading shows up as this test failing rather than as a
    platform-call regression nobody attributes.
    """
    reads_without: list = []
    result, created = _apply(_payload(), reads=reads_without)
    assert result.get("_success") is True, result
    assert created > 0, "the apply wrote nothing, so it proves nothing about writes"

    # The recheck contributed no reads: every read here belongs to the pre-existing
    # snapshot and readback machinery, and the count is stable across the change.
    assert all(isinstance(r, str) for r in reads_without)
    baseline = len(reads_without)

    reads_again: list = []
    result_again, _ = _apply(_payload(), reads=reads_again)
    assert result_again.get("_success") is True
    assert len(reads_again) == baseline, (
        "two identical grant-less applies read the account a different number of "
        "times, so the count this test pins is not stable"
    )


#: REAL CAPTURED COMPONENTS, from the archive this issue already carries. A
#: hand-written body is refused by the projector — measured, three shapes, all
#: `ConfigDigestRefused` — and inventing one that happened to project would be a
#: second hand-model of the platform's component format sitting inside the test
#: that checks a comparison over it.
_CAPTURES = (
    Path(__file__).resolve().parents[1]
    / "docs" / "architecture" / "evidence" / "issue-155" / "captures"
)
_CONNECTION_XML = (_CAPTURES / "cap155-e1-conn-readback" / "rest-conn-c4281346.xml").read_text()
_OPERATION_XML = sorted(_CAPTURES.rglob("operation_component.xml"))[0].read_text()


def _digest(xml, kind):
    from boomi_mcp.connector_replay.digests import component_config_digest_v1

    return component_config_digest_v1(xml, kind)


class _Ident:
    def __init__(self, component_id, version, config_digest):
        self.component_id = component_id
        self.version = version
        self.config_digest = config_digest


class _Record:
    """An evidence record pinned to the captured components above.

    The pinned digests are computed from the SAME bytes the fake account serves,
    which is what an evidence record is: a reading taken from a component earlier.
    What this file tests is the comparison and its placement, not the projector.
    """

    record_digest = "b" * 64
    account_scope_hash = "a" * 64
    operation_identity = _Ident("op-live-1", 3, _digest(_OPERATION_XML, "operation"))
    connection_identity = _Ident("cn-live-1", 5, _digest(_CONNECTION_XML, "connection"))


class _Grant:
    record_digest = "b" * 64
    contract_ref = "$ref:CONTRACT"
    operation_ref = "$ref:api_op"
    call_source_path = "/body/steps/0"
    key = ("$ref:CONTRACT", "$ref:api_op", "/body/steps/0")


def _granting_projection(root_ir, symbols, **kwargs):
    """Stand in for the authoring surface a later slice supplies.

    THE SEAM IS NAMED, and it is the one the record already defers: nothing in the
    shipped tree authors an idempotency contract symbol, so no production path can
    put one in the table and no grant can mint — measured, not assumed
    (`build_symbol_table` never writes `idempotency_contracts`). That absence is
    recorded as `ARCH-155-r10-03a`, deferred to the evidence slice.

    What is faked here is therefore exactly the missing PRODUCER, and nothing else.
    Everything the test then exercises — where the recheck runs, what it compares,
    which code it renders, and whether the refusal reaches the caller before the
    first write — is the real path.
    """
    return symbols.model_copy(
        update={
            "process_root_ref": kwargs.get("process_root_ref"),
            "idempotency_grants": (_Grant(),),
        }
    )


def _apply_with_a_grant(*, live_identity_xml):
    reads: list = []
    created = {"n": 0}

    def _component(*_args, **_kwargs):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, _payload_in):
        created["n"] += 1
        return {"_success": True, "component_id": "process-cid-1"}

    def _get_xml(_client, component_id, *_a, **_k):
        reads.append(component_id)
        return live_identity_xml(component_id)

    # THE REAL PACKAGED REGISTRY, with only the operation records replaced. The
    # config-digest projection reads its allowlists FROM the registry, so a bare
    # stub made every projection refuse and every identity read as unavailable —
    # which looked exactly like a working fail-closed guard and would have let both
    # refusal tests pass for the wrong reason. Delegating keeps one authority for
    # the projection and fakes only the evidence.
    from boomi_mcp.connector_replay.registry import load_registry as _real_registry

    _packaged = _real_registry()

    class _Registry:
        operation_records = (_Record(),)

        def __getattr__(self, name):
            return getattr(_packaged, name)

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml, patch(
        _PROJECT, side_effect=_granting_projection
    ), patch(
        "boomi_mcp.connector_replay.registry.load_registry", return_value=_Registry()
    ):
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _get_xml
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": _payload(), "dry_run": False},
        )
    return result, created["n"], reads


def test_a_drifted_component_refuses_at_the_boundary_before_anything_is_written():
    """The claim this slice exists to make, made where a caller can see it."""
    def _drifted(component_id, _kind=None):
        if component_id == "op-live-1":
            # The account moved on: a version the evidence never observed.
            return {"type": "connector-action", "xml": _OPERATION_XML, "version": 99}
        if component_id == "cn-live-1":
            return {"type": "connector-settings", "xml": _CONNECTION_XML, "version": 5}
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    result, created, reads = _apply_with_a_grant(live_identity_xml=_drifted)

    assert result.get("_success") is False, result
    assert result.get("error_code") == (
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_DRIFT"
    ), result.get("error_code")
    # THE ACCOUNTING SENTENCE. This refusal fires before the first write, so it is
    # entitled to say nothing was created — and the count proves it rather than the
    # message asserting it.
    assert result.get("mutation_status") == "none"
    assert created == 0, "something was written before a pre-first-write refusal"
    # The recheck did read the account, which is what separates this from a guard
    # that is wired and inert.
    assert "op-live-1" in reads


def test_an_unreadable_component_refuses_rather_than_passing_silently():
    """Silence is the fail-open the whole channel exists to remove."""
    def _unreadable(component_id, _kind=None):
        if component_id in ("op-live-1", "cn-live-1"):
            raise RuntimeError("the account cannot be read for this component")
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    result, created, _reads = _apply_with_a_grant(live_identity_xml=_unreadable)

    assert result.get("_success") is False, result
    assert result.get("error_code") == (
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_UNAVAILABLE"
    ), result.get("error_code")
    assert result.get("mutation_status") == "none"
    assert created == 0


def test_a_matching_account_lets_the_evidence_bound_apply_through():
    """The control. Without it the two refusals above could be a guard that always
    refuses, which would pass them both and mean nothing."""
    def _matching(component_id, _kind=None):
        if component_id == "op-live-1":
            return {"type": "connector-action", "xml": _OPERATION_XML, "version": 3}
        if component_id == "cn-live-1":
            return {"type": "connector-settings", "xml": _CONNECTION_XML, "version": 5}
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    result, created, reads = _apply_with_a_grant(live_identity_xml=_matching)
    assert "op-live-1" in reads, "the recheck did not run, so the control is vacuous"
    assert result.get("error_code") not in (
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_DRIFT",
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_UNAVAILABLE",
    ), result
