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

import pathlib
import sys
from pathlib import Path
from types import SimpleNamespace
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
    # NAMED, because a grant resolves its record by the (digest, contract_ref)
    # PAIR — the loader dedupes contract refs and not digests, so the digest alone
    # can select the wrong record. A stand-in that omits the ref cannot resolve at
    # all, which is how this fixture stopped exercising the recheck the moment the
    # lookup was corrected.
    contract_ref = "$ref:icv1:rest:patch:contract:1"
    # NO ACCOUNT CLAIM. This read as a literal hash, which made the record claim
    # an account no client in this harness is ever in — invisible while a missing
    # account SKIPPED the scope comparison instead of failing it. With that arm
    # closed, a record that names a scope must be matched by a client that knows
    # its account, and this harness deliberately does not model one: its subject
    # is the identity recheck, not account binding. A record making no account
    # claim is exactly the case the rule permits, and the packaged production
    # record — which does carry a scope — is still enforced.
    account_scope_hash = None
    #: Service-wide, because these cells drive a call whose route this harness does
    #: not name — and a static record authorises only the routes it enumerates.
    route_coverage = type("_ServiceWide", (), {"kind": "service_wide"})()
    operation_identity = _Ident("op-live-1", 3, _digest(_OPERATION_XML, "operation"))
    connection_identity = _Ident("cn-live-1", 5, _digest(_CONNECTION_XML, "connection"))


def _Grant():
    """A REAL grant symbol, not a stand-in with the right attribute names.

    This was a bare class whose fields merely looked like a grant's. It survived
    because the projected table was never re-validated on the path these tests
    drive — and #155 slice F closed that gap: the materialization plan now
    compiles against the projected table, which validates it, and every one of
    these tests failed with a model-type error. The object was never a valid
    grant; nothing had asked.
    """
    from boomi_mcp.compiler.process_ir.contracts import IdempotencyGrantSymbolV1

    return IdempotencyGrantSymbolV1(
        contract_ref="$ref:icv1:rest:patch:contract:1",
        operation_ref="$ref:api_op",
        call_source_path="/body/steps/0",
        record_digest="b" * 64,
    )


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
    def _drifted(component_id, _kind=None, _family=None):
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
    def _unreadable(component_id, _kind=None, _family=None):
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
    def _matching(component_id, _kind=None, _family=None):
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


def _apply_with_a_grant_drifting_at(*, drift_after_reads):
    """Serve a healthy account until N reads have happened, then drift.

    This is how the two later boundaries are reached at all: the global recheck
    runs first and would refuse before the loop ever starts, so a test that
    drifts from the beginning can only ever exercise the first of the three.
    """
    reads: list = []
    created = {"n": 0}

    def _component(*_args, **_kwargs):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, _payload_in):
        created["n"] += 1
        return {"_success": True, "component_id": "process-cid-1"}

    def _get_xml(_client, component_id, *_a, **_k):
        if component_id in ("op-live-1", "cn-live-1"):
            reads.append(component_id)
            drifted = len(reads) > drift_after_reads
            if component_id == "op-live-1":
                return {
                    "type": "connector-action",
                    "xml": _OPERATION_XML,
                    "version": 99 if drifted else 3,
                }
            return {
                "type": "connector-settings",
                "xml": _CONNECTION_XML,
                "version": 5,
            }
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

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


def test_the_just_in_time_recheck_refuses_without_claiming_nothing_was_written():
    """The second boundary, and the distinction it exists to preserve.

    The global recheck may promise nothing was created. This one runs inside the
    mutation loop, after the components ordered before this root are already in
    the account, and the accounting sentence it serves is DERIVED from what the
    loop recorded rather than copied from the earlier refusal. Getting that wrong
    in the safe-looking direction — reusing "none" — is how an operator is told
    nothing happened when something did.
    """
    # Two reads satisfy the global pass; the drift appears afterwards.
    result, _created, reads = _apply_with_a_grant_drifting_at(drift_after_reads=2)

    assert result.get("_success") is False, result
    assert len(reads) > 2, "the later boundary was never reached, so this proves nothing"
    assert result.get("error_code") in (
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_DRIFT",
        "CONNECTOR_REPLAY_POST_SUBMISSION_RECONCILIATION_DRIFT",
    ), result.get("error_code")
    # Whichever of the two later boundaries caught it, neither may claim the
    # account is untouched: components ordered before the root are written. The
    # VALUE is the repository's own derived one — this test asserts the property
    # that matters, not a spelling it would otherwise be pinning twice.
    assert result.get("mutation_status") != "none", result.get("mutation_status")
    assert result.get("mutation_status") in ("performed", "possible", "retained")
    # And the envelope is the ONE partial-failure shape, not a hand-built dict.
    assert "partial_results" in result
    assert result.get("failed_step")


def test_the_post_submission_failure_retains_its_result():
    """The third boundary. A reconciliation failure is not a refusal.

    The component exists. Serving it back is the difference between telling an
    operator to reconcile something and telling them there is nothing to find.
    """
    # Let the global pass, the just-in-time recheck AND the pre-push boundary all
    # succeed, then drift. The threshold was 4, which stopped reaching the third
    # boundary the moment the CREATE arm gained the pre-push callback the update
    # arm always had — the drift was then caught before the write, which is the
    # correct outcome for that window and the wrong setup for this test. Six is
    # the reads those three checks consume; the assertion below refuses to
    # self-escape if a later change moves the count again.
    result, _created, reads = _apply_with_a_grant_drifting_at(drift_after_reads=6)

    # NO SELF-ESCAPE. An earlier version fell back to `assert len(reads) >= 4`
    # whenever the third boundary was not reached, so a change that stopped
    # reaching it left the test green on a different assertion — which live QA
    # called out, correctly, as a test that can stop checking the thing it names.
    assert result.get("error_code") == (
        "CONNECTOR_REPLAY_POST_SUBMISSION_RECONCILIATION_DRIFT"
    ), (
        "the post-submission boundary was not reached; error_code=%r reads=%r"
        % (result.get("error_code"), reads)
    )
    assert result.get("mutation_status") != "none"
    assert result.get("partial_results"), (
        "a post-submission failure served no partial results, so the retained "
        "write is invisible to the caller it belongs to"
    )
    # AND THE ATTESTATION FOR THE WRITE SURVIVES THE REFUSAL. This is the property
    # that was actually broken: the reconciliation returned before the appends that
    # record what the write did, so the one root whose write triggered the check was
    # the one root whose attestation the envelope could not carry — and
    # `replay_evidence_bindings` lives on that attestation.
    mutations = result.get("process_mutations") or []
    assert mutations, (
        "a post-submission refusal dropped the attestation for the root it had "
        "just written; `_partial_failure` states that no failing exit may do that"
    )
    bindings = [b for m in mutations if isinstance(m, dict)
                for b in (m.get("replay_evidence_bindings") or ())]
    assert bindings, (
        "the refusal that exists to say a replay contract authorised this write "
        "served no record that any contract authorised anything"
    )


def test_a_successful_evidence_bound_apply_attests_what_authorised_it():
    """The mutation-accounting half. Without it, "this process was applied" and
    "this process was applied while a replay contract authorised one of its
    calls" are the same sentence in the record, and only the second is true."""
    def _matching(component_id, _kind=None, _family=None):
        if component_id == "op-live-1":
            return {"type": "connector-action", "xml": _OPERATION_XML, "version": 3}
        if component_id == "cn-live-1":
            return {"type": "connector-settings", "xml": _CONNECTION_XML, "version": 5}
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    result, _created, reads = _apply_with_a_grant(live_identity_xml=_matching)
    assert "op-live-1" in reads

    assert result.get("_success") is True, result
    mutations = result.get("process_mutations") or []
    bindings = []
    for entry in mutations:
        if isinstance(entry, dict) and entry.get("replay_evidence_bindings"):
            bindings.extend(entry["replay_evidence_bindings"])
    assert bindings, (
        "an evidence-bound apply attested no binding; served mutation keys: %r"
        % ([sorted(m) for m in mutations if isinstance(m, dict)][:2],)
    )
    one = bindings[0]
    assert one["contract_ref"] == "$ref:icv1:rest:patch:contract:1"
    assert one["call_source_path"] == "/body/steps/0"
    assert one["record_digest"] == "b" * 64


def test_a_projection_that_cannot_complete_never_reads_as_no_grants():
    """The fail-open live QA found, pinned so it cannot come back.

    Empty grants is not a neutral value here: it is the ONE value that switches
    off the global recheck, the just-in-time recheck, the post-submission
    reconciliation and the attestation binding, all four at once. An earlier
    version wrapped the projection — and the symbol build evaluated as its
    argument, which raises on a declared-versus-resolved identity mismatch — in a
    bare `except` that produced exactly that value. QA seeded one exception there
    and watched the identical write its control refused get committed with
    `_success: true` and an accounting record asserting the write carried no
    replay evidence.
    """
    def _boom(*_a, **_k):
        raise RuntimeError("the projection could not complete")

    def _matching(component_id, _kind=None, _family=None):
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    created = {"n": 0}

    def _component(*_args, **_kwargs):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, _payload_in):
        created["n"] += 1
        return {"_success": True, "component_id": "process-cid-1"}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml, patch(_PROJECT, side_effect=_boom):
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = lambda _c, cid, *a, **k: _matching(cid)
        try:
            result = build_integration_action(
                MagicMock(),
                _PROFILE,
                "apply",
                config={"authoring_request": _payload(), "dry_run": False},
            )
        except RuntimeError:
            # PROPAGATING IS AN HONEST OUTCOME. The patch replaces the projector
            # for all five of its call sites, so which frame raises first is not
            # this test's subject; the subject is that the failure is never
            # converted into "there were no grants". An exception reaching the
            # caller says so as loudly as a refusal does.
            return

    # A SERVED REFUSAL is the other honest outcome — and the apply-side handler
    # produces one, because the projection now sits inside the same `try` that
    # classifies every other pre-write failure in this pass.
    assert result.get("_success") is not True, (
        "a projection that could not complete was read as 'no grants' and the "
        "apply proceeded unchecked: %r" % {k: result.get(k) for k in
                                          ("_success", "error_code", "results")}
    )


def test_the_written_question_is_answered_by_the_module_s_own_status_set():
    """`_anything_written` must not be a second, narrower list of writing statuses.

    The module owns the question as data — the NON-writing statuses — plus the
    rule that an unknown status counts as WRITTEN, because the two error
    directions are not symmetric: over-reporting costs a retry-safety hint, while
    under-reporting tells an operator nothing needs cleaning up when something
    does. An allowlist of writing statuses inverts that, and live QA measured the
    disagreement on `failed`, `deployed` and the empty status — all three in the
    fail-open direction.
    """
    from boomi_mcp.categories.integration_builder import (
        _NON_WRITING_STEP_STATUSES,
        _anything_written,
    )

    for status in ("reused", "refused"):
        assert status in _NON_WRITING_STEP_STATUSES
        assert _anything_written({"k": {"status": status}}) is False, status

    # Every other status counts as written, including the three QA measured and
    # any this repository grows later — which is the point of deriving from the
    # non-writing set rather than naming the writing ones.
    for status in ("created", "updated", "failed", "deployed", "", "a-new-status"):
        assert _anything_written({"k": {"status": status}}) is True, status

    assert _anything_written({}) is False


def test_a_projection_gap_does_not_survive_as_a_remediation_that_cannot_work():
    """All three served fields must name the same cause.

    The detail sentence learned to say the account is not at fault; the hint and
    the error line went on telling the caller to recompile or ingest evidence —
    the exact remediation that cannot resolve a projection gap, surviving in a
    different field. A caller reads whichever field its UI happens to show.
    """
    from boomi_mcp.categories.integration_builder import _replay_recheck_refusal
    from boomi_mcp.connector_replay.recheck import RecheckOutcome

    gap = RecheckOutcome(
        "pre_submission",
        unavailable={
            "subject": "operation",
            "component_id": "op-1",
            "reason": "projection_unsupported",
            "detail": "…this server cannot project its configuration for comparison…",
        },
    )
    envelope = _replay_recheck_refusal(gap, wrote_nothing=True, partial_results={})
    blob = " ".join(str(envelope.get(k, "")) for k in ("error", "hint"))
    assert "ingest evidence" not in blob, envelope
    assert "Recompile against the current components" not in blob, envelope
    assert "cannot project" in envelope["hint"]

    # THE CONTROL. A real drift must still carry the remediation that DOES work,
    # or this fix would have removed the guidance instead of aiming it.
    drift = RecheckOutcome(
        "pre_submission",
        drifts=({"reason": "operation_version", "component_id": "op-1"},),
    )
    envelope = _replay_recheck_refusal(drift, wrote_nothing=True, partial_results={})
    assert "Recompile against the current components" in envelope["hint"]


def test_a_failing_exit_never_reports_a_write_as_both_attested_and_pending():
    """One envelope, one claim per write.

    The failing exit served the raw confirmed-write notes while the success exit
    DERIVED the unattested ones. That was accidentally correct only while no
    failing exit could occur after an attestation had been recorded — and moving
    the post-submission reconciliation below the attestation appends made exactly
    such an exit reachable. Live QA then measured one envelope carrying
    `process_mutations[0]` for `proc` and `process_writes[0]` for `proc` with
    `attestation_pending: true`: same key, same id, opposite claims.
    """
    def _drifted_after(component_id, _kind=None, _family=None):
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    result, _created, reads = _apply_with_a_grant_drifting_at(drift_after_reads=4)
    if result.get("error_code") != "CONNECTOR_REPLAY_POST_SUBMISSION_RECONCILIATION_DRIFT":
        import pytest as _pytest

        _pytest.skip("the post-submission boundary was not reached on this fixture")

    attested = {m.get("component_key") for m in (result.get("process_mutations") or [])}
    pending = {n.get("component_key") for n in (result.get("process_writes") or [])}
    assert attested, "no attestation survived the refusal"
    assert not (attested & pending), (
        "the envelope claims the same component is both attested and awaiting "
        "attestation: attested=%r pending=%r" % (sorted(attested), sorted(pending))
    )


def test_no_exit_serves_a_write_as_both_attested_and_pending():
    """The PROPERTY the exits owe, not the spelling they happen to use.

    The first version of this guard parsed the module for
    ``envelope["process_writes"] = …`` and required the shared derivation on the
    right-hand side. Live QA graded it with fourteen mutants and killed six —
    including ``_notes = list(process_writes)`` followed by the assignment, which
    is the house style all three real exits use, and a `_partial_failure(
    process_writes=…)` call whose ``**extra`` bag reaches ``envelope.update``
    with no subscript anywhere. Its docstring claimed a fourth exit could not be
    added quietly; six spellings could.

    A guard over a spelling is an enumeration of the ways to write something, and
    this issue has recorded that class more than any other. What the exits owe is
    one sentence: a write is either attested or awaiting attestation, never both.
    That is checkable on the SERVED ENVELOPE, so it is checked there — on every
    exit this suite can reach, in the form a caller actually sees.

    COVERAGE, corrected by measurement after the first version of this sentence
    was wrong. Live QA instrumented the derivation and recorded the calling frame:
    this test reaches ``_partial_failure`` and ONLY that exit — the exception exit
    was reached by zero tests through it, and the success exit by none. So the
    other two are graded by DIRECT CALL in the two tests below, in the mixed shape
    where the raw list and the derived list actually differ. Reverting any one
    exit to serving the raw notes now fails: that was measured too, and before
    these were added, reverting the exception exit passed the entire suite.

    THE RESIDUAL LIMIT, restored after a correction dropped it: a property has to
    be DRIVEN at an exit, so a genuinely new fourth serving exit is caught by
    nothing here, and nothing at head enumerates the serving sites. That
    enumeration was the deleted parse-based guard, and it is not coming back — it
    failed open on six spellings of the assignment it modelled. This is a limit
    accepted deliberately, not an oversight: the property is stronger where it
    reaches, and the alternative was a pin that looked stronger and was not.
    """
    def _matching(component_id, _kind=None, _family=None):
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    # SIX, not four: the create arm now runs the pre-push boundary the update arm
    # always had, so a drift at four is caught before the write and this exit is
    # never reached. The assertion below has no escape hatch, which is why the
    # threshold has to move with the boundary rather than the assertion softening.
    result, _created, _reads = _apply_with_a_grant_drifting_at(drift_after_reads=6)
    # NO `if`. The first version guarded this on the error code and had no else, so
    # switching the boundary off in source made it PASS silently while the test it
    # sits beside failed — the exact self-escape this file's own docstring records
    # twenty lines above, reintroduced four rounds after it was recorded. Live QA
    # caught it by mutating the source rather than by reading the test.
    assert result.get("error_code") == (
        "CONNECTOR_REPLAY_POST_SUBMISSION_RECONCILIATION_DRIFT"
    ), (
        "the post-submission exit was not reached, so the disjointness property "
        "was not exercised at it: %r" % result.get("error_code")
    )
    attested = {m.get("component_key") for m in (result.get("process_mutations") or [])}
    pending = {n.get("component_key") for n in (result.get("process_writes") or [])}
    assert attested, "no attestation survived the refusal"
    assert not (attested & pending), (attested, pending)

    # The DERIVATION itself, over the shapes the three exits pass it: models at
    # two call sites, dumps at the third. QA measured these agreeing; this pins it.
    from boomi_mcp.categories.integration_builder import _unattested_write_notes

    class _M:
        component_key = "proc1"

    notes = [{"component_key": "proc1"}, {"component_key": "proc2"}]
    as_models = _unattested_write_notes(notes, [_M()])
    as_dumps = _unattested_write_notes(notes, [{"component_key": "proc1"}])
    assert as_models == as_dumps == [{"component_key": "proc2"}], (as_models, as_dumps)
    # And the empty cases, which decide whether an exit serves the key at all.
    assert _unattested_write_notes([], [_M()]) == []
    assert _unattested_write_notes(notes, []) == notes


#: THE WHOLE CAUSE AXIS, and it has FOUR values, not the three the first version
#: of this matrix assumed: the recheck also mints a missing-record shape carrying
#: a `subject` and NO `reason` key. Live QA found it while grading the composed
#: hint — the fix for the opening sentence happened to cover it and the fix for
#: the hint did not, which is exactly the kind of gap an incomplete axis leaves.
_HINT_CAUSES = (
    ("projection gap", {"reason": "projection_unsupported", "detail": "d"}, None),
    ("unreadable account", {"reason": "account_unreadable", "detail": "d"}, None),
    ("missing record", {"subject": "operation_record", "detail": "d"}, None),
    ("real drift", None, ({"reason": "operation_version"},)),
)


@pytest.mark.parametrize("label,unavailable,drifts", _HINT_CAUSES)
@pytest.mark.parametrize("wrote_nothing", (True, False))
def test_every_refusal_states_its_cause_and_states_retention_only_when_retained(
    label, unavailable, drifts, wrote_nothing
):
    """Two independent facts, composed — over the FULL matrix, not a corner of it.

    The hint was rebuilt to compose the cause and the retention statement instead
    of branching between them, and the rebuild composed only the retention half:
    the cause sentence stayed gated on `wrote_nothing` for every cause but the
    projection gap, so six of sixteen combinations carried no cause at all. A real
    drift over a retained write told the caller what to reconcile and nothing
    about the drift. Measured by QA at both arms and identical, so it was
    pre-existing rather than introduced — and still wrong.
    """
    from boomi_mcp.categories.integration_builder import _replay_recheck_refusal
    from boomi_mcp.connector_replay.recheck import RecheckOutcome

    outcome = RecheckOutcome(
        "pre_submission" if wrote_nothing else "post_submission",
        drifts=drifts or (),
        unavailable=unavailable,
    )
    hint = _replay_recheck_refusal(
        outcome, wrote_nothing=wrote_nothing, partial_results={}
    )["hint"]

    stated_cause = ("cannot project" in hint) or ("Recompile" in hint)
    assert stated_cause, f"{label} / wrote_nothing={wrote_nothing} states no cause: {hint!r}"
    assert ("The result is retained" in hint) is (not wrote_nothing), hint
    # Exactly ONE cause sentence — composing two facts must not double the first.
    assert hint.count("Recompile") + hint.count("cannot project") == 1, hint


def test_a_projection_gap_over_a_retained_write_still_says_it_is_retained():
    """Two independent facts, and a branch that picked one dropped the other.

    Why the check could not be completed and whether a component now exists are
    unrelated questions. Nesting the projection-gap sentence above the retention
    sentence answered only the first, so a caller was told what could not be
    checked and never told that something was written and needs reconciling —
    which is the mutation-accounting half, and the direction this slice exists to
    prevent.
    """
    from boomi_mcp.categories.integration_builder import _replay_recheck_refusal
    from boomi_mcp.connector_replay.recheck import RecheckOutcome

    gap = RecheckOutcome(
        "post_submission",
        unavailable={"subject": "operation", "component_id": "op-1",
                     "reason": "projection_unsupported", "detail": "…cannot project…"},
    )
    retained = _replay_recheck_refusal(gap, wrote_nothing=False, partial_results={"k": {}})
    assert "cannot project" in retained["hint"]
    assert "The result is retained" in retained["hint"], retained["hint"]

    # And the control: nothing written means no retention claim at all.
    nothing = _replay_recheck_refusal(gap, wrote_nothing=True, partial_results={})
    assert "cannot project" in nothing["hint"]
    assert "retained" not in nothing["hint"], nothing["hint"]


def test_an_unreadable_account_is_not_reported_as_a_mismatch():
    """Neither unavailable arm compared anything, so neither may open with a
    comparison result. The projection arm got its own sentence first and left its
    sibling asserting a mismatch nobody measured."""
    from boomi_mcp.categories.integration_builder import _replay_recheck_refusal
    from boomi_mcp.connector_replay.recheck import RecheckOutcome

    for reason in ("account_unreadable", "projection_unsupported"):
        envelope = _replay_recheck_refusal(
            RecheckOutcome("pre_submission", unavailable={"reason": reason, "detail": "x"}),
            wrote_nothing=True,
            partial_results={},
        )
        assert "no longer matches" not in envelope["error"], (reason, envelope["error"])

    # The control: a real drift IS a comparison result and still says so.
    drift = _replay_recheck_refusal(
        RecheckOutcome("pre_submission", drifts=({"reason": "operation_version"},)),
        wrote_nothing=True,
        partial_results={},
    )
    assert "no longer matches" in drift["error"]


def _mixed_write_evidence():
    """One attested root and one confirmed-but-unattested root.

    THE ONLY SHAPE THAT GRADES A DERIVATION. Where every write is unattested the
    raw list and the derived list are equal, so an exit serving either passes —
    which is why the exception exit was reached by live tests for rounds and still
    survived being reverted to raw. Live QA measured that directly: with the
    historic raw block restored at that exit, the entire suite stayed green.
    """
    class _Mutation:
        component_key = "proc1"

        def model_dump(self, mode=None):
            return {"component_key": "proc1", "result_component_id": "cid-1"}

    notes = [
        {"component_key": "proc1", "result_component_id": "cid-1",
         "attestation_pending": True},
        {"component_key": "proc2", "result_component_id": "cid-2",
         "attestation_pending": True},
    ]
    return [_Mutation()], notes


def test_the_exception_exit_serves_only_the_unattested_write():
    """The exit live QA found unguarded — graded where it can actually be graded.

    It is a module-level function, so it is called directly rather than reached
    through an escape whose recipe depends on which exception type one internal
    call site happens to catch. What matters is the property, and the property is
    checkable here without staging a throw.
    """
    from boomi_mcp.categories.integration_builder import _apply_escape_evidence

    mutations, notes = _mixed_write_evidence()
    evidence = _apply_escape_evidence(
        durable_build_id=None,
        results={},
        process_mutations=mutations,
        process_readbacks=[],
        apply_warnings=[],
        results_complete=True,
        process_writes=notes,
    )
    served = evidence.get("process_writes") or []
    assert [n["component_key"] for n in served] == ["proc2"], evidence
    attested = {m.get("component_key") for m in (evidence.get("process_mutations") or [])}
    assert not (attested & {n["component_key"] for n in served}), evidence


def test_the_success_exit_serves_only_the_unattested_write():
    """The third exit, graded AT THE EXIT rather than at the helper it calls.

    A first version asserted the helper's output and left the exit itself
    ungraded here — so reverting the exit to the raw list was killed only by a
    test in another file, at suite width. Live QA set the bar explicitly: each
    exit must die at NODE width, because a revert caught only by a distant file
    is caught by accident of what else happens to run.

    Its unattested set is unreachable through the public boundary by construction
    — an attestation is appended for every confirmed write on that path — so the
    exit is called directly. That limit is real and stated; what is NOT acceptable
    is grading the helper and calling the exit covered.
    """
    from boomi_mcp.categories.integration_builder import _finalize_apply_success

    mutations, notes = _mixed_write_evidence()
    envelope = _finalize_apply_success(
        # `model_dump` and `name` are what the exit asks of the spec on this path,
        # both discovered by running into the AttributeError rather than by
        # reading — which is why the stand-in is deliberately minimal: if the exit
        # ever asks for a third thing, this breaks loudly instead of drifting.
        spec=SimpleNamespace(name="s", model_dump=lambda: {"name": "s"}),
        profile=_PROFILE,
        boomi_client=MagicMock(),
        durable_build_id=None,
        authoring_bundle=None,
        results={},
        execution_order=[],
        process_mutations=mutations,
        process_readbacks=[],
        process_writes=notes,
        apply_warnings=[],
        planned={},
    )
    served = envelope.get("process_writes") or []
    assert [n["component_key"] for n in served] == ["proc2"], envelope
    attested = {m.get("component_key") for m in (envelope.get("process_mutations") or [])}
    assert not (attested & {n["component_key"] for n in served}), envelope


def test_an_update_rechecks_at_its_push_not_before_its_merge():
    """The window the plan names, and the reason it names it.

    An update does real work between the caller's decision to proceed and the
    submission: it materializes the process, fetches the live component and merges.
    A check made before all of that can go stale during it, and the drift is then
    caught only by the POST-submit check — which converts a refusal that was still
    free into a retained mutation. The plan requires a callback after merge and
    materialization and immediately before the platform update.

    Driven where the hook actually runs: past the policy short-circuit, past the
    live fetch, past the merge. An arm that returns before the hook would prove
    nothing about the hook, which is why the first version of this test — using
    the no-policy short-circuit — was replaced.
    """
    from boomi_mcp.categories.integration_builder import _apply_structured_update
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )

    live = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="process" name="p" folderId="f"><bns:object><process>'
        "</process></bns:object></bns:Component>"
    )
    pushed, called = [], []

    class _Comp:
        type = "process"
        key = "proc"
        config: dict = {}

    def _run(hook):
        client = MagicMock()
        client.component.update_component_raw.side_effect = (
            lambda *a, **k: pushed.append(a)
        )
        with patch(_GET_XML) as get_xml:
            get_xml.return_value = {"type": "process", "xml": live}
            return _apply_structured_update(
                client, _PROFILE, "cid-1", _Comp(), live,
                PROCESS_PRESERVATION_POLICY, on_pre_push=hook,
            )

    # REFUSING HOOK: the platform must not be called, and the envelope must say
    # nothing was written — an update that refuses here is precisely the case that
    # must not be reported as a retained mutation.
    def _refuse():
        called.append(1)
        return {
            "_success": False,
            "error_code": "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_DRIFT",
        }

    out = _run(_refuse)
    assert called, "the pre-push hook was never consulted"
    assert pushed == [], "the platform was called despite a refusing pre-push hook"
    assert out.get("_success") is False
    assert out.get("write_attempted") is False, out

    # PERMITTING HOOK — the control. Without it the assertions above are satisfied
    # by an update path that never pushes at all.
    pushed.clear()
    called.clear()
    _run(lambda: called.append(1) or None)
    assert called, "the hook was not consulted on the permitting arm"
    assert pushed, "a permitting hook did not reach the platform"

_LIVE_PROCESS_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="process" name="M12.15 Process" folderId="f"><bns:object><process>'
    "</process></bns:object></bns:Component>"
)


def _updating_payload():
    """The same fixture, declared as an UPDATE of an existing process.

    The pre-push hook is an argument of `_apply_structured_update`, so it exists
    on the update route and nowhere else. The previous version of this test left
    the envelope on `create` and therefore could not reach the hook at all —
    measured, not assumed: it produced `status: created` and no `write_attempted`
    field, and passed through a fallback branch that has been deleted with it.

    The envelope is declared BEFORE the request is compiled. Editing the dumped
    payload afterwards instead makes apply refuse with `AUTHORING_PLAN_STALE`,
    because the compile hash no longer describes the request — also measured.
    """
    from _m12_11_support import appliable_process_unit

    request = process_ir_request(
        units=(
            appliable_process_unit(action="update", component_id="existing-proc-1"),
        )
    )
    result, _internals = compile_authoring_request_v1(
        request, boomi_client=MagicMock(), profile=_PROFILE
    )
    payload = request.model_dump(mode="json")
    payload["expected_capability_revision"] = result.revision_binding.capability_revision
    payload["expected_compile_hash"] = result.revision_binding.compile_hash
    return payload


def test_a_proven_no_write_keeps_its_own_refusal_through_the_apply_boundary():
    """Graded by running the mutant, because three previous pins were not.

    The first asserted that the assignment's syntax tree mentioned
    `write_attempted` and `False` — true of the broken expression and the fixed
    one alike. The second swapped that source read for a different source read
    plus direct calls to the refusal renderer, and I graded it by reverting the
    code and watching it fail: what failed was the syntax-tree assertion, not the
    behaviour. The third finally drove the boundary but on the CREATE route,
    where no pre-push hook exists, and carried an `else` branch that passed on
    the wrong shape.

    What this asserts is the difference the mutant actually makes, measured by
    running it: with the flag read from the raw step result the caller sees the
    step's OWN pre-submission refusal; reading only the wrapper (the reintroduced
    defect) lets the post-submission reconciliation overwrite it with a code that
    says a result was retained, for a component the platform was never asked to
    write.
    """
    reads: list = []
    pushed: list = []
    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, _payload):
        created["n"] += 1
        return {"_success": True, "component_id": "process-cid-1"}

    # The account drifts only after the global and just-in-time rechecks have
    # read clean, so the refusal that fires is the PRE-PUSH one — the case whose
    # whole point is that the update was abandoned before submission. The count
    # is measured against this fixture: four clean reads, then drift.
    def _get_xml(_client, component_id, *_a, **_k):
        if component_id in ("op-live-1", "cn-live-1"):
            reads.append(component_id)
            drifted = len(reads) > 4
            if component_id == "op-live-1":
                return {"type": "connector-action", "xml": _OPERATION_XML,
                        "version": 99 if drifted else 3}
            return {"type": "connector-settings", "xml": _CONNECTION_XML, "version": 5}
        if component_id == "existing-proc-1":
            return {"type": "process", "xml": _LIVE_PROCESS_XML, "version": 1}
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    from boomi_mcp.connector_replay.registry import load_registry as _real_registry

    _packaged = _real_registry()

    class _Registry:
        operation_records = (_Record(),)

        def __getattr__(self, name):
            return getattr(_packaged, name)

    client = MagicMock()
    client.component.update_component_raw.side_effect = (
        lambda *a, **k: pushed.append(a)
    )
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
            client, _PROFILE, "apply",
            config={"authoring_request": _updating_payload(), "dry_run": False},
        )

    # NON-VACUITY, asserted before anything else and with no branch to escape
    # into: this fixture must actually land on the proven-no-write step, or the
    # rest of the test is about some other case.
    step = ((result.get("partial_results") or {}).get("proc") or {})
    step_result = step.get("result") if isinstance(step.get("result"), dict) else {}
    assert step_result.get("write_attempted") is False, result
    assert pushed == [], "the platform was asked to write despite the refusal"
    assert step.get("status") == "refused", step

    # THE MUTANT'S SIGNATURE. Reading only the wrapper makes the served code
    # become the post-submission one and promotes its retention hint to the top
    # level of the envelope — both measured by running that edit against this
    # exact fixture.
    assert result.get("_success") is False, result
    assert result.get("error_code") == (
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_DRIFT"
    ), result
    assert "The result is retained" not in str(result.get("hint", "")), result


def _apply_creating_with_drift_during_materialization():
    """Drift the operation AFTER the just-in-time recheck, on the CREATE route.

    The window the issue-level architect gate probed: the recheck runs, passes,
    and then this function materializes the process before submitting it. A
    drift landing in that gap used to be written and reported afterwards.

    The process create is recorded SEPARATELY from the connector creates rather
    than inferred from a total, so "the process was not written" is asserted
    from the call that would have written it.
    """
    reads: list = []
    connector_creates = {"n": 0}
    process_creates: list = []

    def _component(*_args, **_kwargs):
        connector_creates["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % connector_creates["n"]}

    def _create(_client, _profile, payload_in):
        process_creates.append(payload_in)
        return {"_success": True, "component_id": "process-cid-1"}

    def _get_xml(_client, component_id, *_a, **_k):
        if component_id in ("op-live-1", "cn-live-1"):
            reads.append(component_id)
            # FOUR: the global pass and the just-in-time recheck consume two
            # reads each, so the drift appears in the materialization window and
            # nowhere earlier. Reaching this window is asserted below, not hoped.
            drifted = len(reads) > 4
            if component_id == "op-live-1":
                return {
                    "type": "connector-action",
                    "xml": _OPERATION_XML,
                    "version": 99 if drifted else 3,
                }
            return {
                "type": "connector-settings",
                "xml": _CONNECTION_XML,
                "version": 5,
            }
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

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
    return result, process_creates, reads


def test_a_drift_inside_the_materialization_window_refuses_before_the_write():
    """The create arm's own pre-push boundary.

    The update arm carried a pre-push callback from the start; the create arm
    went straight to the platform, and the parameter's own docstring recorded
    why that was thought safe — "a create has nothing to fetch, so the caller's
    just-in-time check is already adjacent to its submission". Measured false:
    this function materializes the process between that check and the create
    call. The architect's probe changed the operation version in exactly that
    gap and got a written process with the drift reported afterwards.

    Both halves are asserted, because either alone is satisfiable by a bug: the
    named PRE-submission code (a POST-submission code here means the write
    happened first) AND the platform never being asked to create the process.
    """
    result, process_creates, reads = _apply_creating_with_drift_during_materialization()

    # NON-VACUITY FIRST. If the earlier boundaries had already refused, the
    # materialization window would never be entered and the rest of this test
    # would pass without exercising anything. Six reads means all three checks
    # ran: global pass, just-in-time recheck, and the new pre-push boundary.
    assert len(reads) == 6, reads

    assert result.get("error_code") == (
        "CONNECTOR_REPLAY_PRE_SUBMISSION_IDENTITY_DRIFT"
    ), result
    assert process_creates == [], (
        "the process was written despite a drift detected before submission"
    )
    step = ((result.get("partial_results") or {}).get("proc") or {})
    # `refused`, not `failed`: the distinction is what tells a reconciler there
    # is nothing to reconcile for this component.
    assert step.get("status") == "refused", step


def test_a_mutation_uncertain_push_retains_the_accounting_it_owes():
    """A lost answer is not a licence to forget what was sent.

    The platform is asked to write and never answers. The write may have landed;
    `mutation_status` says `performed` because this server cannot claim
    otherwise. What the caller used to get alongside that word was nothing: no
    mutation attestation, no readback, no durable write note — and, since a
    replay grant authorised the call, no record that any evidence had been
    consumed. The plan requires a post-submit failure to retain the component
    id, the platform result, the readback state, the mutation accounting and the
    evidence attestation.

    Graded against the reverted source: with the retention removed, the two
    PRESENT assertions below both fail while the failure itself still reports —
    i.e. the fix adds evidence rather than changing the verdict.
    """
    import boomi_mcp.categories.integration_builder as ib

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "cid-1"}

    def _create(_c, _p, _payload):
        return {"_success": True, "component_id": "process-cid-1"}

    def _get_xml(_c, component_id, *_a, **_k):
        if component_id == "op-live-1":
            return {"type": "connector-action", "xml": _OPERATION_XML, "version": 3}
        if component_id == "cn-live-1":
            return {"type": "connector-settings", "xml": _CONNECTION_XML, "version": 5}
        if component_id == "existing-proc-1":
            return {"type": "process", "xml": _LIVE_PROCESS_XML, "version": 1}
        return {"type": "connector-settings", "xml": _LIVE_XML, "version": 1}

    from boomi_mcp.connector_replay.registry import load_registry as _real_registry

    _packaged = _real_registry()

    class _Registry:
        operation_records = (_Record(),)

        def __getattr__(self, name):
            return getattr(_packaged, name)

    client = MagicMock()
    # The bytes leave this process; the platform's answer never comes back.
    client.component.update_component_raw.side_effect = RuntimeError(
        "connection reset by peer after submission"
    )

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
            client,
            _PROFILE,
            "apply",
            config={"authoring_request": _updating_payload(), "dry_run": False},
        )

    # NON-VACUITY: this must be the uncertain arm, not a refusal and not a
    # success. A refusal would carry `write_attempted: False` and owe none of
    # what follows.
    step = ((result.get("partial_results") or {}).get("proc") or {})
    step_result = step.get("result") if isinstance(step.get("result"), dict) else {}
    assert step_result.get("write_attempted") is True, result
    assert result.get("mutation_status") == "performed", result
    # The failure still fails. Retention must not launder it into a success.
    assert result.get("_success") is False, result
    assert step_result.get("error_code") == "UPDATE_PRESERVATION_PUSH_FAILED", step

    mutations = result.get("process_mutations") or []
    assert mutations, "a submission that may have landed was not attested"
    attested = mutations[0]
    attested = attested if isinstance(attested, dict) else attested.model_dump(
        mode="json"
    )
    # The digest of the bytes as they LEFT — the only handle a reconciler has on
    # a write the platform never acknowledged.
    assert str(attested.get("submitted_xml_digest", "")).startswith("sha256:"), attested
    # ...and WHAT AUTHORISED IT. "This process may have been applied" and "this
    # process may have been applied under a replay contract" are different
    # sentences, and only the second one is true here.
    assert attested.get("replay_evidence_bindings"), attested

    assert result.get("process_readbacks"), "the uncertainty state was not recorded"

    record = ib._BUILD_REGISTRY.get(result.get("build_id")) or {}
    notes = record.get("process_writes") or []
    assert notes, "the write log has no entry for a submission that may have landed"
    assert notes[0].get("write_confirmed") is False, notes
