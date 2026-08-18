"""#146 (M12.11): a stale or mismatched binding can never mutate.

The acceptance criterion is that apply "cannot proceed from a fatal validation
report or stale/mismatched semantic revision; it validates before the first
mutation". Every negative case here asserts BOTH halves: the right refusal code,
and a mutation spy that recorded nothing.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import (  # noqa: E402
    UNRESOLVABLE_IR_DOC,
    MutationSpy,
    appliable_request,
    process_ir_request,
)
from boomi_mcp.authoring.revisions import (  # noqa: E402
    account_scope_fingerprint,
    canonical_json_bytes,
    sha256_fingerprint,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    compile_authoring_request_v1,
    preflight_typed_apply_v1,
)
from boomi_mcp.errors import (  # noqa: E402
    AUTHORING_APPLY_VALIDATION_REQUIRED,
    AUTHORING_CAPABILITY_REVISION_MISMATCH,
    AUTHORING_PLAN_STALE,
)
from boomi_mcp.models.authoring_workflow import AuthoringRequestV1  # noqa: E402


@pytest.fixture
def spy(monkeypatch):
    return MutationSpy().install(monkeypatch)


#: These tests exercise the BINDING, so their fixture is a component-only plan:
#: it keeps every case below about hash comparison rather than about emitting a
#: process. (Until #153 there was a second, stronger reason — a direct ProcessIR
#: intent could not be applied at all — and that is no longer true.)
_appliable_request = appliable_request


def _binding(profile="qa_profile", account_id="qa_account"):
    result, _ = compile_authoring_request_v1(
        _appliable_request(), profile=profile, account_id=account_id
    )
    return result.revision_binding


def _bound_request(binding, **overrides):
    fields = {
        "expected_capability_revision": binding.capability_revision,
        "expected_compile_hash": binding.compile_hash,
    }
    fields.update(overrides)
    return AuthoringRequestV1(intent=_appliable_request().intent, **fields)


# ---------------------------------------------------------------------------
# the fingerprint primitives
# ---------------------------------------------------------------------------


def test_canonical_json_is_insertion_order_independent():
    assert canonical_json_bytes({"b": 1, "a": {"d": 2, "c": 3}}) == canonical_json_bytes(
        {"a": {"c": 3, "d": 2}, "b": 1}
    )


def test_canonical_json_refuses_a_non_finite_float():
    """Hashing ``NaN`` would silently produce a digest no comparison can match."""
    with pytest.raises(ValueError):
        canonical_json_bytes({"x": float("nan")})


def test_canonical_json_refuses_an_unserializable_object():
    with pytest.raises(TypeError):
        canonical_json_bytes({"x": object()})


def test_every_digest_has_one_spelling():
    digest = sha256_fingerprint({"a": 1})
    assert digest.startswith("sha256:")
    assert len(digest) == len("sha256:") + 64
    assert digest[7:] == digest[7:].lower()


def test_the_account_scope_hash_leaks_neither_input():
    scope = account_scope_fingerprint("my_secret_profile", "ACCT-123456")
    assert "my_secret_profile" not in scope
    assert "ACCT-123456" not in scope
    assert scope == account_scope_fingerprint("my_secret_profile", "ACCT-123456")


def test_the_scope_is_the_account_not_the_profile_alias():
    """A profile is an ALIAS; the account is the security boundary.

    Keying on the name got both directions wrong (issue #146 QA, bug #408): two
    profiles addressing one account produced different bindings, so a valid
    compile was refused with a false "stale plan"; and a profile repointed at a
    different account would have kept its binding.
    """
    same_account_two_profiles = {
        account_scope_fingerprint("profile_a", "ACCT-1"),
        account_scope_fingerprint("profile_b", "ACCT-1"),
    }
    assert len(same_account_two_profiles) == 1

    assert account_scope_fingerprint("profile_a", "ACCT-1") != (
        account_scope_fingerprint("profile_a", "ACCT-2")
    )


def test_the_profile_fallback_cannot_collide_with_a_real_account_scope():
    """With no account id the profile is used — and the payload says which,
    so a profile literally named like an account id still hashes differently."""
    fallback = account_scope_fingerprint("ACCT-1", None)
    real = account_scope_fingerprint("anything", "ACCT-1")
    assert fallback != real
    assert account_scope_fingerprint("profile_a", None) != (
        account_scope_fingerprint("profile_b", None)
    )


# ---------------------------------------------------------------------------
# the apply gate
# ---------------------------------------------------------------------------


def test_a_correctly_bound_apply_passes_preflight(spy):
    """Guard the guard: if nothing passed, every refusal below would be vacuous."""
    binding = _binding()
    bundle = preflight_typed_apply_v1(
        _bound_request(binding).model_dump(mode="json"),
        profile="qa_profile",
        account_id="qa_account",
    )
    assert bundle.integration_spec.name == "M12.11 Applied"
    assert bundle.compile_result.mutation_performed is False
    assert spy.calls == []


def test_an_apply_with_no_binding_is_refused(spy):
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            _appliable_request().model_dump(mode="json"),
            profile="qa_profile",
            account_id="qa_account",
        )
    error = excinfo.value
    assert error.code == AUTHORING_APPLY_VALIDATION_REQUIRED
    assert sorted(d.subject_id for d in error.diagnostics) == [
        "expected_capability_revision",
        "expected_compile_hash",
    ]
    assert spy.calls == []


@pytest.mark.parametrize(
    "missing", ["expected_capability_revision", "expected_compile_hash"]
)
def test_a_half_bound_apply_is_refused(spy, missing):
    binding = _binding()
    request = _bound_request(binding, **{missing: None})
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            request.model_dump(mode="json"),
            profile="qa_profile",
            account_id="qa_account",
        )
    assert excinfo.value.code == AUTHORING_APPLY_VALIDATION_REQUIRED
    assert [d.subject_id for d in excinfo.value.diagnostics] == [missing]
    assert spy.calls == []


def test_a_capability_revision_mismatch_takes_precedence(spy):
    """When the SERVER's contract moved, every downstream hash differs too.

    Reporting a stale plan would send the caller to re-plan against a surface
    they have not rediscovered — so the capability code wins (ADR-001 §7).
    """
    binding = _binding()
    request = _bound_request(
        binding, expected_capability_revision="sha256:" + "0" * 64
    )
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            request.model_dump(mode="json"),
            profile="qa_profile",
            account_id="qa_account",
        )
    assert excinfo.value.code == AUTHORING_CAPABILITY_REVISION_MISMATCH
    assert [d.subject_id for d in excinfo.value.diagnostics] == ["capability_revision"]
    assert spy.calls == []


def test_a_stale_compile_hash_is_refused(spy):
    binding = _binding()
    request = _bound_request(binding, expected_compile_hash="sha256:" + "1" * 64)
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            request.model_dump(mode="json"),
            profile="qa_profile",
            account_id="qa_account",
        )
    assert excinfo.value.code == AUTHORING_PLAN_STALE
    assert [d.subject_id for d in excinfo.value.diagnostics] == ["compile_hash"]
    assert spy.calls == []


def test_a_stale_plan_hash_is_refused(spy):
    binding = _binding()
    request = _bound_request(binding, expected_plan_hash="sha256:" + "2" * 64)
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            request.model_dump(mode="json"),
            profile="qa_profile",
            account_id="qa_account",
        )
    assert excinfo.value.code == AUTHORING_PLAN_STALE
    assert spy.calls == []


def test_a_binding_minted_in_another_profile_cannot_be_replayed(spy):
    """The scope is bound TRANSITIVELY: account_scope_hash is a field of the
    revision binding, the binding is hashed into plan_hash, and plan_hash is
    hashed into compile_hash. So a cross-profile replay fails as a compile-hash
    mismatch — correctly, because in the target scope that compile was never
    produced."""
    binding = _binding(profile="profile_a", account_id="account_a")
    request = _bound_request(binding)
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            request.model_dump(mode="json"),
            profile="profile_b",
            account_id="account_b",
        )
    assert excinfo.value.code == AUTHORING_PLAN_STALE
    assert spy.calls == []
    # ...and it DOES work in the scope it was minted in, so the refusal above is
    # about the scope and not about the payload.
    assert preflight_typed_apply_v1(
        request.model_dump(mode="json"), profile="profile_a", account_id="account_a"
    )


def test_a_compile_blocked_payload_cannot_reach_apply(spy):
    binding = _binding()
    request = AuthoringRequestV1(
        intent=process_ir_request(UNRESOLVABLE_IR_DOC).intent,
        expected_capability_revision=binding.capability_revision,
        expected_compile_hash=binding.compile_hash,
    )
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            request.model_dump(mode="json"),
            profile="qa_profile",
            account_id="qa_account",
        )
    assert excinfo.value.code == AUTHORING_APPLY_VALIDATION_REQUIRED
    causes = {c for d in excinfo.value.diagnostics for c in d.cause_codes}
    assert "AUTHORING_COMPILE_BLOCKED" in causes
    assert spy.calls == []


def test_a_payload_edited_after_compile_is_refused(spy):
    """The apply preflight RE-PARSES the raw payload rather than trusting an
    object handed down from an earlier phase — because "the payload changed
    after it was checked" is the failure that matters before a mutation."""
    binding = _binding()
    payload = _bound_request(binding).model_dump(mode="json")
    payload["intent"]["integration_spec"]["name"] = "Tampered Name"
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            payload, profile="qa_profile", account_id="qa_account"
        )
    assert excinfo.value.code == AUTHORING_PLAN_STALE
    assert spy.calls == []


def test_a_component_rename_between_compile_and_apply_is_refused(spy):
    """Reference evidence is part of the plan hash, so a changed component plan
    moves the binding."""
    binding = _binding()
    payload = _bound_request(binding).model_dump(mode="json")
    payload["intent"]["integration_spec"]["components"][0]["name"] = "Renamed"
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        preflight_typed_apply_v1(
            payload, profile="qa_profile", account_id="qa_account"
        )
    assert excinfo.value.code == AUTHORING_PLAN_STALE
    assert spy.calls == []
