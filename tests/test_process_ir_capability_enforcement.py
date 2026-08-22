"""#177 invariant 2: the capability manifest is checked against the enforcement.

`PROCESS_IR_V1_CAPABILITIES` is served to callers and mirrored in `PROCESS_IR_V1.md` §8.
It promises, per key, that a construct is admitted (`supported`), refused (`gated`), or
outside the contract (`unsupported`). Nothing checked any of that, so a capability could be
advertised without an enforcement path or gated without a refusal — DC-175-D read at the
capability level.

The gate is TWO-DIRECTIONAL and MANIFEST-KEYED:

* every manifest key must have a witness, and every witness must name a manifest key
  (`==`, not `<=`), so a new capability with no witness fails and a retired one leaves a
  stale witness that fails;
* the manifest's own live state selects which KIND of witness is required, so flipping a
  capability fails until its witness is deliberately rewritten.

The registry is keyed by the manifest rather than enumerated over today's placement
matrix on purpose: #154 rewrites that matrix, and an enumerated pin would be rewritten
along with the thing it exists to police.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _process_ir_capability_witnesses import (  # noqa: E402
    CAPABILITY_WITNESSES,
    CapabilityWitness,
    UnsupportedDisposition,
)
from boomi_mcp.models.process_ir import PROCESS_IR_V1_CAPABILITIES  # noqa: E402

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"

#: Which witness kind each manifest state requires. Deriving the partition from the LIVE
#: manifest — rather than storing a state beside each witness — is what makes a capability
#: flip fail here instead of passing against a stale copy.
_REQUIRED_KIND = {"supported": "admits", "gated": "refuses"}


def _partitions():
    """The manifest's own partitions, and a hard failure on an unknown state.

    The state domain is owned by `tests/test_process_ir_models.py`; this does not re-type
    it, it just refuses to guess at anything outside `_REQUIRED_KIND` plus `unsupported`.
    """
    partitions = {}
    unknown = []
    for key, state in PROCESS_IR_V1_CAPABILITIES.items():
        if state in _REQUIRED_KIND or state == "unsupported":
            partitions.setdefault(state, set()).add(key)
        else:
            unknown.append((key, state))
    assert unknown == [], unknown
    return partitions


def test_every_manifest_capability_has_a_witness_and_vice_versa():
    """The bidirectional pin — asserted BEFORE anything is executed.

    A witness set that merely covered the manifest would let a renamed key drop out of
    coverage silently; a manifest key with no witness would simply not be tested. Compared
    whole, in both directions.
    """
    assert set(CAPABILITY_WITNESSES) == set(PROCESS_IR_V1_CAPABILITIES), {
        "manifest keys with no witness": sorted(
            set(PROCESS_IR_V1_CAPABILITIES) - set(CAPABILITY_WITNESSES)
        ),
        "witnesses for no manifest key": sorted(
            set(CAPABILITY_WITNESSES) - set(PROCESS_IR_V1_CAPABILITIES)
        ),
    }


def test_every_witness_kind_matches_the_live_manifest_state():
    """A capability flip must FAIL until its witness is rewritten.

    This is the half that makes the registry manifest-KEYED rather than merely
    manifest-sized: the witness declares what it observes, the manifest declares what is
    promised, and the two are reconciled here rather than in a copied constant.
    """
    partitions = _partitions()
    assert partitions, "the manifest is empty — every assertion here would be vacuous"

    mismatched = []
    for key, state in sorted(PROCESS_IR_V1_CAPABILITIES.items()):
        entry = CAPABILITY_WITNESSES[key]
        if state == "unsupported":
            if not isinstance(entry, UnsupportedDisposition):
                mismatched.append((key, state, type(entry).__name__))
            elif not entry.reason.strip():
                mismatched.append((key, state, "blank reason"))
            continue
        if not isinstance(entry, CapabilityWitness):
            mismatched.append((key, state, type(entry).__name__))
        elif entry.kind != _REQUIRED_KIND[state]:
            mismatched.append((key, state, entry.kind))
    assert mismatched == [], mismatched


def test_every_supported_capability_is_reachable():
    """Every `supported` row is admitted, and its own feature is observed.

    Each witness asserts a FEATURE-SPECIFIC fact — two connector families on one path, a
    process call that is a graph exit, a nested Decision, `retry_count == 5` — so a witness
    cannot pass merely by compiling some valid document.
    """
    supported = _partitions().get("supported", set())
    assert supported, "no supported capabilities — this test would be vacuous"

    admitted = set()
    failures = []
    for key in sorted(supported):
        witness = CAPABILITY_WITNESSES[key]
        try:
            witness.observe(witness.run())
        except Exception as exc:  # noqa: BLE001 - aggregated and re-raised below
            failures.append((key, "{0}: {1}".format(type(exc).__name__, exc)))
            continue
        admitted.add(key)

    assert failures == [], failures
    # The coverage claim, computed from the run rather than typed: every supported key in
    # the manifest was executed and observed.
    assert admitted == supported, sorted(supported - admitted)


def test_every_gated_capability_is_refused():
    """Every `gated` row is refused, with the exact code and pointer it serves."""
    gated = _partitions().get("gated", set())
    assert gated, "no gated capabilities — this test would be vacuous"

    refused = set()
    failures = []
    for key in sorted(gated):
        witness = CAPABILITY_WITNESSES[key]
        try:
            witness.observe(witness.run())
        except Exception as exc:  # noqa: BLE001 - aggregated and re-raised below
            failures.append((key, "{0}: {1}".format(type(exc).__name__, exc)))
            continue
        refused.add(key)

    assert failures == [], failures
    assert refused == gated, sorted(gated - refused)


def test_every_unsupported_capability_carries_an_explicit_disposition():
    """`unsupported` rows are ACCOUNTED FOR, not skipped.

    They are neither reachable nor promised a specific refusal — "unsupported" means
    outside the authoring contract, and asserting one stable error for any invented
    payload would invent a promise the contract does not make. So each carries a recorded
    reason, and the dispositioned set must equal the partition exactly.
    """
    unsupported = _partitions().get("unsupported", set())
    assert unsupported, "no unsupported capabilities — this test would be vacuous"

    disposed = {
        key
        for key in unsupported
        if isinstance(CAPABILITY_WITNESSES[key], UnsupportedDisposition)
        and CAPABILITY_WITNESSES[key].reason.strip()
    }
    assert disposed == unsupported, sorted(unsupported - disposed)


def test_the_three_partitions_account_for_every_manifest_key():
    """The coverage claim: derived from the authority's FULL case set, not a sample.

    Executed-admitted plus executed-refused plus dispositioned must be every key in the
    manifest. Counts are reported from the partitions rather than embedded as constants, so
    adding a capability changes the expectation automatically.
    """
    partitions = _partitions()
    covered = set().union(*partitions.values())
    assert covered == set(PROCESS_IR_V1_CAPABILITIES), sorted(
        set(PROCESS_IR_V1_CAPABILITIES) ^ covered
    )
    assert covered == set(CAPABILITY_WITNESSES)
    # Every key lands in exactly one partition.
    assert sum(len(keys) for keys in partitions.values()) == len(
        PROCESS_IR_V1_CAPABILITIES
    )


def test_every_witness_records_its_fixture_provenance():
    """A fixture you wrote is not evidence (#146's lesson, made mechanical).

    Clean-room QA provenance is a repo rule; the same discipline applies to a guard's
    inputs. Every executable witness names where its document came from, so a reviewer can
    tell a frozen pre-baseline capture from a refusal input built here.
    """
    from _process_ir_capability_witnesses import (
        FIXTURE_PROVENANCE,
        PROVENANCE_FROZEN_FIXTURE,
        PROVENANCE_INLINE_ADMISSION,
        PROVENANCE_INLINE_REFUSAL,
        PROVENANCE_KINDS,
        PROVENANCE_SYNTHETIC_CFG,
        compiled_digests,
        expected_model_digest,
        loaded_fixtures,
        record_compiles,
        reset_loaded_fixtures,
    )

    # A closed set was the first shape and it is still fail-open: it validates only the
    # PREFIX, so an admission witness mislabelled `inline refusal document`, or an inline
    # document claiming `frozen fixture nonexistent.json`, both pass — and the mislabel can
    # even make the frozen-count floor look stronger. Provenance is now checked against the
    # witness's own MODE and, for a frozen claim, against the real fixture inventory.
    _ALLOWED_BY_KIND = {
        "admits": {PROVENANCE_FROZEN_FIXTURE, PROVENANCE_INLINE_ADMISSION},
        "refuses": {
            PROVENANCE_FROZEN_FIXTURE,
            PROVENANCE_INLINE_REFUSAL,
            PROVENANCE_SYNTHETIC_CFG,
        },
    }

    bad = []
    frozen = []
    for key, entry in sorted(CAPABILITY_WITNESSES.items()):
        if not isinstance(entry, CapabilityWitness):
            continue
        kind = next(
            (k for k in PROVENANCE_KINDS if entry.provenance.startswith(k)), None
        )
        if kind is None:
            bad.append((key, "undeclared provenance", entry.provenance))
            continue
        if kind not in _ALLOWED_BY_KIND[entry.kind]:
            bad.append((key, "provenance not allowed for a %r witness" % entry.kind, kind))
            continue
        if kind is PROVENANCE_FROZEN_FIXTURE:
            named = [rel for rel in FIXTURE_PROVENANCE if rel in entry.provenance]
            if not named:
                bad.append((key, "claims a frozen fixture not in the inventory", entry.provenance))
                continue
            missing = [rel for rel in named if not (_FIXTURES / rel).is_file()]
            if missing:
                bad.append((key, "claims a frozen fixture that does not exist", missing))
                continue
            # BOUND TO EXECUTION, not to the string. Checking only that the inventory
            # contains the named path let an inline witness label itself with a real
            # fixture it never opened — and such a false claim also counted toward the
            # non-vacuity floor below, making the floor read stronger the more it was
            # lied to. The witness is RUN and the loader records what it actually read.
            reset_loaded_fixtures()
            try:
                with record_compiles():
                    entry.run()
            except Exception as exc:  # noqa: BLE001 - the run itself is graded elsewhere
                bad.append((key, "run failed while checking provenance", repr(exc)))
                continue
            opened = loaded_fixtures()
            unopened = [rel for rel in named if rel not in opened]
            if unopened:
                bad.append((key, "claims a frozen fixture its run never loaded", unopened))
                continue
            # ...and the fixture's CONTENT is the ONLY thing that reached the compiler.
            # Membership alone was still fail-open: a witness could compile the claimed
            # fixture as a throwaway and return the result of compiling an inline document,
            # so the digest was present but the observed proof came from elsewhere. Set
            # EQUALITY binds the claim to what the witness actually demonstrated.
            compiled = set(compiled_digests())
            claimed = {expected_model_digest(rel) for rel in named}
            if compiled != claimed:
                bad.append((
                    key,
                    "frozen claim not bound to what was compiled",
                    {"claimed": sorted(named),
                     "unclaimed compiles": len(compiled - claimed),
                     "claimed but never compiled": sorted(
                         rel for rel in named if opened[rel] not in compiled)},
                ))
                continue
            frozen.append(key)

    assert bad == [], bad
    # Non-vacuity: the strongest kind must really be in use, or the classification would be
    # a formality satisfied entirely by the weakest label.
    assert len(frozen) >= 5, frozen
