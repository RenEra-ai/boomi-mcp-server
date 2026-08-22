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
    from _process_ir_capability_witnesses import PROVENANCE_KINDS

    undeclared = sorted(
        (key, entry.provenance)
        for key, entry in CAPABILITY_WITNESSES.items()
        if isinstance(entry, CapabilityWitness)
        and not any(entry.provenance.startswith(kind) for kind in PROVENANCE_KINDS)
    )
    # A CLOSED set, not "any non-blank string" — the previous check accepted anything,
    # including a description that overstated where the document came from, which is how a
    # provenance note drifts from what the file actually does.
    assert undeclared == [], undeclared

    # Non-vacuity: the strongest kind must actually be in use, or the classification would
    # be a formality satisfied entirely by the weakest label.
    frozen = sorted(
        key
        for key, entry in CAPABILITY_WITNESSES.items()
        if isinstance(entry, CapabilityWitness)
        and entry.provenance.startswith("frozen fixture")
    )
    assert len(frozen) >= 5, frozen
