"""One process preservation policy, referenced by every consumer (issue #153).

Before M12.15 the same ``PreservationPolicy(component_type="process",
owned_paths=(OwnedPath(path="bns:object/process"),))`` was hand-written three
times in ``process_flow_builder.py``, and #153's canonical materialization plan
would have made it four. Four hand-copies of one runtime fact is the mechanism
this repository's structural-fix rule names, so the fact is stated once and
referenced.

These tests are the **non-vacuity witness** that rule demands: it is not enough
to assert the constant exists, because a test that only reads the constant stays
green after a consumer quietly stops using it. Each test below constructs a
concrete case the invariant EXCLUDES.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.components.builders._preservation_policy import (  # noqa: E402
    OwnedPath,
    PreservationPolicy,
)
from boomi_mcp.categories.components.builders._process_preservation import (  # noqa: E402
    PROCESS_OWNED_PATH,
    PROCESS_PRESERVATION_POLICY,
)
from boomi_mcp.categories.components.builders.process_flow_builder import (  # noqa: E402
    PROCESS_FLOW_BUILDERS,
)

#: Every builder that emits a Boomi ``process`` component — DERIVED from the
#: runtime dispatch table, never hand-listed.
#:
#: An adversarial review refuted the hand-listed version: a FOURTH registered
#: process builder carrying its own hand-copied policy passed every test here,
#: because the list the tests parametrized over was itself a hand-copy of the
#: dispatch table. That is the exact defect class this guard exists to close,
#: reintroduced inside the guard. Reading `PROCESS_FLOW_BUILDERS` means a newly
#: registered builder is covered the moment it is registered.
_PROCESS_BUILDERS = tuple(
    dict.fromkeys(PROCESS_FLOW_BUILDERS.values())  # de-duplicated, order-stable
)


@pytest.mark.parametrize("builder", _PROCESS_BUILDERS, ids=lambda b: b.__name__)
def test_every_process_builder_references_the_same_object(builder):
    """IDENTITY, not equality.

    Equality would also hold for a fourth hand-written copy that happens to
    match today and drifts tomorrow — which is precisely the failure being
    designed out. ``is`` can only be satisfied by actually referencing the
    shared constant.
    """
    assert builder.PRESERVATION_POLICY is PROCESS_PRESERVATION_POLICY


def test_the_policy_is_deeply_immutable_so_sharing_cannot_alias():
    """The precondition that makes sharing one instance safe at all.

    If either dataclass were mutable, one consumer mutating the shared instance
    would silently change preservation for all of them — a hazard the three
    per-site instances did not have. Checked rather than assumed, because the
    whole extraction rests on it.
    """
    assert PreservationPolicy.__dataclass_params__.frozen is True
    assert OwnedPath.__dataclass_params__.frozen is True

    with pytest.raises(Exception):
        PROCESS_PRESERVATION_POLICY.component_type = "mutated"
    with pytest.raises(Exception):
        PROCESS_PRESERVATION_POLICY.owned_paths[0].path = "mutated"

    # Every field of the shared graph is an immutable scalar or tuple. Asserted
    # structurally so a future field of mutable type fails here.
    for value in vars(PROCESS_PRESERVATION_POLICY).values():
        assert isinstance(value, (str, tuple, type(None))), value
    for owned in PROCESS_PRESERVATION_POLICY.owned_paths:
        for value in vars(owned).values():
            assert isinstance(value, (str, tuple, type(None))), value


def test_the_policy_still_says_exactly_what_the_three_originals_said():
    """Behaviour preservation, pinned field by field.

    The legacy path is the byte-exact parity oracle until #160, so the extracted
    constant must be the SAME policy — not merely a similar one. The
    ``<bns:processOverrides>`` sibling and the folder attributes staying UNOWNED
    are the two properties whose loss would be silently destructive: the first
    would discard per-environment overrides on every update, the second would
    move components to Home.
    """
    assert PROCESS_PRESERVATION_POLICY.component_type == "process"
    assert PROCESS_PRESERVATION_POLICY.subtype is None
    assert PROCESS_PRESERVATION_POLICY.owned_root_attrs == ("name",)
    assert [p.path for p in PROCESS_PRESERVATION_POLICY.owned_paths] == [
        "bns:object/process"
    ]
    assert PROCESS_OWNED_PATH == "bns:object/process"

    owned_paths = {p.path for p in PROCESS_PRESERVATION_POLICY.owned_paths}
    assert "bns:processOverrides" not in owned_paths
    assert "bns:encryptedValues" not in owned_paths
    assert "folderName" not in PROCESS_PRESERVATION_POLICY.owned_root_attrs
    assert "folderFullPath" not in PROCESS_PRESERVATION_POLICY.owned_root_attrs


def test_all_consumers_observe_one_object_so_a_change_cannot_reach_only_some():
    """THE non-vacuity witness — rewritten after an adversarial review.

    The previous version monkeypatched the module attribute and asserted the
    module attribute had changed. It touched no consumer, and the reviewer
    MEASURED it passing against a pristine pre-extraction tree that still
    carried all three hand-copies — so its docstring claim that "under the old
    three-hand-copy arrangement this test could not pass" was simply false.

    This version asserts the property that actually distinguishes one fact from
    three agreeing copies: every consumer, plus the plan projection, resolves to
    the SAME object, so there is exactly one place a change can be made. Under
    the old arrangement the builders held three distinct instances and the
    identity set below would have had size 3.
    """
    from boomi_mcp.authoring.process_materialization import preservation_policy_v1
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )

    observed = {id(b.PRESERVATION_POLICY) for b in _PROCESS_BUILDERS}
    assert len(observed) == 1, (
        "the process builders hold {0} distinct policy objects — the extraction "
        "has been partially reverted".format(len(observed))
    )
    assert observed == {id(PROCESS_PRESERVATION_POLICY)}

    # The fourth consumer — the #153 plan projection — must describe THAT object
    # and no other. Compared on the projection's own canonical form, which is
    # derived from the runtime constant rather than restated.
    import dataclasses
    import json

    projected = json.loads(preservation_policy_v1().canonical_policy_json)
    assert projected == json.loads(
        json.dumps(dataclasses.asdict(PROCESS_PRESERVATION_POLICY), sort_keys=True)
    )


def test_the_plan_projection_is_complete_not_a_lossy_restatement():
    """Every runtime policy field reaches the plan.

    The first projection read two of ``PreservationPolicy``'s eight fields and
    one of ``OwnedPath``'s eleven — so two materially different policies, one
    replacing a subtree and one merging it, projected to identical bytes and
    therefore to the same plan fingerprint. ``OwnedPath.mode`` and
    ``owned_encrypted_paths`` are named explicitly below because those are the
    two whose loss silently changes whether live state survives an update.
    """
    import dataclasses
    import json

    from boomi_mcp.authoring.process_materialization import preservation_policy_v1
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )

    projected = json.loads(preservation_policy_v1().canonical_policy_json)
    runtime_fields = {f.name for f in dataclasses.fields(PROCESS_PRESERVATION_POLICY)}
    assert set(projected) == runtime_fields, runtime_fields - set(projected)

    owned_fields = {f.name for f in dataclasses.fields(OwnedPath)}
    assert set(projected["owned_paths"][0]) == owned_fields
    assert "mode" in projected["owned_paths"][0]
    assert "owned_encrypted_paths" in projected


def test_no_process_builder_hand_builds_its_own_policy_any_more():
    """Sibling sweep, asserted against the source text.

    The structural-fix rule requires enumerating and fixing every sibling
    instance of the mechanism. This reads the builder module and refuses a
    re-introduced inline ``PreservationPolicy(component_type="process", ...)`` —
    the exact shape that was removed. Source-text rather than behavioural,
    because a fourth copy that happens to be correct today would pass every
    behavioural check and still be the defect.
    """
    source = (
        Path(_src)
        / "boomi_mcp"
        / "categories"
        / "components"
        / "builders"
        / "process_flow_builder.py"
    ).read_text()

    # WHITESPACE-INSENSITIVE. The first version matched the literal string
    # `'PreservationPolicy(\n    component_type="process"'` — a 4-space indent and
    # that exact line break — so a copy written on one line, or at any other
    # indent, was invisible to it. An adversarial review demonstrated exactly
    # that bypass.
    import re

    inline = re.compile(r"PreservationPolicy\s*\(\s*component_type\s*=\s*[\"']process")
    assert not inline.search(source), (
        "a process PreservationPolicy is constructed inline in "
        "process_flow_builder.py — it must reference PROCESS_PRESERVATION_POLICY"
    )

    # Positive control: the pattern really does match the shape it forbids, in
    # BOTH spellings the literal match missed.
    assert inline.search('PreservationPolicy(\n    component_type="process",\n)')
    assert inline.search("PreservationPolicy(component_type='process')")
    assert inline.search('    x = PreservationPolicy(  component_type = "process" )')

    # ...and every process builder assignment now points at the shared name. The
    # count is compared against the RUNTIME dispatch table, so a newly registered
    # builder that forgets the shared constant fails here too.
    assert source.count("PRESERVATION_POLICY = PROCESS_PRESERVATION_POLICY") == len(
        _PROCESS_BUILDERS
    )
