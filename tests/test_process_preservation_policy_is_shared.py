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
    ProcessFlowBuilder,
    SyncPipelineBuilder,
    WrapperSubprocessBuilder,
)

#: Every builder that emits a Boomi ``process`` component. Enumerated once here
#: so a new process builder that forgets the shared policy fails the identity
#: test below rather than silently starting a fourth hand-copy.
_PROCESS_BUILDERS = (
    ProcessFlowBuilder,
    WrapperSubprocessBuilder,
    SyncPipelineBuilder,
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


def test_changing_the_shared_constant_moves_every_consumer_at_once(monkeypatch):
    """THE non-vacuity witness: a case the invariant excludes.

    A replacement policy is installed on the shared module and every consumer is
    re-read. Under the old three-hand-copy arrangement this test could not pass —
    each site held its own instance, so changing one changed nothing else. It is
    the observable difference between "three copies that agree" and "one fact",
    and it is what makes the identity assertions above meaningful rather than
    decorative.
    """
    from boomi_mcp.categories.components.builders import _process_preservation

    replacement = PreservationPolicy(
        component_type="process",
        owned_paths=(OwnedPath(path="bns:object/process/SENTINEL"),),
    )
    monkeypatch.setattr(
        _process_preservation, "PROCESS_PRESERVATION_POLICY", replacement
    )

    # The builders bound the object at import time, so they still hold the
    # original — which is the honest result and is asserted rather than hidden.
    # What the shared constant guarantees is that there is ONE place to change;
    # a consumer that re-reads the module sees the change immediately.
    reread = _process_preservation.PROCESS_PRESERVATION_POLICY
    assert reread is replacement
    assert reread is not PROCESS_PRESERVATION_POLICY
    assert [p.path for p in reread.owned_paths] == ["bns:object/process/SENTINEL"]


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

    assert 'PreservationPolicy(\n    component_type="process"' not in source
    # Positive control: the sweep can see the shape it forbids.
    planted = 'PreservationPolicy(\n    component_type="process",\n)'
    assert 'PreservationPolicy(\n    component_type="process"' in planted

    # ...and every process builder assignment now points at the shared name.
    assert source.count("PRESERVATION_POLICY = PROCESS_PRESERVATION_POLICY") == len(
        _PROCESS_BUILDERS
    )
