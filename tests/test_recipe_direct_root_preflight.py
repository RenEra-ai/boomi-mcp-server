"""Direct process roots are validated BEFORE composition (issue #153, from #149).

#149 recorded this as item 1: ``compose(direct_process_roots=...)`` annotates its
parameter ``Mapping[str, ProcessIRV1]``, but an annotation enforces nothing, so a
caller passing raw dictionaries crashed untyped out of the middle of composition
— naming an internal expression instead of their input.

**Correction to the inherited description, measured here.** #149 names the crash
as a ``KeyError`` at ``roots[key]["body"]`` (phase 2). With the preflight removed
and a raw ``dict`` passed, the ACTUAL exception is
``AttributeError: 'dict' object has no attribute 'model_dump'``, raised earlier —
``_compose_process_roots`` dumps every direct root before phase 2 ever indexes
one, so the cited ``KeyError`` line is unreachable for a plain dictionary. It IS
reachable for a value that has ``model_dump`` but returns a body-less mapping.
Both are the same defect class — an untyped crash on caller input — and the tests
below assert the TYPED refusal rather than any particular raw exception, so they
hold whichever of the two a given malformed value would have produced.

**Scope, stated honestly.** ``direct_process_roots`` still has zero production
callers, and #153's recipe-root lifting reads the recipe RESULT rather than these
roots, so this is preventive hardening of an uncalled public signature rather
than the repair of a live path. The tests below are written to that claim: they
drive ``compose`` directly, which is exactly the surface the hardening protects.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.errors import RECIPE_CONSTRAINT_FAILED  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.recipes.composer import compose  # noqa: E402
from boomi_mcp.recipes.errors import RecipeError  # noqa: E402

from _m12_11_support import VALID_IR_DOC  # noqa: E402


def _compose(direct_roots):
    return compose((), {}, direct_process_roots=direct_roots)


def test_a_valid_parsed_root_still_composes():
    """Positive control. Every refusal below is worthless without it."""
    result = _compose({"proc": parse_process_ir_v1(dict(VALID_IR_DOC))})
    assert [key for key, _ in result.process_roots] == ["proc"]


def test_a_raw_dictionary_root_is_accepted_and_parsed():
    """The annotation promises a model; a caller may hand over plain JSON.

    Coercing here is what makes the promise true for the rest of composition,
    which indexes the root as a mapping.
    """
    result = _compose({"proc": dict(VALID_IR_DOC)})
    assert [key for key, _ in result.process_roots] == ["proc"]


@pytest.mark.parametrize(
    "malformed,label",
    [
        ({}, "empty object"),
        ({"version": "1"}, "no body"),
        ({"body": {"kind": "sequence", "steps": []}}, "no version"),
        ({"version": "1", "body": {"kind": "sequence", "steps": [{"kind": "nope"}]}},
         "unknown node kind"),
        ({"version": "99", "body": {"kind": "sequence", "steps": []}},
         "unsupported version"),
    ],
)
def test_a_malformed_root_fails_typed_never_as_a_raw_keyerror(malformed, label):
    """The exact defect #149 named: a typed refusal, not a raw crash.

    Asserted as "the recipe layer's own typed diagnostic" rather than as "some
    exception happens" — one always did. The raw crash this replaces is an
    ``AttributeError`` or a ``KeyError`` depending on the value (see the module
    docstring); neither is a diagnostic a caller can act on.
    """
    with pytest.raises(RecipeError) as excinfo:
        _compose({"proc": malformed})
    assert not isinstance(excinfo.value, KeyError), label

    diagnostics = excinfo.value.diagnostics
    assert diagnostics, label
    assert diagnostics[0].code == RECIPE_CONSTRAINT_FAILED, label
    assert diagnostics[0].phase == "composition", label
    assert diagnostics[0].target == "direct_process:proc", label


@pytest.mark.parametrize("scalar", ["x", 7, None, True, []])
def test_a_non_object_root_fails_typed_too(scalar):
    """A scalar cannot be indexed at all, so this is the rawest crash of the set."""
    with pytest.raises(RecipeError) as excinfo:
        _compose({"proc": scalar})
    assert excinfo.value.diagnostics[0].code == RECIPE_CONSTRAINT_FAILED


def test_the_process_ir_authority_codes_travel_value_free():
    """The recipe layer carries the canonical code; it never re-diagnoses.

    Same rule every other canonical refusal follows here: the ProcessIR parser
    owns its taxonomy, and the recipe diagnostic points at it rather than
    inventing a parallel explanation.
    """
    with pytest.raises(RecipeError) as excinfo:
        _compose(
            {
                "proc": {
                    "version": "1",
                    "body": {"kind": "sequence", "steps": [{"kind": "nope"}]},
                }
            }
        )
    causes = {c for d in excinfo.value.diagnostics for c in (d.cause_codes or ())}
    assert causes, "the ProcessIR authority's own codes must travel"
    assert any(code.startswith("PROCESS_IR_") for code in causes), causes
    # Value-free: the caller's authored payload never appears in the diagnostic.
    assert "nope" not in repr([d.model_dump() for d in excinfo.value.diagnostics])


@pytest.mark.parametrize("bad_key", ["", " padded", "padded ", "with\tcontrol"])
def test_a_malformed_root_KEY_is_refused_by_the_shared_key_rule(bad_key):
    """Keys use the SAME validator the contribution models use.

    A direct root and a contributed one must not disagree about what a component
    key is — that disagreement is how a root ends up attached to a catalog entry
    it does not equal.
    """
    with pytest.raises(RecipeError) as excinfo:
        _compose({bad_key: dict(VALID_IR_DOC)})
    diagnostic = excinfo.value.diagnostics[0]
    assert diagnostic.code == RECIPE_CONSTRAINT_FAILED
    assert diagnostic.target == "direct_process_root_key"


def test_validation_happens_before_composition_not_during_it(monkeypatch):
    """Ordering is the property, so it is OBSERVED rather than assumed.

    ``_compose_process_roots`` is replaced by a recorder. A malformed root must
    be refused without that phase ever running: if validation happened during
    composition instead, phase 1 would already have established the valid root
    before reaching the malformed one, leaving partial state behind at the moment
    the caller is told.

    The valid root is supplied alongside deliberately — with only a malformed
    one, a "phase never ran" assertion would hold even for an implementation
    that simply crashed earlier for an unrelated reason.
    """
    from boomi_mcp.recipes import composer as composer_module

    ran = []
    monkeypatch.setattr(
        composer_module,
        "_compose_process_roots",
        lambda *a, **k: ran.append(True) or {},
    )

    # Positive control: with only VALID roots, the phase really does run — so
    # the negative assertion below is about ordering, not about a dead patch.
    _compose({"good": parse_process_ir_v1(dict(VALID_IR_DOC))})
    assert ran == [True]

    ran.clear()
    with pytest.raises(RecipeError):
        _compose(
            {
                "good": parse_process_ir_v1(dict(VALID_IR_DOC)),
                "bad": {"version": "1"},
            }
        )
    assert ran == [], "composition began before the roots were validated"


def test_no_direct_roots_is_unchanged_behaviour():
    """The common case — every production caller today passes nothing."""
    assert _compose(None).process_roots == ()
    assert _compose({}).process_roots == ()
