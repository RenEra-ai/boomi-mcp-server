"""#156 T4: canonical Notify on the recovery path.

Notify already existed as legacy-only emission (`render_notify`, #89) with a
single caller in `process_emitters/legacy.py`. This slice gives it an IR node, a
frozen contract, the nineteenth emitter registration and a placement — while
REUSING that renderer unchanged, so the two shipped notify goldens keep their
bytes.

The legacy caller stays in place deliberately: it is the differential oracle. A
canonical emitter that agreed with a golden but disagreed with the code that
produced the golden would be a parity claim resting on one frozen sample.
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.categories.components.builders import process_flow_builder  # noqa: E402
from boomi_mcp.categories.components.builders.process_emitters import (  # noqa: E402
    legacy,
    rendering,
)
from boomi_mcp.compiler.process_ir import emitter_registry as R  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    CATCH_DRAGPOINT_Y,
    CATCH_SHAPE_Y,
    CAUGHT_ERROR_PROPERTY_ID as CONTRACTS_TOKEN,
    EmissionLayoutV1,
    EmissionNodeV1,
    EmissionTransitionV1,
    NotifyInputV1,
    dragpoint_x,
    shape_x,
)
from boomi_mcp.models.process_ir import (  # noqa: E402
    ProcessIRValidationError,
    NotifyNodeV1,
    parse_process_ir_v1,
)
from boomi_mcp.models.process_ir_tokens import (  # noqa: E402
    CAUGHT_ERROR_PROPERTY_ID,
    NOTIFY_LEVELS,
)

GOLDEN = _ROOT / "tests" / "fixtures" / "golden_xml"


# ---------------------------------------------------------------------------
# One authority for the platform facts
# ---------------------------------------------------------------------------


def test_the_caught_error_token_has_exactly_one_authority():
    """BOTH directions, because agreement by inspection is what failed before.

    Three modules need this token: the renderer that binds it as a track
    parameter, the compiler contracts that bind it for an Exception, and the
    model validator that requires it in a Notify template. Until #156 the first
    two were independent literals that happened to match. A test that only
    asserted `rendering == contracts` would still pass if BOTH drifted together,
    so each is compared to the module that now owns the fact.
    """
    assert rendering._NOTIFY_CAUGHT_ERROR_TOKEN == CAUGHT_ERROR_PROPERTY_ID
    assert CONTRACTS_TOKEN == CAUGHT_ERROR_PROPERTY_ID
    # The renderer emits the token into the wire as a property id; if that
    # spelling ever moves, the goldens move with it and this is the first thing
    # to fail.
    assert CAUGHT_ERROR_PROPERTY_ID == "meta.base.catcherrorsmessage"


def test_the_notify_level_vocabulary_has_exactly_one_authority():
    """The legacy validator and the typed model read the same tuple.

    Before #156 the legacy builder owned `_SUPPORTED_NOTIFY_LEVELS` and a
    canonical `Literal[...]` beside it would have been a second hand-model of a
    Boomi contract — the same defect class as the token above, one slice later.
    """
    assert process_flow_builder._SUPPORTED_NOTIFY_LEVELS == frozenset(NOTIFY_LEVELS)
    # The model's own field derives from the same tuple, so the served schema
    # enum and the legacy refusal cannot disagree about what Boomi accepts.
    schema = NotifyNodeV1.model_json_schema()
    assert schema["properties"]["level"]["enum"] == list(NOTIFY_LEVELS)


# ---------------------------------------------------------------------------
# Canonical vs legacy: the differential oracle
# ---------------------------------------------------------------------------

#: LEGAL templates chosen to exercise the escaper, not to look realistic: an
#: apostrophe (doubled), XML metacharacters (escaped by the renderer), a leading
#: bracket that is NOT JSON, and a repeated token (every occurrence binds).
#:
#: A JSON-shaped body used to sit here. It is now refused at both boundaries —
#: the escaper quote-wraps it, which puts the substituted `{1}` inside the quotes
#: and unbinds the caught error — so it belongs with the refusals, not here.
_TEMPLATES = (
    "Integration catch path failed. Caught error: {0}".format(CAUGHT_ERROR_PROPERTY_ID),
    "it's broken: {0}".format(CAUGHT_ERROR_PROPERTY_ID),
    "<a> & \"b\" caught {0}".format(CAUGHT_ERROR_PROPERTY_ID),
    # A LEADING BRACKET that is not JSON. The escaper leaves it alone and emits
    # `[ERROR] caught {1}`, so it must stay authorable — an earlier version of
    # the binding rule refused every leading `[` and took this with it.
    "[ERROR] caught {0}".format(CAUGHT_ERROR_PROPERTY_ID),
    "{0} and again {0}".format(CAUGHT_ERROR_PROPERTY_ID),
    CAUGHT_ERROR_PROPERTY_ID,
)


class _Ctx:
    def __init__(self, node):
        self.node = node


def _canonical_notify(level, template, *, ordinal=6, next_shape="shape7"):
    node = EmissionNodeV1(
        ordinal=ordinal,
        shape_id="shape{0}".format(ordinal),
        origin="ir",
        emitter_input=NotifyInputV1(level=level, message_template=template),
        layout=EmissionLayoutV1(x=shape_x(ordinal), y=CATCH_SHAPE_Y),
        outgoing=(
            EmissionTransitionV1(
                dragpoint_name="shape{0}.dragpoint1".format(ordinal),
                to_shape_id=next_shape,
                x=dragpoint_x(ordinal),
                y=CATCH_DRAGPOINT_Y,
                local_ordinal=1,
                provenance="cfg_edge",
            ),
        ),
    )
    return R.registration_for("notify").emit(node.emitter_input, _Ctx(node))


@pytest.mark.parametrize("template", _TEMPLATES)
@pytest.mark.parametrize("level", NOTIFY_LEVELS)
def test_canonical_notify_equals_the_legacy_emitter_for_the_same_graph(level, template):
    """The canonical emitter and the legacy one must produce the same bytes.

    `legacy._emit_notify` is the code that produced both shipped goldens, so it
    is the oracle. Its `shape_index` is the SAME 1-based ordinal the compiler
    uses (`rendering._shape_x` is `START + (index - 1) * STEP`), and it drives
    both the x coordinate and the dragpoint name — passing the 0-based index
    instead silently renders the previous shape's geometry.
    """
    canonical = _canonical_notify(level, template)
    oracle = legacy._emit_notify(
        "shape6",
        {"level": level, "message_template": template},
        "shape7",
        6,
    )
    assert canonical == oracle


def test_notify_reproduces_the_shipped_golden_shape_bytes():
    """Byte parity against the FROZEN file, not just against the oracle."""
    import re

    golden = GOLDEN / "try_catch_notify_dlq_document_cache.xml"
    expected = re.search(
        r'<shape [^>]*shapetype="notify".*?</shape>', golden.read_text(), re.S
    ).group(0)
    assert (
        _canonical_notify(
            "ERROR",
            "Integration catch path failed. Caught error: {0}".format(
                CAUGHT_ERROR_PROPERTY_ID
            ),
        )
        == expected
    )


def test_escape_then_substitute_and_the_reverse_agree_today():
    """A MEASUREMENT, recorded so the emitter's comment stays checkable.

    `_emit_notify` escapes for MessageFormat and THEN substitutes the token,
    because that is the order the legacy oracle uses and the order the goldens
    were produced under. An earlier version of that comment claimed the reverse
    order corrupts the binding; it does not, for any input reachable today, and a
    comment asserting a defect nobody can produce is worse than no comment.

    This pins the equivalence rather than the claim. The escaper is legacy-owned:
    if it ever gains a rule that treats `{N}` placeholders specially the two
    orders WILL diverge, and this test is where that shows up — as a prompt to
    re-derive the emitter, not as a mystery byte diff in a golden.
    """
    escape = rendering._escape_message_format_text
    for template in _TEMPLATES:
        forward = escape(template).replace(CAUGHT_ERROR_PROPERTY_ID, "{1}")
        reverse = escape(template.replace(CAUGHT_ERROR_PROPERTY_ID, "{1}"))
        assert forward == reverse, template


# ---------------------------------------------------------------------------
# Placement: the recovery path and nowhere else
# ---------------------------------------------------------------------------

_NOTIFY = {
    "kind": "notify",
    "level": "ERROR",
    "message_template": "caught {0}".format(CAUGHT_ERROR_PROPERTY_ID),
}
_CONN = {"kind": "connector_call", "operation_ref": "$ref:OP"}
_STOP = {"kind": "stop"}


def _doc(body_steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": body_steps}}


def _try_catch(*, try_steps=None, catch_steps=None, scope="process"):
    return {
        "kind": "try_catch",
        "scope": scope,
        "try_body": {"steps": try_steps or [dict(_CONN)], "terminal": dict(_STOP)},
        "catch_body": {"steps": catch_steps or [], "terminal": dict(_STOP)},
    }


def test_notify_is_admitted_on_the_recovery_path():
    ir = parse_process_ir_v1(_doc([_try_catch(catch_steps=[dict(_NOTIFY)])]))
    catch = ir.body.steps[0].catch_body
    assert [step.kind for step in catch.steps] == ["notify"]


@pytest.mark.parametrize(
    "document,where",
    [
        (_doc([_try_catch(try_steps=[dict(_CONN), dict(_NOTIFY)])]), "try body"),
        (_doc([dict(_CONN), dict(_NOTIFY), dict(_STOP)]), "root sequence"),
        (
            _doc(
                [
                    dict(_CONN),
                    {
                        "kind": "branch",
                        "legs": [
                            {"steps": [dict(_NOTIFY)], "terminal": dict(_STOP)},
                            {
                                "steps": [{"kind": "message", "text": "b"}],
                                "terminal": dict(_STOP),
                            },
                        ],
                    },
                ]
            ),
            "branch leg",
        ),
        (
            _doc(
                [
                    dict(_CONN),
                    {
                        "kind": "decision",
                        "comparison": "equals",
                        "left": {"value_type": "static", "static_value": "1"},
                        "right": {"value_type": "static", "static_value": "1"},
                        "true_arm": {
                            "steps": [dict(_NOTIFY)],
                            "terminal": dict(_STOP),
                        },
                        "false_arm": {
                            "steps": [{"kind": "message", "text": "f"}],
                            "terminal": dict(_STOP),
                        },
                    },
                ]
            ),
            "decision true arm",
        ),
    ],
)
def test_notify_is_refused_everywhere_else(document, where):
    """#154 collapsed every control body onto ONE step union, so widening that
    union would have admitted Notify in all four of these placements at once.
    This is the test that would have caught it."""
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(document)


@pytest.mark.parametrize(
    "template",
    [
        "no token here at all",
        "   ",
        "",
    ],
)
def test_a_notify_template_must_carry_the_caught_error_token(template):
    """The emitted message always declares a parameter bound to the caught
    error, so a template that never references it logs a failure without the
    failure — and declares a parameter it does not use."""
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(
            _doc(
                [
                    _try_catch(
                        catch_steps=[
                            {
                                "kind": "notify",
                                "level": "ERROR",
                                "message_template": template,
                            }
                        ]
                    )
                ]
            )
        )


def test_a_notify_level_outside_the_platform_vocabulary_is_refused():
    for level in ("error", "INFORMATION", "FATAL", ""):
        with pytest.raises(ProcessIRValidationError):
            parse_process_ir_v1(
                _doc(
                    [
                        _try_catch(
                            catch_steps=[
                                {
                                    "kind": "notify",
                                    "level": level,
                                    "message_template": _NOTIFY["message_template"],
                                }
                            ]
                        )
                    ]
                )
            )


# ---------------------------------------------------------------------------
# The registration's own preflight (the mutated-plan entry point)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "level,template",
    [
        ("FATAL", "caught {0}".format(CAUGHT_ERROR_PROPERTY_ID)),
        ("ERROR", "   "),
        ("ERROR", "no token"),
    ],
)
def test_the_emitter_preflight_refuses_an_input_the_parser_never_produced(
    level, template
):
    """`NotifyInputV1` is built by lowering, but a caller can reach emission with
    a `model_construct`-ed plan that skipped validation entirely. The preflight is
    the independent refusal on that path — a guard that only restated the
    parser's rule on the parser's own inputs would be unreachable."""
    inp = NotifyInputV1.model_construct(
        emitter_kind="notify", level=level, message_template=template
    )
    assert R.registration_for("notify").precondition(inp) is not None


def test_the_emitter_preflight_accepts_every_legal_input():
    """The control. A precondition that refused everything would pass the test
    above and emit nothing."""
    precondition = R.registration_for("notify").precondition
    for level in NOTIFY_LEVELS:
        for template in _TEMPLATES:
            inp = NotifyInputV1(level=level, message_template=template)
            assert precondition(inp) is None


# ---------------------------------------------------------------------------
# T5: the terminal recovery hand-off
# ---------------------------------------------------------------------------

_CALL = {
    "kind": "process_call",
    "process_ref": "$ref:CHILD",
    "wait": True,
    "abort_on_error": True,
}


def _compile_bodies(document):
    """Drive the OTHER public entry point.

    `validate_body_capabilities` is what a caller reaches by handing a mutated
    `ProcessIRV1` straight to `compile_process_ir_v1`. Every assertion below
    checks BOTH paths, because a rule only the parser enforces is not a rule —
    the model is exported and not frozen.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import (
        validate_body_capabilities,
    )

    validate_body_capabilities(parse_process_ir_v1(document))


@pytest.mark.parametrize(
    "catch_steps,label",
    [
        ([], "bare terminal call"),
        ([dict(_NOTIFY)], "notify then call"),
        ([dict(_NOTIFY), dict(_NOTIFY)], "two notifies then call"),
    ],
)
def test_a_recovery_hand_off_is_admitted_on_the_catch_terminal(catch_steps, label):
    document = _doc([_try_catch(catch_steps=catch_steps)])
    document["body"]["steps"][0]["catch_body"]["terminal"] = dict(_CALL)
    ir = parse_process_ir_v1(document)
    assert ir.body.steps[0].catch_body.terminal.kind == "process_call"
    _compile_bodies(document)


@pytest.mark.parametrize(
    "terminal,field",
    [
        ({"kind": "process_call", "process_ref": "$ref:C"}, "abort_on_error"),
        (
            {
                "kind": "process_call",
                "process_ref": "$ref:C",
                "wait": True,
                "abort_on_error": False,
            },
            "abort_on_error",
        ),
        (
            {
                "kind": "process_call",
                "process_ref": "$ref:C",
                "wait": False,
                "abort_on_error": True,
            },
            "wait",
        ),
    ],
)
def test_a_recovery_call_must_author_both_flags_true(terminal, field):
    """The FIRST case is the trap the issue names: `abort_on_error` defaults to
    False on `ProcessCallNodeV1` — the standalone-wrapper default — and on this
    leg the default is REFUSED rather than silently rewritten to true. Legality
    is decided by VALUE, so a defaulted false and an authored false are the same
    refusal, and a document that round-trips through a dump keeps its verdict."""
    document = _doc([_try_catch()])
    document["body"]["steps"][0]["catch_body"]["terminal"] = terminal
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(document)
    diagnostic = excinfo.value.diagnostics[0]
    assert diagnostic.code == "PROCESS_IR_SEMANTIC_RECOVERY_PROCESS_CALL_INVALID"
    assert diagnostic.path.endswith("/catch_body/terminal/" + field)


def test_the_compiler_serves_the_same_identity_for_a_mutated_recovery_call():
    """`ProcessIRV1` is exported and mutable, so the parser is not the only door.
    #178's rule is that both doors serve ONE identity — the same code AND the
    same pointer, not merely the same decision."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    document = _doc([_try_catch(catch_steps=[dict(_NOTIFY)])])
    document["body"]["steps"][0]["catch_body"]["terminal"] = dict(_CALL)
    _compile_bodies(document)  # the control: unmutated, it compiles

    for field in ("wait", "abort_on_error"):
        ir = parse_process_ir_v1(document)
        object.__setattr__(ir.body.steps[0].catch_body.terminal, field, False)
        from boomi_mcp.compiler.process_ir.body_capabilities import (
            validate_body_capabilities,
        )

        with pytest.raises(ProcessIRCompileError) as excinfo:
            validate_body_capabilities(ir)
        assert "PROCESS_IR_SEMANTIC_RECOVERY_PROCESS_CALL_INVALID" in str(excinfo.value)
        assert "/catch_body/terminal/" + field in str(excinfo.value)


@pytest.mark.parametrize(
    "prefix,label",
    [
        ([{"kind": "message", "text": "m"}], "message"),
        (
            [{"kind": "set_dpp", "name": "n",
              "source_values": [{"value_type": "static", "value": "v"}]}],
            "set_dpp",
        ),
        ([dict(_NOTIFY), {"kind": "message", "text": "m"}], "notify then message"),
    ],
)
def test_only_a_notify_prefix_is_admitted_before_a_recovery_call(prefix, label):
    """The exception the live capture attests is `notify -> call`, and only that.
    Widening the terminal slot must not widen the prefix rule with it."""
    document = _doc([_try_catch(catch_steps=prefix)])
    document["body"]["steps"][0]["catch_body"]["terminal"] = dict(_CALL)
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(document)


def test_a_connector_inside_the_catch_body_still_blocks_the_recovery_call():
    """The exemption clears INHERITED connector ancestry only. A connector the
    caller authored inside this very body is refused exactly as before — that
    verdict is body-local and never reads the ancestry flag."""
    document = _doc([_try_catch(catch_steps=[dict(_CONN)])])
    document["body"]["steps"][0]["catch_body"]["terminal"] = dict(_CALL)
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(document)


def test_the_recovery_exemption_does_not_leak_to_a_non_recovery_body():
    """THE leak control, and it is written to reach the ancestry rule.

    A Branch leg with a non-empty prefix would be refused by the PREFIX rule
    before ancestry was ever consulted, so such a test would pass whether or not
    the exemption leaked. This leg's steps are EMPTY: no prefix rule can fire,
    and connector ancestry is the only thing left to refuse it.
    """
    document = _doc(
        [
            dict(_CONN),
            {
                "kind": "branch",
                "legs": [
                    {"steps": [], "terminal": dict(_CALL)},
                    {
                        "steps": [{"kind": "message", "text": "b"}],
                        "terminal": dict(_STOP),
                    },
                ],
            },
        ]
    )
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(document)
    assert (
        excinfo.value.diagnostics[0].code
        == "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY"
    )


def test_a_connector_upstream_of_the_handler_admits_the_recovery_call():
    """The positive half of the leak control, and the shape the live capture ran:
    `connector -> catcherrors -> notify -> processcall`, executed green on the
    renera account with the child observed running once per caught document."""
    document = _doc(
        [
            dict(_CONN),
            _try_catch(catch_steps=[dict(_NOTIFY)], scope="connector"),
        ]
    )
    document["body"]["steps"][1]["catch_body"]["terminal"] = dict(_CALL)
    parse_process_ir_v1(document)
    _compile_bodies(document)


# ---------------------------------------------------------------------------
# Golden parity: the whole graph, not just the notify shape
# ---------------------------------------------------------------------------

_DLQ_DB_CONN = "11111111-1111-1111-1111-111111111111"
_DLQ_DB_OP = "22222222-2222-2222-2222-222222222222"
_DLQ_REST_CONN = "33333333-3333-3333-3333-333333333333"
_DLQ_REST_OP = "44444444-4444-4444-4444-444444444444"
_DLQ_CACHE = "55555555-5555-5555-5555-555555555555"


def _dlq_symbols():
    from boomi_mcp.compiler.process_ir import connector_capabilities as CC
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )

    return SymbolTableV1(
        symbols=(
            ComponentSymbolV1(
                ref="$ref:DBCONN", component_id=_DLQ_DB_CONN,
                component_type="connector-settings", connector_type=CC.DATABASE_FAMILY,
            ),
            ComponentSymbolV1(
                ref="$ref:DBOP", component_id=_DLQ_DB_OP,
                component_type="connector-action", connector_type=CC.DATABASE_FAMILY,
                action_type="Get", connection_ref="$ref:DBCONN",
            ),
            ComponentSymbolV1(
                ref="$ref:RESTCONN", component_id=_DLQ_REST_CONN,
                component_type="connector-settings", connector_type=CC.REST_FAMILY,
            ),
            ComponentSymbolV1(
                ref="$ref:RESTOP", component_id=_DLQ_REST_OP,
                component_type="connector-action", connector_type=CC.REST_FAMILY,
                action_type="POST", connection_ref="$ref:RESTCONN",
            ),
            ComponentSymbolV1(
                ref="$ref:CACHE", component_id=_DLQ_CACHE,
                component_type="documentcache",
            ),
        )
    )


#: The canonical authoring of golden-000059's graph.
#:
#: Every label is authored, and that is the interesting part: the legacy builder
#: HARD-DEFAULTS the DLQ cache userlabel to "Route caught errors to DLQ cache"
#: while the canonical emitter reads whatever the node declares. Measured — the
#: first attempt reproduced all eight shapes and differed on exactly that one
#: attribute. A canonical document therefore has to say out loud what the legacy
#: config left implicit, which is the intended direction: the IR carries the
#: authored intent, not the builder's defaults.
_NOTIFY_DLQ_DOCUMENT = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "try_catch",
                "scope": "process",
                "retry": {"count": 0},
                "try_body": {
                    "steps": [
                        {"kind": "connector_call", "operation_ref": "$ref:DBOP",
                         "action": "get", "label": "DB extract"},
                        {"kind": "connector_call", "operation_ref": "$ref:RESTOP",
                         "action": "send", "label": "REST send"},
                    ],
                    "terminal": {"kind": "stop"},
                },
                "catch_body": {
                    "steps": [
                        {"kind": "notify", "level": "ERROR",
                         "message_template": (
                             "Integration catch path failed. Caught error: "
                             + CAUGHT_ERROR_PROPERTY_ID
                         )},
                        {"kind": "cache_put", "cache_ref": "$ref:CACHE",
                         "label": "Route caught errors to DLQ cache"},
                    ],
                    "terminal": {"kind": "stop"},
                },
            }
        ],
    },
}


def test_the_notify_dlq_golden_is_reproduced_from_canonical_ir():
    """#156's headline acceptance criterion, for `golden-000059`.

    The WHOLE `<shapes>` section — start, catcherrors, both connector actions,
    the success stop, the notify, the DLQ cache write and the recovery stop —
    byte-for-byte against the frozen file, including geometry, dragpoint names
    and wiring. Not the notify shape alone: an emitter can be right about its own
    bytes and still be placed on the wrong row or wired to the wrong shape, and
    only the full section catches that.

    The golden itself is NOT regenerated. It stays the legacy builder's output,
    frozen before this slice, which is what makes it an oracle rather than a
    photograph of the code under test.
    """
    import re

    from boomi_mcp.compiler.process_ir import lowering
    from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

    symbols = _dlq_symbols()
    ir = parse_process_ir_v1(_NOTIFY_DLQ_DOCUMENT)
    cfg = lowering.lower_process_ir_to_cfg(ir)
    plan = lowering.lower_cfg_to_emission_plan(cfg, symbols)
    emitted = emit_process(plan, symbols).process_xml

    frozen = (GOLDEN / "try_catch_notify_dlq_document_cache.xml").read_text()
    expected = re.search(r"<shapes>.*</shapes>", frozen, re.S).group(0)
    actual = re.search(r"<shapes>.*</shapes>", emitted, re.S).group(0)
    assert actual == expected


def test_the_canonical_notify_dlq_graph_agrees_with_the_legacy_builder():
    """The differential, at GRAPH level rather than shape level.

    The frozen golden pins one sample. This re-runs the legacy builder now, for
    the same graph, and requires the two `<shapes>` sections to agree — so a
    change that moved BOTH the golden and the canonical emitter together would
    still be caught here.
    """
    import re
    import sys as _sys

    _tests = str(_ROOT / "tests")
    if _tests not in _sys.path:
        _sys.path.insert(0, _tests)
    import _wave_gate_golden_corpus as corpus

    from boomi_mcp.compiler.process_ir import lowering
    from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

    legacy_xml = corpus.CASE_REGISTRY["trycatch_dlq:notify_document_cache"][1]()

    symbols = _dlq_symbols()
    ir = parse_process_ir_v1(_NOTIFY_DLQ_DOCUMENT)
    plan = lowering.lower_cfg_to_emission_plan(
        lowering.lower_process_ir_to_cfg(ir), symbols
    )
    canonical_xml = emit_process(plan, symbols).process_xml

    pattern = re.compile(r"<shapes>.*</shapes>", re.S)
    assert pattern.search(canonical_xml).group(0) == pattern.search(legacy_xml).group(0)


# ---------------------------------------------------------------------------
# T5: the serialized region chain
# ---------------------------------------------------------------------------


def _handler(op, terminal, *, scope="connector", retry=0):
    return {
        "kind": "try_catch",
        "scope": scope,
        "retry": {"count": retry},
        "try_body": {
            "steps": [{"kind": "connector_call", "operation_ref": op}],
            "terminal": terminal,
        },
        "catch_body": {"steps": [dict(_NOTIFY)], "terminal": dict(_STOP)},
    }


_CONTINUE = {"kind": "continue"}


@pytest.mark.parametrize("handlers", [2, 3, 5])
def test_a_serialized_region_chain_compiles_at_any_length(handlers):
    """The captured double-guard is two regions; nothing about the grammar is
    special to two, and the depth rule in particular used to fail at three."""
    from boomi_mcp.compiler.process_ir import invariants, lowering

    steps = [
        _handler("$ref:OP%d" % i, dict(_CONTINUE)) for i in range(handlers - 1)
    ]
    steps.append(_handler("$ref:OP%d" % (handlers - 1), dict(_STOP)))
    document = _doc(steps)

    ir = parse_process_ir_v1(document)
    _compile_bodies(document)
    cfg = lowering.lower_process_ir_to_cfg(ir)
    invariants.check_cfg_invariants(cfg)

    modes = [
        node.semantic.success_mode
        for node in cfg.nodes
        if node.semantic.semantic_kind == "try_catch"
    ]
    assert modes == ["continue"] * (handlers - 1) + ["terminal"], modes


@pytest.mark.parametrize(
    "steps,label",
    [
        ([_handler("$ref:OP", dict(_CONTINUE), scope="process")], "lone process handler"),
        ([_handler("$ref:OP", dict(_CONTINUE))], "lone connector handler"),
        (
            [dict(_CONN), _handler("$ref:OP", dict(_CONTINUE))],
            "connector_call then a lone handler",
        ),
        (
            [_handler("$ref:OP0", dict(_CONTINUE)), _handler("$ref:OP1", dict(_CONTINUE))],
            "chain whose LAST handler continues",
        ),
        (
            [_handler("$ref:OP0", dict(_STOP)), _handler("$ref:OP1", dict(_STOP))],
            "chain whose FIRST handler terminates",
        ),
    ],
)
def test_an_orphan_continue_is_refused(steps, label):
    """`continue` asserts a following handler exists.

    The first three shapes PARSED before Stage-1 QA raised it: the control-only
    try_catch root and the connector_call-sequence branch both return early
    without inspecting a try terminal, so only a later compiler check refused
    them — and on an unterminated-path code describing a graph defect rather than
    the authoring mistake. Both entry points now refuse them on the authoring
    rule.
    """
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(_doc(steps))
    assert (
        excinfo.value.diagnostics[0].code
        == "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED"
    )

    # ...and the compiler agrees. `_compile_bodies` parses first, so this asserts
    # the pair serve ONE verdict rather than that the compiler is independently
    # reachable here; the mutable-model path for this rule is covered by
    # `test_the_compiler_refuses_an_orphan_continue_on_a_mutated_model`.
    with pytest.raises((ProcessIRValidationError, ProcessIRCompileError)):
        _compile_bodies(_doc(steps))


def test_the_compiler_refuses_an_orphan_continue_on_a_mutated_model():
    """The mutable-model half: a legal chain, mutated to drop its last handler.

    This is the path a caller reaches by handing an exported `ProcessIRV1`
    straight to `compile_process_ir_v1`, and it never re-enters the parser — so
    it is the only way to prove the compiler carries this rule itself rather than
    inheriting the parser's refusal.
    """
    from boomi_mcp.compiler.process_ir.body_capabilities import (
        validate_body_capabilities,
    )
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    ir = parse_process_ir_v1(
        _doc([_handler("$ref:OP0", dict(_CONTINUE)), _handler("$ref:OP1", dict(_STOP))])
    )
    validate_body_capabilities(ir)  # the control: intact, it compiles

    object.__setattr__(ir.body, "steps", [ir.body.steps[0]])
    with pytest.raises(ProcessIRCompileError) as excinfo:
        validate_body_capabilities(ir)
    assert "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED" in str(excinfo.value)


def test_a_map_separated_chain_compiles_through_the_public_pipeline():
    """Stage-2 CDX-156-r1-01. The byte goldens were NOT enough.

    `test_the_notify_dlq_golden_is_reproduced_from_canonical_ir` and its chain
    sibling call `lower_process_ir_to_cfg` + `emit_process` directly, so they
    prove the EMITTER. `compile_process_ir_v1` additionally runs connector
    resolution — and that walk refused any non-call while a map pairing was
    pending, so the `handler(continue) -> map_ref -> handler(stop)` shape that
    `golden-000005` encodes could not be built through the public path at all,
    with matching profiles, while its bytes matched perfectly.

    A golden that only the emitter can produce is not a shipped capability.
    """
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    import _wave_gate_golden_corpus as corpus

    def document(map_ref):
        catch = {
            "steps": [dict(_NOTIFY)],
            "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE",
                         "label": "Route caught errors to DLQ cache"},
        }
        return _doc([
            {"kind": "try_catch", "scope": "connector", "retry": {"count": 0},
             "try_body": {"steps": [{"kind": "connector_call",
                                     "operation_ref": "$ref:GETOP"}],
                          "terminal": {"kind": "continue"}},
             "catch_body": catch},
            {"kind": "map_ref", "map_ref": map_ref},
            {"kind": "try_catch", "scope": "connector", "retry": {"count": 0},
             "try_body": {"steps": [{"kind": "connector_call",
                                     "operation_ref": "$ref:PATCHOP"}],
                          "terminal": dict(_STOP)},
             "catch_body": catch},
        ])

    def symbols(target_profile):
        return corpus.error_symbols(
            ComponentSymbolV1(
                ref="$ref:MAP", component_id="m1", component_type="transform.map",
                input_profile_ref="$ref:P1", output_profile_ref=target_profile,
            )
        )

    # GETOP produces P1; the map takes P1 and must hand PATCHOP the P1 it consumes.
    compile_process_ir_v1(parse_process_ir_v1(document("$ref:MAP")), symbols("$ref:P1"))

    # THE CONTROL. Carrying the pairing across the handler must not stop it being
    # CHECKED there — a map whose target does not match the protected call's
    # request profile is still refused, at the map's own pointer.
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(
            parse_process_ir_v1(document("$ref:MAP")), symbols("$ref:P2")
        )
    assert "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH" in str(excinfo.value)
    assert "/body/steps/1/map_ref" in str(excinfo.value)


def test_a_chain_region_grades_the_success_path_and_no_other_recovery_path():
    """Stage-2 CDX-156-r1-02, then re-cut by the adversarial audit.

    `derive_error_regions` originally walked the protected edge with an UNBOUNDED
    subtree collection, so region N absorbed every later handler AND their catch
    bodies — and `validate_error_handling` / `collect_retry_effect_findings` grade
    a region over exactly that set, so a retried region answered for writes on a
    later handler's RECOVERY path.

    The first fix bounded the walk AT the next handler. That over-corrected into
    the dangerous direction: `continue` means the protected path CONTINUES into
    the next handler, so region N really does re-run handler N+1's protected
    call, and excluding it stopped the retry checks from grading a write they
    should refuse (measured: a non-idempotent DB Send accepted one handler
    downstream of a retried region while the identical Send inside it was
    refused).

    So the boundary is drawn on the SUCCESS/FAILURE split, not on handler
    identity: the protected walk follows every edge except a `catch` edge. Both
    directions are asserted here, because each was a real defect.
    """
    from boomi_mcp.compiler.process_ir import lowering
    from boomi_mcp.compiler.process_ir.error_handling import derive_error_regions

    handlers = [
        _handler("$ref:OP0", dict(_CONTINUE)),
        _handler("$ref:OP1", dict(_CONTINUE)),
        _handler("$ref:OP2", dict(_STOP)),
    ]
    cfg = lowering.lower_process_ir_to_cfg(parse_process_ir_v1(_doc(handlers)))
    by_id = {node.node_id: node for node in cfg.nodes}
    regions = derive_error_regions(cfg)
    assert len(regions) == 3

    for index, region in enumerate(regions):
        protected = {by_id[n].source_path for n in region.try_node_ids}
        # FORWARD: every later handler's protected body IS graded — the flow
        # continues there and a retry re-runs it.
        for later in range(index, 3):
            assert "/body/steps/{0}/try_body/steps/0".format(later) in protected, (
                index, later, sorted(protected)
            )
        # NOT BACKWARD: an earlier handler's protected body ran before this
        # region was entered.
        for earlier in range(0, index):
            assert "/body/steps/{0}/try_body/steps/0".format(earlier) not in protected

        # NO recovery path but its own appears in the protected set, at any index.
        assert not [p for p in protected if "/catch_body/" in p], sorted(protected)
        own = "/body/steps/{0}/catch_body".format(index)
        assert all(
            by_id[n].source_path.startswith(own) for n in region.catch_node_ids
        ), sorted(by_id[n].source_path for n in region.catch_node_ids)


def test_a_retried_region_grades_a_write_in_a_later_handler_it_re_runs():
    """The safety direction, with the control that makes it non-vacuous.

    A non-idempotent write one handler DOWNSTREAM of a retried region is re-run
    by that region's retry, so it must be refused exactly as the identical write
    inside the region is. The control is the same chain with no retry anywhere:
    it must compile, or this test would pass simply because the shape never
    compiles.
    """
    import _wave_gate_golden_corpus as corpus
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    rethrow = {"kind": "exception", "message_template": "rethrow {1}"}

    def chain(retry):
        return _doc([
            _handler("$ref:GETOP", dict(_CONTINUE)),
            _handler("$ref:GETOP2", dict(_CONTINUE), retry=retry),
            {
                "kind": "try_catch", "scope": "connector", "retry": {"count": 0},
                "try_body": {
                    "steps": [{"kind": "connector_call", "operation_ref": "$ref:DBSEND"}],
                    "terminal": dict(_STOP),
                },
                "catch_body": {"steps": [dict(_NOTIFY)], "terminal": rethrow},
            },
        ])

    symbols = corpus.error_symbols()
    compile_process_ir_v1(parse_process_ir_v1(chain(0)), symbols)  # control

    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(chain(2)), symbols)
    codes = {d.code for d in excinfo.value.diagnostics}
    assert "PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE" in codes, codes


def test_a_retried_region_does_not_grade_a_later_recovery_only_write():
    """The other direction, which is what Stage-2 raised: a write that only a
    LATER handler's recovery path performs is never re-run by this region's
    retry, so grading it is a false refusal."""
    import _wave_gate_golden_corpus as corpus
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    staged = {"kind": "cache_put", "cache_ref": "$ref:CACHE", "label": "L"}
    document = _doc([
        _handler("$ref:GETOP", dict(_CONTINUE)),
        _handler("$ref:GETOP2", dict(_CONTINUE), retry=1),
        {
            "kind": "try_catch", "scope": "connector", "retry": {"count": 0},
            "try_body": {
                "steps": [{"kind": "connector_call", "operation_ref": "$ref:GETOP"}],
                "terminal": dict(_STOP),
            },
            # The staging write is on the RECOVERY path only. A read-only
            # protected body is deliberate: a write here would be a legitimate
            # refusal and would mask the thing under test.
            "catch_body": {"steps": [dict(_NOTIFY)], "terminal": staged},
        },
    ])
    compile_process_ir_v1(parse_process_ir_v1(document), corpus.error_symbols())


def test_an_intervening_map_belongs_to_the_preceding_protected_region():
    """The boundary rule the architect specified and live QA measured.

    A map between two handlers sits on the FIRST one's protected path — the
    documents it transforms are the ones that handler produced — so it is graded
    with that region's retries, not the next one's. Stated as a test because it
    is a semantic choice the bytes cannot express: `golden-000005` looks the same
    either way.
    """
    from boomi_mcp.compiler.process_ir import lowering
    from boomi_mcp.compiler.process_ir.error_handling import derive_error_regions

    document = _doc([
        _handler("$ref:OP0", dict(_CONTINUE)),
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        _handler("$ref:OP1", dict(_STOP)),
    ])
    cfg = lowering.lower_process_ir_to_cfg(parse_process_ir_v1(document))
    by_id = {node.node_id: node for node in cfg.nodes}
    first, second = derive_error_regions(cfg)

    assert "/body/steps/1" in {by_id[n].source_path for n in first.try_node_ids}
    assert "/body/steps/1" not in {by_id[n].source_path for n in second.try_node_ids}


# ---------------------------------------------------------------------------
# Adversarial-audit findings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "template,label",
    [
        ('{{"error": "{0}"}}'.format(CAUGHT_ERROR_PROPERTY_ID), "JSON body"),
        ("order {{id}} failed: {0}".format(CAUGHT_ERROR_PROPERTY_ID), "brace run"),
        ("{0} }}".format(CAUGHT_ERROR_PROPERTY_ID), "stray close brace"),
    ],
)
def test_a_braced_notify_template_is_refused(template, label):
    """A brace defeats the binding the token rule exists to require.

    MEASURED on the shipped escaper, both directions:
      `{"e":"<token>"}`      -> `'{"e":"{1}"}'`  — the whole body is quote-wrapped
                                so the substituted {1} is LITERAL and the message
                                carries no error at all;
      `order {id} failed: …` -> passed through unescaped, so `{id}` reaches a
                                MessageFormat pattern as an invalid argument.

    Either way a caller can satisfy "the template references the caught error"
    and still log a failure without the failure — which is exactly the outcome
    `NotifyNodeV1`'s own validator promises to exclude, so it is refused at
    authoring rather than emitted.
    """
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(
            _doc([_try_catch(catch_steps=[
                {"kind": "notify", "level": "ERROR", "message_template": template}
            ])])
        )


def test_an_unbraced_notify_template_still_binds_the_caught_error():
    """The control. A rule that refused every template would pass the test above
    and emit nothing, so the shipped golden's own text must still compile — and
    the emitted message must carry {1} OUTSIDE any quoting."""
    import re

    from boomi_mcp.compiler.process_ir import lowering
    from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

    symbols = _dlq_symbols()
    ir = parse_process_ir_v1(_NOTIFY_DLQ_DOCUMENT)
    plan = lowering.lower_cfg_to_emission_plan(
        lowering.lower_process_ir_to_cfg(ir), symbols
    )
    message = re.search(
        r"<notifyMessage>(.*?)</notifyMessage>", emit_process(plan, symbols).process_xml
    ).group(1)
    assert message.endswith("{1}"), message
    assert not message.startswith("&apos;"), message


def test_the_chain_rule_codes_are_served_by_the_compiler():
    """`_as_compile_error` translates a SHARED model rule into a compile
    diagnostic, so every code it can serve needs text in the COMPILER's tables —
    not just the model's.

    A blanket lookup over `_CUSTOM_ERROR_CODES` was the first shape and it could
    serve three codes this layer has no text for. The set is closed instead, and
    this is the test that keeps it honest: widening the shared rules without
    serving the new code fails here rather than reaching a caller as a diagnostic
    with an empty message.
    """
    from boomi_mcp.compiler.process_ir import diagnostics as compiler_diagnostics
    from boomi_mcp.compiler.process_ir.body_capabilities import _CHAIN_RULE_CODES

    assert _CHAIN_RULE_CODES, "the closed set is empty — the guard would be vacuous"
    for code in sorted(_CHAIN_RULE_CODES):
        assert compiler_diagnostics._MESSAGES.get(code), code
        assert compiler_diagnostics._REMEDIATION.get(code), code


def test_the_continuation_remediation_covers_the_rules_that_serve_it():
    """#156 added four refusals that serve CONTROL_CONTINUATION_UNSUPPORTED, whose
    remediation was written for #141 and told the caller to move steps into legs
    or arms — advice that is meaningless for a misplaced `continue`."""
    from boomi_mcp.models.process_ir import _REMEDIATION
    from boomi_mcp.errors import PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED

    text = _REMEDIATION[PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED]
    assert "continue" in text
    assert "try_catch" in text or "handler" in text


_DLQ_MAP = "88888888-8888-8888-8888-888888888888"

#: The canonical authoring of `golden-000005` — the connector-scoped DOUBLE guard.
#:
#: Region 1 protects the DB read and ends its protected path in `continue`; the
#: map sits between the handlers; region 2 protects the REST write with
#: retryCount=2 and terminates. Both recovery legs are notify -> DLQ cache ->
#: stop. This is the shape that needs `ContinueNodeV1` at all: region 1's
#: protected path has no closing shape in the frozen bytes, it flows into
#: `shape4 map` and on to `shape5 catcherrors`.
_CHAIN_DLQ_DOCUMENT = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "try_catch", "scope": "connector", "retry": {"count": 0},
                "try_body": {
                    "steps": [{"kind": "connector_call", "operation_ref": "$ref:DBOP",
                               "action": "get", "label": "DB extract"}],
                    "terminal": {"kind": "continue"},
                },
                "catch_body": {
                    "steps": [
                        {"kind": "notify", "level": "ERROR",
                         "message_template": "Integration catch path failed. Caught error: "
                                             + CAUGHT_ERROR_PROPERTY_ID},
                        {"kind": "cache_put", "cache_ref": "$ref:CACHE",
                         "label": "Route caught errors to DLQ cache"},
                    ],
                    "terminal": {"kind": "stop"},
                },
            },
            {"kind": "map_ref", "map_ref": "$ref:MAP"},
            {
                "kind": "try_catch", "scope": "connector", "retry": {"count": 2},
                "try_body": {
                    "steps": [{"kind": "connector_call", "operation_ref": "$ref:RESTOP",
                               "action": "send", "label": "REST send"}],
                    "terminal": dict(_STOP),
                },
                "catch_body": {
                    "steps": [
                        {"kind": "notify", "level": "ERROR",
                         "message_template": "Integration catch path failed. Caught error: "
                                             + CAUGHT_ERROR_PROPERTY_ID},
                        {"kind": "cache_put", "cache_ref": "$ref:CACHE",
                         "label": "Route caught errors to DLQ cache"},
                    ],
                    "terminal": dict(_STOP),
                },
            },
        ],
    },
}


def _chain_symbols():
    from boomi_mcp.compiler.process_ir import connector_capabilities as CC
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )

    return SymbolTableV1(symbols=_dlq_symbols().symbols + (
        ComponentSymbolV1(
            ref="$ref:MAP", component_id=_DLQ_MAP, component_type="transform.map",
            input_profile_ref="$ref:DBP", output_profile_ref="$ref:RESTP",
        ),
    ))


def test_the_connector_scoped_double_guard_golden_is_reproduced_from_canonical_ir():
    """#156's OTHER headline acceptance criterion, for `golden-000005`.

    The sibling test pins `golden-000059`, the single process-scoped handler.
    This one pins the double guard, which is the whole reason `ContinueNodeV1`
    exists — and it had NO permanent pin until the architect review asked for it:
    the byte equality was measured by hand mid-implementation and never written
    down, so every later change to region derivation, connector resolution and
    the plan invariants ran without it.

    The whole `<shapes>` section, byte for byte: main spine 1-7 (start,
    catcherrors, DB read, map, catcherrors retryCount=2, REST write, stop) then
    the two recovery blocks 8-10 and 11-13 on the catch row, including both
    catcherrors' labelled Try/Catch dragpoints.
    """
    import re

    from boomi_mcp.compiler.process_ir import lowering
    from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

    symbols = _chain_symbols()
    ir = parse_process_ir_v1(_CHAIN_DLQ_DOCUMENT)
    plan = lowering.lower_cfg_to_emission_plan(
        lowering.lower_process_ir_to_cfg(ir), symbols
    )
    emitted = emit_process(plan, symbols).process_xml

    frozen = (GOLDEN / "connector_scoped_trycatch_notify_dlq_document_cache.xml").read_text()
    expected = re.search(r"<shapes>.*</shapes>", frozen, re.S).group(0)
    assert re.search(r"<shapes>.*</shapes>", emitted, re.S).group(0) == expected


def test_the_recovery_golden_matches_the_archived_live_capture():
    """The new golden is LIVE-ANCHORED, and this is what makes that checkable.

    `golden-000075` was produced by the canonical compiler, so on its own it
    proves only self-consistency. The archived B2 capture was produced by the
    FROZEN legacy builder before this slice's lowering existed, stored by the
    platform, deployed and executed with the recovery child observed running —
    so agreement between the two is what carries the operability claim onto the
    canonical surface.

    Component ids, connector families, actions and userlabels are blinded: the
    capture ran against real `renera` components while the golden uses the
    corpus's deterministic placeholders. What is compared is the graph — shape
    types, order, geometry, wiring, and the processcall's own attributes.
    """
    import re

    capture = (
        _ROOT / "docs" / "architecture" / "evidence" / "issue-156" / "captures"
        / "oracle-graphs" / "B2_shapes.xml"
    )
    if not capture.is_file():  # pragma: no cover - the archive is committed
        pytest.skip("live capture archive is absent")

    def blinded(text):
        shapes = re.search(r"<shapes>.*</shapes>", text, re.S).group(0)
        shapes = re.sub(r'(connectionId|operationId|processId)="[^"]*"', r'\1="ID"', shapes)
        shapes = re.sub(r'connectorType="[^"]*"', 'connectorType="T"', shapes)
        shapes = re.sub(r'actionType="[^"]*"', 'actionType="A"', shapes)
        return re.sub(r'userlabel="[^"]*"', 'userlabel="L"', shapes)

    golden = (GOLDEN / "scoped_try_catch_notify_terminal_process_call.xml").read_text()
    assert blinded(golden) == blinded(capture.read_text())

    # ...and the attributes the capture exists to pin are NOT blinded away.
    assert 'abort="true"' in golden and 'wait="true"' in golden
    assert "<dragpoints/>" in golden  # terminal: no successor, no synthetic Stop


def test_the_notify_goldens_cannot_take_the_canonical_corpus_route_yet():
    """Why the two notify corpus cases still render through the legacy builder.

    The architect asked for them to be re-pointed at `_canonical_envelope_case`,
    and rejected — correctly — my first argument that doing so would make the
    goldens photographs of the code under test: the frozen bytes stay frozen
    either way, so the canonical route would genuinely add the normalize ->
    compile -> late-bind -> materialize -> full-envelope coverage that the
    shapes-level tests above do not have.

    The blocker is not effort, it is a CAPABILITY the canonical surface does not
    model. Both goldens drive a REST **POST** target, and
    `CONNECTOR_CALL_CAPABILITIES_V1` registers only `get` and `patch` for the
    REST family — so the graph is not authorable as a canonical `connector_call`
    chain at all, and `compile_process_ir_v1` refuses it at
    `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED` before any envelope is
    built. Connector capability rows are explicitly OUT of scope for #156 (the
    issue assigns them to #155), so adding one to unblock a golden migration
    would be this slice reaching into another's contract.

    This test exists so the claim is checkable rather than asserted, and so it
    FAILS the day a REST write intent is registered — at which point the
    migration becomes possible and should be done.
    """
    from boomi_mcp.compiler.process_ir.connector_capabilities import (
        CONNECTOR_CALL_CAPABILITIES_V1,
        REST_FAMILY,
    )

    registered = {action for family, action in CONNECTOR_CALL_CAPABILITIES_V1
                  if family == REST_FAMILY}
    assert registered == {"get", "patch"}, (
        "a REST write intent is now registered, so the notify goldens can take "
        "the canonical corpus route — re-point them (issue #156 ARCH-156-r2-06): "
        + repr(sorted(registered))
    )

    # ...and the refusal is real, not inferred from the table.
    import _wave_gate_golden_corpus as corpus
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    document = _doc([
        {"kind": "connector_call", "operation_ref": "$ref:GETOP", "action": "get"},
        {"kind": "connector_call", "operation_ref": "$ref:PATCHOP", "action": "post"},
        dict(_STOP),
    ])
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(document), corpus.error_symbols())
    assert "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED" in str(excinfo.value)
