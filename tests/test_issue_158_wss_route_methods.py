"""#158 QA-158-r2-01: the HTTP method a WSS listener route serves, pinned to the platform.

The formula lived on a claim — "the method derives from the input type" — that
held for the bare ``/ws/simple`` route and was copied to the API Service
``/ws/rest`` route from captures that only ever used EXECUTE operations. QA
measured the platform over all seven operation types x {none, singlejson} x
five methods on both routes. This file reads THAT capture and derives every
expectation from its status codes; nothing here restates a method by hand.

Evidence (committed, checksummed):
``docs/architecture/evidence/issue-158/captures/cap158-r3-wss-method-matrix/``
(renera, 2026-09-11; fixtures from the baseline ``3a8469e`` tools and raw
component XML, never the implementation under test).
"""

import json
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.components.builders._api_service_paths import (  # noqa: E402
    ASC_METHOD_BY_OPERATION_TYPE,
    DEFAULT_WSS_OPERATION_TYPE,
    api_service_http_method,
    effective_api_service_route,
    route_refuses_its_input,
    wss_http_method,
)
from boomi_mcp.categories.components.builders.connector_builder import (  # noqa: E402
    _WSS_OPERATION_TYPES,
    WssListenerOperationBuilder,
)

_CAPTURE = (
    _ROOT
    / "docs/architecture/evidence/issue-158/captures/cap158-r3-wss-method-matrix"
    / "r3-wss-method-evidence.json"
)
_EVIDENCE = json.loads(_CAPTURE.read_text(encoding="utf-8"))
_ASC_CELLS = _EVIDENCE["phase_A_ws_rest_asc_advanced"]["cells"]
_BARE_CELLS = _EVIDENCE["phase_B_bare_ws_simple_intermediate"]["cells"]
#: The 405 body names the method the platform REGISTERED for the route.
_REGISTERED = re.compile(r"on operation ([A-Z]+):/ws/rest/")


def _cell_key(cell_key):
    operation_type, input_type = cell_key.split("|")
    return operation_type, input_type


def _accepted(cell):
    return {m for m, r in cell["methods"].items() if 200 <= int(r["status"]) < 300}


def _served_asc_method(cell):
    """The ONE method the platform serves an all-inherit route on: the method
    it accepted, or — for a route that refuses its input — the method its own
    405 names."""
    accepted = _accepted(cell)
    if accepted:
        assert len(accepted) == 1, accepted
        return next(iter(accepted))
    named = {
        match.group(1)
        for r in cell["methods"].values()
        for match in [_REGISTERED.search(r.get("body_text") or r.get("body_raw") or "")]
        if match
    }
    assert len(named) == 1, (cell, named)
    return next(iter(named))


def test_the_capture_covers_the_whole_vocabulary():
    """Non-vacuity: every operation type the builder admits, with and without input."""
    expected = {(t, i) for t in _WSS_OPERATION_TYPES for i in ("none", "singlejson")}
    assert {_cell_key(k) for k in _ASC_CELLS} == expected
    assert {_cell_key(k) for k in _BARE_CELLS} == expected


def test_the_asc_method_is_the_measured_one_for_every_cell():
    wrong_under_the_input_type_rule = 0
    for cell_key, cell in sorted(_ASC_CELLS.items()):
        operation_type, input_type = _cell_key(cell_key)
        served = _served_asc_method(cell)
        effective = effective_api_service_route(
            "qa158r3a",
            {},
            {"object_name": "obj", "operation_type": operation_type, "input_type": input_type},
        )
        assert effective["method"] == served, cell_key
        # ...and a route is refused exactly where the platform refused it.
        assert route_refuses_its_input(effective["method"], input_type) is (
            not cell["callable"]
        ), cell_key
        wrong_under_the_input_type_rule += wss_http_method(input_type) != served
    # NON-VACUITY: the retired rule is wrong on this capture, as QA reported.
    assert wrong_under_the_input_type_rule == 9


def test_the_asc_table_is_the_builders_vocabulary():
    """Bidirectional pin: one row per operation type the builder admits, and the
    default is the builder's own."""
    assert set(ASC_METHOD_BY_OPERATION_TYPE) == set(_WSS_OPERATION_TYPES)
    assert DEFAULT_WSS_OPERATION_TYPE == WssListenerOperationBuilder.DEFAULT_OPERATION_TYPE
    # A blank type is the default; a type outside the vocabulary is UNKNOWN,
    # never guessed; an explicit override always wins.
    assert api_service_http_method("", operation_type="  ") == ASC_METHOD_BY_OPERATION_TYPE[
        DEFAULT_WSS_OPERATION_TYPE
    ]
    assert api_service_http_method("", operation_type="patch") == ""
    assert api_service_http_method(" put ", operation_type="QUERY") == "PUT"


def test_the_bare_route_method_is_always_one_the_runtime_accepts():
    for cell_key, cell in sorted(_BARE_CELLS.items()):
        _operation_type, input_type = _cell_key(cell_key)
        assert wss_http_method(input_type) in _accepted(cell), cell_key
        # Every bare route is callable with that method; GET with input is not.
        assert route_refuses_its_input("GET", input_type) is (
            "GET" not in _accepted(cell)
        ), cell_key


def test_the_served_rule_texts_are_the_authorities_own():
    """The served wording cannot restate a rule the code does not apply."""
    from boomi_mcp.categories.components.builders._api_service_paths import (
        ASC_METHOD_RULE,
        BARE_METHOD_RULE,
    )
    from boomi_mcp.categories.components.builders.connector_builder import _WSS_INPUT_TYPES

    # ASC: every (types -> method) clause the text states is the table's.
    stated = dict(
        (method.strip(), {t.strip() for t in types.split("/")})
        for clause in ASC_METHOD_RULE.split(": ", 1)[1].split(";")
        for types, method in [clause.split("->")]
    )
    by_method = {}
    for operation_type, method in ASC_METHOD_BY_OPERATION_TYPE.items():
        by_method.setdefault(method, set()).add(operation_type)
    assert stated == by_method
    # Bare: the text's two arms are exactly what `wss_http_method` returns.
    assert BARE_METHOD_RULE == "input_type none -> GET, anything else -> POST"
    assert wss_http_method("none") == "GET"
    assert {wss_http_method(t) for t in _WSS_INPUT_TYPES - {"none"}} == {"POST"}


def test_no_served_string_carries_its_own_copy_of_a_method_rule():
    """Structural guard: the retired rule lived in seven served strings, each its
    own copy. Only `_api_service_paths` may spell a method rule; every other
    served text concatenates its generated constants."""
    # Quoting-tolerant: "none -> GET", "``none`` -> GET", "'none'->GET".
    pattern = re.compile(r"none[`'\"]*\s*-*>\s*GET", re.IGNORECASE)
    # The two homes of the formula: the leaf holding the measured facts and the
    # served texts, and the module applying them.
    owners = {
        _ROOT / "src/boomi_mcp/categories/components/wss_route_methods.py",
        _ROOT / "src/boomi_mcp/categories/components/builders/_api_service_paths.py",
    }
    offenders = [
        str(path.relative_to(_ROOT))
        for path in sorted((_ROOT / "src").rglob("*.py"))
        if path not in owners and pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == [], offenders
    # NON-VACUITY: the owners do spell it, so the pattern matches real text.
    assert all(pattern.search(owner.read_text(encoding="utf-8")) for owner in owners)


# ---------------------------------------------------------------------------
# QA-158-r4-01: the SERVED surfaces, not the source spellings
# ---------------------------------------------------------------------------
#
# The source scan above keys on one spelling ("none -> GET") and QA found three
# served sentences it could not see ("derives from input_type (JSON input ->
# POST)", "both values arrive as HTTP POST", "inherits from the listener
# input_type"). The same defect class twice, so the guard moves to where the
# claim is READ: every served surface, every sentence that ties a listener or
# API Service method to anything.

#: A surface is in the listener / API Service domain by its text or its path.
_DOMAIN_TEXT = re.compile(
    r"listener|\bWSS\b|web services server|api service|\basc\b|/ws/(simple|rest)|"
    r"wss_listen|webservice",
    re.IGNORECASE,
)
_DOMAIN_PATH = re.compile(r"listener|wss|api_service|webservice|asc_wrapper", re.IGNORECASE)
#: ...states a method...
_METHOD = re.compile(r"https?[ _]?method|httpMethod|\bverb\b|\bmethod\b|\b(GET|POST|PUT|DELETE|PATCH)\b")
#: ...as following from something.
_DERIVES = re.compile(
    r"deriv|inherit|follow|arriv|decide|select|determin|called (by|with)|"
    r"method\b[^.]{0,20}\bfrom\b",
    re.IGNORECASE,
)
#: ...or states what the platform does with a GET route (QA-158-r5-01: the
#: consequence was stated by operation type alone, dropping the override).
_CONSEQUENCE = re.compile(r"cannot|can't|can not|refus|uncallable|never be (called|published|served)", re.IGNORECASE)
_GET_SUBJECT = re.compile(r"\bGET\b|\bQUERY\b")
_VERB = re.compile(r"\b(GET|POST|PUT|DELETE|PATCH)\b")
_OPERATION_TYPE = re.compile(r"\b(%s)\b" % "|".join(sorted(ASC_METHOD_BY_OPERATION_TYPE)))


def _walk(value, path, out):
    if isinstance(value, str):
        out.append((path, value))
    elif isinstance(value, dict):
        for key, item in value.items():
            _walk(item, "%s/%s" % (path, key), out)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for index, item in enumerate(value):
            _walk(item, "%s[%d]" % (path, index), out)


def _served_strings():
    """Every string a caller can be served: each schema template by name, each
    MCP tool's description and parameter schema, and every static template
    container `meta_tools` serves from."""
    sys.path.insert(0, str(_ROOT / "tests"))
    import _m12_12_legacy_inventory as inventory
    from boomi_mcp.categories import meta_tools

    out = []
    for name in meta_tools._valid_schema_names():
        _walk(meta_tools.get_schema_template_action(schema_name=name), "schema:" + name, out)
    for name, tool in inventory._served_tools().items():
        _walk(tool.description or "", "tool:%s.description" % name, out)
        _walk(tool.parameters or {}, "tool:%s.parameters" % name, out)
    for name, value in vars(meta_tools).items():
        if not name.startswith("__") and isinstance(value, (dict, list, tuple, str)):
            _walk(value, "meta_tools." + name, out)
    return out


def _consistent_example(sentence):
    """A concrete claim — one operation type and the verb(s) stated for it — that
    the measured table confirms."""
    types = set(_OPERATION_TYPE.findall(sentence))
    verbs = set(_VERB.findall(sentence))
    return len(types) == 1 and verbs == {ASC_METHOD_BY_OPERATION_TYPE[types.pop()]}


def _method_claims(items):
    """(path, sentence, sanctioned) for every served sentence stating a method."""
    from boomi_mcp.categories.components.wss_route_methods import (
        ASC_GET_WITH_INPUT_RULE,
        ASC_METHOD_RULE,
        BARE_METHOD_RULE,
    )

    seen = set()
    for path, text in items:
        if not (_DOMAIN_TEXT.search(text) or _DOMAIN_PATH.search(path)):
            continue
        # The generated texts carry no ". ", so a sentence split never cuts one.
        for sentence in re.split(r"(?<=\.)\s+", text):
            derivation = _METHOD.search(sentence) and _DERIVES.search(sentence)
            consequence = _GET_SUBJECT.search(sentence) and _CONSEQUENCE.search(sentence)
            if not (derivation or consequence):
                continue
            sanctioned = (
                (not consequence or ASC_GET_WITH_INPUT_RULE in sentence)
                and (
                    not derivation
                    or ASC_METHOD_RULE in sentence
                    or BARE_METHOD_RULE in sentence
                    or ASC_GET_WITH_INPUT_RULE in sentence
                    or _consistent_example(sentence)
                )
            )
            if (path, sentence) not in seen:
                seen.add((path, sentence))
                yield path, sentence, sanctioned


def test_every_served_statement_of_a_listener_method_is_the_authoritys():
    claims = list(_method_claims(_served_strings()))
    offenders = [(path, sentence) for path, sentence, ok in claims if not ok]
    assert offenders == [], offenders
    # NON-VACUITY on the REAL collection: the sweep reached the surfaces QA
    # measured, and found sanctioned statements there.
    paths = {path for path, _sentence, _ok in claims}
    for surface in (
        "schema:archetype:http_listener_to_rest/parameter_schema/$defs/ListenerSource/"
        "properties/operation_type/description",
        "schema:archetype:http_listener_to_db/parameter_schema/$defs/AscWrapperConfig/"
        "properties/http_method/description",
        "meta_tools._COMPONENT_CREATE_API_SERVICE/template/routes[0]/http_method",
    ):
        assert surface in paths, surface


def test_the_served_statement_guard_refuses_what_qa_found():
    """The guard's own witness: each sentence QA-158-r4-01 found is refused, a
    generated statement and a table-consistent example pass, and a concrete
    example the table contradicts is refused."""
    from boomi_mcp.categories.components.wss_route_methods import ASC_METHOD_RULE

    listener = "schema:archetype:http_listener_to_db/parameter_schema/$defs/ListenerSource"
    refused = [
        (listener + "/operation_type", "NOT an HTTP verb — the HTTP method derives from input_type (JSON input -> POST)."),
        (listener + "/input_type", "This preset is JSON-input only; both values arrive as HTTP POST."),
        (listener + "/asc", "Default (None) inherits from the listener input_type (JSON input -> POST)."),
        (listener + "/example", "Its all-inherit route serves POST for its UPDATE operation."),
        # QA-158-r5-01: the consequence stated by operation type alone.
        (listener + "/operation_type", "This preset's input is always a document, so a GET or QUERY operation cannot be published through asc_wrapper."),
    ]
    passing = [
        (listener + "/generated", "Default (None) inherits it from the operation: " + ASC_METHOD_RULE + "."),
        (listener + "/example", "Its all-inherit route serves POST for its EXECUTE operation."),
        ("tool:unrelated", "The REST client derives the method from its config."),
    ]
    verdicts = {sentence: ok for _p, sentence, ok in _method_claims(refused + passing)}
    for _path, sentence in refused:
        assert verdicts.get(sentence) is False, sentence
    for _path, sentence in passing[:2]:
        assert verdicts.get(sentence) is True, sentence
    # Outside the listener domain nothing is judged.
    assert passing[2][1] not in verdicts


def _advanced_listener_example():
    """The http_listener_to_db preset's own apiType=advanced example (asc_wrapper
    on, EXECUTE) — a served, pre-existing parameter set, not one invented here."""
    import copy

    from boomi_mcp.patterns.archetypes.http_listener_to_db import HttpListenerToDbArchetype

    [example] = [
        e for e in HttpListenerToDbArchetype.examples
        if e.name == "webhook_to_database_insert_advanced_runtime"
    ]
    params = copy.deepcopy(example.parameters)
    assert params["asc_wrapper"]["enabled"] is True
    return params


def test_the_listener_presets_refuse_an_asc_route_nothing_can_call():
    """QA-158-r4-01's optional half: `build_from_archetype` refuses the one
    combination the platform can never serve, before apply creates it — with the
    operation-type rule, an explicit override honoured."""
    from pydantic import ValidationError

    from boomi_mcp.patterns.archetypes.http_listener_to_db import HttpListenerToDbParameters

    params = _advanced_listener_example()
    HttpListenerToDbParameters.model_validate(params)  # CONTROL: EXECUTE -> POST

    for operation_type in ("QUERY", "GET"):
        refused = _advanced_listener_example()
        refused["listener"]["operation_type"] = operation_type
        try:
            HttpListenerToDbParameters.model_validate(refused)
        except ValidationError as exc:
            assert "asc_wrapper publishes the listener on GET" in str(exc), exc
        else:
            raise AssertionError("%s behind asc_wrapper was accepted" % operation_type)
        # ...unless the route pins a method the platform does serve.
        refused["asc_wrapper"]["http_method"] = "POST"
        HttpListenerToDbParameters.model_validate(refused)

    for operation_type in ("UPDATE", "DELETE", "CREATE", "UPSERT"):
        accepted = _advanced_listener_example()
        accepted["listener"]["operation_type"] = operation_type
        HttpListenerToDbParameters.model_validate(accepted)
    # Without the wrapper a QUERY listener is a bare route, always callable by POST.
    bare = _advanced_listener_example()
    bare["asc_wrapper"]["enabled"] = False
    bare["listener"]["operation_type"] = "QUERY"
    HttpListenerToDbParameters.model_validate(bare)


def test_the_rest_listener_preset_shares_the_refusal():
    import copy

    from pydantic import ValidationError

    from boomi_mcp.patterns.archetypes.http_listener_to_rest import (
        HttpListenerToRestArchetype,
        HttpListenerToRestParameters,
    )

    base = copy.deepcopy(HttpListenerToRestArchetype.examples[0].parameters)
    base["asc_wrapper"] = {"enabled": True, "base_url_path": "qa158rest"}
    HttpListenerToRestParameters.model_validate(base)  # CONTROL: default EXECUTE
    refused = copy.deepcopy(base)
    refused["listener"]["operation_type"] = "QUERY"
    try:
        HttpListenerToRestParameters.model_validate(refused)
    except ValidationError as exc:
        assert "asc_wrapper publishes the listener on GET" in str(exc), exc
    else:
        raise AssertionError("QUERY behind asc_wrapper was accepted by http_listener_to_rest")


def test_a_refusal_names_the_cause_of_its_get():
    """QA-158-r5-01 (b): a GET the caller pinned is fixed on the pin, an inherited
    one on the operation — the preset refusal and the orchestrate refusal both say
    which, and offer choices generated from the table."""
    from pydantic import ValidationError

    from boomi_mcp.categories.components.wss_route_methods import ASC_BODY_METHOD_CHOICES
    from boomi_mcp.categories.deployment import orchestration
    from boomi_mcp.patterns.archetypes.http_listener_to_db import HttpListenerToDbParameters

    def _refusal(**changes):
        params = _advanced_listener_example()
        for section, values in changes.items():
            params[section].update(values)
        try:
            HttpListenerToDbParameters.model_validate(params)
        except ValidationError as exc:
            return str(exc)
        raise AssertionError("accepted: %r" % changes)

    inherited = _refusal(listener={"operation_type": "QUERY"})
    assert "operation_type 'QUERY'" in inherited and ASC_BODY_METHOD_CHOICES in inherited
    pinned = _refusal(asc_wrapper={"http_method": "GET"})  # EXECUTE operation
    assert "asc_wrapper.http_method 'GET'" in pinned
    assert "remove asc_wrapper.http_method so the route inherits the operation type's (EXECUTE -> POST)" in pinned
    assert ASC_BODY_METHOD_CHOICES not in pinned
    # QA-158-r6-01: a pinned GET over a GET/QUERY operation is never told to clear
    # the pin — the method it would inherit is GET again.
    pinned_query = _refusal(listener={"operation_type": "QUERY"}, asc_wrapper={"http_method": "GET"})
    assert "remove asc_wrapper.http_method" not in pinned_query
    assert "set asc_wrapper.http_method to another method" in pinned_query

    target = orchestration.ResolvedBuildTarget(
        integration_name="i", process_key="root", process_component_id="c",
        process_name="n", process_status="created",
        component_summary=orchestration._build_component_summary([], {}, []),
    )
    meta = {"route_refuses_input": True, "http_method": "GET", "input_type": "singlejson"}
    inherited_error = orchestration._uncallable_route_refusal(
        "b", target, dict(meta, operation_type="QUERY")
    )
    pinned_error = orchestration._uncallable_route_refusal(
        "b", target, dict(meta, operation_type="EXECUTE", route_method_explicit=True)
    )
    pinned_query_error = orchestration._uncallable_route_refusal(
        "b", target, dict(meta, operation_type="QUERY", route_method_explicit=True)
    )
    assert ASC_BODY_METHOD_CHOICES in inherited_error.message
    assert "remove the route's http_method" in pinned_error.message
    assert ASC_BODY_METHOD_CHOICES not in pinned_error.message
    assert "remove the route's http_method" not in pinned_query_error.message
    assert "Set the route's http_method to another method" in pinned_query_error.message


def test_every_offered_remedy_makes_the_route_callable():
    """QA-158-r6-01, structurally: over every operation type x {none, singlejson}
    x {inherited, GET, POST} route state, a refused route is offered at least one
    remedy, and EVERY remedy offered — applied to the state — yields a route the
    predicate passes. A remedy is never advice; it is a checked change."""
    from boomi_mcp.categories.components.builders._api_service_paths import (
        uncallable_route_remedies,
    )

    refused_states = 0
    for operation_type in sorted(ASC_METHOD_BY_OPERATION_TYPE):
        for input_type in ("none", "singlejson"):
            for explicit in ("", "GET", "POST"):
                method = api_service_http_method(explicit, operation_type=operation_type)
                if not route_refuses_its_input(method, input_type):
                    continue
                refused_states += 1
                for none_available in (True, False):
                    remedies = uncallable_route_remedies(
                        explicit_method=explicit, operation_type=operation_type,
                        input_type=input_type, method_field="f",
                        input_none_available=none_available,
                    )
                    assert remedies, (operation_type, input_type, explicit)
                    for text, change in remedies:
                        state = {"explicit_method": explicit, "operation_type": operation_type,
                                 "input_type": input_type, **change}
                        fixed = api_service_http_method(
                            state["explicit_method"], operation_type=state["operation_type"]
                        )
                        assert not route_refuses_its_input(fixed, state["input_type"]), (
                            operation_type, input_type, explicit, text)
                        if not none_available:
                            assert "input type 'none'" not in text
    # NON-VACUITY: the refused states include the pinned-GET-over-QUERY case.
    assert refused_states >= 5, refused_states


def test_the_resolver_is_the_one_answer_to_what_needs_the_operation():
    """CDX-158-r1-01: `method_resolved` / `path_resolved` say what is knowable
    WITHOUT the linked operation. Only an explicit http_method pins the method
    (an input_type override does not — the method comes from the operation's
    type), and only an explicit object_name pins the path."""
    unlinked = lambda overrides: effective_api_service_route("base", overrides, None)  # noqa: E731

    assert unlinked({"object_name": "o", "input_type": "none"})["method_resolved"] is False
    assert unlinked({"object_name": "o", "http_method": "PUT"})["method_resolved"] is True
    assert unlinked({"object_name": "o"})["path_resolved"] is True
    assert unlinked({"http_method": "PUT"})["path_resolved"] is False
    linked = effective_api_service_route(
        "base", {"input_type": "none"}, {"object_name": "o", "operation_type": "QUERY"}
    )
    assert (linked["method"], linked["method_resolved"], linked["path_resolved"]) == ("GET", True, True)
    # An operation whose type is outside the vocabulary leaves the method unknown.
    unknown = effective_api_service_route("base", {}, {"object_name": "o", "operation_type": "PATCH"})
    assert (unknown["method"], unknown["method_resolved"]) == ("", False)


def test_the_served_asc_precedence_texts_are_the_measured_ones():
    """#158 QA-158-s2r2-01: a cross-base overlap was refused with the same-base
    text, which blamed the listener that actually served the path. Every served
    text saying which of two API Services answers a path is built from the
    leaf's measured texts, and the refusal names the cause that fired."""
    from boomi_mcp.categories import meta_tools
    from boomi_mcp.categories.components import wss_route_methods as leaf
    from boomi_mcp.categories.deployment import orchestration

    note = meta_tools._COMPONENT_CREATE_API_SERVICE["collision_note"]
    assert leaf.ASC_SAME_BASE_SHADOWING in note
    assert leaf.ASC_CROSS_BASE_MEASURED_PRECEDENCE in note
    assert "LISTENER_ASC_ROUTE_OVERLAP" in note and "LISTENER_ASC_COLLISION" in note

    step = orchestration._next_step_for_failure(
        orchestration.OrchestrateDeployError(
            code=orchestration.LISTENER_ASC_COLLISION, message="x"
        ),
        "listener_verify",
    )
    assert leaf.ASC_SAME_BASE_SHADOWING in step
    assert leaf.ASC_CROSS_BASE_LISTENER_LOSES in step
