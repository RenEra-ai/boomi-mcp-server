"""Tests for the issue #40 map-function registry contract."""

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.map_function_registry import (
    FUNCTION_FAMILIES,
    SUPPORTED_FUNCTION_TYPES,
    SUPPORTED_MATH_OPERATIONS,
    emit_default_entry,
    emit_function_step,
    get_function_family,
    math_input_count_for_operation,
    resolve_math_fn_type,
    validate_function_mapping,
)


SUPPORTED_FAMILIES = (
    "date_format",
    "default_value",
    "trim",
    "left_trim",
    "right_trim",
    "uppercase",
    "lowercase",
    "append",
    "prepend",
    "replace",
    "remove",
    "simple_lookup",
    "sequential_value",
    "math",
)


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


def test_supported_function_types_match_plan_allow_list():
    assert set(SUPPORTED_FUNCTION_TYPES) == set(SUPPORTED_FAMILIES)


def test_all_families_registered():
    for name in SUPPORTED_FAMILIES:
        assert name in FUNCTION_FAMILIES, f"missing family {name!r}"


@pytest.mark.parametrize("name", SUPPORTED_FAMILIES)
def test_get_function_family_resolves(name):
    family = get_function_family(name)
    assert family is not None
    assert family.name == name


def test_get_function_family_normalizes_case():
    assert get_function_family("DateFormat") is None  # not the public name
    assert get_function_family("DATE_FORMAT").name == "date_format"
    assert get_function_family("  trim  ").name == "trim"


def test_get_function_family_returns_none_for_unknown():
    assert get_function_family("totally_bogus") is None
    assert get_function_family("") is None
    assert get_function_family(None) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Math dispatcher
# ---------------------------------------------------------------------------


def test_math_supported_operations():
    assert set(SUPPORTED_MATH_OPERATIONS) == {
        "add",
        "subtract",
        "multiply",
        "divide",
        "set_precision",
        "ceil",
        "floor",
        "abs",
    }


@pytest.mark.parametrize(
    "operation,expected_fn_type,expected_inputs",
    [
        ("add", "MathAdd", 2),
        ("subtract", "MathSubtract", 2),
        ("multiply", "MathMultiply", 2),
        ("divide", "MathDivide", 2),
        ("set_precision", "MathSetPrecision", 1),
        ("ceil", "MathCeil", 1),
        ("floor", "MathFloor", 1),
        ("abs", "MathABS", 1),
    ],
)
def test_math_resolution(operation, expected_fn_type, expected_inputs):
    assert resolve_math_fn_type(operation) == expected_fn_type
    assert math_input_count_for_operation(operation) == expected_inputs


def test_math_resolution_unknown_operation():
    assert resolve_math_fn_type("modulo") is None
    assert math_input_count_for_operation("xor") is None


# ---------------------------------------------------------------------------
# validate_function_mapping
# ---------------------------------------------------------------------------


def _run(family_name, inputs, parameters):
    family = get_function_family(family_name)
    assert family is not None
    return validate_function_mapping(
        family,
        inputs=list(inputs),
        parameters=dict(parameters),
        field_prefix="function_mappings[0]",
    )


def test_validate_date_format_happy_path():
    err = _run(
        "date_format",
        ["src/date"],
        {"input_format": "yyyy-MM-dd", "output_format": "MM/dd/yyyy"},
    )
    assert err is None


def test_validate_date_format_missing_parameter():
    err = _run("date_format", ["src/date"], {"input_format": "yyyy"})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"
    assert "output_format" in err.field


def test_validate_date_format_wrong_input_count():
    err = _run("date_format", [], {"input_format": "y", "output_format": "y"})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_INPUT_COUNT_MISMATCH"


def test_validate_trim_zero_parameters():
    assert _run("trim", ["src/name"], {}) is None


def test_validate_trim_rejects_extra_parameter():
    err = _run("trim", ["src/name"], {"fix_to_length": 4})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"


def test_validate_left_trim_requires_fix_to_length():
    err = _run("left_trim", ["src/name"], {})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"


def test_validate_left_trim_rejects_non_positive_fix_to_length():
    err = _run("left_trim", ["src/name"], {"fix_to_length": 0})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"


def test_validate_append_happy_path():
    err = _run(
        "append",
        ["src/name"],
        {"value": "-suffix", "fix_to_length": 50},
    )
    assert err is None


def test_validate_replace_requires_both_keys():
    err = _run("replace", ["src/name"], {"search": "x"})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"


def test_validate_remove_happy_path():
    assert _run("remove", ["src/name"], {"value": " "}) is None


def test_validate_simple_lookup_requires_non_empty_rows():
    err = _run("simple_lookup", ["src/key"], {"rows": []})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"


def test_validate_simple_lookup_accepts_ref_form():
    err = _run(
        "simple_lookup",
        ["src/key"],
        {"rows": [{"ref1": "A", "ref2": "1"}, {"ref1": "B", "ref2": "2"}]},
    )
    assert err is None


def test_validate_simple_lookup_accepts_from_to_form():
    err = _run(
        "simple_lookup",
        ["src/key"],
        {"rows": [{"from": "A", "to": "1"}]},
    )
    assert err is None


def test_validate_simple_lookup_rejects_mixed_keys():
    err = _run("simple_lookup", ["src/key"], {"rows": [{"ref1": "A"}]})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"


def test_validate_sequential_value_no_inputs_no_parameters():
    # Component-XML form is parameter-free; keyName/batchSize/keyFixToLength
    # live in the Environment Map Extension layer.
    err = _run("sequential_value", [], {})
    assert err is None


def test_validate_sequential_value_rejects_extension_level_params():
    # If a caller mistakenly puts extension-level params in component config,
    # the registry rejects them since the builder cannot serialize them into
    # the component XML.
    err = _run("sequential_value", [], {"key_name": "abc"})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"


def test_validate_math_add_two_inputs():
    assert _run("math", ["a", "b"], {"operation": "add"}) is None


def test_validate_math_subtract_rejects_one_input():
    err = _run("math", ["a"], {"operation": "subtract"})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_INPUT_COUNT_MISMATCH"


def test_validate_math_abs_one_input():
    assert _run("math", ["a"], {"operation": "abs"}) is None


def test_validate_math_set_precision_one_input_with_precision_param():
    err = _run(
        "math", ["a"], {"operation": "set_precision", "precision": 2}
    )
    assert err is None


def test_validate_math_rejects_unsupported_operation():
    err = _run("math", ["a"], {"operation": "modulo"})
    assert err is not None
    assert err.error_code == "UNSUPPORTED_MATH_OPERATION"


def test_validate_default_value_no_inputs_required():
    family = get_function_family("default_value")
    assert family.is_default_value_sentinel is True
    assert family.mapped_input_count == (0, 0)


def test_validate_default_value_requires_value():
    err = _run("default_value", [], {})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"


def test_validate_default_value_accepts_blank_string():
    # Codex r1: blank strings are intentional for default_value (emit an empty
    # <Default value=""/>). The missing-key check must not pre-reject "".
    err = _run("default_value", [], {"value": ""})
    assert err is None


def test_validate_replace_accepts_empty_replacement_string():
    # Codex r1: blank replacement is a valid "remove the search string" use
    # case. The missing-key check must not pre-reject "".
    err = _run("replace", ["src/name"], {"search": "x", "replacement": ""})
    assert err is None


def test_validate_default_value_rejects_none():
    err = _run("default_value", [], {"value": None})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_MISSING"


def test_validate_math_does_not_accept_rounding_mode():
    # Codex r1: rounding_mode was previously optional but never emitted,
    # silently no-op'ing. Now rejected as an unsupported parameter.
    err = _run("math", ["a"], {"operation": "abs", "rounding_mode": "HALF_UP"})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"
    assert "rounding_mode" in err.field


def test_validate_math_precision_rejected_for_non_set_precision_ops():
    # Codex r2: precision was previously accepted for every math operation
    # but only emitted for set_precision — silent no-op for add/subtract/etc.
    # Now narrowed: precision only valid when operation == set_precision.
    for op in ("add", "subtract", "multiply", "divide", "ceil", "floor", "abs"):
        inputs = ["a", "b"] if op in ("add", "subtract", "multiply", "divide") else ["a"]
        err = _run("math", inputs, {"operation": op, "precision": 2})
        assert err is not None, f"precision was silently accepted for math.{op}"
        assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"
        assert "precision" in err.field


def test_validate_math_set_precision_rejects_invalid_precision_value():
    # Codex r2: set_precision must reject non-int / negative precision so the
    # emitted XML never carries a blank or bogus default="<bad>" attribute.
    for bad in (None, "abc", -1, 1.5, True):
        params = {"operation": "set_precision"}
        if bad is not None:
            params["precision"] = bad
        err = _run("math", ["a"], params)
        if bad is None:
            # None precision: missing-but-optional, this is allowed (set_precision
            # has no required precision per the registry).
            assert err is None
        else:
            assert err is not None, f"set_precision accepted bad precision {bad!r}"
            # bool True is technically isinstance(int) but our validator
            # rejects bool explicitly.
            assert err.error_code == "MAP_FUNCTION_PARAMETER_INVALID"


def test_validate_math_set_precision_accepts_valid_precision():
    err = _run("math", ["a"], {"operation": "set_precision", "precision": 0})
    assert err is None
    err2 = _run("math", ["a"], {"operation": "set_precision", "precision": 6})
    assert err2 is None


# ---------------------------------------------------------------------------
# emit_function_step
# ---------------------------------------------------------------------------


def test_emit_function_step_string_to_lower():
    family = get_function_family("lowercase")
    xml = emit_function_step(family, step_key=1, parameters={})
    assert 'type="StringToLower"' in xml
    assert 'category="String"' in xml
    assert 'key="1"' in xml
    assert 'position="1"' in xml
    assert '<Inputs><Input default="" key="1" name="Original String"/></Inputs>' in xml
    # Output key=2 matches live Boomi UI convention (FUNCTION_OUTPUT_KEY).
    assert '<Outputs><Output key="2" name="Result"/></Outputs>' in xml
    assert "<Configuration/>" in xml


def test_emit_function_step_date_format_populates_defaults():
    family = get_function_family("date_format")
    xml = emit_function_step(
        family,
        step_key=2,
        parameters={"input_format": "yyyy-MM-dd", "output_format": "MM/dd/yyyy"},
    )
    assert 'type="DateFormat"' in xml
    assert 'category="Date"' in xml
    assert 'name="Input Mask"' in xml
    assert 'default="yyyy-MM-dd"' in xml
    assert 'name="Output Mask"' in xml
    assert 'default="MM/dd/yyyy"' in xml


def test_emit_function_step_simple_lookup_renders_crossref_table():
    family = get_function_family("simple_lookup")
    xml = emit_function_step(
        family,
        step_key=1,
        parameters={
            "rows": [{"ref1": "A", "ref2": "active"}, {"from": "I", "to": "inactive"}],
        },
    )
    # Live-verified component XML form uses CrossRefTableObj wrapper.
    assert "<SimpleLookup>" in xml
    assert '<Input index="1" name="Key"/>' in xml
    # Codex r4: Output index must match FUNCTION_OUTPUT_KEY (2) so the lookup
    # result binds to the FunctionStep's outer <Output key="2"> port.
    assert '<Output index="2" name="Value"/>' in xml
    assert "<CrossRefTableObj><CrossRefTable>" in xml
    assert "<columnHeader>ref1</columnHeader>" in xml
    assert "<columnHeader>ref2</columnHeader>" in xml
    assert '<ref value="A"/><ref value="active"/>' in xml
    assert '<ref value="I"/><ref value="inactive"/>' in xml


def test_emit_function_step_sequential_value_empty_configuration():
    family = get_function_family("sequential_value")
    xml = emit_function_step(family, step_key=1, parameters={})
    # Live-verified: component XML is empty <SequentialValue/>. The keyName /
    # batchSize / keyFixToLength settings live in environment extensions,
    # NOT in the component XML.
    assert "<Configuration><SequentialValue/></Configuration>" in xml
    assert "keyName" not in xml
    assert "batchSize" not in xml


def test_emit_function_step_output_key_uses_2():
    # Codex r3 finding: live Boomi UI saves single-output FunctionSteps with
    # output key=2 (not 1). The builder must match this convention so the
    # corresponding output-mapping fromKey reference resolves correctly.
    family = get_function_family("lowercase")
    xml = emit_function_step(family, step_key=1, parameters={})
    assert '<Output key="2" name="Result"/>' in xml
    assert '<Output key="1"' not in xml


def test_emit_function_step_math_dispatches_on_operation():
    family = get_function_family("math")
    xml_add = emit_function_step(family, step_key=1, parameters={"operation": "add"})
    assert 'type="MathAdd"' in xml_add
    assert 'name="Value to Add"' in xml_add

    xml_floor = emit_function_step(family, step_key=2, parameters={"operation": "floor"})
    assert 'type="MathFloor"' in xml_floor
    # Floor has 1 input only
    assert 'name="Value"' in xml_floor
    assert "Value to" not in xml_floor


def test_emit_function_step_math_set_precision_input_default():
    family = get_function_family("math")
    xml = emit_function_step(
        family,
        step_key=1,
        parameters={"operation": "set_precision", "precision": 3},
    )
    assert 'type="MathSetPrecision"' in xml
    assert 'name="Number of Precision"' in xml
    assert 'default="3"' in xml


def test_emit_function_step_escapes_user_supplied_strings():
    family = get_function_family("append")
    xml = emit_function_step(
        family,
        step_key=1,
        parameters={"value": "<&>"},
    )
    assert "&lt;&amp;&gt;" in xml
    assert "<&>" not in xml.replace("&lt;&amp;&gt;", "")


def test_emit_function_step_rejects_default_value_sentinel():
    family = get_function_family("default_value")
    with pytest.raises(ValueError):
        emit_function_step(family, step_key=1, parameters={"value": "x"})


def test_emit_default_entry_escapes_value():
    xml = emit_default_entry(42, '<"x">')
    assert xml == '<Default toKey="42" value="&lt;&quot;x&quot;&gt;"/>'


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_emit_function_step_is_byte_stable():
    family = get_function_family("date_format")
    parameters = {"input_format": "yyyy-MM-dd", "output_format": "yyyy/MM/dd"}
    xml_a = emit_function_step(family, step_key=7, parameters=parameters)
    xml_b = emit_function_step(family, step_key=7, parameters=parameters)
    assert xml_a == xml_b


def test_emit_math_step_is_byte_stable_across_operations():
    family = get_function_family("math")
    for op in SUPPORTED_MATH_OPERATIONS:
        params = {"operation": op}
        if op == "set_precision":
            params["precision"] = 4
        a = emit_function_step(family, step_key=1, parameters=params)
        b = emit_function_step(family, step_key=1, parameters=params)
        assert a == b, f"non-deterministic emission for {op}"
