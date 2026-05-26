"""Issue #40: Map function primitives registry for transform.map function maps.

Single source of truth for the structured map-function families supported by
``MapFunctionBuilder``. The registry encodes:

* The public lowercase ``function_type`` name used in caller-authored
  ``function_mappings``.
* The Boomi in-component ``<FunctionStep type="...">`` attribute value.
* The Boomi ``category`` attribute the UI groups the function under.
* The mapped-input count rule (how many source paths the caller must supply).
* The static-input names emitted in the ``<Inputs>`` block in declaration
  order.
* The output name emitted in the ``<Outputs>`` block (always ``"Result"`` for
  M2.6a — multi-output families like ``StringSplit`` belong to a follow-up).
* The required and optional parameter keys per family.
* A per-family ``emit_configuration`` callable that renders the
  ``<Configuration>...</Configuration>`` body when needed (most string ops
  emit empty configuration, while ``SimpleLookup`` and ``SequentialValue``
  emit family-specific blocks).
* A per-family ``parameter_input_defaults`` mapping that names which static
  input slot should be populated from which parameter key (e.g.
  ``StringPrepend``'s "Char to Prepend" input gets ``parameters["value"]``
  on its ``default=""`` attribute).

The ``default_value`` family is a pseudo-entry — Boomi encodes a default
constant as a ``<Default toKey="..." value="..."/>`` element inside the
``<Defaults>`` block, NOT as a ``<FunctionStep>``. The registry exposes a
``DefaultValueFamily`` sentinel so callers can validate the parameter
contract uniformly even though the emission path differs.

Reference XML shape evidence (fetched 2026-05-26):

* reneraai-5RO3DD ``92a8b6a9-9fe4-48c1-87bd-7369acdf6523`` (Map Document to
  Slack Payload) — ``DocumentPropertyGet`` + ``<Defaults><Default toKey
  value/></Defaults>`` envelope.
* reneraai-5RO3DD ``b8a90410-b9c5-401e-80f6-b0544f3a2104`` (Google CSV to
  XML Summary Report) — ``Sum2`` (Numeric) + ``DocumentPropertyGet``
  combination with profile→function-input and function-output→profile
  mappings.
* work ``f5481730-b9b1-4b67-96eb-3a510feaa734`` — ``String2Lower`` (String).
* work ``e9e1a9b6-1dab-45c4-acf5-c6ba610be9ac`` — ``String2Lower`` with
  full FunctionStep attribute set.
* work ``7d835a51-272f-455e-92cf-5c94df024a61`` — ``PropertyGet``
  (ProcessProperty) with caller-supplied static defaults.
* Boomi documentation "Environment Map Extension functions" — API-level
  type catalog (``DateFormat``, ``StringToLower``, ``StringToUpper``,
  ``TrimWhitespace``, ``LeftTrim``, ``RightTrim``, ``StringPrepend``,
  ``StringReplace``, ``StringRemove``, ``SimpleLookup``, ``SequentialValue``,
  ``MathAdd|MathSubtract|MathMultiply|MathDivide|MathSetPrecision|MathCeil|MathFloor|MathABS``).

The registry uses the API-level type names. Live Boomi components may store
the same families under "v2"-suffixed names (``String2Lower``, ``Sum2``)
after a UI save, but the platform accepts the API-level names on insert.
QA validates this assumption at the live-call layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Mapping, Optional, Tuple

from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    MAP_FUNCTION_INPUT_COUNT_MISMATCH,
    MAP_FUNCTION_PARAMETER_INVALID,
    MAP_FUNCTION_PARAMETER_MISSING,
    UNSUPPORTED_MAP_FUNCTION_TYPE,
    UNSUPPORTED_MATH_OPERATION,
)


# ---------------------------------------------------------------------------
# Family descriptor
# ---------------------------------------------------------------------------


# Sentinel returned by FUNCTION_FAMILIES["default_value"] to signal that the
# family bypasses the <FunctionStep> emission path and instead emits a
# <Default toKey="..." value="..."/> inside <Defaults>.
_DEFAULT_VALUE_SENTINEL = "__default_value__"


@dataclass(frozen=True)
class FunctionFamily:
    """Metadata + emitter for one supported map-function family."""

    name: str
    fn_type: str  # value of <FunctionStep type="...">; "" for default_value sentinel
    category: str  # value of <FunctionStep category="..."> attribute
    mapped_input_count: Tuple[int, int]  # (min, max); max=-1 for unbounded
    static_input_names: Tuple[str, ...]
    # parameter_input_defaults[param_name] = static_input_position (0-based
    # index into static_input_names) whose <Input default=""> attribute gets
    # the parameter value.
    parameter_input_defaults: Mapping[str, int]
    required_parameters: Tuple[str, ...]
    optional_parameters: Tuple[str, ...]
    output_name: str
    emit_configuration: Optional[Callable[[Mapping[str, object]], str]]
    parameter_validators: Mapping[str, Callable[[object], Optional[str]]]

    @property
    def is_default_value_sentinel(self) -> bool:
        return self.fn_type == _DEFAULT_VALUE_SENTINEL


# ---------------------------------------------------------------------------
# Per-family configuration emitters
# ---------------------------------------------------------------------------


def _emit_empty_configuration(_parameters: Mapping[str, object]) -> str:
    return "<Configuration/>"


# NOTE: configuration emitters and parameter validators for simple_lookup /
# sequential_value were removed when those families were deferred — the
# in-component Boomi XML shape for both is unknown without live evidence
# (see the deferral comment below the math family registration).


# ---------------------------------------------------------------------------
# Per-parameter validators
# ---------------------------------------------------------------------------


def _validate_non_blank_string(value: object) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return "must be a non-blank string"
    return None


def _validate_string(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return "must be a string"
    return None


def _validate_positive_int(value: object) -> Optional[str]:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return "must be a positive integer"
    return None


_SUPPORTED_MATH_OPERATIONS: Tuple[str, ...] = (
    "add",
    "subtract",
    "multiply",
    "divide",
    "set_precision",
    "ceil",
    "floor",
    "abs",
)


_MATH_OPERATION_TO_FN_TYPE: Mapping[str, str] = {
    "add": "MathAdd",
    "subtract": "MathSubtract",
    "multiply": "MathMultiply",
    "divide": "MathDivide",
    "set_precision": "MathSetPrecision",
    "ceil": "MathCeil",
    "floor": "MathFloor",
    "abs": "MathABS",
}


_MATH_OPERATION_INPUT_COUNT: Mapping[str, int] = {
    "add": 2,
    "subtract": 2,
    "multiply": 2,
    "divide": 2,
    "set_precision": 1,
    "ceil": 1,
    "floor": 1,
    "abs": 1,
}


def _validate_math_operation(value: object) -> Optional[str]:
    if not isinstance(value, str) or value.strip().lower() not in _SUPPORTED_MATH_OPERATIONS:
        return (
            "must be one of "
            f"{', '.join(_SUPPORTED_MATH_OPERATIONS)}"
        )
    return None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


# date_format
_DATE_FORMAT = FunctionFamily(
    name="date_format",
    fn_type="DateFormat",
    category="Date",
    mapped_input_count=(1, 1),
    static_input_names=("Date String", "Input Mask", "Output Mask"),
    parameter_input_defaults={"input_format": 1, "output_format": 2},
    required_parameters=("input_format", "output_format"),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={
        "input_format": _validate_non_blank_string,
        "output_format": _validate_non_blank_string,
    },
)

# default_value (pseudo-family — emits to <Defaults>, not <Functions>)
_DEFAULT_VALUE = FunctionFamily(
    name="default_value",
    fn_type=_DEFAULT_VALUE_SENTINEL,
    category="",
    mapped_input_count=(0, 0),
    static_input_names=(),
    parameter_input_defaults={},
    required_parameters=("value",),
    optional_parameters=(),
    output_name="",
    emit_configuration=None,
    parameter_validators={"value": _validate_string},
)

# trim (TrimWhitespace)
_TRIM = FunctionFamily(
    name="trim",
    fn_type="TrimWhitespace",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String",),
    parameter_input_defaults={},
    required_parameters=(),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={},
)

# left_trim (LeftTrim)
_LEFT_TRIM = FunctionFamily(
    name="left_trim",
    fn_type="LeftTrim",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String", "Fix to Length"),
    parameter_input_defaults={"fix_to_length": 1},
    required_parameters=("fix_to_length",),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={"fix_to_length": _validate_positive_int},
)

# right_trim (RightTrim)
_RIGHT_TRIM = FunctionFamily(
    name="right_trim",
    fn_type="RightTrim",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String", "Fix to Length"),
    parameter_input_defaults={"fix_to_length": 1},
    required_parameters=("fix_to_length",),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={"fix_to_length": _validate_positive_int},
)

# uppercase (StringToUpper)
_UPPERCASE = FunctionFamily(
    name="uppercase",
    fn_type="StringToUpper",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String",),
    parameter_input_defaults={},
    required_parameters=(),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={},
)

# lowercase (StringToLower)
_LOWERCASE = FunctionFamily(
    name="lowercase",
    fn_type="StringToLower",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String",),
    parameter_input_defaults={},
    required_parameters=(),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={},
)

# append (StringAppend) — docs note Append is symmetric with Prepend
_APPEND = FunctionFamily(
    name="append",
    fn_type="StringAppend",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String", "Fix to Length", "Char to Append"),
    parameter_input_defaults={"fix_to_length": 1, "value": 2},
    required_parameters=("value",),
    optional_parameters=("fix_to_length",),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={
        "value": _validate_string,
        "fix_to_length": _validate_positive_int,
    },
)

# prepend (StringPrepend)
_PREPEND = FunctionFamily(
    name="prepend",
    fn_type="StringPrepend",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String", "Fix to Length", "Char to Prepend"),
    parameter_input_defaults={"fix_to_length": 1, "value": 2},
    required_parameters=("value",),
    optional_parameters=("fix_to_length",),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={
        "value": _validate_string,
        "fix_to_length": _validate_positive_int,
    },
)

# replace (StringReplace)
_REPLACE = FunctionFamily(
    name="replace",
    fn_type="StringReplace",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String", "String to Search", "String to Replace"),
    parameter_input_defaults={"search": 1, "replacement": 2},
    required_parameters=("search", "replacement"),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={
        "search": _validate_string,
        "replacement": _validate_string,
    },
)

# remove (StringRemove)
_REMOVE = FunctionFamily(
    name="remove",
    fn_type="StringRemove",
    category="String",
    mapped_input_count=(1, 1),
    static_input_names=("Original String", "String to Remove"),
    parameter_input_defaults={"value": 1},
    required_parameters=("value",),
    optional_parameters=(),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={"value": _validate_string},
)

# NOTE: simple_lookup (SimpleLookup) and sequential_value (SequentialValue) are
# DEFERRED for M2.6a. The Boomi platform rejected the documented API-level
# Configuration shape with schema errors when QA submitted a map containing
# either family on 2026-05-26:
#
#   SimpleLookup:    "Invalid content was found starting with element 'Table'.
#                    One of '{Input}' is expected."
#   SequentialValue: "Attribute 'keyFixToLength' is not allowed to appear in
#                    element 'SequentialValue'." (same for keyName / batchSize)
#
# The in-component XML shape for these two families differs from the
# Environment Map Extension API docs example, and no live transform.map
# component using either family was available on renera or work accounts at
# implementation time. Both are tracked as #40 follow-up work pending live
# Boomi XML evidence.

# math (dispatcher — fn_type resolved from parameters["operation"]).
# Note: ``rounding_mode`` was considered but is NOT accepted as an optional
# parameter because emit_function_step has no way to serialize it into the
# component XML form (no live evidence of where Boomi places this in
# <FunctionStep>). Accepting an unused parameter would silently no-op.
_MATH = FunctionFamily(
    name="math",
    fn_type="",  # resolved per-call via parameters["operation"]
    category="Numeric",
    mapped_input_count=(1, 2),  # depends on operation; narrowed in validate
    static_input_names=(),  # populated per-call
    parameter_input_defaults={},
    required_parameters=("operation",),
    optional_parameters=("precision",),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={"operation": _validate_math_operation},
)


FUNCTION_FAMILIES: Dict[str, FunctionFamily] = {
    family.name: family
    for family in (
        _DATE_FORMAT,
        _DEFAULT_VALUE,
        _TRIM,
        _LEFT_TRIM,
        _RIGHT_TRIM,
        _UPPERCASE,
        _LOWERCASE,
        _APPEND,
        _PREPEND,
        _REPLACE,
        _REMOVE,
        # simple_lookup and sequential_value deferred — see comment above _MATH.
        _MATH,
    )
}


SUPPORTED_FUNCTION_TYPES: Tuple[str, ...] = tuple(FUNCTION_FAMILIES)


SUPPORTED_MATH_OPERATIONS: Tuple[str, ...] = _SUPPORTED_MATH_OPERATIONS


# ---------------------------------------------------------------------------
# Public lookup helpers
# ---------------------------------------------------------------------------


def get_function_family(function_type: str) -> Optional[FunctionFamily]:
    """Look up a function family by its public lowercase name."""
    if not isinstance(function_type, str):
        return None
    return FUNCTION_FAMILIES.get(function_type.strip().lower())


def resolve_math_fn_type(operation: str) -> Optional[str]:
    """Return the Boomi fn_type for a ``math`` operation, or None if unknown."""
    if not isinstance(operation, str):
        return None
    return _MATH_OPERATION_TO_FN_TYPE.get(operation.strip().lower())


def math_input_count_for_operation(operation: str) -> Optional[int]:
    """Return the mapped-input count for a ``math`` operation, or None."""
    if not isinstance(operation, str):
        return None
    return _MATH_OPERATION_INPUT_COUNT.get(operation.strip().lower())


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_function_mapping(
    family: FunctionFamily,
    *,
    inputs: List[object],
    parameters: Mapping[str, object],
    field_prefix: str,
) -> Optional[BuilderValidationError]:
    """Validate input arity + parameter contract for a single mapping.

    Index-sensitive checks (resolving ``inputs`` against the source field
    index) happen in ``MapFunctionBuilder.validate_config`` — this helper
    only validates the shape of the mapping itself.
    """
    # For math, narrow the mapped-input count rule based on the operation.
    effective_input_count = family.mapped_input_count
    if family.name == "math":
        op = parameters.get("operation")
        if isinstance(op, str) and op.strip().lower() in _SUPPORTED_MATH_OPERATIONS:
            count = _MATH_OPERATION_INPUT_COUNT[op.strip().lower()]
            effective_input_count = (count, count)

    min_inputs, max_inputs = effective_input_count
    actual = len(inputs)
    if actual < min_inputs or (max_inputs != -1 and actual > max_inputs):
        if min_inputs == max_inputs:
            expected = f"exactly {min_inputs}"
        elif max_inputs == -1:
            expected = f"at least {min_inputs}"
        else:
            expected = f"{min_inputs}..{max_inputs}"
        return BuilderValidationError(
            f"{family.name} expects {expected} mapped inputs (got {actual})",
            error_code=MAP_FUNCTION_INPUT_COUNT_MISMATCH,
            field=f"{field_prefix}.inputs",
            hint=(
                "Each entry in inputs[] is a source profile leaf path that "
                "feeds one of the function's mapped inputs."
            ),
            details={
                "function_type": family.name,
                "expected": expected,
                "actual": actual,
            },
        )

    # Required parameters present + parameter-level validators pass. Only
    # missing keys / None count as "missing" here — blank strings are
    # delegated to per-family validators (some families accept blank
    # strings as a valid value, e.g. default_value with parameters.value="").
    for required in family.required_parameters:
        if required not in parameters or parameters.get(required) is None:
            return BuilderValidationError(
                f"{family.name} requires parameters.{required}",
                error_code=MAP_FUNCTION_PARAMETER_MISSING,
                field=f"{field_prefix}.parameters.{required}",
                hint=(
                    f"Declare parameters.{required} for function_type "
                    f"{family.name!r}."
                ),
                details={
                    "function_type": family.name,
                    "missing_parameter": required,
                },
            )

    allowed_keys = set(family.required_parameters) | set(family.optional_parameters)
    for key, value in parameters.items():
        if key not in allowed_keys:
            return BuilderValidationError(
                f"{family.name} does not accept parameters.{key}",
                error_code=MAP_FUNCTION_PARAMETER_INVALID,
                field=f"{field_prefix}.parameters.{key}",
                hint=(
                    f"Supported parameter keys for {family.name!r}: "
                    f"{sorted(allowed_keys) or 'none'}."
                ),
                details={
                    "function_type": family.name,
                    "unsupported_parameter": key,
                    "supported": sorted(allowed_keys),
                },
            )

        validator = family.parameter_validators.get(key)
        if validator is not None:
            msg = validator(value)
            if msg is not None:
                error_code = (
                    UNSUPPORTED_MATH_OPERATION
                    if (family.name == "math" and key == "operation")
                    else MAP_FUNCTION_PARAMETER_INVALID
                )
                return BuilderValidationError(
                    f"{family.name}.parameters.{key} {msg}",
                    error_code=error_code,
                    field=f"{field_prefix}.parameters.{key}",
                    details={
                        "function_type": family.name,
                        "parameter": key,
                        "supported_operations": (
                            list(_SUPPORTED_MATH_OPERATIONS)
                            if error_code == UNSUPPORTED_MATH_OPERATION
                            else None
                        ),
                    },
                )

    return None


# ---------------------------------------------------------------------------
# XML emission for one FunctionStep
# ---------------------------------------------------------------------------


def emit_function_step(
    family: FunctionFamily,
    *,
    step_key: int,
    parameters: Mapping[str, object],
) -> str:
    """Emit the ``<FunctionStep>...</FunctionStep>`` XML for a single function.

    ``default_value`` is a special pseudo-family — callers must route it
    through ``<Defaults>`` emission instead of calling this helper.
    """
    if family.is_default_value_sentinel:
        raise ValueError(
            "default_value emits to <Defaults>, not <Functions>; route it via "
            "the map builder's defaults block."
        )

    # Resolve math operation → fn_type.
    if family.name == "math":
        op = str(parameters.get("operation", "")).strip().lower()
        fn_type = _MATH_OPERATION_TO_FN_TYPE.get(op)
        if fn_type is None:
            raise BuilderValidationError(
                f"unsupported math operation {op!r}",
                error_code=UNSUPPORTED_MATH_OPERATION,
                field="parameters.operation",
            )
        # Build static input names from operation.
        if op in ("add", "subtract", "multiply", "divide"):
            verb_map = {
                "add": "Add",
                "subtract": "Subtract",
                "multiply": "Multiply",
                "divide": "Divide",
            }
            static_inputs: Tuple[str, ...] = ("Value", f"Value to {verb_map[op]}")
        elif op == "set_precision":
            static_inputs = ("Value", "Number of Precision")
        else:
            # ceil / floor / abs — single mapped input only.
            static_inputs = ("Value",)
        category = "Numeric"
    else:
        fn_type = family.fn_type
        static_inputs = family.static_input_names
        category = family.category

    # Emit <Inputs>.
    input_parts: List[str] = []
    for i, input_name in enumerate(static_inputs, start=1):
        default_value = ""
        for param_key, position in family.parameter_input_defaults.items():
            # parameter_input_defaults positions are 0-based into static_inputs
            if position + 1 == i and param_key in parameters:
                default_value = str(parameters[param_key])
                break
        if family.name == "math" and op == "set_precision" and i == 2:
            if "precision" in parameters and parameters["precision"] is not None:
                default_value = str(parameters["precision"])
        input_parts.append(
            f'<Input default="{_escape_xml(default_value)}" '
            f'key="{i}" name="{_escape_xml(input_name)}"/>'
        )
    inputs_xml = f"<Inputs>{''.join(input_parts)}</Inputs>" if input_parts else "<Inputs/>"

    # Emit <Outputs>.
    output_name = family.output_name or "Result"
    outputs_xml = f'<Outputs><Output key="1" name="{_escape_xml(output_name)}"/></Outputs>'

    # Emit <Configuration>.
    emit_cfg = family.emit_configuration or _emit_empty_configuration
    configuration_xml = emit_cfg(parameters)

    return (
        f'<FunctionStep cacheEnabled="true" category="{_escape_xml(category)}" '
        f'key="{step_key}" name="{_escape_xml(fn_type)}" position="{step_key}" '
        f'sumEnabled="false" type="{_escape_xml(fn_type)}" '
        f'x="10.0" y="10.0">'
        f"{inputs_xml}"
        f"{outputs_xml}"
        f"{configuration_xml}"
        "</FunctionStep>"
    )


def emit_default_entry(target_key: int, value: str) -> str:
    """Emit a single ``<Default toKey="..." value="..."/>`` entry."""
    return (
        f'<Default toKey="{target_key}" value="{_escape_xml(value)}"/>'
    )


__all__ = [
    "FUNCTION_FAMILIES",
    "FunctionFamily",
    "SUPPORTED_FUNCTION_TYPES",
    "SUPPORTED_MATH_OPERATIONS",
    "emit_default_entry",
    "emit_function_step",
    "get_function_family",
    "math_input_count_for_operation",
    "resolve_math_fn_type",
    "validate_function_mapping",
]
