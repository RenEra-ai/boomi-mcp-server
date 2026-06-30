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

* legacy-ref-acct (decommissioned) ``92a8b6a9-9fe4-48c1-87bd-7369acdf6523`` (Map Document to
  Slack Payload) — ``DocumentPropertyGet`` + ``<Defaults><Default toKey
  value/></Defaults>`` envelope.
* legacy-ref-acct (decommissioned) ``b8a90410-b9c5-401e-80f6-b0544f3a2104`` (Google CSV to
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


# Output port key for every single-output FunctionStep emitted by this builder.
# Live evidence (2026-05-26) from saved Boomi maps consistently shows output
# key=2 for single-output string/math/date families (Sum2, String2Lower,
# MathDivide, DateFormat all stored as key=2). Boomi schema-accepts key=1
# too, but matches the UI convention by using 2 here.
FUNCTION_OUTPUT_KEY: int = 2


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

    # --- Optional metadata (issue: native property map functions) ----------
    # Every field below defaults to the historical behaviour so the 14
    # pre-existing families emit byte-identical XML. They generalise the
    # emitter for the six ProcessProperty families (Get/Set Dynamic Process
    # Property, Get/Set Document Property, Get/Set defined Process Property).

    # FunctionStep ``name="..."`` attribute. None => use ``fn_type``.
    display_name: Optional[str] = None
    # ``cacheOption="..."`` attribute; None => omit it.
    cache_option: Optional[str] = None
    # ``enabled="true"`` attribute; None => omit it (True => emit "true").
    enabled: Optional[bool] = None
    # Single-output port key. None => emit an empty ``<Outputs/>`` (no output,
    # used by the side-effecting "Set" families).
    output_key: Optional[int] = FUNCTION_OUTPUT_KEY
    # When set, the output name is ``output_name_prefix + str(parameters[key])``.
    output_name_parameter: Optional[str] = None
    output_name_prefix: str = ""
    # Extra ``<Output>`` attributes, e.g. (("isReset", "false"),).
    output_extra_attrs: Tuple[Tuple[str, str], ...] = ()
    # Explicit ``toKey`` values for the mapped inputs (in mapped order). Empty
    # => default 1..N. ``PropertySet`` maps its value to input key 2.
    mapped_input_keys: Tuple[int, ...] = ()
    # When True, omit ``default=""`` on a static input whose resolved default
    # is empty (the property families render bare ``<Input key.../>``).
    omit_empty_input_defaults: bool = False
    # When set, emit a single ``<Input key="1" name="{prefix}{parameters[key]}"/>``
    # (no default) instead of static_input_names — the document/defined setters.
    dynamic_input_name: Optional[Tuple[str, str]] = None
    # Parameter holding a ``$ref:`` to an in-spec component (defined process
    # property families) + the required referenced component type.
    component_reference_parameter: Optional[str] = None
    component_reference_type: Optional[str] = None
    # Canonical canvas coordinates emitted in the FunctionStep open tag.
    x: str = "10.0"
    y: str = "10.0"

    @property
    def is_default_value_sentinel(self) -> bool:
        return self.fn_type == _DEFAULT_VALUE_SENTINEL


# ---------------------------------------------------------------------------
# Per-family configuration emitters
# ---------------------------------------------------------------------------


def _emit_empty_configuration(_parameters: Mapping[str, object]) -> str:
    return "<Configuration/>"


def _emit_simple_lookup_configuration(parameters: Mapping[str, object]) -> str:
    """Render the in-component <SimpleLookup> Configuration body.

    Discovered live (2026-05-26) by iteratively probing Boomi's component
    schema. The in-component form is NOT the same as the Environment Map
    Extension API form — Boomi uses a CrossRefTableObj wrapper with
    ColumnHeaders/Rows/Values/<ref value=""/> in the component XML:

    .. code-block:: xml

        <SimpleLookup>
          <Input index="1" name="Key"/>
          <Output index="1" name="Value"/>
          <CrossRefTableObj>
            <CrossRefTable>
              <ColumnHeaders>
                <columnHeader>ref1</columnHeader>
                <columnHeader>ref2</columnHeader>
              </ColumnHeaders>
              <Rows>
                <row><Values><ref value="A"/><ref value="active"/></Values></row>
              </Rows>
            </CrossRefTable>
          </CrossRefTableObj>
        </SimpleLookup>
    """
    rows = parameters.get("rows") or []
    row_xml: List[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        ref1_value = row.get("ref1", row.get("from", ""))
        ref2_value = row.get("ref2", row.get("to", ""))
        row_xml.append(
            "<row><Values>"
            f'<ref value="{_escape_xml(str(ref1_value))}"/>'
            f'<ref value="{_escape_xml(str(ref2_value))}"/>'
            "</Values></row>"
        )
    # The Output index inside the SimpleLookup configuration must match the
    # FunctionStep's outer <Output key="..."> so Boomi binds the lookup
    # result to the correct port at runtime. Both use FUNCTION_OUTPUT_KEY
    # (which is also written into the outer <Outputs><Output key=.../>).
    return (
        "<Configuration><SimpleLookup>"
        '<Input index="1" name="Key"/>'
        f'<Output index="{FUNCTION_OUTPUT_KEY}" name="Value"/>'
        "<CrossRefTableObj><CrossRefTable>"
        "<ColumnHeaders>"
        "<columnHeader>ref1</columnHeader>"
        "<columnHeader>ref2</columnHeader>"
        "</ColumnHeaders>"
        f"<Rows>{''.join(row_xml)}</Rows>"
        "</CrossRefTable></CrossRefTableObj>"
        "</SimpleLookup></Configuration>"
    )


def _emit_sequential_value_configuration(_parameters: Mapping[str, object]) -> str:
    """Render the in-component <SequentialValue/> Configuration body.

    Discovered live (2026-05-26): the in-component <Configuration> block is
    EMPTY. Boomi rejects keyName/batchSize/keyFixToLength as both attributes
    and child elements on the in-component <SequentialValue> node. The
    authorable Key Name / Fix to Length / Batch Size parameters live as
    ``default`` attributes on the FunctionStep's <Input> elements with
    those exact names (emitted by ``emit_function_step`` via the family's
    ``parameter_input_defaults`` map).
    """
    return "<Configuration><SequentialValue/></Configuration>"


def _emit_document_property_configuration(parameters: Mapping[str, object]) -> str:
    """Render the ``<DocumentProperty>`` Configuration body (live-captured).

    The DDP name supplied in ``parameters["document_property_name"]`` is the
    bare name (e.g. ``DDP_FOO``); Boomi stores it as
    ``propertyId="dynamicdocument.<DDP>"`` and
    ``propertyName="Dynamic Document Property - <DDP>"``. Attributes are
    serialised alphabetically (defaultValue, persist, propertyId,
    propertyName) to match the captured XML byte-for-byte.
    """
    ddp = str(parameters.get("document_property_name", ""))
    return (
        "<Configuration>"
        '<DocumentProperty defaultValue="" persist="false" '
        f'propertyId="dynamicdocument.{_escape_xml(ddp)}" '
        f'propertyName="Dynamic Document Property - {_escape_xml(ddp)}"/>'
        "</Configuration>"
    )


def _emit_defined_process_property_configuration(
    parameters: Mapping[str, object],
) -> str:
    """Render the ``<DefinedProcessProperty>`` Configuration body (live-captured).

    References a Process Property COMPONENT: ``componentId`` +
    ``componentName`` identify the component, ``propertyKey`` +
    ``propertyName`` the specific property within it. ``componentId`` is read
    from the resolved ``process_property_component_id`` (a Boomi UUID after
    ``$ref:`` resolution). Attributes are serialised alphabetically
    (componentId, componentName, propertyKey, propertyName).
    """
    component_id = str(parameters.get("process_property_component_id", ""))
    component_name = str(parameters.get("process_property_component_name", ""))
    property_key = str(parameters.get("process_property_key", ""))
    property_name = str(parameters.get("process_property_name", ""))
    return (
        "<Configuration>"
        f'<DefinedProcessProperty componentId="{_escape_xml(component_id)}" '
        f'componentName="{_escape_xml(component_name)}" '
        f'propertyKey="{_escape_xml(property_key)}" '
        f'propertyName="{_escape_xml(property_name)}"/>'
        "</Configuration>"
    )


# ---------------------------------------------------------------------------
# Per-parameter validators
# ---------------------------------------------------------------------------


def _validate_non_blank_string(value: object) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return "must be a non-blank string"
    return None


# Property-namespace prefixes a DDP name must NOT carry. The emitter adds the
# map-form ``dynamicdocument.`` prefix itself; a caller-supplied prefix (either
# the map form, or the scripting form ``document.dynamic.userdefined.`` that the
# Groovy authoring docs use) would be double-prefixed into a wrong propertyId.
_DDP_FORBIDDEN_PREFIXES: Tuple[str, ...] = (
    "dynamicdocument.",
    "document.dynamic.userdefined.",
)


def _validate_ddp_name(value: object) -> Optional[str]:
    """Validate a Dynamic Document Property name.

    Must be the bare name (e.g. ``DDP_FOO``) — the ``dynamicdocument.`` prefix
    is added by the emitter, so a caller-supplied namespace prefix would double
    it (e.g. ``dynamicdocument.document.dynamic.userdefined.DDP_FOO``) and point
    the map at the wrong property.
    """
    if not isinstance(value, str) or not value.strip():
        return "must be a non-blank string"
    stripped = value.strip()
    for prefix in _DDP_FORBIDDEN_PREFIXES:
        if stripped.startswith(prefix):
            return (
                f"must be the bare DDP name without the {prefix!r} prefix "
                "(the builder adds the 'dynamicdocument.' prefix itself)"
            )
    return None


def _validate_string(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return "must be a string"
    return None


def _validate_positive_int(value: object) -> Optional[str]:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return "must be a positive integer"
    return None


def _validate_lookup_rows(value: object) -> Optional[str]:
    if not isinstance(value, list) or len(value) == 0:
        return "must be a non-empty list of {ref1, ref2} (or {from, to}) entries"
    for index, row in enumerate(value):
        if not isinstance(row, Mapping):
            return f"row[{index}] must be a mapping object"
        if "ref1" in row or "ref2" in row:
            if "ref1" not in row or "ref2" not in row:
                return f"row[{index}] must declare both ref1 and ref2"
            if not isinstance(row["ref1"], str) or not isinstance(row["ref2"], str):
                return f"row[{index}].ref1 and ref2 must be strings"
        elif "from" in row or "to" in row:
            if "from" not in row or "to" not in row:
                return f"row[{index}] must declare both from and to"
            if not isinstance(row["from"], str) or not isinstance(row["to"], str):
                return f"row[{index}].from and to must be strings"
        else:
            return f"row[{index}] must declare ref1/ref2 or from/to"
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


def _validate_math_precision(value: object) -> Optional[str]:
    # Boomi accepts integer precision >=0 in the set_precision step. Reject
    # non-int / negative values so the emitted XML never carries a blank or
    # bogus default attribute.
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return "must be a non-negative integer"
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

# simple_lookup (SimpleLookup) — uses CrossRefTableObj wrapper in component XML.
# Shape verified live 2026-05-26 by iterative Boomi schema probing.
_SIMPLE_LOOKUP = FunctionFamily(
    name="simple_lookup",
    fn_type="SimpleLookup",
    category="Lookup",
    mapped_input_count=(1, 1),
    static_input_names=("Key",),
    parameter_input_defaults={},
    required_parameters=("rows",),
    optional_parameters=(),
    output_name="Value",
    emit_configuration=_emit_simple_lookup_configuration,
    parameter_validators={"rows": _validate_lookup_rows},
)

# sequential_value (SequentialValue) — params go as additional <Input> elements
# (NOT on the <SequentialValue/> Configuration block). Live-verified 2026-05-26:
# Boomi stores Key Name / Fix to Length / Batch Size as Input ``default``
# attributes on Input elements with those exact names. The <Configuration>
# block holds an empty <SequentialValue/> placeholder.
#
# Per the Boomi "Map Function components" docs, Sequential Value has three
# authorable fields: Key Name (unique counter identifier), Fix to Length
# (zero-padded length, optional), Batch Size (allocation reservation,
# optional, default 1). Increment Basis is an unmapped trigger input the
# builder always emits with default="" so the function increments per
# source record.
_SEQUENTIAL_VALUE = FunctionFamily(
    name="sequential_value",
    fn_type="SequentialValue",
    category="Sequential",
    mapped_input_count=(0, 0),
    static_input_names=(
        "Increment Basis",
        "Key Name",
        "Fix to Length",
        "Batch Size",
    ),
    # 0-based positions into static_input_names — Key Name is at index 1, etc.
    parameter_input_defaults={
        "key_name": 1,
        "fix_to_length": 2,
        "batch_size": 3,
    },
    required_parameters=("key_name",),
    optional_parameters=("fix_to_length", "batch_size"),
    output_name="Result",
    emit_configuration=_emit_sequential_value_configuration,
    parameter_validators={
        "key_name": _validate_non_blank_string,
        "fix_to_length": _validate_positive_int,
        "batch_size": _validate_positive_int,
    },
)

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
    parameter_validators={
        "operation": _validate_math_operation,
        "precision": _validate_math_precision,
    },
)


# ---------------------------------------------------------------------------
# Native property map functions (category="ProcessProperty").
# Live FunctionStep shapes captured in
# .codex/plans/property-functionstep-live-captures.md. Two distinct
# "process property" families: the DYNAMIC process property (PropertyGet/
# PropertySet, keyed by name, empty Configuration) vs the COMPONENT-backed
# defined Process Property (DefinedProcessPropertyGet/Set, referencing a
# Process Property component via componentId + propertyKey).
# ---------------------------------------------------------------------------

# dynamic_process_property_get (PropertyGet) — reads a Dynamic Process
# Property. No mapped source input; the DPP name is the "Property Name" input
# default; result flows to the target via output key 3.
_DYNAMIC_PROCESS_PROPERTY_GET = FunctionFamily(
    name="dynamic_process_property_get",
    fn_type="PropertyGet",
    category="ProcessProperty",
    mapped_input_count=(0, 0),
    static_input_names=("Property Name", "Default Value"),
    parameter_input_defaults={"property_name": 0, "default_value": 1},
    required_parameters=("property_name",),
    optional_parameters=("default_value",),
    output_name="Result",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={
        "property_name": _validate_non_blank_string,
        "default_value": _validate_string,
    },
    display_name="Get Dynamic Process Property",
    cache_option="none",
    output_key=3,
    omit_empty_input_defaults=True,
)

# dynamic_process_property_set (PropertySet) — writes a Dynamic Process
# Property. The DPP name is the "Property Name" input default; the mapped
# source value flows into "Property Value" (input key 2); no output.
_DYNAMIC_PROCESS_PROPERTY_SET = FunctionFamily(
    name="dynamic_process_property_set",
    fn_type="PropertySet",
    category="ProcessProperty",
    mapped_input_count=(1, 1),
    static_input_names=("Property Name", "Property Value"),
    parameter_input_defaults={"property_name": 0},
    required_parameters=("property_name",),
    optional_parameters=(),
    output_name="",
    emit_configuration=_emit_empty_configuration,
    parameter_validators={"property_name": _validate_non_blank_string},
    display_name="Set Dynamic Process Property",
    cache_option="none",
    output_key=None,
    mapped_input_keys=(2,),
    omit_empty_input_defaults=True,
)

# document_property_get (DocumentPropertyGet) — reads a Dynamic Document
# Property. Empty inputs; the DDP is read into the output (key 3) whose name
# is "Dynamic Document Property - <DDP>"; Configuration carries DocumentProperty.
_DOCUMENT_PROPERTY_GET = FunctionFamily(
    name="document_property_get",
    fn_type="DocumentPropertyGet",
    category="ProcessProperty",
    mapped_input_count=(0, 0),
    static_input_names=(),
    parameter_input_defaults={},
    required_parameters=("document_property_name",),
    optional_parameters=(),
    output_name="",
    emit_configuration=_emit_document_property_configuration,
    parameter_validators={"document_property_name": _validate_ddp_name},
    display_name="Get Document Property",
    cache_option="none",
    enabled=True,
    output_key=3,
    output_name_parameter="document_property_name",
    output_name_prefix="Dynamic Document Property - ",
    output_extra_attrs=(("isReset", "false"),),
)

# document_property_set (DocumentPropertySet) — writes a Dynamic Document
# Property. The mapped source value flows into the single input named
# "Dynamic Document Property - <DDP>" (key 1); no output; DocumentProperty config.
_DOCUMENT_PROPERTY_SET = FunctionFamily(
    name="document_property_set",
    fn_type="DocumentPropertySet",
    category="ProcessProperty",
    mapped_input_count=(1, 1),
    static_input_names=(),
    parameter_input_defaults={},
    required_parameters=("document_property_name",),
    optional_parameters=(),
    output_name="",
    emit_configuration=_emit_document_property_configuration,
    parameter_validators={"document_property_name": _validate_ddp_name},
    display_name="Set Document Property",
    cache_option="none",
    output_key=None,
    dynamic_input_name=("Dynamic Document Property - ", "document_property_name"),
)

# defined_process_property_get (DefinedProcessPropertyGet) — reads a
# component-backed Process Property. Empty inputs; value read into output
# (key 1) named after the property; references a Process Property component.
_DEFINED_PROCESS_PROPERTY_GET = FunctionFamily(
    name="defined_process_property_get",
    fn_type="DefinedProcessPropertyGet",
    category="ProcessProperty",
    mapped_input_count=(0, 0),
    static_input_names=(),
    parameter_input_defaults={},
    required_parameters=(
        "process_property_component_id",
        "process_property_component_name",
        "process_property_key",
        "process_property_name",
    ),
    optional_parameters=(),
    output_name="",
    emit_configuration=_emit_defined_process_property_configuration,
    parameter_validators={
        "process_property_component_id": _validate_non_blank_string,
        "process_property_component_name": _validate_non_blank_string,
        "process_property_key": _validate_non_blank_string,
        "process_property_name": _validate_non_blank_string,
    },
    display_name="Get Process Property",
    output_key=1,
    output_name_parameter="process_property_name",
    component_reference_parameter="process_property_component_id",
    component_reference_type="processproperty",
)

# defined_process_property_set (DefinedProcessPropertySet) — writes a
# component-backed Process Property. The mapped source value flows into the
# single input named after the property (key 1); no output; references a
# Process Property component.
_DEFINED_PROCESS_PROPERTY_SET = FunctionFamily(
    name="defined_process_property_set",
    fn_type="DefinedProcessPropertySet",
    category="ProcessProperty",
    mapped_input_count=(1, 1),
    static_input_names=(),
    parameter_input_defaults={},
    required_parameters=(
        "process_property_component_id",
        "process_property_component_name",
        "process_property_key",
        "process_property_name",
    ),
    optional_parameters=(),
    output_name="",
    emit_configuration=_emit_defined_process_property_configuration,
    parameter_validators={
        "process_property_component_id": _validate_non_blank_string,
        "process_property_component_name": _validate_non_blank_string,
        "process_property_key": _validate_non_blank_string,
        "process_property_name": _validate_non_blank_string,
    },
    display_name="Set Process Property",
    output_key=None,
    dynamic_input_name=("", "process_property_name"),
    component_reference_parameter="process_property_component_id",
    component_reference_type="processproperty",
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
        _SIMPLE_LOOKUP,
        _SEQUENTIAL_VALUE,
        _MATH,
        _DYNAMIC_PROCESS_PROPERTY_GET,
        _DYNAMIC_PROCESS_PROPERTY_SET,
        _DOCUMENT_PROPERTY_GET,
        _DOCUMENT_PROPERTY_SET,
        _DEFINED_PROCESS_PROPERTY_GET,
        _DEFINED_PROCESS_PROPERTY_SET,
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


def function_output_key(
    family: FunctionFamily, parameters: Optional[Mapping[str, object]] = None
) -> Optional[int]:
    """Return the single-output port key for a family, or None for no output.

    None means the family is side-effecting (a "Set" property function) and
    emits an empty ``<Outputs/>`` — the map builder must NOT wire a
    function-output→profile mapping for it.
    """
    return family.output_key


def function_mapped_input_keys(
    family: FunctionFamily,
    parameters: Mapping[str, object],
    input_count: int,
) -> List[int]:
    """Return the ``toKey`` values for a family's mapped inputs, in order.

    Defaults to ``1..input_count``; a family may override via
    ``mapped_input_keys`` (e.g. ``PropertySet`` maps its single source value
    to input key 2, after the static "Property Name" input at key 1).
    """
    if family.mapped_input_keys:
        return list(family.mapped_input_keys)[:input_count]
    return list(range(1, input_count + 1))


def _resolve_output_name(
    family: FunctionFamily, parameters: Mapping[str, object]
) -> str:
    """Resolve the ``<Output name="...">`` for a family.

    Families with a ``output_name_parameter`` derive the name from a
    parameter (e.g. the DDP/process-property name); others use the static
    ``output_name`` (defaulting to "Result").
    """
    if family.output_name_parameter is not None:
        value = str(parameters.get(family.output_name_parameter, ""))
        return f"{family.output_name_prefix}{value}"
    return family.output_name or "Result"


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
    # Per-operation narrowing for math: `precision` is only valid when
    # operation == set_precision; otherwise emit_function_step would silently
    # drop it. Reject at validate time so callers can't accidentally request
    # a no-op transform.
    if family.name == "math":
        op = parameters.get("operation")
        op_str = op.strip().lower() if isinstance(op, str) else None
        if op_str != "set_precision":
            allowed_keys = allowed_keys - {"precision"}
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
    op = None
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

    # FunctionStep ``name="..."`` attribute (display name) defaults to fn_type.
    display_name = family.display_name or fn_type

    # Emit <Inputs>.
    input_parts: List[str] = []
    if family.dynamic_input_name is not None:
        # Single input whose name is derived from a parameter (the document /
        # defined-property setters). No ``default`` attribute.
        prefix, param_key = family.dynamic_input_name
        input_name = f"{prefix}{parameters.get(param_key, '')}"
        input_parts.append(f'<Input key="1" name="{_escape_xml(input_name)}"/>')
    else:
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
            if family.omit_empty_input_defaults and default_value == "":
                # The property families render a bare <Input key.../> when the
                # input carries no caller-supplied default value.
                input_parts.append(
                    f'<Input key="{i}" name="{_escape_xml(input_name)}"/>'
                )
            else:
                input_parts.append(
                    f'<Input default="{_escape_xml(default_value)}" '
                    f'key="{i}" name="{_escape_xml(input_name)}"/>'
                )
    inputs_xml = f"<Inputs>{''.join(input_parts)}</Inputs>" if input_parts else "<Inputs/>"

    # Emit <Outputs>. output_key is None for side-effecting "Set" families
    # (empty <Outputs/>); otherwise emit the single output at the per-family
    # key (live convention: most families key=2, property getters key=3/1).
    if family.output_key is None:
        outputs_xml = "<Outputs/>"
    else:
        output_attrs: Dict[str, str] = {
            attr: value for attr, value in family.output_extra_attrs
        }
        output_attrs["key"] = str(family.output_key)
        output_attrs["name"] = _resolve_output_name(family, parameters)
        attr_str = " ".join(
            f'{attr}="{_escape_xml(str(value))}"'
            for attr, value in sorted(output_attrs.items())
        )
        outputs_xml = f"<Outputs><Output {attr_str}/></Outputs>"

    # Emit <Configuration>.
    emit_cfg = family.emit_configuration or _emit_empty_configuration
    configuration_xml = emit_cfg(parameters)

    # FunctionStep open-tag attributes — emitted in Boomi's alphabetical order
    # (cacheEnabled, cacheOption?, category, enabled?, key, name, position,
    # sumEnabled, type, x, y); cacheOption / enabled are per-family optionals.
    open_attrs: List[str] = ['cacheEnabled="true"']
    if family.cache_option is not None:
        open_attrs.append(f'cacheOption="{_escape_xml(family.cache_option)}"')
    open_attrs.append(f'category="{_escape_xml(category)}"')
    if family.enabled is not None:
        open_attrs.append(f'enabled="{"true" if family.enabled else "false"}"')
    open_attrs.append(f'key="{step_key}"')
    open_attrs.append(f'name="{_escape_xml(display_name)}"')
    open_attrs.append(f'position="{step_key}"')
    open_attrs.append('sumEnabled="false"')
    open_attrs.append(f'type="{_escape_xml(fn_type)}"')
    open_attrs.append(f'x="{_escape_xml(family.x)}"')
    open_attrs.append(f'y="{_escape_xml(family.y)}"')

    return (
        f'<FunctionStep {" ".join(open_attrs)}>'
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
    "FUNCTION_OUTPUT_KEY",
    "FunctionFamily",
    "SUPPORTED_FUNCTION_TYPES",
    "SUPPORTED_MATH_OPERATIONS",
    "emit_default_entry",
    "emit_function_step",
    "function_mapped_input_keys",
    "function_output_key",
    "get_function_family",
    "math_input_count_for_operation",
    "resolve_math_fn_type",
    "validate_function_mapping",
]
