"""The shared component-material boundary (issue #159 S2, interface freeze).

Preset-agnostic authorities every recipe that accepts connection material uses,
from ONE place, rather than re-deriving them per preset: the closed,
repr-redacted material model primitives (DB connection create/reuse and read
leaves, REST connection create/reuse and operation leaves, the closed transform
mapping leaves); the credential-boundary rules (``_SECRET_RULES``,
``_breaches``); the literal-query vocabulary (D12) and the request-target
grammar with their served sentences; the reused-connection target judge; and
the selected-base checks. Moved unchanged out of ``recipes.builtins.fanout``
(fan-out 2.0.0, #159 S1), whose names they keep; the fan-out module re-exports
them.

A recipe whose behaviour depends on this module declares it in
``implementation_dependency_modules`` so its ``implementation_sha256`` covers
this source. This module must never import a preset module.
"""

from __future__ import annotations

from dataclasses import dataclass as _dataclass
from types import MappingProxyType as _MappingProxyType
from typing import Annotated as _Annotated
from typing import Any as _Any
from typing import List as _List
from typing import Literal as _Literal
from typing import Mapping as _Mapping
from typing import Optional, Tuple
from typing import Sequence as _Sequence
from typing import Union as _Union

from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict as _ConfigDict
from pydantic import Field, model_validator
from pydantic import StrictBool as _StrictBool
from pydantic import StrictFloat as _StrictFloat
from pydantic import StrictInt as _StrictInt
from pydantic import StrictStr as _StrictStr
from pydantic import ValidationError as _ValidationError
from pydantic_core import PydanticCustomError

from ..errors import (
    COMPOSITION_CONTRACT_MISMATCH,
    FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED,
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_INPUT_INVALID,
)
from ..models.governance_intent import reject_retired_spellings
from ..models.integration_models import IntegrationComponentSpec
from ..models.recipe_contributions import RecipeComponentKey
from ..patterns.archetype_parameters import (
    DBResultField,
    DbCreateSettings,
    RestSendRequest,
)
from .errors import RecipeError, recipe_diagnostic


def _rt():
    """The surviving primitives and builder authorities, imported at CALL time.

    The recipe layer must load under the ``src.boomi_mcp`` namespace (the
    registry's source digest and ``test_the_recipe_layer_itself_loads_under_the_
    src_namespace`` both import it that way), and the primitives and component
    builders reach ``boomi_mcp`` by ABSOLUTE import. So they are imported where
    they are used, never at module scope. Nothing here is state: the same
    modules, and so the same objects, every call.
    """
    from types import SimpleNamespace

    from ..categories.components.builders.connector_builder import (
        BuilderValidationError,
        RestClientOperationBuilder,
    )
    from ..categories.components.builders.json_profile_builder import (
        JSONGeneratedProfileBuilder,
    )
    from ..categories.components.builders.map_function_registry import (
        FUNCTION_FAMILIES,
    )
    from ..categories.components.builders.profile_generation import (
        profile_from_json_schema,
    )
    from ..categories.components.builders.transform_map_validation import (
        required_target_coverage_gaps,
    )
    from ..patterns import archetype_assembly as assembly
    from ..patterns.primitives import _helpers as helpers
    from ..patterns.primitives import db_extract, field_map, rest_send

    return SimpleNamespace(
        BuilderValidationError=BuilderValidationError,
        RestClientOperationBuilder=RestClientOperationBuilder,
        JSONGeneratedProfileBuilder=JSONGeneratedProfileBuilder,
        FUNCTION_FAMILIES=FUNCTION_FAMILIES,
        profile_from_json_schema=profile_from_json_schema,
        required_target_coverage_gaps=required_target_coverage_gaps,
        REST_CREATE_AUTH_MAP=assembly._REST_CREATE_AUTH_MAP,
        SOURCE_PREFIX=assembly._SOURCE_PREFIX,
        TARGET_PREFIX=assembly._TARGET_PREFIX,
        TRANSFORM_PREFIX=assembly._TRANSFORM_PREFIX,
        ROLE_DB_CONNECTION=helpers.ROLE_DB_CONNECTION,
        ROLE_DB_GET_OPERATION=helpers.ROLE_DB_GET_OPERATION,
        ROLE_DB_READ_PROFILE=helpers.ROLE_DB_READ_PROFILE,
        ROLE_REST_CONNECTION=helpers.ROLE_REST_CONNECTION,
        ROLE_REST_OPERATION=helpers.ROLE_REST_OPERATION,
        ROLE_TARGET_PROFILE=helpers.ROLE_TARGET_PROFILE,
        ROLE_TRANSFORM_MAP=helpers.ROLE_TRANSFORM_MAP,
        primitive_component_key=helpers.primitive_component_key,
        slugify=helpers.slugify,
        DbExtractParameters=db_extract.DbExtractParameters,
        DbExtractPrimitive=db_extract.DbExtractPrimitive,
        db_connection_extension_fields=db_extract.db_connection_extension_fields,
        FieldMapParameters=field_map.FieldMapParameters,
        FieldMapPrimitive=field_map.FieldMapPrimitive,
        RestSendWithRetryParameters=rest_send.RestSendWithRetryParameters,
        RestSendWithRetryPrimitive=rest_send.RestSendWithRetryPrimitive,
        rest_connection_extension_fields=rest_send.rest_connection_extension_fields,
    )


#: DERIVED from the neutral parameter models — the authorities for these value
#: vocabularies — so the v2 surface cannot drift from them.
_RestMethodV2 = RestSendRequest.model_fields["method"].annotation
_DEFAULT_STATUS_CODES_V2: Tuple[int, ...] = tuple(
    RestSendRequest.model_fields["expected_status_codes"].default_factory()
)
_DbAuthModeV2 = DbCreateSettings.model_fields["auth_mode"].annotation
_DbDriverV2 = DbCreateSettings.model_fields["driver"].annotation
_DEFAULT_DB_PORT_V2: int = DbCreateSettings.model_fields["port"].default
_DbFieldTypeV2 = DBResultField.model_fields["data_type"].annotation

def _v2_function_families() -> Tuple[str, ...]:
    """The map-function families the v2 transform may use.

    DERIVED from the ``FunctionFamily`` registry (the parameter authority),
    restricted to the side-effect-free families: a fan-out's shared transform
    writes payload leaves, never process or document properties.
    """
    return tuple(
        sorted(
            name
            for name, family in _rt().FUNCTION_FAMILIES.items()
            if getattr(family, "effect_kind", None) == "pure"
        )
    )


# ---------------------------------------------------------------------------
# v2 component material — closed, strict, repr-redacted (preparer only)
# ---------------------------------------------------------------------------


class _FanoutMaterialModel(_BaseModel):
    """Strict, frozen, and repr-REDACTED: every value renders as ``...``.

    Material carries connection settings, SQL and URLs. A repr reaches logs and
    tracebacks, so no field value — not even a structural one — is rendered.
    Retired #157 spellings are refused by name before validation.
    """

    model_config = _ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, _value in super().__repr_args__():
            yield key, "..."

    @model_validator(mode="before")
    @classmethod
    def _refuse_retired_spellings(cls, value: _Any) -> _Any:
        if isinstance(value, _Mapping):
            reject_retired_spellings(value, path="")
        return value


class FanoutDbCreateMaterialV2(_FanoutMaterialModel):
    """Create a database connection. Value rules are ``DbCreateSettings``'s.

    The credential boundary also refuses a ``host`` carrying userinfo or a JDBC
    property, a ``database`` carrying a JDBC property, and a ``credential_ref``
    that is not an opaque ``credential://`` reference.
    """

    mode: _Literal["create"]
    driver: _DbDriverV2  # type: ignore[valid-type]
    host: str = Field(
        min_length=1,
        description="Database host. No userinfo ('@') and no JDBC property (';' or '=').",
    )
    port: int = Field(default=_DEFAULT_DB_PORT_V2, ge=1, le=65535)
    database: str = Field(
        min_length=1,
        description="Database (catalog) name. No JDBC property (';' or '=').",
    )
    username: Optional[str] = None
    credential_ref: Optional[str] = Field(
        default=None,
        description=(
            "An OPAQUE secret-store reference of the form 'credential://...', "
            "never the secret itself; any other value is refused."
        ),
    )


class FanoutConnectionReuseMaterialV2(_FanoutMaterialModel):
    """Reference an existing connection by exactly one selector."""

    mode: _Literal["reuse"]
    component_id: Optional[str] = Field(default=None, min_length=1)
    component_name: Optional[str] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _exactly_one_selector(self) -> "FanoutConnectionReuseMaterialV2":
        if (self.component_id is None) == (self.component_name is None):
            raise PydanticCustomError(
                "fanout_reuse_selector",
                "a reuse binding needs exactly one of component_id or component_name",
            )
        return self


FanoutDbConnectionMaterialV2 = _Annotated[
    _Union[FanoutDbCreateMaterialV2, FanoutConnectionReuseMaterialV2],
    Field(discriminator="mode"),
]


class FanoutResultFieldV2(_FanoutMaterialModel):
    name: str = Field(min_length=1)
    data_type: _DbFieldTypeV2  # type: ignore[valid-type]
    required: bool = False
    description: Optional[str] = None


class FanoutBindParameterV2(_FanoutMaterialModel):
    name: str = Field(min_length=1)
    direction: _Literal["in", "out"] = "in"


class FanoutDbReadMaterialV2(_FanoutMaterialModel):
    sql: str = Field(min_length=1)
    result_fields: Tuple[FanoutResultFieldV2, ...] = Field(min_length=1)
    bind_parameters: Tuple[FanoutBindParameterV2, ...] = ()
    batch_size: Optional[int] = Field(default=None, ge=1)
    max_rows: Optional[int] = Field(default=None, ge=1)


class FanoutDirectOperationV2(_FanoutMaterialModel):
    operation_type: _Literal["direct"]
    source_field: str = Field(min_length=1)
    target_path: str = Field(min_length=1)
    documentation_hint: Optional[str] = None


class FanoutFunctionParameterV2(_FanoutMaterialModel):
    """One map-function parameter: a name and a SCALAR value — never a mapping."""

    name: str = Field(min_length=1)
    value: _Union[_StrictStr, _StrictBool, _StrictInt, _StrictFloat]


class FanoutFunctionOperationV2(_FanoutMaterialModel):
    """A map-function operation. Families and parameters come from ``FunctionFamily``."""

    operation_type: _Literal["map_function"]
    function_type: str = Field(min_length=1)
    inputs: Tuple[str, ...] = ()
    target_path: str = Field(min_length=1)
    parameters: Tuple[FanoutFunctionParameterV2, ...] = ()
    documentation_hint: Optional[str] = None

    @model_validator(mode="after")
    def _check_against_the_family(self) -> "FanoutFunctionOperationV2":
        if self.function_type not in _v2_function_families():
            raise PydanticCustomError(
                "fanout_function_family_unsupported",
                "function_type is not a side-effect-free registered map-function family",
            )
        family = _rt().FUNCTION_FAMILIES[self.function_type]
        names = [parameter.name for parameter in self.parameters]
        allowed = set(family.required_parameters) | set(family.optional_parameters)
        if len(set(names)) != len(names) or not set(names) <= allowed:
            raise PydanticCustomError(
                "fanout_function_parameter_unknown",
                "parameters must be unique names the family declares",
            )
        if not set(family.required_parameters) <= set(names):
            raise PydanticCustomError(
                "fanout_function_parameter_missing",
                "a required parameter of the family is missing",
            )
        for parameter in self.parameters:
            validator = family.parameter_validators.get(parameter.name)
            if validator is not None and validator(parameter.value) is not None:
                raise PydanticCustomError(
                    "fanout_function_parameter_invalid",
                    "a parameter value fails the family's own validator",
                )
        return self


class FanoutScriptReferenceOperationV2(_FanoutMaterialModel):
    """A map-script operation that REFERENCES a script component by key.

    The script is authored as a canonical supporting component — an intent
    ``base_components`` entry of type ``script.mapping`` — never inline here.
    """

    operation_type: _Literal["map_script"]
    script_component_key: RecipeComponentKey
    inputs: Tuple[str, ...] = Field(min_length=1)
    outputs: Tuple[str, ...] = Field(min_length=1)
    documentation_hint: Optional[str] = None


FanoutTransformOperationV2 = _Annotated[
    _Union[
        FanoutDirectOperationV2,
        FanoutFunctionOperationV2,
        FanoutScriptReferenceOperationV2,
    ],
    Field(discriminator="operation_type"),
]


#: D12 (#159 S1, architect decision A): the ONLY names under which fan-out
#: 2.0.0 emits a literal REST query VALUE — a structured literal parameter, or
#: a query pair inside ``base_url`` / ``path``. Exact and case-sensitive, after
#: the trim for a structured name and URL-decoding for an inline one. A
#: deny-list name classifier cannot close (a provider-specific name carrying an
#: opaque value is indistinguishable from data), so the vocabulary is CLOSED.
#: Watermark-sourced names are recorded, never emitted, and are not judged here.
#: The served text below is RENDERED from this constant.
LITERAL_QUERY_NAMES_V2: Tuple[str, ...] = ("filter", "limit", "mode", "offset")

#: The value-free cause for a literal query name outside that vocabulary, or
#: inline query syntax that cannot be read unambiguously.
_QUERY_NAME_CAUSE = FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED


def _published_query_names() -> str:
    return ", ".join(LITERAL_QUERY_NAMES_V2)


#: The architect's served guarantee (D12, "Served contract text"), with the
#: published names rendered from :data:`LITERAL_QUERY_NAMES_V2`.
LITERAL_QUERY_GUARANTEE_V2 = (
    "Fan-out 2.0.0 requires opaque `credential://` references in credential "
    "fields, rejects URL userinfo and JDBC property injection in checked "
    "fields, and limits literal REST queries to the published names ({names}) "
    "and the REST builder\u2019s value rules. Usernames, SQL, map constants, URL "
    "path text, and values under permitted query names are caller-authored "
    "data that may be persisted in component configuration or XML, so supply "
    "only non-secret data in those fields."
).format(names=_published_query_names())

# The request-target GRAMMAR (#159 S1, B9 structural fix for CDX r6-01..04,
# QA r8-01/02): an ALLOWLIST of what may be EMITTED, not an enumeration of
# refused syntax. ONE function owns it (:func:`_query_value_violation` for a
# value, :func:`_judge_request_target` for a whole target), inline and
# structured values both call it, and the served sentence is rendered from
# these constants.
_URI_UNRESERVED = (
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
#: Every character an EMITTED request target may carry raw: RFC 3986
#: unreserved + reserved, and ``%`` only as a two-hex-digit escape. ``#`` is
#: NOT here (a fragment never reaches the server; text after it would be
#: persisted and ignored), nor whitespace or control characters.
_URI_TARGET_EXTRA = ":/?[]@!$&'()*+,;=%"
#: The characters a query VALUE may carry raw besides unreserved and a valid
#: ``%XX``: never ``+`` (form space), ``;`` (a pair separator to some servers),
#: ``&``, ``?`` or ``#``. Escaped (``%2B``, ``%3B``, ``%26``) they are data.
_QUERY_VALUE_EXTRA = "!$'()*,:@/="
_HEX_DIGITS = "0123456789abcdefABCDEF"
#: Raw characters a PATH may not carry before its query (ARCH-r1-03): either
#: would continue a stored base URL's query as a new pair.
_PATH_FORBIDDEN_RAW = "&;"
#: How the bytes of a run of ``%XX`` escapes must decode (r7-03). ONE place:
#: :func:`_decoded` and :func:`_first_undecodable` decode with these, and the
#: served sentence renders them.
_ESCAPE_ENCODING = "utf-8"
_ESCAPE_ERRORS = "strict"
_TARGET_CHARS = frozenset(_URI_UNRESERVED + _URI_TARGET_EXTRA)
_VALUE_CHARS = frozenset(_URI_UNRESERVED + _QUERY_VALUE_EXTRA)
_NAME_CHARS = frozenset(_URI_UNRESERVED + "%")

#: The ONE rendered statement of the grammar.
QUERY_GRAMMAR_SENTENCE_V2 = (
    "The effective request target (base_url followed by path, judged as "
    "joined) may carry only RFC 3986 characters, with no whitespace, control "
    "character or '#', at most one '?', and '%' only as a two-hex-digit "
    "escape whose escaped bytes decode as {errors} {encoding}; userinfo is "
    "refused in a leading authority; its query must be name=value pairs "
    "separated by '&', each name (after percent-decoding) a published name and "
    "each value only unreserved characters ({unreserved}), %XX escapes and "
    "{extra}. A path may not carry a raw {path_raw} before its query (a "
    "stored connection URL may already end in a query); escaped they are data. "
    "The stored base_url and the stored path are each judged on their "
    "own as well as joined. A structured literal value follows the same value "
    "rule. The two are SENT differently: an inline query value (in base_url or "
    "path) is sent as written, so the server decodes its escapes (%26 arrives "
    "as '&'); a structured literal value is percent-encoded by the platform, "
    "'%' included, so a %XX in a structured value is sent as those literal "
    "characters, not as the character it encodes; a structured value is still "
    "limited to the value alphabet above."
).format(
    unreserved="A-Z a-z 0-9 " + "-._~",
    extra=" ".join(_QUERY_VALUE_EXTRA),
    errors=_ESCAPE_ERRORS,
    encoding=_ESCAPE_ENCODING.upper(),
    path_raw=" or ".join("'{0}'".format(char) for char in _PATH_FORBIDDEN_RAW),
)

#: The per-field rule every query-carrying material field states.
_LITERAL_QUERY_FIELD_RULE = (
    "A literal REST query value is emitted only under the published names "
    "({names}): any other name, or query syntax outside the grammar below "
    "(including a name repeated across base_url, path and query_parameters), "
    "is refused with {cause}; a value the REST operation builder refuses is "
    "refused with PLAINTEXT_SECRET_REJECTED. "
).format(names=_published_query_names(), cause=_QUERY_NAME_CAUSE) + (
    QUERY_GRAMMAR_SENTENCE_V2 + " " + LITERAL_QUERY_GUARANTEE_V2
)


class FanoutRestCreateMaterialV2(_FanoutMaterialModel):
    """Create an UNAUTHENTICATED REST connection (secured auth must be reused)."""

    mode: _Literal["create"]
    base_url: str = Field(
        min_length=1,
        description=(
            "Absolute http(s) base URL. Userinfo ('user:password@') is refused. "
        )
        + _LITERAL_QUERY_FIELD_RULE,
    )


FanoutRestConnectionMaterialV2 = _Annotated[
    _Union[FanoutRestCreateMaterialV2, FanoutConnectionReuseMaterialV2],
    Field(discriminator="mode"),
]


class FanoutQueryParameterV2(_FanoutMaterialModel):
    name: str = Field(
        min_length=1,
        description=(
            "Compared after trimming. A literal parameter's name must be one of "
            "the published names ({names}); a watermark-sourced name is recorded "
            "and never emitted."
        ).format(names=_published_query_names()),
    )
    value_source: _Literal["literal", "watermark"]
    literal_value: Optional[str] = Field(default=None, description=_LITERAL_QUERY_FIELD_RULE)


class FanoutRestOperationMaterialV2(_FanoutMaterialModel):
    path: str = Field(
        min_length=1,
        description="Request path. Userinfo is refused. " + _LITERAL_QUERY_FIELD_RULE,
    )
    query_parameters: Tuple[FanoutQueryParameterV2, ...] = Field(
        default=(),
        description="Names are compared after trimming; a repeated name is refused.",
    )


_CREDENTIAL_REFERENCE_PREFIX = "credential://"
#: The cause a boundary refusal carries: the builders' own code for a plaintext
#: secret offered where only an opaque reference may cross.
_PLAINTEXT_SECRET_CAUSE = "PLAINTEXT_SECRET_REJECTED"

_SECRET_RULES = (
    "credential_reference",
    "jdbc_host",
    "jdbc_property",
    "url",
    "literal_query_name",
    "named_literal",
)


#: The REST builder's own code for a query/header pair it refuses as a secret.
_REST_SECRET_CODE = "REST_SECRET_VALUE_FORBIDDEN"


def _rest_query_value_is_secret(name: str, value: str) -> bool:
    """Whether the REST builder refuses ``name=value`` — its customProperties
    validator, the check the emitted operation's ``query_parameters`` meet at
    build time, read by error CODE only. Under a published name this judges
    the VALUE (JWT, ``Bearer``/``Basic`` schemes, long base64, ``[encrypted]``)."""
    error = _rt().RestClientOperationBuilder._validate_dict_param(
        {name: value}, "query_parameters"
    )
    return getattr(error, "error_code", None) == _REST_SECRET_CODE


def _escape_ok(text: str, index: int) -> bool:
    """Whether ``text[index]`` (a ``%``) starts a valid two-hex-digit escape."""
    escape = text[index + 1:index + 3]
    return len(escape) == 2 and all(char in _HEX_DIGITS for char in escape)


def _query_value_violation(raw: str) -> Optional[int]:
    """THE value grammar: the offset of the first character a query VALUE may
    not carry raw, or ``None``. Allowed: unreserved, a valid ``%XX`` escape,
    and :data:`_QUERY_VALUE_EXTRA`. Inline and structured values both call this."""
    for index, char in enumerate(raw):
        if char == "%":
            if not _escape_ok(raw, index):
                return index
        elif char not in _VALUE_CHARS:
            return index
    return None


def _decoded(raw: str) -> Optional[str]:
    """``raw`` percent-decoded ONCE, as strict UTF-8; ``None`` if undecodable."""
    from urllib.parse import unquote

    try:
        return unquote(raw, encoding=_ESCAPE_ENCODING, errors=_ESCAPE_ERRORS)
    except UnicodeDecodeError:
        return None


def _first_undecodable(text: str) -> Optional[int]:
    """The offset of the first ``%`` whose escaped byte cannot be decoded, or
    ``None`` (r7-05: so a pointer names the field that holds the bad escape).

    Escapes are decoded per RUN of consecutive ``%XX`` (a multi-byte UTF-8
    character spans one run; any raw character ends it). Assumes every ``%``
    already starts a well-formed escape."""
    index = 0
    length = len(text)
    while index < length:
        if text[index] != "%":
            index += 1
            continue
        start = index
        data = bytearray()
        while index + 2 < length and text[index] == "%":
            data.append(int(text[index + 1:index + 3], 16))
            index += 3
        try:
            bytes(data).decode(_ESCAPE_ENCODING, errors=_ESCAPE_ERRORS)
        except UnicodeDecodeError as exc:
            return start + 3 * exc.start
    return None


def _value_is_secret(name: str, raw: str) -> bool:
    """The REST builder's value check on the raw AND the once-decoded value."""
    decoded = _decoded(raw)
    return _rest_query_value_is_secret(name, raw) or (
        decoded is not None and _rest_query_value_is_secret(name, decoded)
    )


@_dataclass(frozen=True)
class _TargetVerdict:
    """A request-target judgment: ``cause``/``offset`` of the first refusal
    (``None`` when clean) and the decoded query names it carries."""

    cause: Optional[str]
    offset: int = 0
    names: Tuple[str, ...] = ()


def _judge_request_target(text: str) -> _TargetVerdict:
    """THE request-target grammar, applied to exactly the text that is EMITTED.

    1. every character is an RFC 3986 target character (``#``, whitespace and
       control characters never are), a ``%`` starts a valid escape, at most
       one ``?``, and no userinfo before the path;
    2. the query (after the ``?``) is ``pair ("&" pair)*`` with
       ``pair = name "=" value``: a raw name of unreserved characters and
       escapes that DECODES to a published name, never repeated, and a value
       that meets :func:`_query_value_violation`; an empty query is refused;
    3. only then each value meets the REST builder's value check (raw and
       once-decoded) — ``PLAINTEXT_SECRET_REJECTED``.
    """
    for index, char in enumerate(text):
        if char not in _TARGET_CHARS:
            return _TargetVerdict(_QUERY_NAME_CAUSE, index)
        if char == "%" and not _escape_ok(text, index):
            return _TargetVerdict(_QUERY_NAME_CAUSE, index)
    undecodable = _first_undecodable(text)
    if undecodable is not None:
        return _TargetVerdict(_QUERY_NAME_CAUSE, undecodable)
    authority_start = _leading_authority_start(text)
    if authority_start is not None:
        authority_end = len(text)
        for marker in "/?":
            found = text.find(marker, authority_start)
            if found >= 0:
                authority_end = min(authority_end, found)
        at = text.find("@", authority_start, authority_end)
        if at >= 0:
            return _TargetVerdict(_PLAINTEXT_SECRET_CAUSE, at)
    mark = text.find("?")
    if mark < 0:
        return _TargetVerdict(None)
    second = text.find("?", mark + 1)
    if second >= 0:
        return _TargetVerdict(_QUERY_NAME_CAUSE, second)
    query_start = mark + 1
    if query_start == len(text):
        return _TargetVerdict(_QUERY_NAME_CAUSE, mark)
    pairs: _List[Tuple[int, str, str]] = []
    seen: set = set()
    offset = query_start
    for segment in text[query_start:].split("&"):
        if "=" not in segment:
            return _TargetVerdict(_QUERY_NAME_CAUSE, offset)
        raw_name, raw_value = segment.split("=", 1)
        if any(char not in _NAME_CHARS for char in raw_name):
            return _TargetVerdict(_QUERY_NAME_CAUSE, offset)
        name = _decoded(raw_name)
        if name is None or name not in LITERAL_QUERY_NAMES_V2 or name in seen:
            return _TargetVerdict(_QUERY_NAME_CAUSE, offset)
        seen.add(name)
        violation = _query_value_violation(raw_value)
        if violation is not None or _decoded(raw_value) is None:
            return _TargetVerdict(
                _QUERY_NAME_CAUSE, offset + len(raw_name) + 1 + (violation or 0)
            )
        pairs.append((offset, name, raw_value))
        offset += len(segment) + 1
    for pair_offset, name, raw_value in pairs:
        if _value_is_secret(name, raw_value):
            return _TargetVerdict(_PLAINTEXT_SECRET_CAUSE, pair_offset)
    return _TargetVerdict(None, names=tuple(name for _o, name, _v in pairs))


def _leading_authority_start(text: str) -> Optional[int]:
    """Where the target's LEADING authority starts, or ``None`` (r7-02).

    Only a ``scheme://`` AT THE START (RFC 3986 scheme characters) or a
    scheme-relative ``//`` at the start of a path opens an authority; a
    ``://`` later in a path is path data (``/v1/tags://alice@example.org``)."""
    if text.startswith("//"):
        return 2
    marker = text.find("://")
    if marker <= 0:
        return None
    scheme = text[:marker]
    if not scheme[0].isascii() or not scheme[0].isalpha():
        return None
    if all(char.isascii() and (char.isalnum() or char in "+-.") for char in scheme):
        return marker + 3
    return None


def _userinfo_cause(value: str) -> Optional[str]:
    """The PER-FIELD userinfo check (a field's own authority, before joining)."""
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return _PLAINTEXT_SECRET_CAUSE  # unparseable: refused rather than judged clean
    return _PLAINTEXT_SECRET_CAUSE if "@" in parts.netloc else None


def _breaches(rule: str, value: _Any, owner: _Any) -> Optional[str]:
    """The refusal CAUSE for ``value`` of a field classified ``rule``, or ``None``."""
    if value is None:
        return None
    text = str(value).strip()
    if rule == "credential_reference":
        if text.startswith(_CREDENTIAL_REFERENCE_PREFIX) and len(text) > len(
            _CREDENTIAL_REFERENCE_PREFIX
        ):
            return None
        return _PLAINTEXT_SECRET_CAUSE
    if rule == "jdbc_host":
        return _PLAINTEXT_SECRET_CAUSE if any(char in text for char in "@;=") else None
    if rule == "jdbc_property":
        return _PLAINTEXT_SECRET_CAUSE if any(char in text for char in ";=") else None
    if rule == "url":
        # Per-field userinfo only: the query grammar is judged on the JOINED
        # target (base_url + path), which only the preparer can assemble.
        return _userinfo_cause(text)
    if rule == "literal_query_name":
        if getattr(owner, "value_source", None) != "literal":
            return None  # a watermark-sourced name is recorded, never emitted
        return None if text in LITERAL_QUERY_NAMES_V2 else _QUERY_NAME_CAUSE
    if rule == "named_literal":
        name = str(getattr(owner, "name", "") or "").strip()
        if getattr(owner, "value_source", None) == "literal":
            # The SAME value grammar as an inline value (QA r8-02) — only for a
            # value that is EMITTED (r7-04) — then the builder.
            if _query_value_violation(text) is not None or _decoded(text) is None:
                return _QUERY_NAME_CAUSE
            return _PLAINTEXT_SECRET_CAUSE if _value_is_secret(name, text) else None
        # A watermark-sourced parameter's literal_value is never emitted, so no
        # grammar applies; the builder's secret check still does, as before B9.
        return _PLAINTEXT_SECRET_CAUSE if _rest_query_value_is_secret(name, text) else None
    return _PLAINTEXT_SECRET_CAUSE  # an unknown rule fails closed


# ---------------------------------------------------------------------------
# Value-free refusals
# ---------------------------------------------------------------------------


def _refusal(
    cause: str,
    path: str,
    *,
    phase: str,
    code: str = RECIPE_CONSTRAINT_FAILED,
    target: Optional[str] = None,
) -> RecipeError:
    """One value-free diagnostic: a static message, a cause code and a POINTER.

    The pointer is structural (positions and field names from the caller's own
    request). No value — not a key, not a label, not a setting — is rendered.
    """
    return RecipeError(
        (
            recipe_diagnostic(
                code,
                phase=phase,  # type: ignore[arg-type]
                path=path,
                target=target,
                cause_codes=(cause,),
            ),
        )
    )


def _material_failure(path: str, error: BaseException) -> RecipeError:
    """A value-free refusal for a value rule an authority model enforced."""
    cause = getattr(error, "error_code", None) or RECIPE_INPUT_INVALID
    return _refusal(str(cause), path, phase="input", code=RECIPE_INPUT_INVALID)


def _authority_check(pointer: str, model: _Any, /, **values: _Any) -> _Any:
    """Validate ``values`` through a neutral authority model, value-free on failure."""
    try:
        return model(**values)
    except _ValidationError:
        raise _material_failure(pointer, ValueError()) from None


def _builder_check(path: str, build):
    """Run a primitive build; a builder refusal becomes a value-free diagnostic."""
    try:
        return build()
    except _rt().BuilderValidationError as exc:
        raise _refusal(
            str(getattr(exc, "error_code", "") or RECIPE_CONSTRAINT_FAILED),
            path,
            phase="input",
        ) from None
    except _ValidationError:
        raise _material_failure(path, ValueError()) from None


def _check_base_component(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    key: str,
    *,
    path: str,
    component_type: str,
    connector_type: Optional[str] = None,
    binding_mode: Optional[str] = None,
) -> None:
    component = base_catalog.get(key)
    if component is None or component.type != component_type:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
    config = component.config or {}
    if connector_type is not None and config.get("connector_type") != connector_type:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
    if binding_mode is None:
        return
    # ARCH-r1-01: what a base IS is decided by what apply DOES with it — asked of
    # apply's own predicate (``apply_writes_component_config``) under EVERY
    # served conflict policy, never read off a truthy flag. A reuse binding
    # needs a genuinely reference-only base that apply writes under NO policy
    # (``reference_only`` the boolean True — a string ``"true"`` beside an
    # ``update`` action is WRITTEN). A create binding needs a base apply writes
    # under EVERY policy: one it would reuse (a ``component_id`` under
    # ``reuse``) stores structured auth/URL fields that describe nothing written.
    written = _base_written_under(component)
    if binding_mode == "reuse":
        if config.get("reference_only") is not True or any(written):
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
    elif bool(config.get("reference_only")) or not all(written):
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")


def _served_conflict_policies() -> Tuple[str, ...]:
    """The conflict policies the authoring surface serves — DERIVED from its model."""
    from typing import get_args

    from ..models.authoring_workflow import RecipeAuthoringIntentV1

    return tuple(get_args(RecipeAuthoringIntentV1.model_fields["conflict_policy"].annotation))


def _base_written_under(component: IntegrationComponentSpec) -> Tuple[bool, ...]:
    """Per served conflict policy: does apply WRITE this base's configuration?"""
    from ..categories.integration_builder import apply_writes_component_config

    # ``existing_ids=None``: the recipe layer touches no account (like the
    # engine), so the answer is the DECLARED one — a create matched to an
    # existing component by NAME is decided at apply. That only ever counts a
    # base as written, so its config is checked; it never skips a check.
    return tuple(
        bool(apply_writes_component_config(component, policy, existing_ids=None))
        for policy in _served_conflict_policies()
    )


#: ARCH-r1-02: the config keys a selected CREATE-mode base may carry, per family,
#: each mapped to the material field whose credential-boundary rule governs it
#: (so the rule is looked up in ONE table, :func:`_material_field_classes_v2`).
#: ``None`` = a key that is not a value carrier (closed literal, identity,
#: placement, or checked by its own rule elsewhere). EVERY OTHER KEY is refused
#: (fail closed): the material cannot express it, so neither may a selected base
#: — notably the DB builder's ``additional`` (a JDBC suffix appended verbatim),
#: ``connection_url``/``custom_class_name`` (the custom-URL shape), ``pooling``,
#: and the REST builder's ``username``/``credential_ref``/``oauth2``/
#: ``default_headers`` (a created REST connection is unauthenticated).
def _selected_base_key_classes() -> _Mapping[str, _Mapping[str, Optional[Tuple[type, str]]]]:
    shared = {
        "connector_type": None,  # checked: must equal the family
        "component_name": None,  # the component's display name
        "folder_name": None,  # placement
        "folder_path": None,  # placement
        "folder_id": None,  # placement
        "description": None,  # free text: component docs
        "reference_only": None,  # checked by the binding rule
        "component_id": None,  # checked by the binding rule (apply's predicate)
    }
    return _MappingProxyType(
        {
            "database": _MappingProxyType(
                dict(
                    shared,
                    auth_mode=None,  # checked: must equal the declared auth mode
                    driver_id=(FanoutDbCreateMaterialV2, "driver"),
                    host=(FanoutDbCreateMaterialV2, "host"),
                    port=(FanoutDbCreateMaterialV2, "port"),
                    dbname=(FanoutDbCreateMaterialV2, "database"),
                    username=(FanoutDbCreateMaterialV2, "username"),
                    credential_ref=(FanoutDbCreateMaterialV2, "credential_ref"),
                )
            ),
            "rest": _MappingProxyType(
                dict(
                    shared,
                    auth=None,  # checked: must be the unauthenticated wire value
                    base_url=(FanoutRestCreateMaterialV2, "base_url"),
                )
            ),
        }
    )


def _selected_base_url(base_catalog: _Mapping[str, IntegrationComponentSpec], key: str) -> str:
    config = (base_catalog[key].config or {}) if key in base_catalog else {}
    value = config.get("base_url")
    return value if isinstance(value, str) else ""


def _path_continues_a_query(path: str) -> bool:
    """Whether a path's PRE-QUERY portion carries a raw pair separator
    (:data:`_PATH_FORBIDDEN_RAW`) — ARCH-r1-03. The path is appended to the
    connection URL, and a stored URL this request cannot see (a reused
    connection) may already end in a query: ``&k=v`` would then be a pair.
    Escaped (``%26``) it stays path data."""
    head = path.split("?", 1)[0]
    return any(char in head for char in _PATH_FORBIDDEN_RAW)


#: Which field a reused-target refusal points at (B15-4): the connection's
#: STORED base URL (it carries a query or fragment — the binding is at fault),
#: or the authored PATH.
REUSED_TARGET_STORED_BASE = "stored_base"
REUSED_TARGET_PATH = "path"


#: B20-5: the rules for a path sent through a REUSED connection, as constants
#: the judge enforces and the served sentence renders. A stored base carrying
#: one of these is refused for reuse (the path would land inside it).
_STORED_BASE_FORBIDDEN = "?#"
#: With an UNKNOWN base the path must start with this ...
_UNKNOWN_BASE_PATH_PREFIX = "/"
#: ... and carry no such character before its query.
_UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY = "="

#: The ONE rendered statement of the reuse rules (B20-5), served in the
#: fan-out material guarantee. The joined grammar alone does not imply them: a
#: stored base ending in '?mode=full' joined with '/orders' satisfies it.
REUSED_BASE_SENTENCE_V2 = (
    "A path sent through a REUSED connection is judged against the base URL "
    "that connection stores in the account: a stored base carrying {forbidden} "
    "is refused for reuse (the path would be appended inside it); a stored "
    "base that cannot be read, is blank, is an unresolved extension "
    "placeholder, or is not an absolute http(s) URL is UNKNOWN, and then the "
    "path must start with '{prefix}' and carry no '{before}' before any '?'."
).format(
    forbidden=" or ".join("'{0}'".format(char) for char in _STORED_BASE_FORBIDDEN),
    prefix=_UNKNOWN_BASE_PATH_PREFIX,
    before=_UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY,
)


def _stored_base_is_known(base: str) -> bool:
    """Whether a stored url is a CONCRETE base (B20-1): not blank, not the
    connector builder's unresolved-endpoint placeholder (its own definition,
    ``is_unresolved_endpoint_value``), and an absolute http(s) URL with an
    authority (the route digest's own bar). Anything else addresses nothing
    knowable, so the path is judged as against an unknown base."""
    from urllib.parse import urlsplit

    from ..categories.components.builders.connector_builder import (
        is_unresolved_endpoint_value,
    )

    candidate = base.strip()
    if not candidate or is_unresolved_endpoint_value(candidate):
        return False
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return False
    return parsed.scheme.lower() in ("http", "https") and bool(parsed.netloc)


def judge_reused_request_target(
    stored_base_url: Optional[str], path: str
) -> Optional[Tuple[str, str]]:
    """``(cause, fault)`` for a path appended to a REUSED connection's stored
    base URL, or ``None`` (B14 ARCH-r1-03; B15). ``fault`` is
    :data:`REUSED_TARGET_STORED_BASE` or :data:`REUSED_TARGET_PATH`.

    With the stored base URL in hand (read from the account): a base that
    already carries a query or fragment would receive the path INSIDE it, so it
    is refused as the STORED BASE's fault, whatever the path; otherwise every
    joined target meets the grammar, judged only for what the PATH contributes
    (the stored URL is the account's, not the caller's). Without it (no
    account, an unresolved, id-less or unreadable reuse, a blank stored url) the rule FAILS CLOSED
    on the path: it must start with '/' and carry no '=' before any '?' — the
    two shapes that could complete a pair a stored query left open."""
    # B21-3: ONE ordering. A READABLE stored value carrying a delimiter is
    # refused for reuse FIRST, whether or not it is a well-formed absolute URL
    # (``https:///base?mode=full`` still has a query the path would land in).
    if stored_base_url is not None and any(
        char in stored_base_url for char in _STORED_BASE_FORBIDDEN
    ):
        return (_QUERY_NAME_CAUSE, REUSED_TARGET_STORED_BASE)
    if stored_base_url is None or not _stored_base_is_known(stored_base_url):
        # B16-2 / B20-1: no concrete base (blank, the unresolved placeholder,
        # not an absolute http(s) URL) — unknown, so the fail-closed rule
        # judges the path.
        head = path.split("?", 1)[0]
        if (
            not path.startswith(_UNKNOWN_BASE_PATH_PREFIX)
            or _UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY in head
        ):
            return (_QUERY_NAME_CAUSE, REUSED_TARGET_PATH)
        return None
    base = stored_base_url.strip()
    for joined, path_start in _joined_targets(base, path):
        verdict = _judge_request_target(joined)
        if verdict.cause is not None and verdict.offset >= path_start:
            return (verdict.cause, REUSED_TARGET_PATH)
    return None


def _joined_targets(base: str, path: str) -> Tuple[Tuple[str, int], ...]:
    """The request target(s) a base URL and a path can be joined into, each
    with the offset where the PATH's contribution starts.

    The REST Client operation path is APPENDED to the connection URL (the
    builders' own contract: "Path appended onto the connection's base_url ...
    REST Client preserves the path verbatim"); whether the runtime inserts a
    single ``/`` at the boundary is not documented, and the route digest
    (``connector_replay.digests._effective_path``) models that slash join. Both
    are judged, so a split that is clean under one join cannot smuggle a pair
    under the other."""
    if not base:
        return ((path, 0),)
    concatenated = (base + path, len(base))
    trimmed = base.rstrip("/")
    slashed = (trimmed + "/" + path.lstrip("/"), len(trimmed))
    return (concatenated,) if slashed[0] == concatenated[0] else (concatenated, slashed)


def check_rest_request(
    *,
    connection_key: str,
    path: str,
    path_pointer: str,
    created_base_url: Optional[str],
    connection_pointer: str,
    selected_key: Optional[str],
    selected_key_pointer: str,
    binding_mode: str,
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    query_parameters: _Sequence[_Any] = (),
    query_parameters_pointer: str = "",
) -> Tuple[str, str, str, str]:
    """Judge ONE REST request's EFFECTIVE request target (QA r8-01) — a send
    or a fetch alike — and return the ``rest_request_targets`` entry the
    workflow rechecks: ``(connection key, path, path pointer, binding pointer)``.

    The target is the connection URL joined with the operation path, exactly
    as emitted, judged AFTER each stored field is judged on its own (r7-01).
    The connection URL is ``created_base_url`` when the material creates the
    connection (pass the value the builder receives); a create-mode selected
    base's stored ``base_url`` when ``selected_key`` names one; otherwise the
    connection is reused, its URL is not in hand, and the path is judged alone
    (the workflow judges it against the stored URL later).

    The refusal points at the field that CONTRIBUTES the offending text: the
    path when the offset falls in the path's portion, else the base URL (or,
    for a selected base, the raw-input field that selected it). A structured
    parameter (anything with a ``name``) repeating an inline name is refused
    at its name. The binding pointer is the selecting raw-input field when a
    base is selected, else the material ``connection``.
    """
    if created_base_url is not None:
        base_url, base_field = created_base_url, connection_pointer + "/base_url"
    elif selected_key is not None and binding_mode == "create":
        base_url = _selected_base_url(base_catalog, selected_key)
        base_field = selected_key_pointer
    else:
        base_url, base_field = "", path_pointer
    # r7-01: every PERSISTED field is judged on its own first — the stored
    # base_url (material or selected base) and the stored path — because
    # each is written as-is, and appending the path can hide a base value
    # from the builder's whole-value patterns.
    if base_url:
        standalone = _judge_request_target(base_url)
        if standalone.cause is not None:
            raise _refusal(standalone.cause, base_field, phase="input")
    standalone = _judge_request_target(path)
    if standalone.cause is None and _path_continues_a_query(path):
        standalone = _TargetVerdict(_QUERY_NAME_CAUSE)
    if standalone.cause is not None:
        raise _refusal(standalone.cause, path_pointer, phase="input")
    names: set = set()
    for joined, path_start in _joined_targets(base_url, path):
        verdict = _judge_request_target(joined)
        if verdict.cause is not None:
            field = path_pointer if verdict.offset >= path_start else base_field
            raise _refusal(verdict.cause, field, phase="input")
        names.update(verdict.names)
    for qp_index, parameter in enumerate(query_parameters):
        if parameter.name.strip() in names:
            raise _refusal(
                _QUERY_NAME_CAUSE,
                f"{query_parameters_pointer}/{qp_index}/name",
                phase="input",
            )
    binding_pointer = selected_key_pointer if selected_key is not None else connection_pointer
    return (connection_key, path, path_pointer, binding_pointer)


def _check_base_auth(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    key: str,
    *,
    config_key: str,
    expected: str,
    path: str,
) -> None:
    """A CREATE-mode base connection's auth must be the one the recipe declares.

    The declared auth decides which credential fields become per-environment
    override points (``_extension_bindings_v2``), so a base connection whose own
    config says otherwise would ship credentials nobody can override — or
    declare override points for credentials it does not have (CDX-r1-04). The
    builders REQUIRE the auth key on a created connection (no default), so an
    absent value is a mismatch too.

    A base connection whose write submits raw ``config.xml``
    (``submits_raw_component_xml``, the predicate every write route asks) is
    REFUSED (CDX-r2-03): the route posts those bytes verbatim, so the
    structured auth field beside them describes nothing that will be written,
    and no authority in this repository reads a connection's auth out of
    submitted XML (``live_identity_from_component_xml`` reads only type,
    subtype, verb and path). Unverifiable is not verified.
    """
    from ..categories.components._shared import submits_raw_component_xml

    config = (base_catalog[key].config or {}) if key in base_catalog else {}
    if submits_raw_component_xml(config) or config.get(config_key) != expected:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")


def _connection_presence(
    raw_key: Optional[str], material: _Any, binding_mode: str, *, path: str
) -> None:
    """Material carries a connection exactly when no base key is named, in the
    binding mode the recipe input declares."""
    if raw_key is not None:
        if material is not None:
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
        return
    if material is None or material.mode != binding_mode:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
