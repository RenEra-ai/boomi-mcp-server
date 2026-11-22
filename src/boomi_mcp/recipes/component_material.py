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
from typing import Callable as _Callable
from typing import Dict as _Dict
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
from ..models.derived_flows import RecipeUnitProjectionFactsV1
from ..models.governance_intent import reject_retired_spellings
from ..models.integration_models import IntegrationComponentSpec
from ..models.recipe_contributions import RecipeComponentKey
from ..patterns.archetype_parameters import (
    DBResultField,
    DbCreateSettings,
    JSONPayloadProfile,
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
    from ..categories.components.builders.profile_builder import (
        DatabaseWriteProfileBuilder,
    )
    from ..patterns.primitives import db_extract, db_write, field_map, rest_send

    return SimpleNamespace(
        BuilderValidationError=BuilderValidationError,
        RestClientOperationBuilder=RestClientOperationBuilder,
        JSONGeneratedProfileBuilder=JSONGeneratedProfileBuilder,
        FUNCTION_FAMILIES=FUNCTION_FAMILIES,
        profile_from_json_schema=profile_from_json_schema,
        required_target_coverage_gaps=required_target_coverage_gaps,
        REST_CREATE_AUTH_MAP=assembly._REST_CREATE_AUTH_MAP,
        script_var_name=assembly._script_var_name,
        SOURCE_PREFIX=assembly._SOURCE_PREFIX,
        TARGET_PREFIX=assembly._TARGET_PREFIX,
        TRANSFORM_PREFIX=assembly._TRANSFORM_PREFIX,
        ROLE_DB_CONNECTION=helpers.ROLE_DB_CONNECTION,
        ROLE_DB_GET_OPERATION=helpers.ROLE_DB_GET_OPERATION,
        ROLE_DB_READ_PROFILE=helpers.ROLE_DB_READ_PROFILE,
        ROLE_DB_WRITE_OPERATION=helpers.ROLE_DB_WRITE_OPERATION,
        ROLE_DB_WRITE_PROFILE=helpers.ROLE_DB_WRITE_PROFILE,
        ROLE_REST_CONNECTION=helpers.ROLE_REST_CONNECTION,
        ROLE_REST_OPERATION=helpers.ROLE_REST_OPERATION,
        ROLE_TARGET_PROFILE=helpers.ROLE_TARGET_PROFILE,
        ROLE_TRANSFORM_MAP=helpers.ROLE_TRANSFORM_MAP,
        primitive_component_key=helpers.primitive_component_key,
        slugify=helpers.slugify,
        DbExtractParameters=db_extract.DbExtractParameters,
        DbExtractPrimitive=db_extract.DbExtractPrimitive,
        db_connection_extension_fields=db_extract.db_connection_extension_fields,
        DatabaseWriteProfileBuilder=DatabaseWriteProfileBuilder,
        DbSendOperationParams=db_write.DbSendOperationParams,
        DbWriteParameters=db_write.DbWriteParameters,
        DbWritePrimitive=db_write.DbWritePrimitive,
        DbWriteProfileParams=db_write.DbWriteProfileParams,
        write_profile_config=db_write._write_profile_config,
        raise_for_builder_error=helpers.raise_for_builder_error,
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


#: The database WRITE field types: a deliberately PINNED copy of the
#: ``db_write`` primitive's ``_DB_FIELD_TYPE`` (the write-profile builder's
#: supported set). The primitive reaches ``boomi_mcp`` by absolute import, so it
#: cannot be read at module scope here; ``tests/test_recipe_component_material.py``
#: pins the two equal in both directions.
_DbWriteFieldTypeV1 = _Literal["character", "number", "datetime"]


class DbWriteFieldMaterialV1(_FanoutMaterialModel):
    """One written column. Value rules are ``db_write.DbWriteField``'s."""

    name: str = Field(min_length=1)
    data_type: _DbWriteFieldTypeV1 = "character"
    mandatory: bool = False
    enforce_unique: bool = False


class DbWriteConditionMaterialV1(_FanoutMaterialModel):
    """One WHERE-key column (dynamic update/delete)."""

    name: str = Field(min_length=1)
    data_type: _DbWriteFieldTypeV1 = "character"


class DbWriteProfileMaterialV1(_FanoutMaterialModel):
    """A database Write profile. Per-statement-type rules are the write-profile
    builder's (``DatabaseWriteProfileBuilder``), applied before any component
    is assembled."""

    statement_type: str = Field(
        min_length=1,
        description=(
            "Write statement variant: standardinsertupdatedelete, dynamicinsert, "
            "dynamicupdate, dynamicdelete, or storedprocedurewrite. Any other "
            "value (e.g. 'upsert') is rejected by the write-profile builder."
        ),
    )
    sql: Optional[str] = Field(
        default=None,
        description="Caller-authored write SQL (required for standard / stored-procedure; omitted for dynamic*).",
    )
    table_name: Optional[str] = Field(
        default=None, description="Target table for the dynamic* statement types."
    )
    stored_procedure: Optional[str] = Field(
        default=None, description="Procedure name for storedprocedurewrite."
    )
    fields: Tuple[DbWriteFieldMaterialV1, ...] = ()
    conditions: Tuple[DbWriteConditionMaterialV1, ...] = ()


class DbSendOperationMaterialV1(_FanoutMaterialModel):
    """Optional database Send tuning. Value rules are the Send builder's."""

    commit_option: Optional[str] = Field(
        default=None, description="'commitprofile' (default) or 'commitrows'."
    )
    batch_count: Optional[int] = Field(default=None, ge=0)
    enable_batching: Optional[bool] = None


class DbWriteMaterialV1(_FanoutMaterialModel):
    """The shared DB-WRITE block: connection (create or reuse) + write profile +
    Send operation. Prepared by :func:`authorize_db_write_material` then
    :func:`emit_db_write_components`."""

    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutDbConnectionMaterialV2] = None
    write_profile: DbWriteProfileMaterialV1
    operation: DbSendOperationMaterialV1 = Field(default_factory=DbSendOperationMaterialV1)


#: The shared material BLOCKS a preset composes its root from (#159 S2). The
#: shared classification table covers exactly the fields these reach — pinned
#: by ``tests/test_recipe_component_material.py`` — so a block added here
#: without its rows, or a row left behind by a removed field, fails the pin.
SHARED_MATERIAL_BLOCKS: Tuple[type, ...] = (
    FanoutDbCreateMaterialV2,
    FanoutConnectionReuseMaterialV2,
    FanoutDbReadMaterialV2,
    FanoutDirectOperationV2,
    FanoutFunctionOperationV2,
    FanoutScriptReferenceOperationV2,
    FanoutRestCreateMaterialV2,
    FanoutRestOperationMaterialV2,
    JSONPayloadProfile,
    DbWriteMaterialV1,
)


# ---------------------------------------------------------------------------
# The credential boundary (#159 S1, CDX-r1-01/02 structural fix)
# ---------------------------------------------------------------------------
#
# ONE check, applied to every field of the material that can carry a secret,
# driven by ONE table that classifies EVERY field of every model reachable from
# a registered material ROOT (including the shared ``JSONPayloadProfile``
# tree). #159 S2: the table is the UNION of the shared table below (every
# field of every shared block) and the root's own table, which each preset
# keeps beside its root model and registers (:func:`register_material_root`). The walk reads the table for every field it visits and FAILS CLOSED on
# a field the table does not classify, so a field added to the material later
# cannot bypass the boundary silently: ``tests/test_recipe_component_material.py``
# pins the table's keys to the set derived from the models, and the
# secret-capable subset to the enumeration below.
#
# Secret-capable (a rule that can REFUSE plaintext):
#   FanoutDbCreateMaterialV2.credential_ref   credential_reference: the opaque
#       ``credential://`` form the REST/SOAP builders already enforce.
#   FanoutDbCreateMaterialV2.host             jdbc_host: no userinfo (``@``) and
#       no JDBC property (``;`` / ``=``) — the two ways a password rides a host.
#   FanoutDbCreateMaterialV2.database         jdbc_property: no ``;`` / ``=``
#       (a ``;password=...`` suffix lands in the JDBC URL).
#   FanoutRestCreateMaterialV2.base_url       url: no userinfo; every inline query
#       pair readable, under a published name, and with a value the REST builder
#       admits (D12).
#   FanoutRestOperationMaterialV2.path        url: same rule (a path may carry a
#       query string, or an authority when written scheme-/protocol-relative).
#   FanoutQueryParameterV2.name               literal_query_name: a LITERAL
#       parameter's name must be a published name (D12).
#   FanoutQueryParameterV2.literal_value      named_literal: the value must pass
#       the REST builder's value check.
#
# Every other field is classified ``None`` with its reason in the table. There
# is no ``jdbc_options`` (or other open map) in the shared material: the only maps it
# can reach are the closed models below.
#
# Literal REST query NAMES are a closed vocabulary (D12,
# :data:`LITERAL_QUERY_NAMES_V2`); no name classifier exists. Query VALUES are
# judged by the REST BUILDER's own authority — the customProperties check
# ``RestClientOperationBuilder`` applies to the very ``query_parameters`` these
# values become (``REST_SECRET_VALUE_FORBIDDEN``) — asked, never re-typed.

def _common_material_field_classes() -> _Mapping[Tuple[type, str], Optional[str]]:
    """``(model, field) -> rule`` for EVERY field of every SHARED material
    block (:data:`SHARED_MATERIAL_BLOCKS` and what they reach); ``None`` = not
    a carrier. A preset's ROOT models are classified by the preset, beside its
    root model, and registered with :func:`register_material_root`.

    The reasons for ``None`` are stated per entry. "Free text" entries CAN hold
    arbitrary characters; they are classified non-carrier because no shape rule
    can tell a secret from legitimate content there and the contract declares a
    different, checked channel for the secret (``credential_ref``), not because
    they are physically incapable of holding one.
    """
    from ..patterns.archetype_parameters import JSONProfileNode

    return _MappingProxyType(
        {
            (FanoutDbCreateMaterialV2, "mode"): None,  # closed literal
            (FanoutDbCreateMaterialV2, "driver"): None,  # closed literal
            (FanoutDbCreateMaterialV2, "host"): "jdbc_host",
            (FanoutDbCreateMaterialV2, "port"): None,  # bounded integer
            (FanoutDbCreateMaterialV2, "database"): "jdbc_property",
            # An identity, emitted as the connection's user; the secret half is
            # credential_ref. Azure-style ``user@server`` identities are
            # legitimate, so no shape rule separates a smuggled secret.
            (FanoutDbCreateMaterialV2, "username"): None,
            (FanoutDbCreateMaterialV2, "credential_ref"): "credential_reference",
            (FanoutConnectionReuseMaterialV2, "mode"): None,  # closed literal
            # Selectors of an EXISTING component (platform id / display name),
            # served on the endpoint rows by design; they name, never unlock.
            (FanoutConnectionReuseMaterialV2, "component_id"): None,
            (FanoutConnectionReuseMaterialV2, "component_name"): None,
            # Free text: query material emitted into the read profile by design
            # (the addendum's D1). A literal in it cannot be told from data.
            (FanoutDbReadMaterialV2, "sql"): None,
            (FanoutDbReadMaterialV2, "result_fields"): None,  # structural (models)
            (FanoutDbReadMaterialV2, "bind_parameters"): None,  # structural (models)
            (FanoutDbReadMaterialV2, "batch_size"): None,  # bounded integer
            (FanoutDbReadMaterialV2, "max_rows"): None,  # bounded integer
            # A field name (``DBResultField`` refuses path separators).
            (FanoutResultFieldV2, "name"): None,
            (FanoutResultFieldV2, "data_type"): None,  # closed literal
            (FanoutResultFieldV2, "required"): None,  # boolean
            (FanoutResultFieldV2, "description"): None,  # free text: profile docs
            (FanoutBindParameterV2, "name"): None,  # a statement parameter NAME; no value
            (FanoutBindParameterV2, "direction"): None,  # closed literal
            (FanoutDirectOperationV2, "operation_type"): None,  # closed literal
            # Field references, each checked against a DECLARED source field or
            # payload leaf by the producer -> consumer contract.
            (FanoutDirectOperationV2, "source_field"): None,
            (FanoutDirectOperationV2, "target_path"): None,
            (FanoutDirectOperationV2, "documentation_hint"): None,  # free text: served hint
            (FanoutFunctionOperationV2, "operation_type"): None,  # closed literal
            (FanoutFunctionOperationV2, "function_type"): None,  # a registered family name
            (FanoutFunctionOperationV2, "inputs"): None,  # declared source fields
            (FanoutFunctionOperationV2, "target_path"): None,  # a declared payload leaf
            (FanoutFunctionOperationV2, "parameters"): None,  # structural (models)
            (FanoutFunctionOperationV2, "documentation_hint"): None,  # free text: served hint
            # A name the family DECLARES (closed by ``FunctionFamily``).
            (FanoutFunctionParameterV2, "name"): None,
            # A scalar transform constant emitted into the map by design; its
            # name is family-declared (never secret-shaped), so no named rule
            # applies, and a constant cannot be told from a secret by shape.
            (FanoutFunctionParameterV2, "value"): None,
            (FanoutScriptReferenceOperationV2, "operation_type"): None,  # closed literal
            (FanoutScriptReferenceOperationV2, "script_component_key"): None,  # a bounded key
            (FanoutScriptReferenceOperationV2, "inputs"): None,  # declared source fields
            (FanoutScriptReferenceOperationV2, "outputs"): None,  # declared payload leaves
            (FanoutScriptReferenceOperationV2, "documentation_hint"): None,  # free text
            (JSONPayloadProfile, "format"): None,  # closed literal
            (JSONPayloadProfile, "root"): None,  # structural (model)
            (JSONProfileNode, "name"): None,  # a schema node name
            (JSONProfileNode, "kind"): None,  # closed literal
            (JSONProfileNode, "data_type"): None,  # closed literal
            (JSONProfileNode, "required"): None,  # boolean
            (JSONProfileNode, "description"): None,  # free text: profile docs
            (JSONProfileNode, "children"): None,  # structural (models)
            (FanoutRestCreateMaterialV2, "mode"): None,  # closed literal
            (FanoutRestCreateMaterialV2, "base_url"): "url",
            (FanoutRestOperationMaterialV2, "path"): "url",
            (FanoutRestOperationMaterialV2, "query_parameters"): None,  # structural (models)
            # A LITERAL parameter's name must be published (D12).
            (FanoutQueryParameterV2, "name"): "literal_query_name",
            (FanoutQueryParameterV2, "value_source"): None,  # closed literal
            (FanoutQueryParameterV2, "literal_value"): "named_literal",
            # -- the shared DB-write block (#159 S2) --------------------------
            (DbWriteMaterialV1, "connection"): None,  # structural (variant)
            (DbWriteMaterialV1, "write_profile"): None,  # structural (model)
            (DbWriteMaterialV1, "operation"): None,  # structural (model)
            # Closed by the write-profile builder (UNSUPPORTED_DB_STATEMENT_TYPE).
            (DbWriteProfileMaterialV1, "statement_type"): None,
            # Free text: the write statement, emitted into the write profile by
            # design (like the read SQL). A literal in it cannot be told from data.
            (DbWriteProfileMaterialV1, "sql"): None,
            (DbWriteProfileMaterialV1, "table_name"): None,  # an identifier emitted by design
            (DbWriteProfileMaterialV1, "stored_procedure"): None,  # an identifier emitted by design
            (DbWriteProfileMaterialV1, "fields"): None,  # structural (models)
            (DbWriteProfileMaterialV1, "conditions"): None,  # structural (models)
            (DbWriteFieldMaterialV1, "name"): None,  # a column name
            (DbWriteFieldMaterialV1, "data_type"): None,  # closed literal
            (DbWriteFieldMaterialV1, "mandatory"): None,  # boolean
            (DbWriteFieldMaterialV1, "enforce_unique"): None,  # boolean
            (DbWriteConditionMaterialV1, "name"): None,  # a column name
            (DbWriteConditionMaterialV1, "data_type"): None,  # closed literal
            (DbSendOperationMaterialV1, "commit_option"): None,  # closed by the Send builder
            (DbSendOperationMaterialV1, "batch_count"): None,  # bounded integer
            (DbSendOperationMaterialV1, "enable_batching"): None,  # boolean
        }
    )




#: Material ROOT model -> provider of its root-field table (#159 S2).
_MATERIAL_ROOTS: _Dict[type, _Callable[[], _Mapping[Tuple[type, str], Optional[str]]]] = {}


def register_material_root(
    root_model: type,
    root_field_classes: _Callable[[], _Mapping[Tuple[type, str], Optional[str]]],
) -> None:
    """Register a preset's material ROOT model with the credential walker.

    ``root_field_classes`` returns the ``(model, field) -> rule`` rows for the
    preset's OWN models (the root and any preset-specific part block). It may
    not classify a field of a shared block: those rules have one authority,
    :func:`_common_material_field_classes`. Registered once per root."""
    if root_model in _MATERIAL_ROOTS:
        raise ValueError(f"{root_model.__name__} is already a registered material root")
    _MATERIAL_ROOTS[root_model] = root_field_classes


def registered_material_roots() -> Tuple[type, ...]:
    """Every registered material root, in registration order."""
    return tuple(_MATERIAL_ROOTS)


def material_field_classes(root_model: type) -> _Mapping[Tuple[type, str], Optional[str]]:
    """The table the walker reads for ``root_model``: the shared table and the
    root's registered table, unioned. Fails closed on an unregistered root and
    on a root table that reclassifies a shared model's field."""
    if root_model not in _MATERIAL_ROOTS:
        raise ValueError(f"{root_model.__name__} is not a registered material root")
    common = _common_material_field_classes()
    shared_models = {model for model, _name in common}
    root = _MATERIAL_ROOTS[root_model]()
    if any(model in shared_models for model, _name in root):
        raise ValueError(f"{root_model.__name__} reclassifies a shared material field")
    return _MappingProxyType({**common, **root})


def check_credential_boundary(
    material: _BaseModel,
    classes: Optional[_Mapping[Tuple[type, str], Optional[str]]] = None,
    *,
    pointer: str = "/component_material",
) -> None:
    """Refuse, value-free, the first material field that carries a plaintext secret.

    Walks the validated material; every field visited is looked up in
    ``classes`` (default: :func:`material_field_classes` of the material's own
    root model). An unclassified field is refused too (fail closed). The
    pointer is built from DECLARED field names and indices only, so it never
    carries caller content. Run it BEFORE any component is assembled.
    """
    if classes is None:
        classes = material_field_classes(type(material))

    def walk(node: _Any, here_pointer: str) -> None:
        if isinstance(node, _BaseModel):
            for name in type(node).model_fields:
                here = f"{here_pointer}/{name}"
                key = (type(node), name)
                if key not in classes:
                    raise _refusal(_PLAINTEXT_SECRET_CAUSE, here, phase="input")
                value = getattr(node, name)
                rule = classes[key]
                cause = _breaches(rule, value, node) if rule is not None else None
                if cause is not None:
                    raise _refusal(cause, here, phase="input")
                walk(value, here)
        elif isinstance(node, (tuple, list)):
            for index, item in enumerate(node):
                walk(item, f"{here_pointer}/{index}")

    walk(material, pointer)


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
#: (so the rule is looked up in ONE table, :func:`_common_material_field_classes`).
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


def _check_selected_base_config(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    key: str,
    *,
    family: str,
    path: str,
) -> None:
    """The credential boundary over a WRITTEN selected base's config (ARCH-r1-02).

    The same rules the material walk applies (looked up in the shared table,
    :func:`_common_material_field_classes`, through the key map above), plus fail
    closed on any key the material cannot express. Value-free: the pointer is
    the raw-input field that selected the base."""
    config = (base_catalog[key].config or {}) if key in base_catalog else {}
    classes = _common_material_field_classes()
    allowed = _selected_base_key_classes()[family]
    for name, value in config.items():
        if name not in allowed:
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
        field = allowed[name]
        rule = classes.get(field) if field is not None else None
        cause = _breaches(rule, value, None) if rule is not None else None
        if cause is not None:
            raise _refusal(cause, path, phase="input")


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


@_dataclass(frozen=True, repr=False)
class PreparedRecipeComponentsV1:
    """What a component-material preparer hands the workflow. NEVER reaches a
    recipe executor. (#159 S2: generalized from fan-out 2.0.0's
    ``PreparedFanoutV2``, which stays an alias; the fields are unchanged.)

    ``supporting_components`` are the generated specs, in the recipe's
    component order. ``selected_base_keys`` name intent ``base_components``
    entries the generated specs reference (a base-key connection, a referenced
    script) — the workflow adds those catalog entries to the run exactly once.
    ``deferred_query_parameter_names`` maps ``(owning root, operation key)`` to
    the watermark-sourced query parameter names the preparer validated: they
    are recorded intent, kept OUT of the emitted operation config, and handed
    to governance's watermark rule scoped to that operation AND its root
    (ARCH-r1-04: keyed by the operation alone, another root that also reaches
    the operation could borrow them).

    The repr names counts and keys only; component configs carry material.
    """

    supporting_components: Tuple[IntegrationComponentSpec, ...]
    selected_base_keys: Tuple[str, ...] = ()
    #: B12-01: ``(base key, raw-input pointer)`` per selected base bound in
    #: CREATE mode. Whether apply really writes it depends on the component
    #: plan's account resolution (a create matched BY NAME is reused), which
    #: only the workflow holds — it rechecks each one against that authority.
    create_bound_base_keys: Tuple[Tuple[str, str], ...] = ()
    #: B15: ``(connection key, path, path pointer, binding pointer)`` for EVERY
    #: REST request, whatever its declared binding (:func:`check_rest_request`
    #: returns exactly this entry). Whether apply writes the connection (its
    #: base URL was judged here, joined) or reuses one (its STORED base URL
    #: governs) is the component plan's decision, which only the workflow
    #: holds: it judges each request apply will not write against the stored
    #: URL (:func:`judge_reused_request_target`), at compile and again at apply
    #: before the first write.
    rest_request_targets: Tuple[Tuple[str, str, str, str], ...] = ()
    deferred_query_parameter_names: Tuple[Tuple[Tuple[str, str], Tuple[str, ...]], ...] = ()
    #: #159 S1 (C8): the trusted facts the #157 projection reads for this
    #: invocation's unit. Their presence is what admits the unit's recipe-route
    #: rows. Keys and classifications only, EXCEPT the transform operations'
    #: documentation hints — caller content from the material, served on the
    #: transform row exactly where the legacy row served it (repr-redacted).
    projection_facts: Optional[RecipeUnitProjectionFactsV1] = None

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"{type(self).__name__}("
            f"supporting_keys={tuple(c.key for c in self.supporting_components)!r}, "
            f"selected_base_keys={self.selected_base_keys!r})"
        )


def check_connection_binding(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    *,
    selected_key: Optional[str],
    selected_key_pointer: str,
    material_connection: _Any,
    material_pointer: str,
    binding_mode: str,
    family: str,
    auth_config_key: str,
    auth_expected: str,
    auth_pointer: str,
) -> Tuple[Optional[str], Optional[Tuple[str, str]]]:
    """The selected-base rules for ONE connection, in their load-bearing order.

    Material carries the connection exactly when no base key is selected, in
    the declared binding mode (:func:`_connection_presence`). A selected base
    must be a ``connector-settings`` of ``family`` bound per apply's own
    predicate (:func:`_check_base_component`); a CREATE-mode selected base must
    also carry the declared auth (:func:`_check_base_auth`) and meet the
    credential boundary over its config (:func:`_check_selected_base_config`).

    Returns ``(selected base key or None, (key, pointer) for
    ``create_bound_base_keys`` or None)`` — the caller adds them to its
    prepared result once."""
    _connection_presence(selected_key, material_connection, binding_mode, path=material_pointer)
    if selected_key is None:
        return None, None
    _check_base_component(
        base_catalog,
        selected_key,
        path=selected_key_pointer,
        component_type="connector-settings",
        connector_type=family,
        binding_mode=binding_mode,
    )
    if binding_mode != "create":
        return selected_key, None
    _check_base_auth(
        base_catalog,
        selected_key,
        config_key=auth_config_key,
        expected=auth_expected,
        path=auth_pointer,
    )
    _check_selected_base_config(base_catalog, selected_key, family=family, path=selected_key_pointer)
    return selected_key, (selected_key, selected_key_pointer)


def authorize_db_connection(connection: _Any, *, pointer: str, auth_mode: Optional[str]) -> _Any:
    """A DB connection material through its neutral authority: the validated
    ``DbCreateSettings`` (create, under the input's declared ``auth_mode``),
    the validated ``DbConnectionBinding`` (reuse), or ``None`` (base-key)."""
    from ..patterns.archetype_parameters import DbConnectionBinding

    if isinstance(connection, FanoutDbCreateMaterialV2):
        return _authority_check(
            pointer,
            DbCreateSettings,
            driver=connection.driver,
            auth_mode=auth_mode,
            host=connection.host,
            port=connection.port,
            database=connection.database,
            username=connection.username,
            credential_ref=connection.credential_ref,
        )
    if isinstance(connection, FanoutConnectionReuseMaterialV2):
        return _authority_check(
            pointer,
            DbConnectionBinding,
            mode="reuse",
            component_id=connection.component_id,
            component_name=connection.component_name,
        )
    return None


@_dataclass(frozen=True, repr=False)
class AuthorizedDbWriteV1:
    """A DB-write block's values AS THE AUTHORITIES RETURNED THEM (repr-redacted).

    ``connection``: validated ``DbCreateSettings`` / ``DbConnectionBinding``,
    or ``None`` for a base-key binding. ``write_profile`` / ``operation``: the
    validated ``db_write`` parameter models. ``field_index``: the write
    profile's map-target field index (``Fields/<col>``, ``Conditions/<col>``),
    which a transform's target paths are matched against."""

    connection: _Any
    write_profile: _Any
    operation: _Any
    field_index: _Mapping[str, _Any]

    def __repr__(self) -> str:  # noqa: D105
        return "AuthorizedDbWriteV1(...)"


def authorize_db_write_material(
    material: DbWriteMaterialV1, *, pointer: str, auth_mode: Optional[str]
) -> AuthorizedDbWriteV1:
    """Validate a DB-write block through the EXISTING authorities, BEFORE any
    component is assembled: the connection (:func:`authorize_db_connection`),
    the write profile (``db_write.DbWriteProfileParams`` and then the
    write-profile BUILDER's per-statement-type rules), and the Send tuning
    (``db_write.DbSendOperationParams``). Refusals are value-free, at
    ``pointer`` + the block field. Call after the credential walk and
    :func:`check_connection_binding`."""
    rt = _rt()
    connection = authorize_db_connection(
        material.connection, pointer=pointer + "/connection", auth_mode=auth_mode
    )
    profile = material.write_profile
    write_profile = _authority_check(
        pointer + "/write_profile",
        rt.DbWriteProfileParams,
        statement_type=profile.statement_type,
        sql=profile.sql,
        table_name=profile.table_name,
        stored_procedure=profile.stored_procedure,
        fields=[field.model_dump() for field in profile.fields],
        conditions=[condition.model_dump() for condition in profile.conditions],
    )
    _builder_check(
        pointer + "/write_profile",
        lambda: rt.raise_for_builder_error(
            rt.DatabaseWriteProfileBuilder.validate_config(
                rt.write_profile_config(write_profile, component_name="_")
            )
        ),
    )
    operation = _authority_check(
        pointer + "/operation",
        rt.DbSendOperationParams,
        commit_option=material.operation.commit_option,
        batch_count=material.operation.batch_count,
        enable_batching=material.operation.enable_batching,
    )
    return AuthorizedDbWriteV1(
        connection=connection,
        write_profile=write_profile,
        operation=operation,
        field_index=_MappingProxyType(dict(rt.DbWritePrimitive.build_field_index(write_profile))),
    )


def db_connection_params(connection: _Any, *, base_connection_key: str) -> _Dict[str, _Any]:
    """The ``db_extract`` / ``db_write`` primitive ``connection`` parameters for
    an AUTHORIZED DB connection (:func:`authorize_db_connection`): create
    settings, a reuse selector, or — for a base-key binding (``None``) — a
    placeholder reuse naming ``base_connection_key`` that never reaches a spec
    (the base-bound path emits no connection)."""
    if isinstance(connection, DbCreateSettings):
        params: _Dict[str, _Any] = {
            "mode": "create",
            "driver_id": connection.driver,
            "auth_mode": connection.auth_mode,
            "host": connection.host,
            "port": connection.port,
            "dbname": connection.database,
        }
        if connection.username is not None:
            params["username"] = connection.username
        if connection.credential_ref is not None:
            params["credential_ref"] = connection.credential_ref
        return params
    if connection is not None:
        params = {"mode": "reuse"}
        if connection.component_id:
            params["component_id"] = connection.component_id
        if connection.component_name:
            params["component_name"] = connection.component_name
        return params
    return {"mode": "reuse", "component_id": base_connection_key}


def emit_db_write_components(
    authorized: AuthorizedDbWriteV1,
    *,
    context: _Any,
    key_prefix: str,
    pointer: str,
    base_connection_key: Optional[str] = None,
    component_names: Optional[_Mapping[str, str]] = None,
) -> _List[IntegrationComponentSpec]:
    """The DB-write component group through the ``db_write`` primitive.

    Material connection (create or reuse): connection, write profile, Send
    operation, keyed from ``key_prefix`` by the primitive's own role keys.
    Base-key binding (``base_connection_key``): the write profile and Send
    operation only, the operation bound to the base connection key.
    ``component_names`` may name ``connection`` / ``write_profile`` /
    ``send_operation``. Builder refusals are value-free at ``pointer``."""
    rt = _rt()
    params = _builder_check(
        pointer,
        lambda: rt.DbWriteParameters.model_validate(
            {
                "key_prefix": key_prefix,
                "connection": db_connection_params(
                    authorized.connection,
                    base_connection_key=base_connection_key or "",
                ),
                "write_profile": authorized.write_profile,
                "operation": authorized.operation,
                "component_names": dict(component_names or {}),
            }
        ),
    )
    if base_connection_key is None:
        return _builder_check(pointer, lambda: rt.DbWritePrimitive.emit_components(context, params))
    write_key = rt.primitive_component_key(key_prefix, rt.ROLE_DB_WRITE_PROFILE)
    operation_key = rt.primitive_component_key(key_prefix, rt.ROLE_DB_WRITE_OPERATION)
    folder = context.folder_path
    return [
        _builder_check(
            pointer + "/write_profile",
            lambda: rt.DbWritePrimitive._emit_write_profile(context, params, write_key, folder),
        ),
        _builder_check(
            pointer + "/operation",
            lambda: rt.DbWritePrimitive._emit_send_operation(
                context, params, operation_key, base_connection_key, write_key, folder
            ),
        ),
    ]


def authorize_db_read(read: FanoutDbReadMaterialV2, *, pointer: str) -> _Any:
    """A DB read leaf through its neutral authorities: each result field
    (``DBResultField``), unique result names, each bind parameter
    (``DbReadParameter``), then the whole ``DbReadOperation``. Returns the
    validated ``DbReadOperation``; refusals point under ``pointer``."""
    from ..patterns.archetype_parameters import DbReadOperation, DbReadParameter

    result_fields = tuple(
        _authority_check(
            f"{pointer}/result_fields/{field_index}",
            DBResultField,
            name=field.name,
            data_type=field.data_type,
            required=field.required,
            description=field.description,
        )
        for field_index, field in enumerate(read.result_fields)
    )
    result_names = [field.name for field in result_fields]
    if len(set(result_names)) != len(result_names):
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, f"{pointer}/result_fields", phase="input")
    bind_parameters = [
        _authority_check(
            f"{pointer}/bind_parameters/{bind_index}",
            DbReadParameter,
            name=parameter.name,
            direction=parameter.direction,
        )
        for bind_index, parameter in enumerate(read.bind_parameters)
    ]
    return _authority_check(
        pointer,
        DbReadOperation,
        sql=read.sql,
        result_schema={"fields": list(result_fields)},
        parameters=bind_parameters,
        batch_size=read.batch_size,
        max_rows=read.max_rows,
    )


def authorize_rest_connection(connection: _Any, *, pointer: str) -> _Any:
    """A REST connection material through its neutral authority: validated
    ``RestCreateSettings`` (the URL-shape authority; a created REST connection
    is always unauthenticated — the auth vocabulary's ONE create mode), a
    validated ``RestConnectionBinding`` (reuse), or ``None`` (base-key)."""
    from ..patterns.archetype_parameters import RestConnectionBinding, RestCreateSettings

    if isinstance(connection, FanoutRestCreateMaterialV2):
        return _authority_check(
            pointer, RestCreateSettings, base_url=connection.base_url, auth_mode="none"
        )
    if isinstance(connection, FanoutConnectionReuseMaterialV2):
        return _authority_check(
            pointer,
            RestConnectionBinding,
            mode="reuse",
            component_id=connection.component_id,
            component_name=connection.component_name,
        )
    return None


def authorize_rest_request(
    operation: FanoutRestOperationMaterialV2,
    *,
    pointer: str,
    method: str,
    expected_status_codes: _Sequence[int],
) -> RestSendRequest:
    """A REST operation leaf through its neutral authorities: each query
    parameter (``RestQueryParameter``), names unique AFTER normalization, then
    the request (``RestSendRequest``) with the recipe input's ``method`` and
    status expectations. Returns the validated request; refusals point under
    ``pointer``."""
    from ..patterns.archetype_parameters import RestQueryParameter

    parameters = [
        _authority_check(
            f"{pointer}/query_parameters/{qp_index}",
            RestQueryParameter,
            name=parameter.name,
            value_source=parameter.value_source,
            literal_value=parameter.literal_value,
        )
        for qp_index, parameter in enumerate(operation.query_parameters)
    ]
    # CDX-r2-01: names are compared AFTER the authority normalized them, so
    # ``" mode "`` and ``"mode"`` are one parameter. The emitted query is a
    # name -> value map; a second entry would silently replace the first (the
    # legacy composition's dict comprehension does exactly that), so it is
    # refused at the SECOND entry's declared position instead.
    seen_names: set = set()
    for qp_index, parameter in enumerate(parameters):
        if parameter.name in seen_names:
            raise _refusal(
                COMPOSITION_CONTRACT_MISMATCH,
                f"{pointer}/query_parameters/{qp_index}",
                phase="input",
            )
        seen_names.add(parameter.name)
    return _authority_check(
        pointer,
        RestSendRequest,
        method=method,
        path=operation.path,
        query_parameters=parameters,
        expected_status_codes=list(expected_status_codes),
    )


def authorize_transform_operations(
    operations: Tuple[_Any, ...],
    *,
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    pointer: str,
) -> Tuple[_Any, ...]:
    """Each shared transform mapping leaf normalized through its legacy
    authority (ARCH-r1-11); refusals point at ``pointer/<index>``.

    A map-script operation goes through ``MapScriptTransformOperation`` (B12-03:
    it strips ``inputs``/``outputs`` and the hint). Its ``language`` is the
    REFERENCED script component's own (that component is the source of truth);
    the reference itself is checked later against the base catalog, so a
    script with no readable language is normalized by the SAME function the
    model's validators call rather than by an invented language."""
    from typing import get_args

    from ..patterns.archetype_parameters import (
        DirectTransformOperation,
        MapFunctionTransformOperation,
        MapScriptTransformOperation,
        _stripped_nonblank,
    )

    languages = get_args(MapScriptTransformOperation.model_fields["language"].annotation)

    normalized: _List[_Any] = []
    for op_index, operation in enumerate(operations):
        here = f"{pointer}/{op_index}"
        if isinstance(operation, FanoutDirectOperationV2):
            checked = _authority_check(
                here,
                DirectTransformOperation,
                operation_type="direct",
                source_field=operation.source_field,
                target_path=operation.target_path,
                documentation_hint=operation.documentation_hint,
            )
            normalized.append(
                operation.model_copy(
                    update={
                        "source_field": checked.source_field,
                        "target_path": checked.target_path,
                        "documentation_hint": checked.documentation_hint,
                    }
                )
            )
        elif isinstance(operation, FanoutFunctionOperationV2) and not operation.inputs:
            # A family that maps NO input (``default_value``): the legacy
            # model requires at least one input and so cannot carry it; its
            # names are normalized by the SAME function its validators call.
            try:
                target_path = _stripped_nonblank(operation.target_path)
                hint = (
                    None
                    if operation.documentation_hint is None
                    else _stripped_nonblank(operation.documentation_hint)
                )
            except ValueError:
                raise _material_failure(here, ValueError()) from None
            normalized.append(
                operation.model_copy(update={"target_path": target_path, "documentation_hint": hint})
            )
        elif isinstance(operation, FanoutFunctionOperationV2):
            checked = _authority_check(
                here,
                MapFunctionTransformOperation,
                operation_type="map_function",
                function_type=operation.function_type,
                inputs=list(operation.inputs),
                target_path=operation.target_path,
                parameters=(
                    {parameter.name: parameter.value for parameter in operation.parameters}
                    or None
                ),
                documentation_hint=operation.documentation_hint,
            )
            normalized.append(
                operation.model_copy(
                    update={
                        "inputs": tuple(checked.inputs),
                        "target_path": checked.target_path,
                        "documentation_hint": checked.documentation_hint,
                    }
                )
            )
        else:
            script = base_catalog.get(operation.script_component_key)
            language = ((script.config or {}) if script is not None else {}).get("language")
            if language in languages:
                checked = _authority_check(
                    here,
                    MapScriptTransformOperation,
                    operation_type="map_script",
                    script_slot=operation.script_component_key,
                    language=language,
                    inputs=list(operation.inputs),
                    outputs=list(operation.outputs),
                    script_component_ref="$ref:" + operation.script_component_key,
                    documentation_hint=operation.documentation_hint,
                )
                inputs, outputs, hint = checked.inputs, checked.outputs, checked.documentation_hint
            else:
                try:
                    inputs = [_stripped_nonblank(item) for item in operation.inputs]
                    outputs = [_stripped_nonblank(item) for item in operation.outputs]
                    hint = (
                        None
                        if operation.documentation_hint is None
                        else _stripped_nonblank(operation.documentation_hint)
                    )
                except ValueError:
                    raise _material_failure(here, ValueError()) from None
            normalized.append(
                operation.model_copy(
                    update={
                        "inputs": tuple(inputs),
                        "outputs": tuple(outputs),
                        "documentation_hint": hint,
                    }
                )
            )
    return tuple(normalized)


# ---------------------------------------------------------------------------
# Component assembly a preset may reuse (#159 S2, lifted from fan-out 2.0.0)
# ---------------------------------------------------------------------------
#
# Each takes AUTHORITY-validated values (never raw material), a key prefix the
# primitive derives its role keys from, and the pointer a builder refusal names.
# No topology, ordinal, handoff, branch or watermark logic lives here.


def emit_db_read_components(
    connection: _Any,
    read: _Any,
    *,
    context: _Any,
    key_prefix: str,
    pointer: str,
    base_connection_key: Optional[str] = None,
    component_names: Optional[_Mapping[str, str]] = None,
) -> _List[IntegrationComponentSpec]:
    """The DB-read group through the ``db_extract`` primitive: connection, read
    profile and Get operation — or, for a base-key binding
    (``base_connection_key``), the read profile and Get operation only, bound
    to that key. ``connection`` is :func:`authorize_db_connection`'s result;
    ``read`` the validated ``DbReadOperation``. Builder refusals point at
    ``pointer`` (the group) or ``pointer + "/read"`` (base-bound)."""
    rt = _rt()
    connection_params = db_connection_params(
        connection, base_connection_key=base_connection_key or ""
    )
    read_profile: _Dict[str, _Any] = {
        "query": read.sql,
        "output_fields": [
            {"name": field.name, "data_type": field.data_type, "mandatory": field.required}
            for field in read.result_schema.fields
        ],
    }
    if read.parameters:
        read_profile["parameters"] = [
            {"name": parameter.name, "mappable": parameter.direction == "in"}
            for parameter in read.parameters
        ]
    operation: _Dict[str, _Any] = {}
    if read.batch_size is not None:
        operation["batch_count"] = read.batch_size
    if read.max_rows is not None:
        operation["max_rows"] = read.max_rows
    params = _builder_check(
        pointer,
        lambda: rt.DbExtractParameters.model_validate(
            {
                "key_prefix": key_prefix,
                "connection": connection_params,
                "read_profile": read_profile,
                "operation": operation,
                "component_names": dict(component_names or {}),
            }
        ),
    )
    if base_connection_key is None:
        return _builder_check(pointer, lambda: rt.DbExtractPrimitive.emit_components(context, params))
    read_profile_key = rt.primitive_component_key(key_prefix, rt.ROLE_DB_READ_PROFILE)
    get_operation_key = rt.primitive_component_key(key_prefix, rt.ROLE_DB_GET_OPERATION)
    folder = context.folder_path
    return [
        _builder_check(
            pointer + "/read",
            lambda: rt.DbExtractPrimitive._emit_read_profile(
                context, params, read_profile_key, folder
            ),
        ),
        _builder_check(
            pointer + "/read",
            lambda: rt.DbExtractPrimitive._emit_get_operation(
                context,
                params,
                get_operation_key,
                base_connection_key,
                read_profile_key,
                folder,
            ),
        ),
    ]


def emit_transform_components(
    operations: Tuple[_Any, ...],
    *,
    context: _Any,
    key_prefix: str,
    pointer: str,
    source: _Mapping[str, _Any],
    target_payload_profile: Optional[_Mapping[str, _Any]] = None,
    target_binding: Optional[_Mapping[str, _Any]] = None,
    component_names: Optional[_Mapping[str, str]] = None,
) -> _List[IntegrationComponentSpec]:
    """The transform group through the ``field_map`` primitive, from the
    shared mapping leaves AFTER their legacy authorities normalized them
    (direct / map_function / map_script reference). ``source`` is the
    primitive's ``SourceBinding``; give exactly one of
    ``target_payload_profile`` (generate a JSON target profile) or
    ``target_binding`` (bind an existing profile, e.g. a DB write profile).
    Map scripts take the primitive's script-component REFERENCE path: the
    script is a base component, referenced by ``$ref``, never inlined."""
    rt = _rt()
    direct: _List[_Dict[str, _Any]] = []
    functions: _List[_Dict[str, _Any]] = []
    scripts: _List[_Dict[str, _Any]] = []
    for operation in operations:
        if isinstance(operation, FanoutDirectOperationV2):
            direct.append(
                {"source_field": operation.source_field, "target_path": operation.target_path}
            )
        elif isinstance(operation, FanoutFunctionOperationV2):
            entry: _Dict[str, _Any] = {
                "function_type": operation.function_type,
                "inputs": list(operation.inputs),
                "target_path": operation.target_path,
            }
            if operation.parameters:
                entry["parameters"] = {
                    parameter.name: parameter.value for parameter in operation.parameters
                }
            functions.append(entry)
        else:
            # A script port is named by the legacy authority's rule (the
            # source/target path's sanitized final segment), so a JSON leaf
            # PATH such as ``Root/id`` binds port ``id``; a column name that is
            # already an identifier is its own port, unchanged.
            scripts.append(
                {
                    "inputs": [
                        {"source_path": name, "input_name": rt.script_var_name(name)}
                        for name in operation.inputs
                    ],
                    "outputs": [
                        {"output_name": rt.script_var_name(path), "target_path": path}
                        for path in operation.outputs
                    ],
                    "script_component_ref": f"$ref:{operation.script_component_key}",
                }
            )
    values: _Dict[str, _Any] = {"key_prefix": key_prefix, "source": dict(source)}
    if target_payload_profile is not None:
        values["target_payload_profile"] = target_payload_profile
    if target_binding is not None:
        values["target_binding"] = dict(target_binding)
    values.update(
        {
            "direct": direct,
            "map_function": functions,
            "map_script": scripts,
            "component_names": dict(component_names or {}),
        }
    )
    params = _builder_check(pointer, lambda: rt.FieldMapParameters.model_validate(values))
    return _builder_check(pointer, lambda: rt.FieldMapPrimitive.emit_components(context, params))


def rest_connection_params(connection: _Any, *, base_connection_key: str) -> _Dict[str, _Any]:
    """The REST primitive ``connection`` parameters for an AUTHORIZED REST
    connection: validated ``RestCreateSettings`` (always unauthenticated), a
    validated ``RestConnectionBinding`` reuse, or — base-key binding
    (``None``) — a placeholder reuse naming ``base_connection_key``."""
    from ..patterns.archetype_parameters import RestCreateSettings

    if isinstance(connection, RestCreateSettings):
        return {
            "mode": "create",
            "base_url": connection.base_url,
            "auth": _rt().REST_CREATE_AUTH_MAP["none"],
        }
    if connection is not None:
        params: _Dict[str, _Any] = {"mode": "reuse"}
        if connection.component_id:
            params["component_id"] = connection.component_id
        if connection.component_name:
            params["component_name"] = connection.component_name
        return params
    return {"mode": "reuse", "component_id": base_connection_key}


def emit_rest_send_components(
    connection: _Any,
    request: RestSendRequest,
    *,
    context: _Any,
    key_prefix: str,
    pointer: str,
    request_profile_key: str,
    component_names: Optional[_Mapping[str, str]] = None,
    base_connection_key: Optional[str] = None,
) -> _List[IntegrationComponentSpec]:
    """The REST SEND group through the ``rest_send`` primitive: connection and
    operation — or, for a base-key binding, the operation only, bound to
    ``base_connection_key``. ``request`` is the validated ``RestSendRequest``;
    only its LITERAL query parameters are emitted (a watermark-sourced one is
    recorded intent, never config). The operation sends the JSON profile
    ``request_profile_key``. Builder refusals point at ``pointer`` (the group)
    or ``pointer + "/operation"`` (base-bound)."""
    rt = _rt()
    connection_params = rest_connection_params(
        connection, base_connection_key=base_connection_key or ""
    )
    operation: _Dict[str, _Any] = {
        "method": request.method,
        "path": request.path,
        "request_profile_id": f"$ref:{request_profile_key}",
        "request_profile_type": "json",
    }
    literal = {
        parameter.name: parameter.literal_value
        for parameter in request.query_parameters
        if parameter.value_source == "literal" and parameter.literal_value is not None
    }
    if literal:
        operation["query_parameters"] = literal
    params = _builder_check(
        pointer,
        lambda: rt.RestSendWithRetryParameters.model_validate(
            {
                "key_prefix": key_prefix,
                "connection": connection_params,
                "operation": operation,
                "component_names": dict(component_names or {}),
            }
        ),
    )
    if base_connection_key is None:
        return _builder_check(
            pointer, lambda: rt.RestSendWithRetryPrimitive.emit_components(context, params)
        )
    operation_key = rt.primitive_component_key(key_prefix, rt.ROLE_REST_OPERATION)
    return [
        _builder_check(
            pointer + "/operation",
            lambda: rt.RestSendWithRetryPrimitive._emit_operation(
                context, params, operation_key, base_connection_key, context.folder_path
            ),
        )
    ]


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


#: The public surface a preset uses (``SHARED_BOUNDARY_API.md``, #159 S2).
__all__ = [
    # material blocks
    "FanoutDbCreateMaterialV2",
    "FanoutConnectionReuseMaterialV2",
    "FanoutDbConnectionMaterialV2",
    "FanoutResultFieldV2",
    "FanoutBindParameterV2",
    "FanoutDbReadMaterialV2",
    "FanoutDirectOperationV2",
    "FanoutFunctionParameterV2",
    "FanoutFunctionOperationV2",
    "FanoutScriptReferenceOperationV2",
    "FanoutTransformOperationV2",
    "FanoutRestCreateMaterialV2",
    "FanoutRestConnectionMaterialV2",
    "FanoutQueryParameterV2",
    "FanoutRestOperationMaterialV2",
    "DbWriteFieldMaterialV1",
    "DbWriteConditionMaterialV1",
    "DbWriteProfileMaterialV1",
    "DbSendOperationMaterialV1",
    "DbWriteMaterialV1",
    "SHARED_MATERIAL_BLOCKS",
    # served text and vocabulary
    "LITERAL_QUERY_NAMES_V2",
    "LITERAL_QUERY_GUARANTEE_V2",
    "QUERY_GRAMMAR_SENTENCE_V2",
    "REUSED_BASE_SENTENCE_V2",
    "REUSED_TARGET_STORED_BASE",
    "REUSED_TARGET_PATH",
    # credential boundary
    "register_material_root",
    "registered_material_roots",
    "material_field_classes",
    "check_credential_boundary",
    # selected bases and requests
    "check_connection_binding",
    "check_rest_request",
    "judge_reused_request_target",
    # authorization (value rules through the neutral authorities)
    "authorize_db_connection",
    "authorize_db_read",
    "authorize_db_write_material",
    "authorize_rest_connection",
    "authorize_rest_request",
    "authorize_transform_operations",
    "AuthorizedDbWriteV1",
    # assembly
    "db_connection_params",
    "rest_connection_params",
    "emit_db_read_components",
    "emit_db_write_components",
    "emit_rest_send_components",
    "emit_transform_components",
    # prepared result
    "PreparedRecipeComponentsV1",
]
