"""Issue #27: ``xml_json_convert`` transform primitive.

Represents an explicit XML→JSON or JSON→XML conversion as a single direct
Boomi ``transform.map``. One side must be ``profile.xml`` and the other
``profile.json`` (in either direction); all mappings are explicit
caller-authored direct field mappings validated against caller-supplied
profile field indexes. No standalone conversion / data-process component is
emitted in M2, and implicit conversion, XSLT, function, script, raw-XML, and
payload-template requests are rejected.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...categories.components.builders.map_builder import DirectMapBuilder
from ...categories.components.builders.profile_generation import (
    MAP_PROFILE_INDEX_UNAVAILABLE,
)
from ...models.integration_models import IntegrationComponentSpec
from ..base import (
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)
from ._helpers import (
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
    raise_for_builder_error,
)

# Only XML and JSON profile families participate — profile.db / unknown
# families are rejected at the parameter boundary by the Literal type.
_CONVERT_PROFILE_TYPE = Literal["profile.xml", "profile.json"]


class ConvertFieldMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_path: str
    target_path: str


class XmlJsonConvertParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key_prefix: str = Field(..., description="Stable key prefix for the deterministic map key")
    source_profile_id: str = Field(..., description="'$ref:KEY' or literal profile UUID")
    source_profile_type: _CONVERT_PROFILE_TYPE
    target_profile_id: str = Field(..., description="'$ref:KEY' or literal profile UUID")
    target_profile_type: _CONVERT_PROFILE_TYPE
    field_mappings: List[ConvertFieldMapping] = Field(..., min_length=1)
    source_field_index: Dict[str, Dict[str, Any]] = Field(
        ..., description="Per-leaf source index ({path: {data_type, mappable, ...}})"
    )
    target_field_index: Dict[str, Dict[str, Any]] = Field(
        ..., description="Per-leaf target index ({path: {data_type, mappable, ...}})"
    )
    component_name: Optional[str] = Field(default=None)

    @model_validator(mode="after")
    def _require_xml_json_pair(self) -> "XmlJsonConvertParameters":
        if {self.source_profile_type, self.target_profile_type} != {
            "profile.xml",
            "profile.json",
        }:
            raise ValueError(
                "xml_json_convert requires exactly one profile.xml side and one "
                "profile.json side (XML→JSON or JSON→XML); same-family and "
                "non-XML/JSON conversions are not supported"
            )
        return self


class XmlJsonConvertPrimitive(PrimitivePattern):
    """Emit a single direct map representing an explicit XML↔JSON conversion."""

    metadata = PatternMetadata(
        name="xml_json_convert",
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=(
            "Represent an explicit XML-to-JSON or JSON-to-XML conversion as a "
            "single direct Boomi map between an XML profile and a JSON "
            "profile. All mappings are explicit; no implicit conversion is "
            "performed."
        ),
        tags=["transform", "convert", "xml", "json"],
        use_cases=[
            "Convert an XML source document to a JSON payload via a direct map",
            "Convert a JSON source document to an XML payload via a direct map",
        ],
        not_for=[
            "Implicit / automatic conversion without explicit field mappings",
            "XSLT, function, or script transforms",
            "Same-family (XML→XML, JSON→JSON) or database conversions",
        ],
    )
    parameters_model = XmlJsonConvertParameters

    output_contract = PatternIOContract(
        name="converted_payload",
        description="Direct transform.map converting between an XML and a JSON profile.",
        schema_={
            "type": "object",
            "properties": {
                "map_key": {"type": "string"},
                "map_route": {"type": "string", "const": "direct"},
            },
        },
    )
    required_builders = ["DirectMapBuilder"]

    @classmethod
    def emit_components(
        cls,
        context: PrimitiveBuildContext,
        parameters: BaseModel,
    ) -> List[IntegrationComponentSpec]:
        params: XmlJsonConvertParameters = parameters  # type: ignore[assignment]

        cls._require_index("source", params.source_field_index)
        cls._require_index("target", params.target_field_index)

        map_key = primitive_component_key(params.key_prefix, ROLE_TRANSFORM_MAP)
        component_name = (
            params.component_name or f"{context.component_prefix} XML/JSON Convert"
        )
        config: Dict[str, Any] = {
            "map_type": "direct",
            "component_name": component_name,
            "source_profile_id": params.source_profile_id,
            "source_profile_type": params.source_profile_type,
            "target_profile_id": params.target_profile_id,
            "target_profile_type": params.target_profile_type,
            "field_mappings": [m.model_dump() for m in params.field_mappings],
        }
        if context.folder_path:
            config["folder_path"] = context.folder_path

        raise_for_builder_error(
            DirectMapBuilder.validate_config(
                config,
                source_index=params.source_field_index,
                target_index=params.target_field_index,
            )
        )
        return [
            IntegrationComponentSpec(
                key=map_key,
                type="transform.map",
                action="create",
                name=component_name,
                config=config,
            )
        ]

    @staticmethod
    def _require_index(side: str, index: Dict[str, Dict[str, Any]]) -> None:
        if not index:
            raise BuilderValidationError(
                f"{side}_field_index is required — xml_json_convert validates "
                "explicit field mappings against the profile field index",
                error_code=MAP_PROFILE_INDEX_UNAVAILABLE,
                field=f"{side}_field_index",
                hint=(
                    "Supply the field index for both profiles. M2 does not "
                    "parse existing profile XML (discovery is issue #47); the "
                    "caller provides the per-leaf index."
                ),
                details={"side": side},
            )
