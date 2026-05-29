"""Issue #27: source and transform primitive package.

Concrete ``PrimitivePattern`` subclasses discovered by
``PatternRegistry.from_package('boomi_mcp.patterns')``. Primitives validate
caller-authored parameters and emit deterministic ``IntegrationComponentSpec``
objects for the existing builder layer. They are internal building blocks —
no MCP tools are exposed for them in issue #27; issue #29 composes them into
the executable ``database_to_api_sync`` archetype.
"""

from .db_extract import DbExtractPrimitive
from .field_map import FieldMapPrimitive
from .xml_json_convert import XmlJsonConvertPrimitive

__all__ = [
    "DbExtractPrimitive",
    "FieldMapPrimitive",
    "XmlJsonConvertPrimitive",
]
