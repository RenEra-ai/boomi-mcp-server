"""Source, transform, target, and operational primitive package.

Concrete ``PrimitivePattern`` subclasses discovered by
``PatternRegistry.from_package('boomi_mcp.patterns')``. Primitives validate
caller-authored parameters and either emit deterministic
``IntegrationComponentSpec`` objects for the existing builder layer
(``emit_components``) or return structured process fragments
(``emit_fragment``). They are internal building blocks — no MCP tools are
exposed for them; issue #29 composes them into the executable
``database_to_api_sync`` archetype.

Issue #27 added the source/transform primitives (``db_extract``,
``field_map``, ``xml_json_convert``); issue #28 adds the REST target primitive
(``rest_send_with_retry``) and the operational reliability primitives
(``schedule_envelope``, ``watermark_state``, ``error_classifier``,
``dlq_writer``, ``run_metadata``). Issue #72 (M5.4) adds the REST source
primitive (``rest_fetch``) — the REST counterpart to ``db_extract``.
"""

from .branch import BranchPrimitive
from .data_process import DataProcessPrimitive
from .db_extract import DbExtractPrimitive
from .decision import DecisionPrimitive
from .document_cache_remove import DocumentCacheRemovePrimitive
from .document_cache_retrieve import DocumentCacheRetrievePrimitive
from .field_map import FieldMapPrimitive
from .flow_control import FlowControlPrimitive
from .operational import (
    DlqWriterPrimitive,
    ErrorClassifierPrimitive,
    RunMetadataPrimitive,
    ScheduleEnvelopePrimitive,
    WatermarkStatePrimitive,
)
from .rest_fetch import RestFetchPrimitive
from .rest_send import RestSendWithRetryPrimitive
from .return_documents import ReturnDocumentsPrimitive
from .throw_exception import ThrowExceptionPrimitive
from .xml_json_convert import XmlJsonConvertPrimitive

__all__ = [
    "BranchPrimitive",
    "DataProcessPrimitive",
    "DbExtractPrimitive",
    "DecisionPrimitive",
    "DocumentCacheRemovePrimitive",
    "DocumentCacheRetrievePrimitive",
    "FieldMapPrimitive",
    "FlowControlPrimitive",
    "XmlJsonConvertPrimitive",
    "RestFetchPrimitive",
    "RestSendWithRetryPrimitive",
    "ReturnDocumentsPrimitive",
    "ThrowExceptionPrimitive",
    "ScheduleEnvelopePrimitive",
    "WatermarkStatePrimitive",
    "ErrorClassifierPrimitive",
    "DlqWriterPrimitive",
    "RunMetadataPrimitive",
]
