"""The HTTP method a WSS listener route is served on — the measured platform facts.

A leaf module with no package imports, so a caller that must stay importable
without the builders package (``categories.meta_tools`` serves these texts at
import time) can read them. ``builders._api_service_paths`` applies them and
re-exports them. Evidence and derivation: that module's docstring and
``docs/architecture/evidence/issue-158/captures/cap158-r3-wss-method-matrix/``.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Dict, Mapping

#: The method an ALL-INHERIT API Service route serves, by the linked WSS Listen
#: operation's ``operationType`` — the platform's own table ("API service REST
#: tab": "GET — GET or QUERY operation. POST — CREATE, EXECUTE or UPSERT
#: operation. PUT — UPDATE operation. DELETE — DELETE operation."), measured
#: cell-for-cell in the capture named in the module docstring. The keys are
#: pinned to the WSS builder's operation-type vocabulary by a test.
ASC_METHOD_BY_OPERATION_TYPE: Mapping[str, str] = MappingProxyType(
    {
        "GET": "GET",
        "QUERY": "GET",
        "CREATE": "POST",
        "EXECUTE": "POST",
        "UPSERT": "POST",
        "UPDATE": "PUT",
        "DELETE": "DELETE",
    }
)

#: The operation type a WSS Listen operation has when none is stated — the WSS
#: builder's own default (``WssListenerOperationBuilder.DEFAULT_OPERATION_TYPE``,
#: pinned by a test; it cannot be imported here without a cycle).
DEFAULT_WSS_OPERATION_TYPE = "EXECUTE"


def _clauses(methods=None) -> str:
    """``TYPES -> METHOD`` clauses from the table, for ``methods`` (all by default)."""
    by_method: Dict[str, list] = {}
    for operation_type, method in ASC_METHOD_BY_OPERATION_TYPE.items():
        if methods is None or method in methods:
            by_method.setdefault(method, []).append(operation_type)
    return "; ".join(
        "{0} -> {1}".format(" / ".join(sorted(types)), method)
        for method, types in sorted(by_method.items(), key=lambda item: sorted(item[1]))
    )


#: The API Service method rule as SERVED text — generated from the table, so no
#: served string can restate it differently (the retired rule survived in seven
#: served strings because each carried its own copy).
ASC_METHOD_RULE = "the linked operation's type decides it: " + _clauses()
#: The bare ``/ws/simple`` method rule as served text; pinned to
#: :func:`wss_http_method` by a test.
BARE_METHOD_RULE = "input_type none -> GET, anything else -> POST"


#: The operation types an all-inherit route serves on GET, as served text.
_GET_OPERATION_TYPES = " / ".join(
    sorted(t for t, m in ASC_METHOD_BY_OPERATION_TYPE.items() if m == "GET")
)
#: The CONSEQUENCE of the rule as served text (#158 QA-158-r5-01): the runtime
#: refuses GET for an operation expecting input, and the method a route serves
#: is its explicit http_method when one is set — so the refusal is about the
#: EFFECTIVE method, never the operation type alone. Generated, like the rules.
ASC_GET_WITH_INPUT_RULE = (
    "an API Service route whose method is GET — an explicit http_method GET, or "
    "an all-inherit route for a " + _GET_OPERATION_TYPES + " operation — cannot "
    "be called for an operation expecting input; an explicit http_method other "
    "than GET is used as given"
)
#: The operation types whose all-inherit route carries a request body, as the
#: served choices a refusal offers.
ASC_BODY_METHOD_CHOICES = _clauses({"POST", "PUT", "DELETE", "PATCH"})


__all__ = [
    "ASC_BODY_METHOD_CHOICES",
    "ASC_GET_WITH_INPUT_RULE",
    "ASC_METHOD_BY_OPERATION_TYPE",
    "ASC_METHOD_RULE",
    "BARE_METHOD_RULE",
    "DEFAULT_WSS_OPERATION_TYPE",
]
