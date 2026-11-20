"""Shared fixtures for the fan-out 2.0.0 tests (issue #159 S1).

The flagship v2 invocation is DERIVED from the ``compose_request`` frozen inside
the independent oracle (``tests/fixtures/issue_159/flagship_fanout_legacy_spec.json``,
captured from the legacy composition path at cb7034b BEFORE ``fanout.py``
changed). Nothing here is copied from what the v2 code emits: the inputs are the
legacy request's own values, re-expressed in the v2 split (secret-free recipe
input + closed component material), and the oracle's ``integration_spec`` is the
expected output.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ORACLE_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "issue_159" / "flagship_fanout_legacy_spec.json"
)

RECIPE_ID = "boomi.compose.db_rest_fanout"
V2 = "2.0.0"


def oracle() -> Dict[str, Any]:
    return json.loads(ORACLE_PATH.read_text())


def flagship_v2() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """``(raw_input, component_material)`` for the flagship, from the oracle request."""
    return v2_from_compose_request(oracle()["compose_request"])


def _connection_material(binding: Dict[str, Any], *, rest: bool) -> Dict[str, Any]:
    if binding["mode"] == "reuse":
        return {
            "mode": "reuse",
            **{k: binding[k] for k in ("component_id", "component_name") if binding.get(k)},
        }
    settings = binding["settings"]
    if rest:
        return {"mode": "create", "base_url": settings["base_url"]}
    material = {
        "mode": "create",
        "driver": settings["driver"],
        "host": settings["host"],
        "database": settings["database"],
    }
    for key in ("port", "username", "credential_ref"):
        if settings.get(key) is not None:
            material[key] = settings[key]
    return material


def _operation(operation: Dict[str, Any]) -> Dict[str, Any]:
    kind = operation["operation_type"]
    converted = {k: v for k, v in operation.items() if k != "parameters"}
    if kind == "map_function" and operation.get("parameters"):
        converted["parameters"] = [
            {"name": name, "value": value} for name, value in operation["parameters"].items()
        ]
    return converted


def v2_from_compose_request(request: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Re-express a LEGACY ``compose_archetypes`` request in the v2 split.

    A mechanical re-spelling of the legacy request's own values — secret-free
    identities and classifications into ``raw_input``, connection settings, SQL,
    paths and mapping into the closed component material. Nothing is taken
    from what the v2 code emits.
    """
    parts = {part["key"]: part for part in request["parts"]}
    db_key = next(p["key"] for p in request["parts"] if p["kind"] == "db_source")
    shape_key = next(p["key"] for p in request["parts"] if p["kind"] == "transform")
    db = parts[db_key]["parameters"]
    shape = parts[shape_key]["parameters"]
    targets = [part for part in request["parts"] if part["kind"] == "rest_target"]
    options = request.get("options") or {}
    naming = options.get("naming") or {}
    source = {"part_key": db_key, "binding_mode": db["binding"]["mode"]}
    if db["binding"]["mode"] == "create":
        source["auth_mode"] = db["binding"]["settings"]["auth_mode"]
    raw_input: Dict[str, Any] = {
        "version": "2",
        "process_key": "main_process",
        "naming": {"component_prefix": naming["component_prefix"]},
        "source": source,
        "transform": {"part_key": shape_key},
        "targets": [
            {
                "part_key": part["key"],
                "label": part.get("label"),
                "binding_mode": part["parameters"]["binding"]["mode"],
                "method": part["parameters"]["send_request"].get("method", "POST"),
                **(
                    {"expected_status_codes": part["parameters"]["send_request"]["expected_status_codes"]}
                    if "expected_status_codes" in part["parameters"]["send_request"]
                    else {}
                ),
            }
            for part in targets
        ],
    }
    if naming.get("folder_path"):
        raw_input["naming"]["folder_name"] = naming["folder_path"]
    if options.get("links") is not None:
        raw_input["links"] = copy.deepcopy(options["links"])
    read = db["read_operation"]
    read_material: Dict[str, Any] = {
        "sql": read["sql"],
        "result_fields": read["result_schema"]["fields"],
    }
    if read.get("parameters"):
        read_material["bind_parameters"] = [
            {"name": p["name"], "direction": p.get("direction", "in")} for p in read["parameters"]
        ]
    for legacy, v2 in (("batch_size", "batch_size"), ("max_rows", "max_rows")):
        if read.get(legacy) is not None:
            read_material[v2] = read[legacy]
    material = {
        "version": "2",
        "source": {
            "connection": _connection_material(db["binding"], rest=False),
            "read": read_material,
        },
        "transform": {"operations": [_operation(op) for op in shape["operations"]]},
        "payload_profile": targets[0]["parameters"]["payload_profile"],
        "targets": [
            {
                "part_key": part["key"],
                "connection": _connection_material(part["parameters"]["binding"], rest=True),
                "operation": {
                    "path": part["parameters"]["send_request"]["path"],
                    "query_parameters": part["parameters"]["send_request"].get(
                        "query_parameters", []
                    ),
                },
            }
            for part in targets
        ],
    }
    return copy.deepcopy(raw_input), copy.deepcopy(material)


def recipe_request(
    raw_input: Dict[str, Any],
    material: Optional[Dict[str, Any]],
    *,
    base_components=(),
    recipe_version: Optional[str] = V2,
    invocation_id: str = "fanout",
    extra_invocations=(),
    process_envelopes=(),
):
    """A typed ``AuthoringRequestV1`` carrying one v2 invocation."""
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    invocation = {
        "recipe_id": RECIPE_ID,
        "invocation_id": invocation_id,
        "raw_input": copy.deepcopy(raw_input),
    }
    if recipe_version is not None:
        invocation["recipe_version"] = recipe_version
    if material is not None:
        invocation["component_material"] = copy.deepcopy(material)
    return AuthoringRequestV1.model_validate(
        {
            "intent": {
                "intent_kind": "recipe",
                "integration_name": "Fanout v2",
                "invocations": [invocation, *extra_invocations],
                "base_components": list(base_components),
                "process_envelopes": list(process_envelopes),
            }
        }
    )
