"""
JSON parser for Boomi process configurations.

Converts JSON dictionaries to validated Pydantic models for the orchestrator.
Supports both single-process and multi-component formats.
"""

from typing import Any, Dict, List

from ..models.process_models import ComponentSpec, ProcessConfig


def parse_json_to_specs(config: Dict[str, Any]) -> List[ComponentSpec]:
    """
    Parse a process configuration dict into a list of ComponentSpec objects.

    Supported formats:
    1. Single process:
       {
         "name": "My Process",
         "folder_name": "Integrations",
         "shapes": [...]
       }

    2. Multi-component:
       {
         "components": [
           {"name": "Map A", "type": "map", "dependencies": []},
           {"name": "Main Process", "type": "process", "dependencies": ["Map A"], "config": {...}}
         ]
       }
    """
    if not isinstance(config, dict):
        raise ValueError("Process config must be a JSON object")

    if "components" in config:
        return _parse_multi_component(config["components"])
    return _parse_single_process(config)


def _parse_single_process(data: Dict[str, Any]) -> List[ComponentSpec]:
    process_config = ProcessConfig(**data)
    spec = ComponentSpec(
        name=process_config.name,
        type="process",
        dependencies=[],
        config=process_config.model_dump(),
    )
    return [spec]


def _parse_multi_component(components_data: Any) -> List[ComponentSpec]:
    if not isinstance(components_data, list):
        raise ValueError("'components' must be an array of component specs")

    specs: List[ComponentSpec] = []
    for comp_data in components_data:
        if not isinstance(comp_data, dict):
            raise ValueError("Each item in 'components' must be an object")

        comp_type = comp_data.get("type", "process")
        normalized = dict(comp_data)

        if comp_type == "process" and isinstance(normalized.get("config"), dict):
            process_config = ProcessConfig(**normalized["config"])
            normalized["config"] = process_config.model_dump()

        specs.append(ComponentSpec(**normalized))

    return specs

