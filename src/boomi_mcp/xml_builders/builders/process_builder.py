"""
Process Builder using Hybrid Architecture.

This builder demonstrates the hybrid approach:
- Templates (XML structure) from templates/ module
- Builders (logic, calculations) in this module

Based on real Boomi process: "Aggregate Prompt Messages"
"""

from typing import List, Dict, Any, Optional
from ..templates import PROCESS_COMPONENT_WRAPPER
from ..templates.shapes import (
    START_SHAPE_TEMPLATE,
    RETURN_DOCUMENTS_SHAPE_TEMPLATE,
    STOP_SHAPE_TEMPLATE,
    MAP_SHAPE_TEMPLATE,
    MESSAGE_SHAPE_TEMPLATE,
    CONNECTOR_SHAPE_TEMPLATE,
    DECISION_SHAPE_TEMPLATE,
    DOCUMENT_PROPERTIES_SHAPE_TEMPLATE,
    BRANCH_SHAPE_TEMPLATE,
    NOTE_SHAPE_TEMPLATE,
    DRAGPOINT_TEMPLATE,
    DRAGPOINT_BRANCH_TEMPLATE,
)
from .coordinate_calculator import CoordinateCalculator


class ProcessBuilder:
    """
    Build Boomi process components using hybrid architecture.

    This class demonstrates the separation of concerns:
    - XML templates define STRUCTURE
    - Builder methods handle LOGIC (coordinates, connections, validation)
    """

    def __init__(self):
        """Initialize process builder with coordinate calculator."""
        self.coord_calc = CoordinateCalculator()
        self.shape_counter = 1  # Auto-increment shape names

    def build_linear_process(
        self,
        name: str,
        shapes_config: List[Dict[str, Any]],
        folder_name: str = "Home",
        description: str = "",
        **process_attrs
    ) -> str:
        """
        Build a process with shapes in linear (horizontal) flow.

        This demonstrates the hybrid approach:
        1. User provides high-level config (what shapes, what order)
        2. Builder calculates positions automatically
        3. Templates render the XML structure
        4. Result: Valid Boomi process XML

        Args:
            name: Process name
            shapes_config: List of shape configurations, e.g.:
                [
                    {'type': 'start', 'name': 'start'},
                    {'type': 'map', 'name': 'transform', 'map_id': '...'},
                    {'type': 'return', 'name': 'end'}
                ]
            folder_name: Folder path (default: "Home")
            description: Process description
            **process_attrs: Process-level attributes (allow_simultaneous, etc.)

        Returns:
            Complete process XML string ready for Boomi API

        Example:
            >>> builder = ProcessBuilder()
            >>> shapes = [
            ...     {'type': 'start', 'name': 'start'},
            ...     {'type': 'map', 'name': 'transform_data', 'map_id': 'abc-123'},
            ...     {'type': 'return', 'name': 'end'}
            ... ]
            >>> xml = builder.build_linear_process(
            ...     name="Simple ETL Process",
            ...     shapes_config=shapes,
            ...     folder_name="Integrations/Production"
            ... )
        """
        # Validate
        if not shapes_config:
            raise ValueError("At least one shape is required")

        if shapes_config[0]['type'] != 'start':
            raise ValueError("First shape must be 'start' type")

        if shapes_config[-1]['type'] not in ('stop', 'return'):
            raise ValueError("Last shape must be 'stop' or 'return' type")

        # Calculate positions automatically
        num_shapes = len(shapes_config)
        coordinates = self.coord_calc.calculate_linear_layout(num_shapes)

        # Build individual shapes
        shapes_xml = []
        for i, shape_cfg in enumerate(shapes_config):
            x, y = coordinates[i]

            # Determine next shape for connection (None for last shape)
            next_shape = shapes_config[i + 1]['name'] if i < num_shapes - 1 else None

            # Build shape XML using template
            shape_xml = self._build_shape(
                shape_type=shape_cfg['type'],
                name=shape_cfg['name'],
                x=x,
                y=y,
                next_shape=next_shape,
                userlabel=shape_cfg.get('userlabel', ''),
                **shape_cfg.get('config', {})
            )
            shapes_xml.append(shape_xml)

        # Render final process using template
        shapes_block = '\n'.join(shapes_xml)

        # Include folderId attribute if provided (Boomi requires folderId for folder placement)
        folder_id = process_attrs.get('folder_id', '')
        folder_id_attr = f'folderId="{folder_id}"' if folder_id else ''

        return PROCESS_COMPONENT_WRAPPER.format(
            name=name,
            folder_name=folder_name,
            description=description,
            folder_id_attr=folder_id_attr,
            allow_simultaneous=process_attrs.get('allow_simultaneous', 'false'),
            enable_user_log=process_attrs.get('enable_user_log', 'false'),
            process_log_on_error_only=process_attrs.get('process_log_on_error_only', 'false'),
            purge_data_immediately=process_attrs.get('purge_data_immediately', 'false'),
            update_run_dates=process_attrs.get('update_run_dates', 'false'),
            workload=process_attrs.get('workload', 'general'),
            shapes=shapes_block
        )

    def _build_shape(
        self,
        shape_type: str,
        name: str,
        x: float,
        y: float,
        next_shape: Optional[str] = None,
        userlabel: str = "",
        **config
    ) -> str:
        """
        Build individual shape XML using appropriate template.

        Args:
            shape_type: Type of shape ('start', 'map', 'return', etc.)
            name: Shape name
            x, y: Coordinates
            next_shape: Name of next shape for connection (None if last)
            userlabel: User-friendly label
            **config: Shape-specific configuration

        Returns:
            Shape XML string
        """
        # Build dragpoints if there's a next shape
        dragpoints_xml = ""
        if next_shape:
            drag_x, drag_y = self.coord_calc.calculate_dragpoint(x, y)
            dragpoints_xml = DRAGPOINT_TEMPLATE.format(
                name=f"{name}.dragpoint1",
                to_shape=next_shape,
                x=drag_x,
                y=drag_y
            )

        # Select template and build based on shape type
        if shape_type == 'start':
            return START_SHAPE_TEMPLATE.format(
                name=name,
                userlabel=userlabel or 'Start',
                x=x,
                y=y,
                dragpoints=dragpoints_xml
            )

        elif shape_type == 'stop':
            # Use dict to avoid 'continue' keyword conflict
            stop_params = {
                'name': name,
                'userlabel': userlabel or 'Stop',
                'x': x,
                'y': y,
                'continue': config.get('continue', 'true')
            }
            return STOP_SHAPE_TEMPLATE.format(**stop_params)

        elif shape_type == 'return':
            return RETURN_DOCUMENTS_SHAPE_TEMPLATE.format(
                name=name,
                userlabel=userlabel or 'Return Documents',
                x=x,
                y=y,
                label=config.get('label', '')
            )

        elif shape_type == 'map':
            if 'map_id' not in config:
                raise ValueError("'map_id' required for map shape")

            return MAP_SHAPE_TEMPLATE.format(
                name=name,
                userlabel=userlabel or 'Map',
                x=x,
                y=y,
                map_id=config['map_id'],
                dragpoints=dragpoints_xml
            )

        elif shape_type == 'message':
            if 'message_text' not in config:
                raise ValueError("'message_text' required for message shape")

            return MESSAGE_SHAPE_TEMPLATE.format(
                name=name,
                userlabel=userlabel or 'Message',
                x=x,
                y=y,
                message_text=config['message_text'],
                dragpoints=dragpoints_xml
            )

        elif shape_type == 'connector':
            required_fields = ['connector_id', 'operation']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"'{field}' required for connector shape")

            return CONNECTOR_SHAPE_TEMPLATE.format(
                name=name,
                userlabel=userlabel or 'Connector',
                x=x,
                y=y,
                connector_id=config['connector_id'],
                operation=config['operation'],
                object_type=config.get('object_type', ''),
                dragpoints=dragpoints_xml
            )

        elif shape_type == 'decision':
            if 'expression' not in config:
                raise ValueError("'expression' required for decision shape")

            return DECISION_SHAPE_TEMPLATE.format(
                name=name,
                userlabel=userlabel or 'Decision',
                x=x,
                y=y,
                expression=config['expression'],
                dragpoints=dragpoints_xml
            )

        elif shape_type == 'note':
            return NOTE_SHAPE_TEMPLATE.format(
                name=name,
                x=x,
                y=y,
                created_by=config.get('created_by', 'claude@renera.ai'),
                note_text=config.get('note_text', '')
            )

        else:
            raise ValueError(f"Unsupported shape type: {shape_type}")

    def reset_counter(self):
        """Reset shape counter for testing."""
        self.shape_counter = 1
