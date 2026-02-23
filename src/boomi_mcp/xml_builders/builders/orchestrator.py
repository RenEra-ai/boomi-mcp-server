"""
Component Orchestrator for managing dependencies between Boomi components.

This module implements the top layer (Layer 3) of the hybrid architecture:
- Topological sorting of components by dependencies
- Fuzzy ID resolution (component names → IDs via API)
- Multi-component workflow management
- Session-based component registry

Based on MCP_TOOL_DESIGN.md lines 2700-2925.
"""

from typing import List, Dict, Any, Optional
from collections import defaultdict, deque

from boomi import Boomi
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
    FolderQueryConfig,
    FolderQueryConfigQueryFilter,
    FolderSimpleExpression,
    FolderSimpleExpressionOperator,
    FolderSimpleExpressionProperty,
)

from ...models.process_models import ComponentSpec, ProcessConfig
from .process_builder import ProcessBuilder


class ComponentOrchestrator:
    """
    Orchestrate creation of multiple Boomi components with dependency management.

    Features:
    - Topological sort (dependencies created first)
    - Fuzzy ID resolution (resolve component names to IDs)
    - Component registry (session cache of created components)
    - Error handling with descriptive messages

    Example:
        sdk = Boomi(account_id, username, password)
        orchestrator = ComponentOrchestrator(sdk)

        specs = [
            ComponentSpec(
                name="Transform Map",
                type="map",
                dependencies=[]
            ),
            ComponentSpec(
                name="Main Process",
                type="process",
                dependencies=["Transform Map"],
                config=ProcessConfig(...)
            )
        ]

        result = orchestrator.build_with_dependencies(specs)
    """

    def __init__(self, boomi_client: Boomi):
        """
        Initialize orchestrator.

        Args:
            boomi_client: Authenticated Boomi SDK client
        """
        self.client = boomi_client
        self.registry: Dict[str, Dict[str, Any]] = {}  # name → {id, type, xml}
        self.warnings: List[str] = []
        self.process_builder = ProcessBuilder()

    def build_with_dependencies(
        self,
        component_specs: List[ComponentSpec]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Build multiple components with dependency resolution.

        This is the main entry point for the orchestrator. It:
        1. Topologically sorts components by dependencies
        2. For each component in order:
           - Resolves references (names → IDs)
           - Builds XML using appropriate builder
           - Creates via Boomi API
           - Registers in session registry

        Args:
            component_specs: List of component specifications with dependencies

        Returns:
            Registry dict mapping component names to {id, type, xml}

        Raises:
            ValueError: If circular dependencies detected
            ValueError: If referenced component not found
            Exception: If API call fails

        Example:
            specs = [
                ComponentSpec(name="Map A", type="map", dependencies=[]),
                ComponentSpec(name="Process B", type="process", dependencies=["Map A"])
            ]
            result = orchestrator.build_with_dependencies(specs)
            # result = {
            #     "Map A": {"id": "abc-123", "type": "map", "xml": "..."},
            #     "Process B": {"id": "def-456", "type": "process", "xml": "..."}
            # }
        """
        # Step 1: Topological sort (dependencies first)
        sorted_specs = self._topological_sort(component_specs)

        # Step 2: Build each component in sorted order
        for spec in sorted_specs:
            # Step 3: Resolve references if it's a process
            if spec.type == 'process':
                self._resolve_process_references(spec)

            # Step 4: Build XML using appropriate builder
            if spec.type == 'process':
                xml = self._build_process(spec)
            elif spec.type == 'map':
                raise NotImplementedError("Map builder not implemented yet")
            elif spec.type == 'connection':
                raise NotImplementedError("Connection builder not implemented yet")
            else:
                raise ValueError(f"Unknown component type: {spec.type}")

            # Step 5: Create in Boomi API
            component_id = None
            try:
                result = self.client.component.create_component(request_body=xml)
                # Component API returns component_id (not id_)
                component_id = result.component_id if hasattr(result, 'component_id') else getattr(result, 'id_', None)
            except Exception as e:
                # Component may have been created but SDK response parsing failed
                # (e.g. 'bytes' object has no attribute 'items').
                # Query back by name to verify and retrieve the component_id.
                component_id = self._recover_created_component(spec.name, spec.type)
                if component_id is None:
                    raise Exception(
                        f"Failed to create component '{spec.name}': {str(e)}"
                    ) from e

            # Step 6: Register in session registry
            self.registry[spec.name] = {
                'id': component_id,  # For backward compatibility
                'type': spec.type,
                'xml': xml,
                'component_id': component_id
            }

        return self.registry

    def _build_process(self, spec: ComponentSpec) -> str:
        """
        Build process XML from specification.

        Args:
            spec: Component specification with ProcessConfig

        Returns:
            Complete process XML

        Raises:
            ValueError: If config is not ProcessConfig
        """
        # Parse config as ProcessConfig if it's a dict
        if isinstance(spec.config, dict):
            process_config = ProcessConfig(**spec.config)
        elif isinstance(spec.config, ProcessConfig):
            process_config = spec.config
        else:
            raise ValueError(f"Invalid config type for process: {type(spec.config)}")

        # Convert Pydantic model to dict for builder
        shapes_config = []
        for shape in process_config.shapes:
            shape_dict = {
                'type': shape.type,
                'name': shape.name,
                'userlabel': shape.userlabel or '',
                'config': shape.config or {}
            }
            shapes_config.append(shape_dict)

        # Resolve folder_name to folder_id (Boomi needs folderId for placement)
        folder_id = self._resolve_folder_id(process_config.folder_name)

        # Build using ProcessBuilder (convert Python bools to XML-compatible lowercase strings)
        xml = self.process_builder.build_linear_process(
            name=process_config.name,
            shapes_config=shapes_config,
            folder_name=process_config.folder_name,
            description=process_config.description,
            folder_id=folder_id,
            allow_simultaneous=str(process_config.allow_simultaneous).lower(),
            enable_user_log=str(process_config.enable_user_log).lower(),
            process_log_on_error_only=str(process_config.process_log_on_error_only).lower(),
            purge_data_immediately=str(process_config.purge_data_immediately).lower(),
            update_run_dates=str(process_config.update_run_dates).lower(),
            workload=process_config.workload
        )

        return xml

    def _resolve_folder_id(self, folder_name: str) -> str:
        """Resolve folder_name to folder_id via Folder API. Returns '' if not found.

        Tries multiple strategies:
        1. Exact match on fullPath (e.g. "Ren Era/Renera/Tests")
        2. LIKE match on fullPath ending with the given name (e.g. "%/Renera/Tests")
        3. Exact match on folder name (last segment, e.g. "Tests")

        Adds a warning to self.warnings if resolution fails.
        """
        if not folder_name or folder_name == "Home":
            return ''

        def _query_folder(prop, op, arg):
            try:
                expr = FolderSimpleExpression(operator=op, property=prop, argument=[arg])
                filt = FolderQueryConfigQueryFilter(expression=expr)
                res = self.client.folder.query_folder(request_body=FolderQueryConfig(query_filter=filt))
                if hasattr(res, 'result') and res.result:
                    if len(res.result) == 1:
                        f = res.result[0]
                        return getattr(f, 'id_', '') or getattr(f, 'id', '') or ''
                    else:
                        ids = [getattr(f, 'id_', '') or getattr(f, 'id', '') for f in res.result]
                        self.warnings.append(
                            f"Folder query '{arg}' matched {len(res.result)} folders "
                            f"(IDs: {ids}). Skipping — specify a unique folder path or use folderId directly."
                        )
                        return ''
            except Exception as e:
                self.warnings.append(f"Folder query error ({arg}): {e}")
            return ''

        # Strategy 1: exact fullPath
        fid = _query_folder(FolderSimpleExpressionProperty.FULLPATH,
                            FolderSimpleExpressionOperator.EQUALS, folder_name)
        if fid:
            return fid

        # Strategy 2: fullPath ending with the given name (user may omit account root)
        fid = _query_folder(FolderSimpleExpressionProperty.FULLPATH,
                            FolderSimpleExpressionOperator.LIKE, f"%/{folder_name}")
        if fid:
            return fid

        # Strategy 3: match last segment as folder name
        leaf = folder_name.rsplit('/', 1)[-1]
        fid = _query_folder(FolderSimpleExpressionProperty.NAME,
                            FolderSimpleExpressionOperator.EQUALS, leaf)
        if fid:
            return fid

        self.warnings.append(
            f"Could not resolve folder '{folder_name}' to a folderId. "
            f"Process will be created in the account's root folder. "
            f"Use 'manage_process list' after creation to verify the folder."
        )
        return ''

    def _recover_created_component(self, name: str, component_type: str) -> Optional[str]:
        """Query API for a just-created component when response parsing failed.

        Only returns a component_id if exactly one matching component was
        modified within the last 60 seconds — avoids false-positives from
        pre-existing components with the same name.
        """
        from datetime import datetime, timedelta, timezone
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=60)
        try:
            expression = ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[component_type]
            )
            query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
            query_config = ComponentMetadataQueryConfig(query_filter=query_filter)
            result = self.client.component_metadata.query_component_metadata(
                request_body=query_config
            )
            if hasattr(result, 'result') and result.result:
                matches = [
                    comp for comp in result.result
                    if (getattr(comp, 'name', '') == name
                        and str(getattr(comp, 'current_version', 'false')).lower() == 'true'
                        and str(getattr(comp, 'deleted', 'true')).lower() == 'false')
                ]
                recent = []
                for comp in matches:
                    mod_str = getattr(comp, 'modified_date', '')
                    if mod_str:
                        try:
                            mod_dt = datetime.fromisoformat(str(mod_str).replace('Z', '+00:00'))
                            if mod_dt >= cutoff:
                                cid = getattr(comp, 'component_id', None)
                                if cid:
                                    recent.append(cid)
                        except (ValueError, TypeError):
                            pass
                if len(recent) == 1:
                    return recent[0]
        except Exception:
            pass
        return None

    def _resolve_process_references(self, spec: ComponentSpec) -> None:
        """
        Resolve component references in process shapes.

        Converts reference names to component IDs:
        - 'map_ref': 'Transform Map' → 'map_id': 'abc-123-def'
        - 'connector_ref': 'SF Connector' → 'connector_id': 'xyz-789'
        - 'subprocess_ref': 'Validator' → 'process_id': 'uvw-456'

        Args:
            spec: ComponentSpec with ProcessConfig containing shapes

        Raises:
            ValueError: If referenced component not found
        """
        # Parse config
        if isinstance(spec.config, dict):
            process_config = ProcessConfig(**spec.config)
        elif isinstance(spec.config, ProcessConfig):
            process_config = spec.config
        else:
            return

        # Resolve references in each shape
        for shape in process_config.shapes:
            if not shape.config:
                continue

            # Map reference
            if 'map_ref' in shape.config:
                map_name = shape.config['map_ref']
                map_id = self._resolve_component_id(map_name, 'map')
                shape.config['map_id'] = map_id
                del shape.config['map_ref']  # Remove reference, keep only ID

            # Connector reference
            if 'connector_ref' in shape.config:
                connector_name = shape.config['connector_ref']
                connector_id = self._resolve_component_id(connector_name, 'connector')
                shape.config['connector_id'] = connector_id
                del shape.config['connector_ref']

            # Connection reference
            if 'connection_ref' in shape.config:
                connection_name = shape.config['connection_ref']
                connection_id = self._resolve_component_id(connection_name, 'connection')
                shape.config['connection_id'] = connection_id
                del shape.config['connection_ref']

            # Subprocess reference
            if 'subprocess_ref' in shape.config:
                subprocess_name = shape.config['subprocess_ref']
                subprocess_id = self._resolve_component_id(subprocess_name, 'process')
                shape.config['process_id'] = subprocess_id
                del shape.config['subprocess_ref']

        # Update spec config with resolved references
        spec.config = process_config

    def _resolve_component_id(self, name: str, component_type: str) -> str:
        """
        Resolve component name to ID with fuzzy matching.

        Resolution strategy:
        1. Check session registry (already created)
        2. Query API by name and type
        3. Handle 0, 1, or multiple matches

        Args:
            name: Component name to resolve
            component_type: Component type (map, connector, connection, process)

        Returns:
            Component ID

        Raises:
            ValueError: If component not found or multiple matches

        Examples:
            # From registry
            id = resolver._resolve_component_id("Transform Map", "map")

            # From API
            id = resolver._resolve_component_id("Salesforce Connector", "connector")
        """
        # Step 1: Check session registry
        if name in self.registry:
            registry_entry = self.registry[name]
            if registry_entry['type'] == component_type:
                return registry_entry['component_id']
            else:
                raise ValueError(
                    f"Component '{name}' found in registry but has type "
                    f"'{registry_entry['type']}', expected '{component_type}'"
                )

        # Step 2: Query API by name and type
        try:
            # Build query for component type
            expression = ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[component_type]
            )

            query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
            query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

            result = self.client.component_metadata.query_component_metadata(
                request_body=query_config
            )

            # Filter results by name (API doesn't support name filtering directly)
            if hasattr(result, 'result') and result.result:
                matches = [
                    comp for comp in result.result
                    if getattr(comp, 'name', '') == name
                    and str(getattr(comp, 'current_version', 'false')).lower() == 'true'
                    and str(getattr(comp, 'deleted', 'true')).lower() == 'false'
                ]

                # Step 3: Handle matches
                if len(matches) == 1:
                    component_id = getattr(matches[0], 'component_id', None)
                    if component_id:
                        return component_id
                    else:
                        raise ValueError(f"Component '{name}' found but has no component_id")

                elif len(matches) == 0:
                    raise ValueError(
                        f"{component_type.title()} '{name}' not found. "
                        f"Create it first or use exact component ID."
                    )

                else:  # Multiple matches
                    raise ValueError(
                        f"Multiple {component_type}s named '{name}' found. "
                        f"Use component ID instead: {[getattr(m, 'component_id', 'N/A') for m in matches]}"
                    )

            else:
                raise ValueError(
                    f"{component_type.title()} '{name}' not found in account. "
                    f"Create it first or check the name."
                )

        except Exception as e:
            if "not found" in str(e).lower() or "Multiple" in str(e):
                raise  # Re-raise our descriptive errors
            else:
                raise Exception(
                    f"Failed to query for {component_type} '{name}': {str(e)}"
                ) from e

    def _topological_sort(
        self,
        specs: List[ComponentSpec]
    ) -> List[ComponentSpec]:
        """
        Topologically sort component specs by dependencies.

        Uses Kahn's algorithm to ensure dependencies are created before
        components that depend on them.

        Args:
            specs: List of component specifications

        Returns:
            Sorted list with dependencies first

        Raises:
            ValueError: If circular dependency detected

        Example:
            specs = [
                ComponentSpec(name="B", dependencies=["A"]),
                ComponentSpec(name="A", dependencies=[])
            ]
            sorted_specs = self._topological_sort(specs)
            # Result: [A, B]
        """
        # Build dependency graph
        graph = {spec.name: spec.dependencies for spec in specs}
        spec_map = {spec.name: spec for spec in specs}

        # Calculate in-degree (number of incoming edges)
        in_degree = {name: 0 for name in graph}
        for deps in graph.values():
            for dep in deps:
                if dep in in_degree:
                    in_degree[dep] += 1

        # Find nodes with no incoming edges
        queue = deque([name for name in graph if in_degree[name] == 0])
        sorted_names = []

        # Process queue
        while queue:
            current = queue.popleft()
            sorted_names.append(current)

            # Reduce in-degree of neighbors
            for neighbor in graph.get(current, []):
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        # Check for circular dependencies
        if len(sorted_names) != len(graph):
            # Find components involved in cycle
            remaining = set(graph.keys()) - set(sorted_names)
            raise ValueError(
                f"Circular dependency detected among components: {remaining}. "
                f"Check dependencies and remove cycles."
            )

        # Return specs in sorted order
        return [spec_map[name] for name in sorted_names]
