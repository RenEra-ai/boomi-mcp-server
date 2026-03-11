# Boomi MCP Server: Connector Component Architecture Analysis

**Date**: 2026-02-24  
**Scope**: Can existing architecture support `manage_connector` tool, or redesign needed?  
**Verdict**: **Existing architecture is excellent — extend, don't redesign**

---

## Executive Summary

The boomi-mcp-server (dev branch) already has a well-designed, layered architecture that was built with extensibility in mind. The trading partner implementation went through two phases — XML builders first, then JSON model migration — and **both paths are preserved**. The XML builder infrastructure is exactly what connector components need, since connectors (`connector-settings` and `connector-action`) have **no JSON API equivalent** — they are XML-only through the Component API.

**Recommendation**: Reuse the existing 4-layer architecture. Estimated effort: ~60% of what trading partners took, because the infrastructure layers already exist.

---

## Architecture Layers (What Exists Today)

```
Layer 4: MCP Tool Interface (server.py — @mcp.tool() registrations)
│
├── Domain-Specific Tools (simplified config → correct API calls)
│   ├── manage_trading_partner   → trading_partners.py (B2B/EDI CRUD)
│   ├── manage_process           → processes.py (process CRUD + YAML)
│   └── manage_connector         → connectors.py [TO BUILD]
│
├── Generic Component Tools (work on ANY component type via raw XML)
│   ├── query_components         → query_components.py (list, get, search, bulk_get)
│   ├── manage_component         → manage_component.py (create from XML, update, clone, delete)
│   └── analyze_component        → analyze_component.py (where_used, dependencies, compare_versions)
│
└── Shared Utilities
    └── _shared.py               → component_get_xml(), paginate_metadata(), soft_delete

Layer 3: Orchestration / Business Logic
├── orchestrator.py              → Multi-component workflows, dependency resolution
├── trading_partner_builders.py  → JSON model builder (SDK TradingPartnerComponent)
└── yaml_parser.py               → YAML → ComponentSpec conversion

Layer 2: XML Builders (Reusable)
├── base_builder.py              → BaseXMLBuilder, ComponentXMLWrapper, TradingPartnerBuilder
├── communication.py             → 7 protocol builders (AS2, FTP, SFTP, HTTP, Disk, MLLP, OFTP)
├── x12_builder.py               → X12TradingPartnerBuilder
├── process_builder.py           → ProcessBuilder (shapes, coordinates, connections)
└── coordinate_calculator.py     → Auto-positioning logic

Layer 1: SDK / API
├── boomi-python SDK             → ComponentService (XML CRUD), ConnectorService (catalog)
└── Boomi Platform API           → /Component (XML), /Connector/{type} (catalog)
```

### Generic vs Domain-Specific Tools: How They Fit Together

The 3 generic tools (`query_components`, `manage_component`, `analyze_component`) and the domain-specific tools (`manage_trading_partner`, `manage_process`, `manage_connector`) operate at **different abstraction levels** and are fully complementary:

| Abstraction | Tool | Analogy |
|-------------|------|---------|
| **Generic** | `query_components`, `manage_component`, `analyze_component` | SQL — works on any table, you write raw queries |
| **Domain-specific** | `manage_connector`, `manage_trading_partner`, `manage_process` | ORM — knows the domain, translates simple config → correct API calls |

**What generic tools CAN do for connectors** (already working):
- `query_components list` with `config='{"type": "connector-settings"}'` lists connections
- `query_components get` retrieves full connector XML
- `manage_component create` creates any component given raw XML
- `manage_component clone/delete` works on any component type
- `analyze_component where_used` finds processes referencing a connector

**What generic tools CANNOT do** (the manage_connector value proposition):
- **Connector catalog discovery** — `list_types`/`get_type` use `sdk.connector.get_connector()`, a completely different API
- **Builder-based creation from JSON config** — translating `{"url": "...", "auth_type": "OAUTH2"}` → valid connector XML
- **Smart config-merge updates** — changing just the URL in an HTTP connection without full XML manipulation

**Shared infrastructure** (connectors.py should import from `_shared.py`):
- `component_get_xml()` — XML retrieval with proper Accept header
- `paginate_metadata()` — ComponentMetadata query pagination (to be extracted from query_components.py)
- `soft_delete_component()` — soft-delete + metadata-delete fallback (to be extracted from manage_component.py)

---

## What Can Be Reused Directly

### ComponentXMLWrapper (Layer 2 — base_builder.py)

Already designed as a generic wrapper for ALL component types:

```python
class ComponentXMLWrapper:
    """Generic wrapper for Boomi Component XML structure.
    
    This wrapper is reusable across ALL component types:
    - Trading Partners (though they have JSON alternative)
    - Processes (XML only)
    - Connections (XML only)      ← EXACTLY THIS
    - Web Services (XML only)
    - Maps (XML only)
    """
    
    @staticmethod
    def wrap(name, component_type, folder_name, inner_xml, description=""):
        # Returns: <bns:Component type="..." name="..." folderName="...">
        #            <bns:object>{inner_xml}</bns:object>
        #          </bns:Component>
```

**No changes needed.** Pass `component_type="connector"` for connections.

### BaseXMLBuilder (Layer 2 — base_builder.py)

Abstract base with `build()`, `validate()`, `_escape_xml()`. Connector builders would extend this directly — same pattern as `TradingPartnerBuilder` and `CommunicationProtocolBuilder`.

### Communication Protocol Builders (Layer 2 — communication.py)

The 7 protocol builders (AS2, FTP, SFTP, HTTP, Disk, MLLP, OFTP) generate `<CommunicationOption>` XML for trading partners. While connector-settings uses a different XML structure than trading partner communication, the **builder pattern** (registry + factory + strategy) is directly reusable.

### ComponentOrchestrator (Layer 3 — orchestrator.py)

Already has placeholder logic for connections:

```python
elif spec.type == 'connection':
    raise NotImplementedError("Connection builder not implemented yet")
```

And reference resolution:

```python
if 'connection_ref' in shape.config:
    connection_name = shape.config['connection_ref']
    connection_id = self._resolve_component_id(connection_name, 'connection')
```

**The orchestrator already anticipates connector components.** Just need to wire in the builder.

### SDK Component Service (Layer 1 — boomi-python)

```python
# Create any component type via XML
sdk.component.create_component(request_body=xml_string)

# Get existing component XML (for templates)
sdk.component.get_component_raw(component_id)

# Update component
sdk.component.update_component_raw(component_id, xml_string)
```

Plus the **Connector catalog API** for discovery:

```python
# List available connector types
sdk.connector.get_connector(connector_type="http")
sdk.connector.query_connector(request_body=query_config)
```

### Categories Package Structure

```python
# categories/components/__init__.py (on main branch) now includes:
"""
Component management category for Boomi MCP Server.
- Trading Partners (B2B/EDI)
- Processes
- Component Query (list, get, search, bulk_get)
- Component Management (create, update, clone, delete)
- Component Analysis (where_used, dependencies, compare_versions)
- Connections (future)    ← manage_connector goes here
"""
```

### _shared.py Utilities (main branch)

The `_shared.py` module provides shared helpers that connector implementation should import directly:

```python
# _shared.py — already implemented:
component_get_xml(boomi_client, component_id)   # GET as raw XML + parsed metadata
set_description_element(root, text)              # Set description child element
parse_component_xml(raw_xml, fallback_id)        # Parse XML string → metadata dict

# _shared.py — to be extracted (see generic-tools-issues-task.md):
paginate_metadata(boomi_client, query_config)    # ComponentMetadata pagination
soft_delete_component(boomi_client, component_id) # Soft-delete + fallback
```

---

## What Needs to Be Built

### New Layer 2: Connector XML Builders

```
src/boomi_mcp/categories/components/builders/
├── base_builder.py              (existing — reuse as-is)
├── communication.py             (existing — reuse pattern)
├── x12_builder.py               (existing — trading partner)
├── connector_builder.py         (NEW — base connector builder)
├── http_connector_builder.py    (NEW — HTTP/HTTPS connection + operations)
├── database_connector_builder.py (NEW — DB connection + operations)
├── ftp_connector_builder.py     (NEW — FTP/SFTP connection + operations)
└── ...                          (one per connector type)
```

**Pattern** (mirrors X12TradingPartnerBuilder exactly):

```python
class ConnectorBuilder(BaseXMLBuilder):
    """Base class for connector component builders."""
    
    @abstractmethod
    def get_connector_type(self) -> str:
        """Return connector subtype (e.g., 'http', 'database', 'ftp')"""
    
    @abstractmethod
    def build_connection(self, **params) -> str:
        """Build connector-settings (connection) XML"""
    
    @abstractmethod
    def build_operation(self, operation_type: str, **params) -> str:
        """Build connector-action (operation) XML"""


class HTTPConnectorBuilder(ConnectorBuilder):
    def get_connector_type(self) -> str:
        return "http"
    
    def build_connection(self, url, auth_type="NONE", **params) -> str:
        inner_xml = f'''<Overrides>
            <Overrideable>
                <OverrideValues>
                    <httpconnectionfield url="{url}" 
                        authType="{auth_type}" ... />
                </OverrideValues>
            </Overrideable>
        </Overrides>'''
        
        return ComponentXMLWrapper.wrap(
            name=params.get("name", "HTTP Connection"),
            component_type="connector",
            folder_name=params.get("folder_name", "Home"),
            inner_xml=inner_xml
        )
```

**Key approach**: The XML structure for connector-settings varies per connector type:

1. **Template from existing**: Use `sdk.component.get_component_raw(existing_connector_id)` to capture XML templates from connectors created in the Boomi UI
2. **Parameterize**: Replace values with builder parameters  
3. **Register**: Add to `CONNECTOR_BUILDERS` registry (same pattern as `STANDARD_BUILDERS` and `PROTOCOL_BUILDERS`)

### New Layer 4: MCP Tool — connectors.py

```
src/boomi_mcp/categories/components/
├── trading_partners.py          (existing — 3107 lines, full CRUD)
├── processes.py                 (existing — 565 lines, full CRUD)
├── organizations.py             (existing)
├── query_components.py          (existing — generic component discovery)
├── manage_component.py          (existing — generic component CRUD)
├── analyze_component.py         (existing — dependency/version analysis)
├── _shared.py                   (existing — shared XML utilities)
├── connectors.py                (NEW — manage_connector_action)
└── __init__.py                  (update exports)
```

Tool interface mirrors `manage_trading_partner`, importing shared utilities:

```python
from ._shared import component_get_xml, paginate_metadata

def manage_connector_action(boomi_client, profile, action, **params):
    """
    Actions: list, get, create, update, delete, list_types, get_type
    
    list_types — uses ConnectorService to browse available connector types
    get_type   — uses ConnectorService for field definitions
    list       — uses paginate_metadata() from _shared (same as query_components)
    get        — uses component_get_xml() from _shared (same as query_components)
    create     — uses ConnectorBuilder → ComponentService.create_component(xml)
    update     — get XML → merge config → ComponentService.update_component_raw(id, xml)
    delete     — uses soft_delete_component() from _shared (same as manage_component)
    """
```

### Server Registration

```python
# server.py addition (same pattern as trading partners)
from boomi_mcp.categories.components.connectors import manage_connector_action

@mcp.tool()
def manage_connector(profile, action, component_id=None, config=None):
    ...
    return manage_connector_action(sdk, profile, action, **params)
```

---

## Architecture Comparison: Trading Partners vs Connectors

| Aspect | Trading Partners | Connectors |
|--------|-----------------|------------|
| **Boomi API** | Dedicated JSON API (`TradingPartnerComponent`) | Generic XML API (`Component`) only |
| **SDK Support** | `sdk.trading_partner_component.*` (typed models) | `sdk.component.*` (raw XML) + `sdk.connector.*` (catalog) |
| **Creation approach** | Started XML, migrated to JSON models | XML only (no JSON path exists) |
| **XML builders preserved?** | Yes — `x12_builder.py`, `communication.py` | These become the primary path |
| **Config complexity** | 7 standards x 7 protocols = high | Per-connector-type (HTTP, DB, FTP, etc.) |
| **Template source** | Manually crafted from Boomi UI exports | Same approach: export from UI, parameterize |

**Key insight**: Trading partners had the luxury of migrating to JSON because Boomi provided `TradingPartnerComponentService`. Connectors have **no such luxury** — the XML builder path that was "preserved as reference" in the trading partner implementation **becomes the production path** for connectors.

---

## The XML Builder Pattern Preserved in Trading Partners

The dev branch preserves the original XML approach in two places:

### 1. `categories/components/builders/` (Layer 2)

- `base_builder.py` — `ComponentXMLWrapper.wrap()` handles the `<bns:Component>` envelope
- `x12_builder.py` — Complete X12 trading partner via XML (f-string templates)
- `communication.py` — 7 protocol builders, each returns `<CommunicationOption>` XML

### 2. `xml_builders/` (Layer 2, process-specific)

- `templates/shapes/` — Hardcoded XML shape templates (Start, Map, Return, etc.)
- `builders/process_builder.py` — Assembles shapes into process XML
- `builders/orchestrator.py` — Multi-component workflow with dependency resolution

Both of these map directly to what connectors need. The `categories/components/builders/` path is the right home for connector builders since they are component builders (same level as trading partners), not process shapes.

---

## Recommended Implementation Order

### Phase 1: Foundation + HTTP Connector (MVP)

1. Create `connector_builder.py` (base class extending `BaseXMLBuilder`)
2. Create `http_connector_builder.py` — HTTP is the most common connector
3. Create `connectors.py` tool with actions: `list_types`, `list`, `get`, `create`
4. Register in `server.py`
5. Use `sdk.connector.get_connector("http")` for catalog/discovery
6. Capture real HTTP connector XML via `sdk.component.get_component_raw()` for template

### Phase 2: Database + FTP Connectors

7. `database_connector_builder.py`
8. `ftp_connector_builder.py` / `sftp_connector_builder.py`
9. Add `update` and `delete` actions

### Phase 3: Operations (connector-action)

10. Extend builders with `build_operation()` method
11. Support GET/SEND/QUERY/UPSERT operation types per connector
12. Wire operations into the orchestrator for process creation flows

### Phase 4: Orchestrator Integration

13. Remove `raise NotImplementedError("Connection builder not implemented yet")` from orchestrator
14. Enable `connection_ref` resolution in process shapes
15. Full workflow: create connection → create operation → create process referencing both

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Connector XML structure varies wildly per type | Medium | Start with HTTP (well-known), capture real XML templates |
| No JSON API means harder testing | Low | `get_component_raw()` provides round-trip verification |
| Many connector types to support | Low | Registry pattern makes adding new types ~1 file each |
| Encrypted fields (passwords) in XML | Medium | SDK handles this; omitting encrypted field preserves existing value |
| Unknown connector subtypes in account | Low | `sdk.connector.query_connector()` provides catalog discovery |

---

## Prerequisites

**Merge main → dev BEFORE starting connector work.** The `_shared.py` module and the 3 generic component tools only exist on `main`. See `generic-tools-issues-task.md` for the merge procedure and expected conflicts.

Also fix Issue 1 (component type names in query_components docstring) before or during the merge, since the connector tool will depend on correct type names for ComponentMetadata queries.

---

## Files to Create/Modify

### New Files
```
src/boomi_mcp/categories/components/connectors.py                      (~500-800 lines)
src/boomi_mcp/categories/components/builders/connector_builder.py      (~100 lines)
src/boomi_mcp/categories/components/builders/http_connector_builder.py (~200 lines)
```

### Modified Files
```
src/boomi_mcp/categories/components/_shared.py              (add paginate_metadata, soft_delete_component)
src/boomi_mcp/categories/components/__init__.py             (add connector exports)
src/boomi_mcp/categories/components/builders/__init__.py    (add CONNECTOR_BUILDERS registry)
src/boomi_mcp/categories/components/query_components.py     (import from _shared instead of local functions)
src/boomi_mcp/categories/components/manage_component.py     (import soft_delete from _shared, update hint)
src/boomi_mcp/xml_builders/builders/orchestrator.py         (wire connection builder)
server.py                                                    (register manage_connector tool)
```

### No Changes Needed
```
base_builder.py          — ComponentXMLWrapper works as-is
communication.py         — Pattern reused, code untouched
process_builder.py       — Untouched
yaml_parser.py           — May add connector YAML support later
trading_partners.py      — Untouched
analyze_component.py     — Already works for connectors (where_used, dependencies)
```

---

## Conclusion

The existing architecture is **well-designed for this extension**. The original decision to preserve the XML builder infrastructure alongside the JSON migration was prescient — it is exactly what is needed for connectors. The 4-layer separation (SDK → XML Builders → Business Logic → MCP Tool) maps cleanly to connector requirements.

The 3 generic component tools (`query_components`, `manage_component`, `analyze_component`) on the `main` branch provide shared infrastructure that the connector tool should build upon — specifically `_shared.py` for XML retrieval, metadata pagination, and soft-delete. These generic tools handle any-component-type operations; `manage_connector` adds the domain-specific intelligence (connector catalog discovery, builder-based creation from JSON config, smart config-merge updates).

**No redesign needed. Extend the existing patterns. Merge main → dev first.**

The preserved XML trading partner builders serve as both a working reference implementation and a code template. The `ComponentXMLWrapper`, `BaseXMLBuilder` base class, builder registry pattern, orchestrator placeholder code, and `_shared.py` utilities all confirm this was anticipated in the original architecture.

Next step: Merge main → dev, fix component type names (Issue 1), then capture a real HTTP connector-settings XML from the account via `component_get_xml()` to use as the template for `HTTPConnectorBuilder`.
