"""
Boomi Component XML Builders.

This package provides modular, reusable XML builders for creating
Boomi components via the Component API.

Modules:
    base_builder: Abstract base classes and component wrapper
    communication: Communication protocol builders (AS2, FTP, HTTP, etc.)
    x12_builder: X12 trading partner builder
    edifact_builder: EDIFACT trading partner builder
    hl7_builder: HL7 trading partner builder
    ... (additional standard builders)

Usage:
    from boomi_mcp.categories.components.builders import ComponentXMLWrapper
    from boomi_mcp.categories.components.builders.communication import build_communication_xml

    # Build communication XML
    comm_xml = build_communication_xml(['ftp', 'http'])

    # Wrap in component structure
    component_xml = ComponentXMLWrapper.wrap(
        name="MyComponent",
        component_type="tradingpartner",
        folder_name="Home",
        inner_xml=trading_partner_xml
    )
"""

from .base_builder import (
    BaseXMLBuilder,
    ComponentXMLWrapper,
    TradingPartnerBuilder
)

from .communication import (
    CommunicationProtocolBuilder,
    AS2ProtocolBuilder,
    DiskProtocolBuilder,
    FTPProtocolBuilder,
    HTTPProtocolBuilder,
    MLLPProtocolBuilder,
    OFTPProtocolBuilder,
    SFTPProtocolBuilder,
    build_communication_xml,
    get_supported_protocols,
    PROTOCOL_BUILDERS
)

from .x12_builder import X12TradingPartnerBuilder

from .connector_builder import (
    HttpConnectorBuilder,
    CONNECTOR_BUILDERS,
    get_connector_builder,
)

# Registry of standard-specific builders
# Maps standard name -> builder class
STANDARD_BUILDERS = {
    "x12": X12TradingPartnerBuilder,
    # Additional standards will be added here:
    # "edifact": EDIFACTTradingPartnerBuilder,
    # "hl7": HL7TradingPartnerBuilder,
    # "rosettanet": RosettaNetTradingPartnerBuilder,
    # "custom": CustomTradingPartnerBuilder,
    # "tradacoms": TradacomsTradingPartnerBuilder,
    # "odette": OdetteTradingPartnerBuilder,
}


def get_builder_for_standard(standard: str) -> type:
    """
    Get the appropriate builder class for a given EDI standard.

    Args:
        standard: Standard name (e.g., 'x12', 'edifact', 'hl7')

    Returns:
        Builder class for the standard

    Raises:
        ValueError: If standard is not supported
    """
    builder_class = STANDARD_BUILDERS.get(standard.lower())
    if not builder_class:
        supported = ", ".join(STANDARD_BUILDERS.keys())
        raise ValueError(
            f"Unsupported standard: {standard}. "
            f"Supported standards: {supported}"
        )
    return builder_class


__all__ = [
    # Base classes
    "BaseXMLBuilder",
    "ComponentXMLWrapper",
    "TradingPartnerBuilder",

    # Communication builders
    "CommunicationProtocolBuilder",
    "AS2ProtocolBuilder",
    "DiskProtocolBuilder",
    "FTPProtocolBuilder",
    "HTTPProtocolBuilder",
    "MLLPProtocolBuilder",
    "OFTPProtocolBuilder",
    "SFTPProtocolBuilder",
    "build_communication_xml",
    "get_supported_protocols",
    "PROTOCOL_BUILDERS",

    # Standard-specific builders
    "X12TradingPartnerBuilder",
    "STANDARD_BUILDERS",
    "get_builder_for_standard",

    # Connector builders
    "HttpConnectorBuilder",
    "CONNECTOR_BUILDERS",
    "get_connector_builder",
]
