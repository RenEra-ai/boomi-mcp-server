"""
Base builder classes for Boomi component XML generation.

This module provides abstract base classes and utilities for building
XML representations of Boomi components.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseXMLBuilder(ABC):
    """
    Abstract base class for all XML builders.

    Provides common validation and building interface that all
    concrete builders must implement.
    """

    @abstractmethod
    def build(self, **params) -> str:
        """
        Build and return XML string.

        Args:
            **params: Builder-specific parameters

        Returns:
            Valid XML string

        Raises:
            ValueError: If validation fails
        """
        pass

    def validate(self, **params) -> None:
        """
        Validate parameters before building.

        Override in subclasses to add specific validation logic.

        Args:
            **params: Parameters to validate

        Raises:
            ValueError: If validation fails
        """
        pass

    def _escape_xml(self, text: str) -> str:
        """
        Escape special XML characters.

        Args:
            text: Text to escape

        Returns:
            XML-safe text
        """
        if not text:
            return ""

        replacements = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&apos;'
        }

        for char, escaped in replacements.items():
            text = text.replace(char, escaped)

        return text


class ComponentXMLWrapper:
    """
    Generic wrapper for Boomi Component XML structure.

    This wrapper is reusable across ALL component types:
    - Trading Partners (though they have JSON alternative)
    - Processes (XML only)
    - Connections (XML only)
    - Web Services (XML only)
    - Maps (XML only)
    """

    @staticmethod
    def wrap(
        name: str,
        component_type: str,
        folder_name: str,
        inner_xml: str,
        description: str = "",
        sub_type: str = ""
    ) -> str:
        """
        Wrap component-specific XML in standard Component envelope.

        Args:
            name: Component name
            component_type: Type (e.g., "process", "tradingpartner", "connector-settings")
            folder_name: Folder path (e.g., "Home", "Integrations/Production")
            inner_xml: Component-specific XML content
            description: Optional description
            sub_type: Optional subType (e.g., "http" for connectors)

        Returns:
            Complete component XML wrapped in bns:Component element

        Example:
            >>> inner = "<Process>...</Process>"
            >>> xml = ComponentXMLWrapper.wrap(
            ...     "MyProcess", "process", "Home", inner
            ... )
        """
        # Escape text content
        safe_name = ComponentXMLWrapper._escape_text(name)
        safe_desc = ComponentXMLWrapper._escape_text(description)
        safe_folder = ComponentXMLWrapper._escape_text(folder_name)

        sub_type_attr = ""
        if sub_type:
            sub_type_attr = f' subType="{ComponentXMLWrapper._escape_text(sub_type)}"'

        return f'''<?xml version="1.0" encoding="UTF-8"?>
<bns:Component xmlns:bns="http://api.platform.boomi.com/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               type="{component_type}"{sub_type_attr}
               name="{safe_name}"
               folderName="{safe_folder}">
    <bns:description>{safe_desc}</bns:description>
    <bns:object>
        {inner_xml}
    </bns:object>
</bns:Component>'''

    @staticmethod
    def _escape_text(text: str) -> str:
        """Escape XML special characters in text content."""
        if not text:
            return ""

        replacements = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&apos;'
        }

        for char, escaped in replacements.items():
            text = text.replace(char, escaped)

        return text


class TradingPartnerBuilder(BaseXMLBuilder):
    """
    Abstract base class for trading partner component builders.

    Each EDI standard (X12, EDIFACT, HL7, etc.) should extend this
    class and implement the build() method with standard-specific logic.
    """

    @abstractmethod
    def get_standard_name(self) -> str:
        """
        Return the standard name (e.g., 'x12', 'edifact', 'hl7').

        Returns:
            Standard name in lowercase
        """
        pass

    def build_contact_info_xml(
        self,
        contact_name: str = "",
        contact_email: str = "",
        contact_phone: str = "",
        contact_fax: str = "",
        contact_address: str = "",
        contact_address2: str = "",
        contact_city: str = "",
        contact_state: str = "",
        contact_country: str = "",
        contact_postalcode: str = ""
    ) -> str:
        """
        Build ContactInfo XML section (common across all standards).

        Args:
            contact_*: Contact information fields

        Returns:
            ContactInfo XML string
        """
        if not any([contact_name, contact_email, contact_phone, contact_fax,
                   contact_address, contact_city, contact_state, contact_country]):
            return "<ContactInfo />"

        return f'''<ContactInfo>
            <name>{self._escape_xml(contact_name)}</name>
            <emailAddress>{self._escape_xml(contact_email)}</emailAddress>
            <phoneNumber>{self._escape_xml(contact_phone)}</phoneNumber>
            <faxNumber>{self._escape_xml(contact_fax)}</faxNumber>
            <address>{self._escape_xml(contact_address)}</address>
            <address2>{self._escape_xml(contact_address2)}</address2>
            <city>{self._escape_xml(contact_city)}</city>
            <state>{self._escape_xml(contact_state)}</state>
            <country>{self._escape_xml(contact_country)}</country>
            <postalCode>{self._escape_xml(contact_postalcode)}</postalCode>
          </ContactInfo>'''
