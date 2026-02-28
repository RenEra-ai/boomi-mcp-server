"""
Connector component XML builders for Boomi.

Builds XML for connector-settings (connections) via the Component API.
HTTP connectors use <HttpSettings> with structured attributes and nested elements.

The SDK's create_component() cannot parse the XML response for connectors,
so creation uses raw Serializer POST (see connectors.py _create_component_raw).
"""

from typing import Dict, Any, Optional


def _escape_xml(text: str) -> str:
    """Escape special XML characters in attribute values."""
    if not text:
        return ""
    for char, escaped in [('&', '&amp;'), ('<', '&lt;'), ('>', '&gt;'),
                          ('"', '&quot;'), ("'", '&apos;')]:
        text = text.replace(char, escaped)
    return text


class HttpConnectorBuilder:
    """Builder for HTTP/HTTPS connector-settings components.

    Generates <HttpSettings> XML matching the real Boomi UI export structure.
    Supports NONE, BASIC, and OAUTH2 authentication types.

    Config keys (all optional except url):
        url:                    Connection URL (required)
        auth_type:              NONE, BASIC, PASSWORD_DIGEST, CUSTOM, OAUTH, OAUTH2
        username:               Username for BASIC auth
        connect_timeout:        Connection timeout in ms (not in HttpSettings attrs)
        read_timeout:           Read timeout in ms (not in HttpSettings attrs)
        trust_all_certs:        Trust all SSL certificates (true/false)
        client_ssl_alias:       Client SSL certificate alias
        oauth2_grant_type:      OAuth2 grant type (e.g., client_credentials)
        oauth2_client_id:       OAuth2 client ID
        oauth2_client_secret:   OAuth2 client secret
        oauth2_scope:           OAuth2 scope
        oauth2_token_url:       OAuth2 access token endpoint URL
        oauth2_auth_url:        OAuth2 authorization endpoint URL
    """

    # Attributes on <HttpSettings> element
    HTTP_SETTINGS_ATTRS = {
        'url': 'url',
        'auth_type': 'authenticationType',
    }

    # Attributes on <AuthSettings> element
    AUTH_SETTINGS_ATTRS = {
        'username': 'user',
    }

    # Attributes on <SSLOptions> element
    SSL_OPTIONS_ATTRS = {
        'trust_all_certs': 'trustServerCert',
        'client_ssl_alias': 'clientauth',
    }

    def build(self, **params) -> str:
        """Build complete component XML for an HTTP connector-settings component."""
        component_name = params.get('component_name', '')
        if not component_name:
            raise ValueError("component_name is required")
        url = params.get('url', '')
        if not url:
            raise ValueError("url is required for HTTP connectors")

        folder_name = params.get('folder_name', 'Home')
        description = params.get('description', '')
        auth_type = params.get('auth_type', 'NONE')

        inner_xml = self._build_http_settings(**params)

        safe_name = _escape_xml(component_name)
        safe_folder = _escape_xml(folder_name)
        safe_desc = _escape_xml(description)

        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
            '               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
            f'               type="connector-settings" subType="http"\n'
            f'               name="{safe_name}"\n'
            f'               folderName="{safe_folder}">\n'
            f'    <bns:description>{safe_desc}</bns:description>\n'
            f'    <bns:object>\n{inner_xml}\n    </bns:object>\n'
            '</bns:Component>'
        )

    def _build_http_settings(self, **params) -> str:
        """Build <HttpSettings> inner XML."""
        url = _escape_xml(params.get('url', ''))
        auth_type = _escape_xml(params.get('auth_type', 'NONE'))
        username = _escape_xml(params.get('username', ''))
        trust_all = params.get('trust_all_certs', 'false')
        client_ssl = _escape_xml(params.get('client_ssl_alias', ''))

        # SSL options
        ssl_attrs = f'clientauth="{client_ssl or "false"}" trustServerCert="{trust_all}"'

        # Build auth-specific sections
        auth_sections = ''
        if auth_type == 'OAUTH2':
            auth_sections = self._build_oauth2_section(**params)

        return (
            f'        <HttpSettings authenticationType="{auth_type}" url="{url}">\n'
            f'            <AuthSettings user="{username}"/>\n'
            f'{auth_sections}'
            f'            <SSLOptions {ssl_attrs}/>\n'
            f'        </HttpSettings>'
        )

    def _build_oauth2_section(self, **params) -> str:
        """Build <OAuth2Settings> XML section."""
        grant_type = _escape_xml(params.get('oauth2_grant_type', 'client_credentials'))
        client_id = _escape_xml(params.get('oauth2_client_id', ''))
        client_secret = _escape_xml(params.get('oauth2_client_secret', ''))
        scope = _escape_xml(params.get('oauth2_scope', ''))
        token_url = _escape_xml(params.get('oauth2_token_url', ''))
        auth_url = _escape_xml(params.get('oauth2_auth_url', ''))

        # Boomi requires strict element ordering:
        # credentials, authorizationTokenEndpoint, authorizationParameters,
        # accessTokenEndpoint, accessTokenParameters, scope
        return (
            f'            <OAuth2Settings grantType="{grant_type}">\n'
            f'                <credentials clientId="{client_id}" clientSecret="{client_secret}"/>\n'
            f'                <authorizationTokenEndpoint url="{auth_url}">\n'
            f'                    <sslOptions/>\n'
            f'                </authorizationTokenEndpoint>\n'
            f'                <authorizationParameters/>\n'
            f'                <accessTokenEndpoint url="{token_url}">\n'
            f'                    <sslOptions/>\n'
            f'                </accessTokenEndpoint>\n'
            f'                <accessTokenParameters/>\n'
            f'                <scope>{scope}</scope>\n'
            f'            </OAuth2Settings>\n'
        )


# ============================================================================
# Smart-merge helpers for update
# ============================================================================

# Maps config key -> (element_name, attribute_name) for HttpSettings updates.
# element_name None means the HttpSettings root element itself.
HTTP_UPDATE_MAP = {
    'url':              (None, 'url'),
    'auth_type':        (None, 'authenticationType'),
    'username':         ('AuthSettings', 'user'),
    'trust_all_certs':  ('SSLOptions', 'trustServerCert'),
    'client_ssl_alias': ('SSLOptions', 'clientauth'),
}


def find_http_settings(obj_elem):
    """Find the <HttpSettings> element inside <bns:object>.

    Handles both namespaced and non-namespaced variants.
    Returns (element, tag_without_ns) or (None, None).
    """
    for child in obj_elem:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        if tag == 'HttpSettings':
            return child
    return None


def find_child_element(parent, tag_name: str):
    """Find a direct child element by tag name (namespace-agnostic)."""
    for child in parent:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        if tag == tag_name:
            return child
    return None


def update_http_settings_fields(http_settings_elem, config: Dict[str, Any]) -> bool:
    """Update fields on HttpSettings and its child elements.

    Returns True if any changes were made.
    """
    changed = False

    for config_key, (elem_name, attr_name) in HTTP_UPDATE_MAP.items():
        if config_key not in config:
            continue
        value = str(config[config_key])

        if elem_name is None:
            # Update attribute on HttpSettings itself
            http_settings_elem.set(attr_name, value)
            changed = True
        else:
            child = find_child_element(http_settings_elem, elem_name)
            if child is not None:
                child.set(attr_name, value)
                changed = True

    return changed


# ============================================================================
# Registry
# ============================================================================

CONNECTOR_BUILDERS: Dict[str, type] = {
    "http": HttpConnectorBuilder,
}


def get_connector_builder(connector_type: str) -> Optional['HttpConnectorBuilder']:
    """Get a connector builder instance for the given type, or None."""
    builder_class = CONNECTOR_BUILDERS.get(connector_type.lower())
    if builder_class:
        return builder_class()
    return None
