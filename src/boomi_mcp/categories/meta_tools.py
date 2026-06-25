"""
Meta tools — schema templates and generic API invoker.

- get_schema_template_action: self-documenting reference data (no API calls)
- invoke_api: generic escape-hatch for any Boomi REST API endpoint
"""

from typing import Dict, Any, Optional

from boomi import Boomi
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment

from ..errors import (
    INVALID_INPUT,
    PATTERN_NOT_FOUND,
    RAW_WRITE_CONFIRMATION_REQUIRED,
    SCHEMA_LOOKUP_FAILED,
    SCHEMA_NAME_UNSUPPORTED,
    SCHEMA_SELECTOR_REQUIRED,
    WORKFLOW_SEQUENCE_NOT_FOUND,
)
from ..models.integration_models import IntegrationSpecV1
from ..kb.design_doctrine import (
    get_design_doctrine_catalog,
    get_design_pattern,
    list_design_doctrine_index,
    valid_design_pattern_names,
)
from ..kb.account_governance import (
    get_account_governance_catalog,
    get_governance_pattern,
    list_account_governance_index,
    valid_governance_pattern_names,
)


# ============================================================================
# Contact Fields (shared across trading partners and organizations)
# ============================================================================

_CONTACT_FIELDS = {
    "contact_name": "John Doe",
    "contact_email": "john@acme.com",
    "contact_phone": "555-1234",
    "contact_fax": "",
    "contact_address": "123 Main St",
    "contact_address2": "",
    "contact_city": "New York",
    "contact_state": "NY",
    "contact_country": "USA",
    "contact_postalcode": "10001",
}


# ============================================================================
# Trading Partner Templates
# ============================================================================

_TP_OVERVIEW = {
    "resource_type": "trading_partner",
    "tool": "manage_trading_partner",
    "available_actions": [
        "list", "get", "create", "update", "delete",
        "analyze_usage", "list_options",
        "org_list", "org_get", "org_create", "org_update", "org_delete",
    ],
    "standards": ["x12", "edifact", "hl7", "rosettanet", "tradacoms", "odette", "custom"],
    "classifications": ["tradingpartner", "mycompany"],
    "communication_protocols": ["http", "as2", "ftp", "sftp", "disk", "mllp", "oftp"],
    "hint": "Use operation='create' with standard='x12' for a full create template. "
            "Use protocol='http' to see HTTP-specific fields.",
}

_TP_CREATE = {
    "x12": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "x12",
        "template": {
            "component_name": "Acme Corp (REQUIRED)",
            "standard": "x12",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "isa_id": "ACME (REQUIRED for x12)",
            "isa_qualifier": "ZZ (default)",
            "isa_auth_qualifier": "00 (default, optional)",
            "isa_sec_qualifier": "00 (default, optional)",
            "gs_id": "ACMECORP",
            "organization_id": "(optional) link to existing organization",
            "communication_protocols": ["http", "as2"],
            **_CONTACT_FIELDS,
        },
        "enums": {
            "classification": ["tradingpartner", "mycompany"],
            "isa_qualifier": [
                "01", "02", "03", "04", "07", "08", "09",
                "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
                "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
                "30", "31", "32", "33", "34", "35", "36", "37", "38",
                "AM", "NR", "SN", "ZZ",
            ],
        },
        "required_fields": ["component_name", "standard", "isa_id"],
        "protocol_fields": "Use protocol='http' or protocol='as2' etc. to see protocol-specific fields",
    },
    "edifact": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "edifact",
        "template": {
            "component_name": "REQUIRED",
            "standard": "edifact",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "edifact_interchange_id": "REQUIRED",
            "edifact_interchange_id_qual": "",
            "edifact_syntax_id": "UNOA",
            "edifact_syntax_version": "3",
            "edifact_test_indicator": "NA (production) | 1 (test)",
            "communication_protocols": [],
            **_CONTACT_FIELDS,
        },
        "enums": {
            "edifact_syntax_id": ["UNOA", "UNOB", "UNOC", "UNOD", "UNOE", "UNOF"],
            "edifact_syntax_version": ["1", "2", "3"],
            "edifact_test_indicator": ["1", "NA"],
        },
        "required_fields": ["component_name", "standard", "edifact_interchange_id"],
    },
    "hl7": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "hl7",
        "template": {
            "component_name": "REQUIRED",
            "standard": "hl7",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "hl7_application": "REQUIRED",
            "hl7_facility": "REQUIRED",
            "communication_protocols": ["mllp"],
            **_CONTACT_FIELDS,
        },
        "required_fields": ["component_name", "standard", "hl7_application", "hl7_facility"],
    },
    "rosettanet": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "rosettanet",
        "template": {
            "component_name": "REQUIRED",
            "standard": "rosettanet",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "rosettanet_partner_id": "REQUIRED",
            "rosettanet_partner_location": "",
            "rosettanet_global_usage_code": "production | test",
            "rosettanet_supply_chain_code": "",
            "rosettanet_classification_code": "",
            "communication_protocols": ["http"],
            **_CONTACT_FIELDS,
        },
        "enums": {
            "rosettanet_global_usage_code": ["production", "test"],
        },
        "required_fields": ["component_name", "standard", "rosettanet_partner_id"],
    },
    "tradacoms": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "tradacoms",
        "template": {
            "component_name": "REQUIRED",
            "standard": "tradacoms",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "tradacoms_interchange_id": "REQUIRED",
            "tradacoms_interchange_id_qualifier": "",
            "communication_protocols": [],
            **_CONTACT_FIELDS,
        },
        "required_fields": ["component_name", "standard", "tradacoms_interchange_id"],
    },
    "odette": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "odette",
        "template": {
            "component_name": "REQUIRED",
            "standard": "odette",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "odette_interchange_id": "REQUIRED",
            "odette_interchange_id_qual": "",
            "odette_syntax_id": "UNOA",
            "odette_syntax_version": "3",
            "odette_test_indicator": "NA (production) | 1 (test)",
            "communication_protocols": ["oftp"],
            **_CONTACT_FIELDS,
        },
        "enums": {
            "odette_syntax_id": ["UNOA", "UNOB", "UNOC", "UNOD", "UNOE", "UNOF"],
            "odette_syntax_version": ["1", "2", "3"],
            "odette_test_indicator": ["1", "NA"],
        },
        "required_fields": ["component_name", "standard", "odette_interchange_id"],
    },
    "custom": {
        "resource_type": "trading_partner",
        "operation": "create",
        "standard": "custom",
        "template": {
            "component_name": "REQUIRED",
            "standard": "custom",
            "classification": "tradingpartner | mycompany",
            "folder_name": "Home",
            "communication_protocols": [],
            **_CONTACT_FIELDS,
        },
        "required_fields": ["component_name", "standard"],
    },
}

_TP_PROTOCOLS = {
    "http": {
        "resource_type": "trading_partner",
        "protocol": "http",
        "template": {
            "http_url": "https://api.example.com/edi (REQUIRED)",
            "http_authentication_type": "NONE | BASIC | PASSWORD_DIGEST | CUSTOM | OAUTH | OAUTH2",
            "http_username": "(for BASIC/PASSWORD_DIGEST auth)",
            "http_password": "(for BASIC/PASSWORD_DIGEST auth)",
            "http_data_content_type": "application/json",
            "http_connect_timeout": "60000 (ms)",
            "http_read_timeout": "60000 (ms)",
            "http_method_type": "POST",
            "http_follow_redirects": "true | false",
            "http_return_errors": "true | false",
            "http_return_responses": "true | false",
            "http_cookie_scope": "IGNORED | GLOBAL | CONNECTOR_SHAPE",
            "http_client_auth": "true | false (enable client SSL)",
            "http_trust_server_cert": "true | false",
            "http_client_ssl_alias": "(certificate component ID)",
            "http_trusted_cert_alias": "(certificate component ID)",
            "http_request_profile_type": "NONE | XML | JSON",
            "http_request_profile": "(profile component ID)",
            "http_response_profile_type": "NONE | XML | JSON",
            "http_response_profile": "(profile component ID)",
            "http_use_custom_auth": "true | false",
            "http_use_basic_auth": "true | false",
            "http_use_default_settings": "true | false",
        },
        "oauth2_fields": {
            "http_oauth_token_url": "https://auth.example.com/token",
            "http_oauth_client_id": "",
            "http_oauth_client_secret": "",
            "http_oauth_scope": "",
            "http_oauth_grant_type": "client_credentials | password | code",
            "http_oauth2_authorization_token_url": "",
            "http_oauth2_access_token": "",
            "http_oauth2_use_refresh_token": "true | false",
            "http_oauth2_access_token_params": '(JSON string)',
            "http_oauth2_authorization_params": '(JSON string)',
        },
        "oauth1_fields": {
            "http_oauth1_consumer_key": "",
            "http_oauth1_consumer_secret": "",
            "http_oauth1_access_token": "",
            "http_oauth1_token_secret": "",
            "http_oauth1_realm": "",
            "http_oauth1_signature_method": "SHA1 | SHA256",
            "http_oauth1_request_token_url": "",
            "http_oauth1_access_token_url": "",
            "http_oauth1_authorization_url": "",
            "http_oauth1_suppress_blank_access_token": "true | false",
        },
        "get_specific_fields": {
            "http_get_method_type": "GET | POST | PUT | DELETE",
            "http_get_content_type": "",
            "http_get_follow_redirects": "true | false",
            "http_get_return_errors": "true | false",
            "http_get_request_profile": "(profile component ID)",
            "http_get_request_profile_type": "NONE | XML | JSON",
            "http_get_response_profile": "(profile component ID)",
            "http_get_response_profile_type": "NONE | XML | JSON",
            "http_get_request_headers": '(JSON array)',
        },
        "listen_fields": {
            "http_listen_mime_passthrough": "true | false",
            "http_listen_object_name": "",
            "http_listen_operation_type": "",
            "http_listen_username": "",
            "http_listen_password": "",
            "http_listen_use_default": "true | false",
        },
        "header_fields": {
            "http_request_headers": '[{"headerName": "X-Custom", "headerValue": "value"}]',
            "http_response_header_mapping": '[{"headerFieldName": "X-Response", "targetPropertyName": "prop"}]',
            "http_reflect_headers": '[{"name": "X-Reflect"}]',
            "http_path_elements": '[{"name": "resource"}]',
        },
        "enums": {
            "http_authentication_type": ["NONE", "BASIC", "PASSWORD_DIGEST", "CUSTOM", "OAUTH", "OAUTH2"],
            "http_cookie_scope": ["IGNORED", "GLOBAL", "CONNECTOR_SHAPE"],
            "http_method_type": ["GET", "POST", "PUT", "DELETE"],
            "http_request_profile_type": ["NONE", "XML", "JSON"],
            "http_response_profile_type": ["NONE", "XML", "JSON"],
            "http_oauth_grant_type": ["client_credentials", "password", "code"],
            "http_oauth1_signature_method": ["SHA1", "SHA256"],
        },
        "aliases": {
            "http_content_type": "http_data_content_type",
            "http_connection_timeout": "http_connect_timeout",
            "http_send_method": "http_method_type",
            "http_ssl_cert_id": "http_client_ssl_alias",
        },
    },
    "as2": {
        "resource_type": "trading_partner",
        "protocol": "as2",
        "template": {
            "as2_url": "https://as2.example.com (REQUIRED for tradingpartner)",
            "as2_partner_id": "AS2 identity (AS2-From for tradingpartner, AS2-To for mycompany)",
            "as2_authentication_type": "NONE | BASIC",
            "as2_username": "(for BASIC auth)",
            "as2_password": "(for BASIC auth)",
            "as2_verify_hostname": "true | false",
            "as2_signed": "true | false",
            "as2_encrypted": "true | false",
            "as2_compressed": "true | false",
            "as2_signing_digest_alg": "SHA1 | SHA256 | SHA384 | SHA512",
            "as2_encryption_algorithm": "tripledes | rc2-40 | rc2-64 | rc2-128 | aes128 | aes192 | aes256",
            "as2_data_content_type": "application/edi-x12 | application/edifact | text/plain | text/xml | application/xml | application/octet-stream",
            "as2_subject": "AS2 message subject",
            "as2_sign_alias": "(signing certificate component ID)",
            "as2_encrypt_alias": "(encryption certificate component ID)",
            "as2_client_ssl_alias": "(client SSL certificate component ID)",
            "as2_multiple_attachments": "true | false",
            "as2_max_document_count": "(integer)",
            "as2_attachment_option": "BATCH | DOCUMENT_CACHE",
            "as2_attachment_cache": "(document cache component ID)",
        },
        "mdn_fields": {
            "as2_request_mdn": "true | false",
            "as2_mdn_signed": "true | false",
            "as2_mdn_digest_alg": "SHA1 | SHA256 | SHA384 | SHA512",
            "as2_synchronous_mdn": "true | false (default: true)",
            "as2_mdn_external_url": "(URL for async MDN delivery)",
            "as2_mdn_use_external_url": "true | false",
            "as2_mdn_use_ssl": "true | false",
            "as2_mdn_client_ssl_cert": "(certificate component ID)",
            "as2_mdn_ssl_cert": "(certificate component ID)",
            "as2_mdn_alias": "(MDN signature certificate component ID)",
        },
        "partner_info_fields": {
            "as2_reject_duplicates": "true | false",
            "as2_duplicate_check_count": "(integer)",
            "as2_legacy_smime": "true | false",
        },
        "enums": {
            "as2_authentication_type": ["NONE", "BASIC"],
            "as2_signing_digest_alg": ["SHA1", "SHA256", "SHA384", "SHA512"],
            "as2_encryption_algorithm": ["tripledes", "rc2-40", "rc2-64", "rc2-128", "aes128", "aes192", "aes256"],
            "as2_data_content_type": ["text/plain", "text/xml", "application/xml", "application/edi-x12", "application/edifact", "application/octet-stream"],
            "as2_attachment_option": ["BATCH", "DOCUMENT_CACHE"],
            "as2_mdn_digest_alg": ["SHA1", "SHA256", "SHA384", "SHA512"],
        },
        "aliases": {
            "as2_sign_algorithm": "as2_signing_digest_alg",
            "as2_mdn_required": "as2_request_mdn",
            "as2_signing_cert_id": "as2_sign_alias",
            "as2_encryption_cert_id": "as2_encrypt_alias",
            "as2_content_type": "as2_data_content_type",
        },
        "note": "Structure differs for mycompany vs tradingpartner classification. "
                "mycompany builds receive-side (AS2ReceiveOptions), tradingpartner builds send-side (AS2SendOptions).",
    },
    "ftp": {
        "resource_type": "trading_partner",
        "protocol": "ftp",
        "template": {
            "ftp_host": "ftp.example.com (REQUIRED)",
            "ftp_port": "21",
            "ftp_username": "",
            "ftp_password": "",
            "ftp_remote_directory": "/edi/inbound (used for get; also for send if ftp_send_remote_directory not set)",
            "ftp_send_remote_directory": "/edi/outbound (optional, falls back to ftp_remote_directory)",
            "ftp_ssl_mode": "NONE | EXPLICIT | IMPLICIT",
            "ftp_connection_mode": "active | passive (default: passive)",
            "ftp_transfer_type": "ascii | binary (default: binary; also used for send if ftp_send_transfer_type not set)",
            "ftp_send_transfer_type": "(optional, falls back to ftp_transfer_type)",
            "ftp_get_action": "actionget | actiongetdelete | actiongetmove",
            "ftp_send_action": "actionputrename | actionputappend | actionputerror | actionputoverwrite",
            "ftp_max_file_count": "(integer, max files per poll)",
            "ftp_file_to_move": "(directory to move files after get when action=actiongetmove)",
            "ftp_move_to_directory": "(directory to move files after send)",
            "ftp_move_force_override": "true | false",
            "ftp_client_ssl_alias": "(certificate component ID for mutual TLS)",
        },
        "enums": {
            "ftp_ssl_mode": ["NONE", "EXPLICIT", "IMPLICIT"],
            "ftp_connection_mode": ["active", "passive"],
            "ftp_transfer_type": ["ascii", "binary"],
            "ftp_get_action": ["actionget", "actiongetdelete", "actiongetmove"],
            "ftp_send_action": ["actionputrename", "actionputappend", "actionputerror", "actionputoverwrite"],
        },
        "aliases": {
            "ftp_directory": "sets both ftp_remote_directory (get) and ftp_send_remote_directory (send)",
            "ftp_remote_dir": "same as ftp_directory",
            "ftp_use_ssl": "true maps to ftp_ssl_mode=EXPLICIT",
        },
    },
    "sftp": {
        "resource_type": "trading_partner",
        "protocol": "sftp",
        "template": {
            "sftp_host": "sftp.example.com (REQUIRED)",
            "sftp_port": "22",
            "sftp_username": "",
            "sftp_password": "",
            "sftp_remote_directory": "/edi/inbound (used for get; also for send if sftp_send_remote_directory not set)",
            "sftp_send_remote_directory": "/edi/outbound (optional, falls back to sftp_remote_directory)",
            "sftp_ssh_key_auth": "true | false",
            "sftp_known_host_entry": "",
            "sftp_ssh_key_path": "(path to SSH private key file)",
            "sftp_ssh_key_password": "(password for encrypted SSH key)",
            "sftp_dh_key_max_1024": "true | false (legacy server support)",
            "sftp_get_action": "actionget | actiongetdelete | actiongetmove",
            "sftp_send_action": "actionputrename | actionputappend | actionputerror | actionputoverwrite",
            "sftp_max_file_count": "(integer, max files per poll)",
            "sftp_file_to_move": "(directory to move files after get)",
            "sftp_move_to_directory": "(directory to move files after operation)",
            "sftp_move_force_override": "true | false",
            "sftp_proxy_enabled": "true | false",
            "sftp_proxy_host": "",
            "sftp_proxy_port": "",
            "sftp_proxy_user": "",
            "sftp_proxy_password": "",
            "sftp_proxy_type": "ATOM | HTTP | SOCKS4 | SOCKS5",
        },
        "enums": {
            "sftp_get_action": ["actionget", "actiongetdelete", "actiongetmove"],
            "sftp_send_action": ["actionputrename", "actionputappend", "actionputerror", "actionputoverwrite"],
            "sftp_proxy_type": ["ATOM", "HTTP", "SOCKS4", "SOCKS5"],
        },
        "aliases": {
            "sftp_directory": "sftp_remote_directory",
            "sftp_use_key_auth": "sftp_ssh_key_auth",
            "sftp_known_hosts_file": "sftp_known_host_entry",
        },
    },
    "disk": {
        "resource_type": "trading_partner",
        "protocol": "disk",
        "template": {
            "disk_get_directory": "/path/to/inbound",
            "disk_send_directory": "/path/to/outbound",
            "disk_file_filter": "* (default wildcard pattern)",
            "disk_filter_match_type": "wildcard | regex",
            "disk_delete_after_read": "true | false",
            "disk_max_file_count": "(integer)",
            "disk_create_directory": "true | false",
            "disk_write_option": "unique | overwrite | append | abort",
        },
        "enums": {
            "disk_filter_match_type": ["wildcard", "regex"],
            "disk_write_option": ["unique", "overwrite", "append", "abort"],
        },
        "aliases": {
            "disk_directory": "sets both disk_get_directory and disk_send_directory",
        },
    },
    "mllp": {
        "resource_type": "trading_partner",
        "protocol": "mllp",
        "template": {
            "mllp_host": "hl7.example.com (REQUIRED)",
            "mllp_port": "2575 (REQUIRED)",
            "mllp_use_ssl": "true | false",
            "mllp_persistent": "true | false",
            "mllp_receive_timeout": "(milliseconds)",
            "mllp_send_timeout": "(milliseconds)",
            "mllp_max_connections": "(integer)",
            "mllp_inactivity_timeout": "60 (seconds, default)",
            "mllp_max_retry": "1-5 (default: 1)",
            "mllp_halt_timeout": "true | false",
            "mllp_use_client_ssl": "true | false",
            "mllp_client_ssl_alias": "(certificate component ID)",
            "mllp_ssl_alias": "(server certificate component ID)",
        },
        "note": "Typically used with HL7 standard trading partners.",
    },
    "oftp": {
        "resource_type": "trading_partner",
        "protocol": "oftp",
        "template": {
            "oftp_host": "oftp.example.com (REQUIRED)",
            "oftp_port": "3305 (default)",
            "oftp_tls": "true | false",
            "oftp_ssid_code": "ODETTE Session ID code",
            "oftp_ssid_password": "ODETTE Session ID password",
            "oftp_compress": "true | false",
            "oftp_ssid_auth": "true | false",
            "oftp_sfid_cipher": "0 (none) | 1 (3DES) | 2 (AES-128) | 3 (AES-192) | 4 (AES-256)",
            "oftp_use_gateway": "true | false",
            "oftp_use_client_ssl": "true | false",
            "oftp_client_ssl_alias": "(certificate component ID)",
            "oftp_sfid_sign": "true | false",
            "oftp_sfid_encrypt": "true | false",
            "oftp_encrypting_cert": "(certificate alias)",
            "oftp_session_challenge_cert": "(certificate alias)",
            "oftp_verifying_eerp_cert": "(certificate alias)",
            "oftp_verifying_signature_cert": "(certificate alias)",
        },
        "enums": {
            "oftp_sfid_cipher": ["0", "1", "2", "3", "4"],
        },
        "note": "Typically used with ODETTE standard trading partners.",
    },
}


# ============================================================================
# Process Templates
# ============================================================================

_PROCESS_OVERVIEW = {
    "resource_type": "process",
    "tool": "manage_process",
    "available_actions": ["list", "get"],
    "read_only": True,
    "hint": (
        "manage_process is read-only (list/get). Author processes with "
        "build_from_archetype()/build_integration using a typed "
        "config.process_kind; use get_schema_template(resource_type='process', "
        "protocol='database_to_api_sync'|'wrapper_subprocess'|'sync_pipeline') "
        "for protocol templates."
    ),
}

# Legacy freeform process JSON authoring has been removed. The
# operation='create' schema now returns removal guidance instead of a
# shape-graph template, steering callers to typed process_kind authoring.
_PROCESS_CREATE_REMOVED = {
    "resource_type": "process",
    "operation": "create",
    "removed": True,
    "message": (
        "Freeform process JSON (shape-graph) authoring has been removed. "
        "manage_process is read-only and build_integration no longer accepts "
        "untyped process components."
    ),
    "use_instead": {
        "archetype_first": (
            "list_integration_archetypes() → build_from_archetype() → "
            "build_integration(action='plan'|'apply')"
        ),
        "typed_process_kind": (
            "Set config.process_kind to one of "
            "['database_to_api_sync', 'wrapper_subprocess', 'sync_pipeline'] on a "
            "build_integration process component. See "
            "get_schema_template(resource_type='process', protocol=...)."
        ),
        "raw_xml_escape_hatch": (
            "manage_component (type='component', config.xml) for hand-authored "
            "process XML."
        ),
    },
    "process_protocols": ["database_to_api_sync", "wrapper_subprocess", "sync_pipeline"],
}

_PROCESS_LIST = {
    "resource_type": "process",
    "operation": "list",
    "filters_param": "filters (JSON string)",
    "template": '{"folder_name": "Home"}',
    "available_filters": ["folder_name"],
}


# ============================================================================
# Integration Builder Templates
# ============================================================================

_INTEGRATION_OVERVIEW = {
    "resource_type": "integration",
    "tool": "build_integration",
    "available_actions": ["plan", "apply", "verify"],
    "config_format": "JSON (config parameter)",
    "conflict_policy": ["reuse", "clone", "fail"],
    "hint": "Use operation='plan' for full IntegrationSpecV1 templates and routing behavior.",
    # Issue #102 (M9.8) — mechanical build-basics guards enforced at plan time.
    "build_basics_guards": {
        "hard_failures": [
            "ENV_VAR_LITERAL_REJECTED — a literal ${ENV_VAR} token in a connection field (Boomi stores it verbatim and never interpolates it; use credential_ref / environment extensions).",
            "STOP_CONTINUE_MISSING / RETURN_DOCS_STOP_EXCLUSIVE — process-graph shape errors (verify action).",
        ],
        "warnings": [
            "DUPLICATE_CONNECTION — two in-spec create connections to the same endpoint each burn a separate connection license; compatible-auth duplicates are aliased to the canonical one (see connection_aliases), differing-auth duplicates are flagged distinct. Advisory: it never blocks the build (auto-reference, not refuse).",
            "CONNECTION_ENDPOINT_IP_LITERAL (prefer FQDN), CONNECTION_BASE_URL_HAS_PATH / CONNECTION_CARRIES_PER_CALL_FIELDS (minimal-connection), FOLDER_REQUIRED_ON_CREATE (no folder → account root), PROPERTY_NAMING (DPP_/DDP_ UPPER_SNAKE), and — when naming.convention=='bracketed' — NAMING_CONVENTION_BRACKETED per-type checks.",
        ],
        "naming_convention": "bracketed naming is a CHOSEN account convention (Boomi mandates no single style); activate it with naming.convention='bracketed'. The lint flags, never rewrites.",
        "extensions": "Environment-extension declarations cover DB (xpath-keyed) and REST (id-keyed, no xpath) connections, including reuse-mode REST credentials; SET_BY_EXTENSION is the fail-fast placeholder convention for a non-secret extension-bound field.",
    },
}

_INTEGRATION_PLAN = {
    "resource_type": "integration",
    "operation": "plan",
    "tool": "build_integration (action='plan')",
    "template": {
        "name": "Order Sync",
        "mode": "lift_shift",
        "conflict_policy": "reuse",
        "source_description": {
            "name": "Order Sync from legacy iPaaS",
            "goals": ["Receive orders", "Transform payloads", "Deliver to ERP"],
            "components": [
                {
                    "key": "rest_connection",
                    "type": "connector-settings",
                    "action": "create",
                    "name": "Order API Connection",
                    "config": {
                        "connector_type": "rest",
                        "component_name": "Order API Connection",
                        "base_url": "https://api.example.com",
                        "auth": "OAUTH2",
                        "oauth2": {
                            "grant_type": "client_credentials",
                            "client_id": "<<client id>>",
                            "client_secret_ref": "credential://<<vendor>>/oauth-client-secret",
                            "access_token_url": "https://api.example.com/oauth/token",
                            "scope": "",
                            "credentials_assertion_type": "client_secret",
                        },
                    },
                },
            ],
        },
    },
    "notes": [
        "You can also provide integration_spec directly instead of source_description.",
        "plan is read-only and returns deterministic execution order with endpoint routes.",
        "Dependency tokens in config can reference previous components with $ref:<component_key>.",
        "Process components require a typed config.process_kind "
        "(database_to_api_sync / wrapper_subprocess / sync_pipeline) — freeform shape-graph "
        "process JSON is no longer supported. Author processes with "
        "build_from_archetype()/build_integration, and inspect the per-kind "
        "shape via get_schema_template(resource_type='process', protocol=...).",
    ],
}

_INTEGRATION_APPLY = {
    "resource_type": "integration",
    "operation": "apply",
    "tool": "build_integration (action='apply')",
    "template": {
        "dry_run": False,
        "conflict_policy": "reuse",
        "integration_spec": {
            "version": "1.0",
            "name": "Order Sync",
            "mode": "lift_shift",
            "components": [
                {
                    "key": "order_partner",
                    "type": "trading_partner",
                    "action": "create",
                    "name": "ACME Partner",
                    "config": {
                        "component_name": "ACME Partner",
                        "standard": "x12",
                        "classification": "tradingpartner",
                        "isa_id": "ACME",
                    },
                }
            ],
        },
    },
    "notes": [
        "dry_run defaults to true; set dry_run=false to mutate Boomi resources.",
        "apply returns build_id; use it with verify.",
        "After apply returns build_id, call orchestrate_deploy(profile=..., build_id=..., "
        "environment_id=..., runtime_id=..., dry_run=true) to preview package -> deploy -> "
        "runtime binding -> optional schedule/test; use dry_run=false to execute. Stages run in "
        "that order — deployment always precedes any schedule/test.",
    ],
}

_INTEGRATION_VERIFY = {
    "resource_type": "integration",
    "operation": "verify",
    "tool": "build_integration (action='verify')",
    "template": {
        "build_id": "<uuid-from-apply>",
    },
    "notes": [
        "verify is read-only and validates component existence plus dependency resolution.",
        "For process components it also reports verification[<process_key>].process_graph "
        "({errors, warnings, shapes_checked}); graph errors fail verification, attribute lints are warnings.",
    ],
}


# ============================================================================
# Component Templates
# ============================================================================

_COMPONENT_OVERVIEW = {
    "resource_type": "component",
    "tools": {
        "query_components": ["list", "get", "search", "bulk_get"],
        "manage_component": ["create", "update", "clone", "delete"],
        "analyze_component": ["where_used", "dependencies", "compare_versions", "merge"],
        "prepare_component_edit": ["prepare"],
        "apply_component_edit": ["apply"],
    },
    "component_types": [
        "process", "processproperty", "processroute",
        "connector-settings", "connector-action",
        "profile.db", "profile.edi", "profile.flatfile", "profile.json", "profile.xml",
        "tradingpartner", "tpgroup", "tporganization", "tpcommoptions",
        "transform.map", "transform.function", "xslt", "script.processing", "script.mapping",
        "flowservice", "webservice", "webservice.external",
        "certificate", "certificate.pgp", "crossref", "customlibrary", "documentcache",
        "edistandard", "queue",
    ],
}

_COMPONENT_CREATE = {
    "resource_type": "component",
    "operation": "create",
    "note": "Boomi's Component API requires type-specific XML. For processes, prefer build_from_archetype()/build_integration with a typed config.process_kind; use config.xml here only as an explicit raw-XML escape hatch.",
    "xml_template": (
        '<Component xmlns="http://api.platform.boomi.com/"\n'
        '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
        '    name="Component Name"\n'
        '    type="process"\n'
        '    folderName="Home">\n'
        '  <description>Component description</description>\n'
        '  <object>\n'
        '    <!-- Type-specific XML structure here -->\n'
        '  </object>\n'
        '</Component>'
    ),
    "recommended_workflow": [
        "1. Use query_components list action to find an existing component of same type",
        "2. Use query_components get action to retrieve its full XML",
        "3. Modify the XML for your new component",
        "4. Pass modified XML as config.xml to manage_component create action",
        "   OR for processes: use build_from_archetype()/build_integration with a typed config.process_kind",
    ],
}

_COMPONENT_CREATE_CUSTOMLIBRARY = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "customlibrary",
    "note": (
        "Custom Library components wrap JAR files uploaded to Settings > "
        "Development Resources > Account Libraries. Library_type determines "
        "where JARs land in the runtime: general → /userlib (restart required), "
        "scripting → /userlib/script, connector → /userlib/<connector_type>."
    ),
    "xml_template": (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
        '               xmlns:bns="http://api.platform.boomi.com/"\n'
        '               name="{name}"\n'
        '               type="customlibrary"\n'
        '               folderFullPath="{folder_full_path}">\n'
        '  <bns:encryptedValues/>\n'
        '  <bns:description>{description}</bns:description>\n'
        '  <bns:object>\n'
        '    <CustomLibrary xmlns="">\n'
        '      <Type>{library_type}</Type>{connector_type_element}\n'
        '      <Files checksum="{checksum}" checksumType="SHA-256"'
        ' guid="{guid}" md5="{md5}" name="{jar_name}" size="{size}"/>\n'
        '    </CustomLibrary>\n'
        '  </bns:object>\n'
        '</bns:Component>'
    ),
    "placeholders": {
        "name": "Component display name (max 255 chars)",
        "folder_full_path": "Slash-separated folder path, e.g. 'Home' or 'Home/Libraries'",
        "description": "(optional) free-text description",
        "library_type": "general | scripting | connector",
        "connector_type_element": (
            "When library_type='connector': '<connectorType>database</connectorType>' "
            "(or disk, http, ftp, sftp, etc.). Otherwise: empty string."
        ),
        "jar_name": "Exact filename as uploaded, e.g. mydb-jdbc-1.0.0.jar",
        "checksum": "SHA-256 hex digest recorded by Boomi when the JAR was uploaded",
        "guid": "GUID Boomi assigned to the uploaded JAR (Files.guid attribute)",
        "md5": "MD5 hex digest recorded by Boomi when the JAR was uploaded",
        "size": "Byte size recorded by Boomi when the JAR was uploaded",
    },
    "multiple_files": (
        "To include several JARs, repeat the <Files .../> element with each JAR's "
        "name/checksum/guid/md5/size. All entries belong inside the single <CustomLibrary> element."
    ),
    "metadata_lookup": (
        "checksum/guid/md5/size come from the Account Library entry — there is no "
        "public REST endpoint to list them. Easiest sources: (a) create one Custom "
        "Library via the Boomi UI, then query_components get its XML to copy the "
        "Files attributes; (b) inspect an existing customlibrary component in the "
        "account; (c) compute SHA-256/MD5/size locally if you have the JAR file, "
        "but the guid is server-assigned and must still come from Boomi."
    ),
    "common_failure": (
        "create_package fails with 'Custom library references deleted jars and cannot "
        "be packaged' when Files is missing any of checksum/guid/md5/size or the "
        "values don't match the current Account Library entry. Component create "
        "succeeds with just name= which is misleading — the error surfaces at packaging."
    ),
    "recommended_workflow": [
        "1. Ensure the JAR is uploaded under Settings > Development Resources > Account Libraries.",
        "2. Look up the JAR's checksum/guid/md5/size (see 'metadata_lookup').",
        "3. Substitute placeholders in xml_template and call manage_component create.",
        "4. manage_deployment create_package with component_type='customlibrary'.",
        "5. manage_deployment deploy to the target environment — JARs land in "
        "/<atom>/userlib (or /userlib/script, /userlib/<connector_type>).",
    ],
    "example_connector_database": (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
        '               xmlns:bns="http://api.platform.boomi.com/"\n'
        '               name="MyDatabase JDBC Driver"\n'
        '               type="customlibrary"\n'
        '               folderFullPath="Home/Libraries">\n'
        '  <bns:encryptedValues/>\n'
        '  <bns:description></bns:description>\n'
        '  <bns:object>\n'
        '    <CustomLibrary xmlns="">\n'
        '      <Type>connector</Type>\n'
        '      <connectorType>database</connectorType>\n'
        '      <Files checksum="REPLACE_WITH_SHA256_FROM_ACCOUNT_LIBRARY"'
        ' checksumType="SHA-256"'
        ' guid="REPLACE_WITH_GUID_FROM_ACCOUNT_LIBRARY"'
        ' md5="REPLACE_WITH_MD5_FROM_ACCOUNT_LIBRARY"'
        ' name="mydb-jdbc-1.0.0.jar"'
        ' size="REPLACE_WITH_BYTE_SIZE"/>\n'
        '    </CustomLibrary>\n'
        '  </bns:object>\n'
        '</bns:Component>'
    ),
}


_HOST_PORT_DB_REQUIRED = [
    "component_name", "driver_id", "auth_mode",
    "host", "dbname", "username", "credential_ref",
]
_HOST_PORT_DB_OPTIONAL = [
    "folder_name", "description", "port", "additional",
    "pooling", "write_options",
]
_HOST_PORT_DB_FORBIDDEN = ["custom_class_name", "connection_url"]

_CUSTOM_URL_REQUIRED = [
    "component_name", "driver_id", "auth_mode",
    "custom_class_name", "connection_url", "username", "credential_ref",
]
_CUSTOM_URL_OPTIONAL = [
    "folder_name", "description", "pooling", "write_options",
]
_CUSTOM_URL_FORBIDDEN = ["host", "port", "dbname", "additional"]

_COMPONENT_CREATE_CONNECTOR_DATABASE_SQLSERVER = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-settings",
    "protocol": "database.sqlserver",
    "tool": "manage_connector (action='create')",
    "note": (
        "Database (Legacy) connector. One builder, one outer "
        "<DatabaseConnectionSettings> envelope, two driver shapes — "
        "host_port_db (Microsoft SQL Server / jTDS / Oracle / MySQL / SAP HANA) "
        "and custom_url (caller-supplied JDBC class + URL). Dispatched through "
        "the builder registry (CONNECTOR_BUILDERS['database']) so callers pass "
        "JSON config — not raw XML. The builder emits "
        "<DatabaseConnectionSettings> with Boomi's <WriteOptions> and "
        "<AdapterPoolInfo> blocks."
    ),
    "template": {
        "connector_type": "database",
        "driver_id": "sqlserver",
        "auth_mode": "username_password",
        "component_name": "My SQL Server Connection",
        "folder_name": "Home",
        "description": "(optional) free-text description",
        "host": "host.example.com",
        "port": 1433,
        "dbname": "MyDatabase",
        "username": "sa",
        "credential_ref": "credential://your-vault/sqlserver/password",
        "additional": "(optional) JDBC URL suffix appended to urlFormat {3}, e.g. ';encrypt=true;trustServerCertificate=true'",
    },
    "required": [
        "connector_type",
        "driver_id",
        "auth_mode",
        "component_name",
        "host",
        "dbname",
        "username",
        "credential_ref",
    ],
    "defaults": {"connector_type": "database", "driver_id": "sqlserver", "auth_mode": "username_password", "port": 1433, "additional": ""},
    # All driver IDs (Issue #31) — every entry below is now buildable.
    # Aliases (microsoft_jdbc, sap_hana) are listed so LLM clients can pick
    # either form without consulting DRIVER_ALIASES.
    "supported_driver_ids": [
        "sqlserver", "microsoft_jdbc", "jtds",
        "oracle", "mysql", "sap_hana", "sap-hana", "custom",
    ],
    "recognized_driver_ids": [
        "sqlserver", "microsoft_jdbc", "jtds",
        "oracle", "mysql", "sap_hana", "sap-hana", "custom",
    ],
    "shape_metadata": {
        "host_port_db": {
            "required": _HOST_PORT_DB_REQUIRED,
            "optional": _HOST_PORT_DB_OPTIONAL,
            "forbidden": _HOST_PORT_DB_FORBIDDEN,
            "applies_to": [
                "sqlserver", "microsoft_jdbc", "jtds",
                "oracle", "mysql", "sap_hana", "sap-hana",
            ],
            "description": (
                "Boomi substitutes host/port/dbname into the driver's "
                "urlFormat placeholders ({0},{1},{2}); `additional` is "
                "appended verbatim as {3}. Pick this shape when Boomi knows "
                "the driver's URL template."
            ),
        },
        "custom_url": {
            "required": _CUSTOM_URL_REQUIRED,
            "optional": _CUSTOM_URL_OPTIONAL,
            "forbidden": _CUSTOM_URL_FORBIDDEN,
            "applies_to": ["custom"],
            "description": (
                "Caller supplies the full JDBC driver class FQCN "
                "(custom_class_name → className) and a complete JDBC URL "
                "(connection_url → urlFormat). host/port/dbname/additional "
                "are emitted as empty XML attributes to match Boomi's live "
                "Custom export shape — do not pass them in JSON."
            ),
        },
    },
    "driver_variants": {
        "sqlserver": {
            "shape": "host_port_db",
            "buildable": True,
            "emits_driver_id": "sqlserver",
            "class_name": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "url_format": "jdbc:sqlserver://{0}:{1};database={2}{3}",
            "default_port": 1433,
            "required": ["component_name", "host", "dbname", "username", "credential_ref"],
            "recommended_additional": ";encrypt=true;trustServerCertificate=true",
            "live_reference_component_id": "4ace95d7-6ee4-4f83-8fad-723d3fabdb2f",
            "note": (
                "Microsoft JDBC. Driver 12+ defaults to encrypt=true; the "
                "recommended_additional clause adds trustServerCertificate=true "
                "for Docker SQL Server with a self-signed cert. Caller decides — "
                "the builder does not auto-inject or warn."
            ),
        },
        "microsoft_jdbc": {"alias_of": "sqlserver"},
        "jtds": {
            "shape": "host_port_db",
            "buildable": True,
            "emits_driver_id": "jtds",
            "class_name": "net.sourceforge.jtds.jdbc.Driver",
            "url_format": "jdbc:jtds:sqlserver://{0}:{1}/{2}{3}",
            "default_port": 1433,
            "required": ["component_name", "host", "dbname", "username", "credential_ref"],
            "live_reference_component_id": "107aaef1-cb1e-4975-be44-69d120803864",
            "note": (
                "Legacy jTDS driver. Pre-loaded in Boomi runtime; no TLS by "
                "default. For SQL Server Windows-domain auth, callers add "
                ";domain=<domain> via `additional` and order matters: "
                ";instance=<name>;domain=<value>. Boomi's UI auth mode for "
                "Windows-integrated is not yet buildable in this MCP."
            ),
        },
        "oracle": {
            "shape": "host_port_db",
            "buildable": True,
            "emits_driver_id": "oracle",
            "class_name": "oracle.jdbc.driver.OracleDriver",
            "url_format": "jdbc:oracle:thin:@{0}:{1}:{2}",
            "default_port": 1521,
            "required": ["component_name", "host", "dbname", "username", "credential_ref"],
            "live_reference_component_id": "6adf9e1e-39c8-4104-bc6c-9769b93aa161",
            "note": (
                "Oracle Thin driver. `dbname` is the SID (Boomi's urlFormat "
                "uses the colon-SID form). Boomi appends `additional` to the "
                "end of the formed URL (per Database Legacy docs), but Oracle "
                "Thin SID syntax may not accept arbitrary trailing semicolon "
                "options — if your scenario needs service-name URLs or "
                "vendor-style options, use driver_id='custom' with a "
                "connection_url like "
                "jdbc:oracle:thin:@//host:port/service_name?option=value."
            ),
        },
        "mysql": {
            "shape": "host_port_db",
            "buildable": True,
            "emits_driver_id": "mysql",
            "class_name": "com.mysql.jdbc.Driver",
            "url_format": "jdbc:mysql://{0}:{1}/{2}{3}",
            "default_port": 3306,
            "required": ["component_name", "host", "dbname", "username", "credential_ref"],
            "live_reference_component_id": "bfbfea6f-39c7-498e-859b-6036959a20c8",
            "runtime_driver_prerequisite": (
                "MySQL Connector/J is not bundled with the Boomi runtime. "
                "Upload the driver as a Custom Library and deploy it to the "
                "runtime/environment before testing the connection. The "
                "builder emits XML but does not deploy jars."
            ),
            "note": (
                "Emits the legacy com.mysql.jdbc.Driver class (matches "
                "Boomi's live #Common reference). Newer MySQL Connector/J "
                "releases prefer com.mysql.cj.jdbc.Driver — use "
                "driver_id='custom' to pin a different class name."
            ),
        },
        "sap-hana": {
            "shape": "host_port_db",
            "buildable": True,
            "emits_driver_id": "sap-hana",
            "class_name": "com.sap.db.jdbc.Driver",
            "url_format": "jdbc:sap://{0}:{1}/?databaseName={2}{3}",
            "default_port": None,
            "port_required": True,
            "required": [
                "component_name", "host", "port", "dbname", "username", "credential_ref",
            ],
            "live_reference_component_id": "c9077711-39a4-4d52-9f91-27bdf1f5b8ec",
            "runtime_driver_prerequisite": (
                "SAP HANA JDBC (ngdbc) is not bundled with the Boomi runtime. "
                "Deploy ngdbc.jar via Custom Library before connection tests."
            ),
            "note": (
                "No verified default port — callers MUST supply `port` "
                "explicitly. Cloud HANA listens on 443/30015/etc; on-prem "
                "varies. Missing port fails with "
                "DATABASE_CONNECTOR_VALIDATION_FAILED before mutation."
            ),
        },
        "sap_hana": {"alias_of": "sap-hana"},
        "custom": {
            "shape": "custom_url",
            "buildable": True,
            "emits_driver_id": "custom",
            "class_name_source": "custom_class_name",
            "url_format_source": "connection_url",
            "default_port": None,
            "required": [
                "component_name", "custom_class_name", "connection_url",
                "username", "credential_ref",
            ],
            "live_reference_component_id": "39fb519d-e970-4aaf-a1f7-4eba39158e9d",
            "runtime_driver_prerequisite": (
                "Custom JDBC drivers require an Account Library + Custom "
                "Library component deployed to the runtime/environment. The "
                "builder emits the XML envelope but does not deploy jars."
            ),
            "note": (
                "Caller supplies the FQCN via custom_class_name and the full "
                "JDBC URL via connection_url — Boomi does not substitute "
                "{0}{1}{2}{3} for this shape. host/port/dbname/additional "
                "are forbidden in the JSON contract; the XML emits them as "
                "empty attributes to match Boomi's live Custom export."
            ),
            "example": {
                "connector_type": "database",
                "driver_id": "custom",
                "auth_mode": "username_password",
                "component_name": "Snowflake via Custom JDBC",
                "folder_name": "Process Library",
                "username": "INTEG_USER",
                "credential_ref": "credential://prod/snowflake/password",
                "custom_class_name": "net.snowflake.client.jdbc.SnowflakeDriver",
                "connection_url": "jdbc:snowflake://acct.snowflakecomputing.com/?db=PROD&schema=PUBLIC",
            },
        },
    },
    "supported_auth_modes": ["username_password"],
    "unsupported_future_auth_modes": ["windows_integrated"],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    ],
    "pooling": {
        "description": (
            "Optional. Omit (or pass enabled=false) to keep the M2.2 default of "
            "pooling disabled. Set enabled=true to use Boomi's connection pool; "
            "max_active=-1 / max_idle=-1 mean unbounded (matches the CDS reference)."
        ),
        "fields": {
            "enabled":           {"type": "boolean", "default_when_disabled": False, "default_when_enabled": True},
            "exhausted_action":  {"type": "integer", "default_when_disabled": 1,     "default_when_enabled": 1},
            "max_active":        {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": -1},
            "max_idle":          {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": -1},
            "max_idle_time":     {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": 0},
            "max_wait":          {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": 0},
            "min_idle":          {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": 0},
            "number_of_tests":   {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": 0},
            "test_idle":         {"type": "boolean", "default_when_disabled": False, "default_when_enabled": False},
            "test_on_borrow":    {"type": "boolean", "default_when_disabled": False, "default_when_enabled": False},
            "test_on_return":    {"type": "boolean", "default_when_disabled": False, "default_when_enabled": False},
            "time_between_runs": {"type": "integer", "default_when_disabled": 0,     "default_when_enabled": 0},
            "validation_query":  {"type": "string",  "default_when_disabled": "",    "default_when_enabled": ""},
        },
        "defaults_when_omitted": {
            "enabled": False,
            "exhausted_action": 1,
            "max_active": 0,
            "max_idle": 0,
            "max_idle_time": 0,
            "max_wait": 0,
            "min_idle": 0,
            "number_of_tests": 0,
            "test_idle": False,
            "test_on_borrow": False,
            "test_on_return": False,
            "time_between_runs": 0,
            "validation_query": "",
        },
        "defaults_when_enabled": {
            "enabled": True,
            "exhausted_action": 1,
            "max_active": -1,
            "max_idle": -1,
            "max_idle_time": 0,
            "max_wait": 0,
            "min_idle": 0,
            "number_of_tests": 0,
            "test_idle": False,
            "test_on_borrow": False,
            "test_on_return": False,
            "time_between_runs": 0,
            "validation_query": "",
        },
        "error_code": "DATABASE_POOLING_VALIDATION_FAILED",
    },
    "write_options": {
        "description": (
            "Optional. Omit to keep the M2.2 default "
            "(writeSQLToFile=false, sqlFilePath=tmp/sqldebug.txt)."
        ),
        "fields": {
            "write_sql_to_file": {
                "type": "boolean",
                "default": False,
                "note": "When True, sql_file_path is required.",
            },
            "sql_file_path": {"type": "string", "default": "tmp/sqldebug.txt"},
        },
        "defaults": {"write_sql_to_file": False, "sql_file_path": "tmp/sqldebug.txt"},
        "error_code": "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED",
    },
    "password_note": (
        "Plaintext secrets are rejected before any mutation. Pass credential_ref="
        "'credential://...' as an opaque placeholder — the builder never writes it "
        "into the emitted XML. Boomi stores passwords as ciphertext produced by its "
        "own encryption; there is no public API to encrypt a plaintext value. New "
        "components are created with <encryptedValue ... isSet=\"false\"/>. Set the "
        "password in the Boomi UI after create, or supply an existing ciphertext "
        "via the raw-XML escape hatch (config.xml=...). Forbidden secret-shaped "
        "keys (password, password_ref, secret, token, access_token, client_secret) "
        "fail validation with error_code=PLAINTEXT_SECRET_REJECTED."
    ),
    "driver_note": (
        "driver_id='sqlserver' and driver_id='microsoft_jdbc' both emit Boomi's "
        "Microsoft JDBC driver (className=com.microsoft.sqlserver.jdbc.SQLServerDriver, "
        "urlFormat=jdbc:sqlserver://{0}:{1};database={2}{3}); the alias is a caller "
        "convenience. driver_id='jtds' emits the legacy jTDS driver "
        "(className=net.sourceforge.jtds.jdbc.Driver, "
        "urlFormat=jdbc:jtds:sqlserver://{0}:{1}/{2}{3}). driver_id='oracle' emits "
        "the Oracle Thin driver, driver_id='mysql' emits com.mysql.jdbc.Driver, "
        "and driver_id='sap_hana' (canonical 'sap-hana') emits com.sap.db.jdbc.Driver. "
        "driver_id='custom' switches to the custom_url shape — caller supplies the "
        "full JDBC class via custom_class_name and the full URL via connection_url. "
        "Postgres and other JDBC families without a verified Boomi reference export "
        "remain unsupported and return error_code=UNSUPPORTED_DB_DRIVER."
    ),
    "gotchas": [
        (
            "Atom-in-Docker networking: if the Boomi atom runs in a Docker container "
            "and SQL Server is reachable via a host port mapping, host='localhost' "
            "resolves to the atom container itself. Use host='host.docker.internal' "
            "(Docker Desktop on Mac/Windows) to reach the host's port."
        ),
        (
            "Microsoft JDBC driver 12+ defaults to encrypt=true. The "
            "mcr.microsoft.com/mssql/server image ships a self-signed certificate, "
            "so the driver rejects the TLS handshake with a PKIX path error. Set "
            "additional=';encrypt=true;trustServerCertificate=true' (or "
            "';encrypt=false' for non-TLS dev only) to proceed."
        ),
        (
            "SAP HANA has no verified default port. Callers MUST pass `port` "
            "explicitly (e.g. 30015 for system DB, 443 for cloud HANA TLS, "
            "39015/39041 for on-prem tenants). Missing port fails with "
            "DATABASE_CONNECTOR_VALIDATION_FAILED field='port' before mutation."
        ),
        (
            "Custom / MySQL / SAP HANA drivers are not bundled with the Boomi "
            "runtime. The connector component XML lands successfully, but "
            "Connection Tests will fail until you deploy the JDBC driver "
            "(ngdbc.jar for SAP HANA, mysql-connector-j for MySQL, vendor JAR "
            "for Custom) via an Account Library + Custom Library component."
        ),
        (
            "driver_id='custom' rejects host/port/dbname/additional in the JSON "
            "contract — Boomi's Custom shape carries them as empty XML attrs "
            "and the full URL lives in connection_url. Supplying host on a "
            "Custom config fails with DATABASE_CONNECTOR_VALIDATION_FAILED."
        ),
    ],
    "recommended_workflow": [
        "1. manage_connector list_types — confirm 'database' appears.",
        "2. manage_connector create with the JSON config above (credential_ref is opaque; no password).",
        "3. Set the password in the Boomi UI (or update via raw XML with pre-encrypted ciphertext).",
        "4. Test the connection from the UI (Connection Test) against an online runtime.",
        "5. Deploy via manage_deployment.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' use "
        "read-merge-write semantics (issue #45): the builder produces "
        "desired XML, the current live XML is fetched, and the merge "
        "updates only the builder-owned DatabaseConnectionSettings attrs "
        "(host/dbname/driver/pooling/etc.) and the WriteOptions / "
        "AdapterPoolInfo child blocks. Unknown/future attributes or child "
        "elements on DatabaseConnectionSettings — plus bns:encryptedValues "
        "(e.g. the password slot), unknown root attributes, and unknown "
        "<bns:object> siblings — are preserved. Direct manage_connector "
        "action='update' with config.xml remains a full XML replacement "
        "(no preservation); its JSON smart-merge supports only "
        "name/description/folder fields."
    ),
    "example": {
        "connector_type": "database",
        "driver_id": "microsoft_jdbc",
        "auth_mode": "username_password",
        "component_name": "MS SQL Server Microsoft",
        "folder_name": "Process Library",
        "description": "Connection to the MS SQL Server order entry database.",
        "host": "host.docker.internal",
        "port": 11433,
        "dbname": "Expert",
        "username": "sa",
        "credential_ref": "credential://prod/sqlserver/password",
        "additional": ";encrypt=true;trustServerCertificate=true",
        # pooling and write_options are shown explicitly at their omitted-defaults
        # for discoverability — both keys may be omitted entirely with no XML diff.
        "pooling": {"enabled": False},
        "write_options": {"write_sql_to_file": False, "sql_file_path": "tmp/sqldebug.txt"},
    },
}


_COMPONENT_CREATE_CONNECTOR_REST_CLIENT = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-settings",
    "protocol": "rest.client",
    "boomi_subtype": "officialboomi-X3979C-rest-prod",
    "public_aliases": ["rest", "rest_client", "officialboomi-X3979C-rest-prod"],
    "tool": "manage_connector (action='create')",
    "note": (
        "Boomi REST Client connector-settings (connection). Models the API "
        "base URL plus authentication settings. Buildable auth modes: NONE "
        "(no auth, optionally with client-cert refs or connection pooling), "
        "BASIC (username + credential_ref), NTLM (username + credential_ref "
        "+ domain + workstation), and OAUTH2 client_credentials. Remaining "
        "REST Client auth modes (CUSTOM, PASSWORD_DIGEST, AWS_SIGNATURE, "
        "AWS_IAM_ROLES_ANYWHERE) return UNSUPPORTED_REST_AUTH_MODE until a "
        "verified live export exists. Cert refs (private_certificate_ref / "
        "public_certificate_ref) are an INDEPENDENT client-cert option — "
        "they may co-occur with any auth selection. REST is the canonical "
        "target API connector — use connector_type='rest'."
    ),
    "template": {
        "connector_type": "rest",
        "component_name": "<<target REST connection>>",
        "folder_name": "<<folder>>",
        "description": "<<optional description>>",
        "base_url": "https://<<host>>",
        "auth": "OAUTH2",
        "oauth2": {
            "grant_type": "client_credentials",
            "client_id": "<<client id>>",
            "client_secret_ref": "credential://<<vendor>>/<<role>>",
            "access_token_url": "https://<<host>>/oauth/token",
            "scope": "",
            "credentials_assertion_type": "client_secret",
        },
        "preemptive": False,
        "connect_timeout_ms": -1,
        "read_timeout_ms": -1,
        "cookie_scope": "GLOBAL",
        "connection_pooling": {"enabled": False},
    },
    "required": [
        "connector_type",
        "component_name",
        "base_url",
        "auth",
    ],
    "defaults": {
        "connector_type": "rest",
        "auth": "OAUTH2",
        "folder_name": "Home",
        "preemptive": False,
        "connect_timeout_ms": -1,
        "read_timeout_ms": -1,
        "cookie_scope": "GLOBAL",
        "connection_pooling": {"enabled": False},
    },
    "supported_auth_modes": ["NONE", "BASIC", "NTLM", "OAUTH2"],
    "unsupported_future_auth_modes": [
        "PASSWORD_DIGEST",
        "CUSTOM",
        "AWS_SIGNATURE",
        "AWS_IAM_ROLES_ANYWHERE",
    ],
    "independent_options": {
        "private_certificate_ref": (
            "Optional X509 private-key Boomi certificate component id "
            "(GUID). Works with ANY auth mode (NONE or OAUTH2) — not tied "
            "to a specific auth selection."
        ),
        "public_certificate_ref": (
            "Optional X509 public-cert Boomi certificate component id "
            "(GUID). Works with ANY auth mode."
        ),
        "connection_pooling": (
            "Optional pooling block {enabled: bool, max_total: int, "
            "idle_timeout_seconds: int}. Works with ANY auth mode."
        ),
    },
    "field_auth_dependency_map": {
        "summary": (
            "Machine-readable map of which connection fields are "
            "independent of auth selection (work with any auth) vs "
            "auth-tied (rejected with REST_CONNECTOR_VALIDATION_FAILED "
            "when paired with the wrong auth). Live exports often pair "
            "independent fields with a particular auth in their "
            "example — but that pairing is incidental, not a binding "
            "rule. Use this map to drive caller-side validation."
        ),
        "independent": [
            "base_url",
            "connect_timeout_ms",
            "read_timeout_ms",
            "cookie_scope",
            "private_certificate_ref",
            "public_certificate_ref",
            "connection_pooling",
        ],
        "auth_tied": {
            "username": ["BASIC", "NTLM"],
            "credential_ref": ["BASIC", "NTLM"],
            "domain": ["NTLM"],
            "workstation": ["NTLM"],
            "preemptive": ["BASIC", "OAUTH2"],
            "oauth2": ["OAUTH2"],
        },
        "grant_tied": {
            "oauth2.authorization_url": ["authorization_code"],
        },
        "always_rejected": {
            "oauth2.access_token": "ciphertext emission is token-not-set only",
            "oauth2.cached_token": "ciphertext emission is token-not-set only",
        },
    },
    "buildable_oauth2_grant_types": ["client_credentials", "authorization_code"],
    "oauth2_grant_type_aliases": {
        "code": "authorization_code",
        "authorization_code": "authorization_code",
        "client_credentials": "client_credentials",
    },
    "unsupported_future_oauth2_grant_types": [
        "resource_owner_credentials",
        "jwt_bearer",
        "authorization_code_with_cached_access_token",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    ],
    "password_note": (
        "Plaintext secret-shaped fields are rejected with "
        "PLAINTEXT_SECRET_REJECTED (top-level) or REST_SECRET_VALUE_FORBIDDEN "
        "(under oauth2). Pass an opaque oauth2.client_secret_ref starting "
        "with 'credential://'; the builder never writes the actual secret "
        "into XML. Boomi stores the OAuth2 client secret as ciphertext set "
        "via the UI after create (or via a pre-encrypted XML payload)."
    ),
    "error_codes": {
        "REST_CONNECTOR_VALIDATION_FAILED": "shape / type / required-field issue",
        "REST_BASE_URL_REQUIRED": "base_url absent or empty",
        "REST_BASE_URL_INVALID": "base_url scheme is not http:// or https://",
        "UNSUPPORTED_REST_AUTH_MODE": "auth or oauth2.grant_type is not buildable in issue #24",
        "UNSUPPORTED_REST_OAUTH2_PARAMETERS": "oauth2.authorization_parameters or oauth2.access_token_parameters supplied non-empty (emission deferred)",
        "REST_SECRET_VALUE_FORBIDDEN": "raw secret value under oauth2.client_secret_ref or oauth2.client_secret",
        "REST_POOLING_INVALID": "connection_pooling shape, type, or pool-dependent field paired with enabled=False",
        "PLAINTEXT_SECRET_REJECTED": "a forbidden secret-shaped key appeared in config",
    },
    "gotchas": [
        (
            "Boomi subtype is officialboomi-X3979C-rest-prod and the "
            "operation step uses GenericOperationConfig. Always pass "
            "connector_type='rest' (or one of the documented aliases) "
            "for target sends."
        ),
        (
            "Buildable auth modes: NONE, BASIC, NTLM, OAuth2 "
            "client_credentials, and OAuth2 authorization_code "
            "(token-not-set — no cached access token emitted). "
            "CUSTOM, PASSWORD_DIGEST, AWS_SIGNATURE, and "
            "AWS_IAM_ROLES_ANYWHERE remain UNSUPPORTED_REST_AUTH_MODE "
            "until a verified live export exists for each; "
            "resource_owner_credentials and jwt_bearer OAuth2 grants "
            "are likewise deferred."
        ),
        (
            "OAuth2 client secret is stored as Boomi ciphertext. The builder "
            "emits an empty clientSecret attribute and an encryptedValues "
            "header marking the path isSet=false. After create, the value "
            "is supplied via the Boomi UI (or a pre-encrypted raw-XML "
            "payload via config.xml=...)."
        ),
    ],
    "recommended_workflow": [
        "1. Resolve the API base URL and OAuth2 access token endpoint.",
        "2. Create the REST connector-settings with auth='OAUTH2' and the "
        "oauth2 sub-block populated with placeholder client_id / "
        "client_secret_ref / access_token_url.",
        "3. Supply the real client_secret via the Boomi UI after create "
        "(builder never writes it into XML).",
        "4. Create the operation (connector-action rest.operation) and "
        "add this connection's key to depends_on plus connection_ref_key.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): GenericConnectionConfig "
        "<field id=\"...\"> children owned by the builder are replaced "
        "while unknown field ids and unknown siblings survive. "
        "bns:encryptedValues entries are merged by xpath — existing "
        "isSet=true secret slots survive; auth-mode changes (e.g. "
        "BASIC → NONE) prune the stale credential slot. KNOWN "
        "LIMITATION (Codex r6/r7 trade-off): structured updates reset "
        "the live OAuth2 token cache (accessToken/accessTokenKey live "
        "inside <field id=\"oauthContext\">, which the builder emits as "
        "a token-not-set skeleton) — re-authorize after a structured "
        "update. Use metadata-only build_integration update (only "
        "name/description/folder fields) or direct manage_connector "
        "action='update' to rename/move without touching the body, "
        "which keeps the token cache intact."
    ),
    "example": {
        "key": "target_rest_connection",
        "type": "connector-settings",
        "action": "create",
        "name": "<<target REST connection>>",
        "config": {
            "connector_type": "rest",
            "component_name": "<<target REST connection>>",
            "base_url": "https://<<host>>",
            "auth": "OAUTH2",
            "oauth2": {
                "grant_type": "client_credentials",
                "client_id": "<<client id>>",
                "client_secret_ref": "credential://<<vendor>>/<<role>>",
                "access_token_url": "https://<<host>>/oauth/token",
                "scope": "",
                "credentials_assertion_type": "client_secret",
            },
        },
        "_example_note": (
            "Placeholder values only. Fill in the API base URL, OAuth2 "
            "client id, token endpoint, and credential reference using the "
            "deployment context."
        ),
    },
    "out_of_scope": {
        "non_emitted_auth_modes": (
            "Connection emission for CUSTOM / PASSWORD_DIGEST / AWS_SIGNATURE "
            "/ AWS_IAM_ROLES_ANYWHERE is deferred until a verified live "
            "Boomi XML reference is available for each."
        ),
        "non_client_credentials_oauth2_grants": (
            "resource_owner_credentials and jwt_bearer grant types are "
            "deferred until verified live exports exist. The token-not-set "
            "authorization_code grant IS buildable; only authorization_code "
            "with cached access-token emission remains out of scope."
        ),
        "oauth2_parameter_blocks": (
            "oauth2.authorization_parameters and oauth2.access_token_parameters "
            "emission is deferred. The builder always emits empty "
            "<authorizationParameters/> and <accessTokenParameters/> elements; "
            "non-empty caller values are rejected with "
            "UNSUPPORTED_REST_OAUTH2_PARAMETERS until a verified live export "
            "shows how to render populated parameter children."
        ),
    },
    "alternative_examples": {
        "none_auth_minimal": {
            "key": "target_rest_none_connection",
            "type": "connector-settings",
            "action": "create",
            "name": "<<target REST connection>>",
            "config": {
                "connector_type": "rest",
                "component_name": "<<target REST connection>>",
                "base_url": "https://<<host>>",
                "auth": "NONE",
            },
            "_example_note": (
                "NONE auth: no credentials required. Use for public APIs or "
                "APIs behind a network ACL. Add private_certificate_ref / "
                "public_certificate_ref if the upstream requires mTLS — "
                "those refs are independent of auth selection."
            ),
        },
        "none_auth_with_cert_refs": {
            "key": "target_rest_mtls_connection",
            "type": "connector-settings",
            "action": "create",
            "name": "<<target REST mTLS connection>>",
            "config": {
                "connector_type": "rest",
                "component_name": "<<target REST mTLS connection>>",
                "base_url": "https://<<host>>",
                "auth": "NONE",
                "private_certificate_ref": "<<Boomi private cert component id>>",
                "public_certificate_ref": "<<Boomi public cert component id>>",
            },
            "_example_note": (
                "Cert refs accept Boomi certificate component IDs (GUIDs). "
                "Refs work with any auth — auth='NONE' is the typical mTLS "
                "shape but BASIC/OAUTH2 can also carry cert refs."
            ),
        },
        "none_auth_with_pooling": {
            "key": "target_rest_pooled_connection",
            "type": "connector-settings",
            "action": "create",
            "name": "<<target REST pooled connection>>",
            "config": {
                "connector_type": "rest",
                "component_name": "<<target REST pooled connection>>",
                "base_url": "https://<<host>>",
                "auth": "NONE",
                "connection_pooling": {
                    "enabled": True,
                    "max_total": 20,
                    "idle_timeout_seconds": 30,
                },
            },
            "_example_note": (
                "Connection pooling reduces handshake cost for high-RPS "
                "targets. Boomi defaults: max_total=20, "
                "idle_timeout_seconds=30."
            ),
        },
        "basic_auth_minimal": {
            "key": "target_rest_basic_connection",
            "type": "connector-settings",
            "action": "create",
            "name": "<<target REST BASIC connection>>",
            "config": {
                "connector_type": "rest",
                "component_name": "<<target REST BASIC connection>>",
                "base_url": "https://<<host>>",
                "auth": "BASIC",
                "username": "<<username>>",
                "credential_ref": "credential://<<vendor>>/<<role>>",
                "preemptive": False,
            },
            "_example_note": (
                "BASIC auth: username + opaque credential_ref. The actual "
                "password is supplied via the Boomi UI after create (or via "
                "a pre-encrypted raw-XML payload). preemptive=true sends "
                "the Authorization header on the first request without "
                "waiting for a 401 challenge — Boomi default is false."
            ),
        },
        "ntlm_auth_minimal": {
            "key": "target_rest_ntlm_connection",
            "type": "connector-settings",
            "action": "create",
            "name": "<<target REST NTLM connection>>",
            "config": {
                "connector_type": "rest",
                "component_name": "<<target REST NTLM connection>>",
                "base_url": "https://<<host>>",
                "auth": "NTLM",
                "username": "<<username>>",
                "credential_ref": "credential://<<vendor>>/<<role>>",
                "domain": "<<AD domain, e.g. corp.example.com>>",
                "workstation": "<<client workstation identity>>",
            },
            "_example_note": (
                "NTLM auth: username + opaque credential_ref + domain + "
                "workstation (all required). Used for Windows-integrated "
                "REST endpoints behind IIS or similar. preemptive is "
                "irrelevant for NTLM; Boomi emits an empty value."
            ),
        },
        "oauth2_authorization_code_token_not_set": {
            "key": "target_rest_oauth2_authcode_connection",
            "type": "connector-settings",
            "action": "create",
            "name": "<<target REST OAuth2 AuthCode connection>>",
            "config": {
                "connector_type": "rest",
                "component_name": "<<target REST OAuth2 AuthCode connection>>",
                "base_url": "https://<<host>>",
                "auth": "OAUTH2",
                "oauth2": {
                    "grant_type": "authorization_code",
                    "client_id": "<<client id>>",
                    "client_secret_ref": "credential://<<vendor>>/<<role>>",
                    "authorization_url": "https://<<host>>/oauth/authorize",
                    "access_token_url": "https://<<host>>/oauth/token",
                    "scope": "<<space-separated scopes>>",
                },
            },
            "_example_note": (
                "OAuth2 authorization_code (token-not-set) shape. The user "
                "completes the authorization handshake in the Boomi UI "
                "after create — the builder NEVER emits cached access "
                "tokens. Pass 'code' or 'authorization_code' as aliases "
                "for grant_type."
            ),
        },
    },
}


_COMPONENT_CREATE_CONNECTOR_SETTINGS_OVERVIEW = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-settings",
    "tool": "manage_connector (action='create')",
    "note": (
        "connector-settings (connections) are created via manage_connector with a "
        "connector_type that maps to a builder in CONNECTOR_BUILDERS. Pick a protocol "
        "for a fully-shaped template, or use the raw-XML escape hatch for unsupported "
        "connector types."
    ),
    "available_protocols": ["database.sqlserver", "rest.client"],
    "hint": (
        "Re-call get_schema_template(resource_type='component', operation='create', "
        "component_type='connector-settings', protocol='<protocol>') for the chosen "
        "protocol's full JSON template."
    ),
    "escape_hatch": (
        "For connector types without a builder, use manage_connector action='get' on "
        "an existing connector to export its XML, then pass as config.xml to "
        "manage_connector action='create'."
    ),
}


# ============================================================================
# Issue #23 (M2.3) — Database Read Profile (Select + Stored Procedure) +
# Database Get Operation templates
#
# Examples MUST use angle-bracket placeholders (<<task-authored SQL>>,
# <<field_name>>, <<schema.procedure>>, $ref:<key>). No canned SQL,
# procedure names, table/column names, CDS wrapper snippets, or payload
# templates. Database Send/write is OUT OF SCOPE — defer to issue #32
# (DatabaseSendAction, WriteProfile, commit semantics, JDBC batching,
# dynamic insert/update/delete).
# ============================================================================

_COMPONENT_CREATE_PROFILE_DB_DATABASE_READ = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.db",
    "protocol": "database.read",
    "tool": "manage_component (action='create')",
    "note": (
        "Database (Legacy) Read profile. Holds the Select SQL statement plus "
        "the result-set output-field shape and any input parameters. The "
        "operation step (connector-action database.get) references this "
        "profile by profileId — keep them in the same integration plan so "
        "$ref resolution wires the IDs together at apply time."
    ),
    "template": {
        "component_type": "profile.db",
        "profile_type": "database.read",
        "component_name": "<<read profile name>>",
        "folder_name": "<<folder>>",
        "description": "<<optional description>>",
        "query": "<<task-authored SQL>>",
        "output_fields": [
            {
                "name": "<<column_name>>",
                "data_type": "character",
                "mandatory": False,
                "enforce_unique": False,
            },
        ],
        "parameters": [
            {
                "name": "<<parameter_name>>",
                "data_type": "character",
                "mappable": False,
            },
        ],
    },
    "required": [
        "component_type",
        "profile_type",
        "component_name",
        "query",
        "output_fields",
    ],
    "defaults": {
        "profile_type": "database.read",
        "folder_name": "Home",
        "parameters": [],
    },
    "output_field_shape": {
        "name": {"type": "string", "required": True},
        "data_type": {
            "type": "string",
            "default": "character",
            "supported": ["character", "number", "datetime"],
            "note": (
                "Each Boomi dataType maps to a <DataFormat> child: "
                "character→ProfileCharacterFormat, number→ProfileNumberFormat, "
                "datetime→ProfileDateFormat."
            ),
        },
        "mandatory": {"type": "boolean", "default": False},
        "enforce_unique": {"type": "boolean", "default": False},
    },
    "parameter_shape": {
        "name": {"type": "string", "required": True},
        "data_type": {
            "type": "string",
            "default": "character",
            "supported": ["character", "number", "datetime"],
        },
        "mappable": {"type": "boolean", "default": False},
    },
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    ],
    "error_codes": {
        "MISSING_DB_QUERY": "query is absent or blank",
        "MISSING_DB_OUTPUT_FIELDS": "output_fields list is missing or empty",
        "UNSUPPORTED_DB_PROFILE_MODE": "profile_type is not database.read",
        "UNSUPPORTED_DB_PROFILE_FIELD_TYPE": "data_type not in character/number/datetime",
        "DATABASE_OPERATION_VALIDATION_FAILED": "shape / type / cross-field issue",
        "PLAINTEXT_SECRET_REJECTED": "a forbidden secret-shaped key appeared in config",
    },
    "gotchas": [
        (
            "SQL text is stored verbatim — the builder does not validate SQL "
            "syntax or auto-generate queries. Task-authored SQL only."
        ),
        (
            "Output fields must match the result-set column shape exactly. "
            "Boomi will not auto-derive them — the LLM declares them."
        ),
    ],
    "recommended_workflow": [
        "1. Author the Select SQL for the extraction step.",
        "2. Declare one output_fields entry per result-set column (name = column alias).",
        "3. Declare parameters[] for any '?' bind variables in the SQL.",
        "4. Plan the read profile alongside the matching connector-action "
        "(database.get) — depends_on the read profile key from the operation.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned "
        "DatabaseProfile/DataElements statement subtree is replaced while "
        "ProfileProperties, Namespaces, tagLists, and any unknown future "
        "DatabaseProfile siblings survive the update. Direct "
        "manage_component action='update' with config.xml remains a full "
        "XML replacement (no preservation)."
    ),
    "example": {
        "key": "db_read_profile",
        "type": "profile.db",
        "action": "create",
        "name": "<<read profile name>>",
        "config": {
            "component_type": "profile.db",
            "profile_type": "database.read",
            "component_name": "<<read profile name>>",
            "query": "<<task-authored SQL>>",
            "output_fields": [
                {"name": "<<column_name>>", "data_type": "character"},
            ],
            "parameters": [],
        },
        "_example_note": (
            "Placeholder values only. Do not copy this example as a starting "
            "SQL template — author the query based on task requirements."
        ),
    },
    "out_of_scope": {
        "write_profile": (
            "Database write profiles (Standard/Dynamic Insert/Update/Delete, "
            "Stored Procedure Write) are tracked by issue #32."
        ),
    },
    "see_also": {
        "stored_procedure_read": (
            "For procedure-based Read profiles, use "
            "protocol='database.stored_procedure_read' instead — that "
            "template emits statementType='spread', accepts a procedure_name "
            "config key, and supports parameters[].mode='in'/'out'/'in_out'/'return'."
        ),
    },
}


_COMPONENT_CREATE_PROFILE_DB_DATABASE_STORED_PROCEDURE_READ = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.db",
    "protocol": "database.stored_procedure_read",
    "tool": "manage_component (action='create')",
    "note": (
        "Database (Legacy) Read profile that invokes a Stored Procedure. "
        "Holds the procedure's fully-qualified name plus the result-set "
        "output-field shape and any IN/OUT/IN_OUT/RETURN parameters. The operation "
        "step (connector-action database.get) references this profile by "
        "profileId — keep them in the same integration plan so $ref "
        "resolution wires the IDs together at apply time."
    ),
    "template": {
        "component_type": "profile.db",
        "profile_type": "database.stored_procedure_read",
        "component_name": "<<read profile name>>",
        "folder_name": "<<folder>>",
        "description": "<<optional description>>",
        "procedure_name": "<<fully-qualified procedure name>>",
        "output_fields": [
            {
                "name": "<<column_name>>",
                "data_type": "character",
                "mandatory": False,
                "enforce_unique": False,
            },
        ],
        "parameters": [
            {
                "name": "<<parameter_name>>",
                "data_type": "character",
                "mode": "in",
                "mappable": False,
            },
        ],
    },
    "required": [
        "component_type",
        "profile_type",
        "component_name",
        "procedure_name",
        "output_fields",
    ],
    "defaults": {
        "profile_type": "database.stored_procedure_read",
        "folder_name": "Home",
        "parameters": [],
    },
    "output_field_shape": {
        "name": {"type": "string", "required": True},
        "data_type": {
            "type": "string",
            "default": "character",
            "supported": ["character", "number", "datetime"],
            "note": (
                "Each Boomi dataType maps to a <DataFormat> child: "
                "character→ProfileCharacterFormat, number→ProfileNumberFormat, "
                "datetime→ProfileDateFormat."
            ),
        },
        "mandatory": {"type": "boolean", "default": False},
        "enforce_unique": {"type": "boolean", "default": False},
    },
    "parameter_shape": {
        "name": {"type": "string", "required": True},
        "data_type": {
            "type": "string",
            "default": "character",
            "supported": ["character", "number", "datetime"],
        },
        "mode": {
            "type": "string",
            "default": "in",
            "supported": ["in", "out", "in_out", "return"],
            "note": (
                "Stored-procedure parameter direction. Default 'in' covers "
                "input parameters; 'out' for output parameters; 'in_out' "
                "(note underscore — Boomi's exact XML attribute value) for "
                "bidirectional parameters; 'return' for the procedure return "
                "value. At most one 'return' parameter is allowed per "
                "statement."
            ),
        },
        "mappable": {"type": "boolean", "default": False},
    },
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    ],
    "error_codes": {
        "MISSING_DB_PROCEDURE_NAME": "procedure_name is absent or blank",
        "MISSING_DB_OUTPUT_FIELDS": "output_fields list is missing or empty",
        "INVALID_DB_PARAMETER_MODE": "parameters[i].mode not in in/out/in_out/return",
        "MULTIPLE_DB_RETURN_PARAMETERS": "more than one parameter has mode='return' (Boomi allows at most one per statement)",
        "UNSUPPORTED_DB_PROFILE_MODE": "profile_type is not database.stored_procedure_read",
        "UNSUPPORTED_DB_PROFILE_FIELD_TYPE": "data_type not in character/number/datetime",
        "DATABASE_OPERATION_VALIDATION_FAILED": "shape / type / cross-field issue",
        "PLAINTEXT_SECRET_REJECTED": "a forbidden secret-shaped key appeared in config",
    },
    "gotchas": [
        (
            "procedure_name is stored verbatim — include schema and any "
            "vendor-specific syntax your database requires. The builder "
            "does not parse, validate, or normalize the procedure name. "
            "Vendor-specific examples (placeholders only): SQL Server "
            "'<<schema>>.<<proc>>;<<version>>', Oracle "
            "'<<package>>.<<proc>>', MySQL '<<db>>.<<proc>>', PostgreSQL "
            "'<<schema>>.<<proc>>'."
        ),
        (
            "parameters[].mode defaults to 'in'. Other valid values are "
            "'out', 'in_out' (note underscore — matches Boomi's XML "
            "attribute value exactly, NOT 'inout'), and 'return'."
        ),
        (
            "Only ONE parameter may have mode='return' per statement "
            "(Boomi platform constraint). Boomi UI guidance is to place "
            "the return parameter first in the list, but the builder "
            "preserves caller order and Boomi does not hard-enforce position."
        ),
        (
            "Stored procedures with no result set are not supported in "
            "v1 — output_fields must have at least one entry describing "
            "the procedure's result-set columns."
        ),
        (
            "The XML <sql/> element is emitted self-closing for SP "
            "profiles (the procedure dispatch comes from the storedProcedure "
            "attribute, not from inline SQL text)."
        ),
    ],
    "recommended_workflow": [
        "1. Identify the fully-qualified procedure name as your database "
        "vendor expects it.",
        "2. Declare one output_fields entry per result-set column.",
        "3. Declare parameters[] for each procedure parameter, choosing the "
        "correct mode ('in', 'out', 'in_out', or 'return') per the "
        "procedure signature. If the procedure exposes a return value, "
        "place the 'return' parameter first.",
        "4. Plan the read profile alongside the matching connector-action "
        "(database.get) — depends_on the read profile key from the operation.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned "
        "DatabaseProfile/DataElements statement subtree is replaced while "
        "ProfileProperties, Namespaces, tagLists, and any unknown future "
        "DatabaseProfile siblings survive the update. Direct "
        "manage_component action='update' with config.xml remains a full "
        "XML replacement (no preservation)."
    ),
    "example": {
        "key": "db_sp_read_profile",
        "type": "profile.db",
        "action": "create",
        "name": "<<read profile name>>",
        "config": {
            "component_type": "profile.db",
            "profile_type": "database.stored_procedure_read",
            "component_name": "<<read profile name>>",
            "procedure_name": "<<fully-qualified procedure name>>",
            "output_fields": [
                {"name": "<<column_name>>", "data_type": "character"},
            ],
            "parameters": [
                {"name": "<<parameter_name>>", "mode": "in"},
            ],
        },
        "_example_note": (
            "Placeholder values only. Do not copy this example as a starting "
            "template — supply procedure and parameter names that match the "
            "actual stored procedure being invoked."
        ),
    },
    "out_of_scope": {
        "no_result_set": (
            "Stored procedures that return no result set (pure-action procs) "
            "are not supported in v1. The output_fields list must have at "
            "least one entry."
        ),
        "write_profile": (
            "Database write profiles (Standard/Dynamic Insert/Update/Delete, "
            "Stored Procedure Write) are tracked by issue #32."
        ),
    },
    "see_also": {
        "select_statement_read": (
            "For Select-statement Read profiles, use "
            "protocol='database.read' instead — that template emits "
            "statementType='select' with caller-authored SQL."
        ),
    },
}


_COMPONENT_CREATE_PROFILE_DB_OVERVIEW = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.db",
    "tool": "manage_component (action='create')",
    "note": (
        "Database (Legacy) Read profile builders. Two statement-type variants "
        "are supported (issue #23): database.read (Select statement) and "
        "database.stored_procedure_read (Stored Procedure). Database write "
        "profiles are tracked by issue #32."
    ),
    "available_protocols": [
        "database.read",
        "database.stored_procedure_read",
    ],
    "hint": (
        "Re-call get_schema_template(resource_type='component', operation='create', "
        "component_type='profile.db', protocol='database.read') for a "
        "Select-statement Read profile, or protocol='database.stored_procedure_read' "
        "for a Stored Procedure Read profile."
    ),
    "escape_hatch": (
        "For profile shapes without a builder, use query_components action='get' "
        "on an existing profile to export its XML, then pass as config.xml to "
        "manage_component action='create'."
    ),
}


_COMPONENT_CREATE_CONNECTOR_ACTION_DATABASE_GET = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-action",
    "protocol": "database.get",
    "tool": "manage_connector (action='create')",
    "note": (
        "Database Get (read) operation. Wraps a previously-created database "
        "Read profile (profile.db) in a DatabaseGetAction envelope. The "
        "read profile may be a Select-statement Read (profile_type="
        "'database.read') or a Stored Procedure Read (profile_type="
        "'database.stored_procedure_read') — both are supported. The "
        "connection itself is bound at the process connector step, not in "
        "the operation XML — connection_ref_key is a plan-only dependency."
    ),
    "template": {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "get",
        "component_name": "<<operation name>>",
        "folder_name": "<<folder>>",
        "description": "<<optional description>>",
        "connection_ref_key": "<<db connection key>>",
        "read_profile_id": "$ref:<<db read profile key>>",
        "batch_count": 0,
        "max_rows": 0,
    },
    "required": [
        "component_type",
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "read_profile_id",
    ],
    "defaults": {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "get",
        "batch_count": 0,
        "max_rows": 0,
        "folder_name": "Home",
    },
    "supported_operation_modes": ["get"],
    "unsupported_operation_modes": ["send"],
    "unsupported_operation_modes_note": (
        "Database Send/write operations require DatabaseSendAction + "
        "WriteProfile and are tracked by issue #32 (M5.x). They will fail "
        "with UNSUPPORTED_DB_OPERATION_MODE in plan preflight."
    ),
    "link_element_status": "unsupported_pending_shape_verification",
    "link_element_note": (
        "Link Element groups/splits documents per Boomi docs, but its live "
        "XML attribute name has not been verified. Passing link_element "
        "fails with UNSUPPORTED_DB_GET_FIELD until a verified reference is "
        "available."
    ),
    "depends_on_requirements": [
        "Include connection_ref_key in depends_on (so the connector-settings runs first).",
        "When read_profile_id uses '$ref:KEY', include KEY in depends_on too.",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    ],
    "error_codes": {
        "UNSUPPORTED_DB_OPERATION_MODE": "operation_mode is 'send' or anything other than 'get'",
        "MISSING_DB_READ_PROFILE_REF": "read_profile_id absent or empty",
        "MISSING_DB_DEPENDENCY": "connection_ref_key / $ref target missing from depends_on",
        "DB_REF_TYPE_MISMATCH": "connection_ref_key or read_profile_id $ref points to a component of the wrong type at plan time (issue #49)",
        "UNSUPPORTED_DB_GET_FIELD": "link_element passed (deferred until shape confirmed)",
        "DATABASE_OPERATION_VALIDATION_FAILED": "shape / type / cross-field issue",
    },
    "gotchas": [
        (
            "Boomi binds the connection at the process connector step, not in "
            "the operation XML. The builder will NOT emit a connection ID — "
            "connection_ref_key is plan-only metadata for dependency ordering."
        ),
        (
            "batch_count=0 means no batching (one document per result-set row "
            "is the Boomi default). CDS-style large extracts use 50000."
        ),
        (
            "max_rows=0 means no limit."
        ),
    ],
    "recommended_workflow": [
        "1. Create the database connector-settings (manage_connector, connector_type=database).",
        "2. Create the read profile (manage_component, component_type=profile.db, profile_type=database.read OR database.stored_procedure_read).",
        "3. Plan this Get operation with depends_on=[<connection_key>, <read_profile_key>] and read_profile_id='$ref:<read_profile_key>'.",
        "4. Apply — $ref is resolved to the read profile's component_id from the id_registry.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' use "
        "read-merge-write semantics (issue #45): inside DatabaseGetAction "
        "the merge updates only the owned batchCount/maxRows attrs and the "
        "ReadProfile child, preserving unknown/future attrs or children on "
        "the action element. Operation-level Archiving, Tracking, Caching, "
        "and any unknown future siblings also survive. Direct "
        "manage_component action='update' with config.xml remains a full "
        "XML replacement (no preservation)."
    ),
    "example": {
        "key": "db_query_operation",
        "type": "connector-action",
        "action": "create",
        "name": "<<operation name>>",
        "depends_on": ["db_connection", "db_read_profile"],
        "config": {
            "component_type": "connector-action",
            "connector_type": "database",
            "operation_mode": "get",
            "component_name": "<<operation name>>",
            "connection_ref_key": "db_connection",
            "read_profile_id": "$ref:db_read_profile",
            "batch_count": 0,
            "max_rows": 0,
        },
        "_example_note": (
            "Placeholder values only. The read_profile_id $ref is substituted "
            "with the created profile's component_id during apply."
        ),
    },
    "out_of_scope": {
        "database_send": (
            "Database Send/write (DatabaseSendAction) is tracked by issue #32."
        ),
    },
}


# ---------------------------------------------------------------------------
# Issue #26 (M2.6): generated JSON profile, generated XML profile, direct map
# ---------------------------------------------------------------------------

_COMPONENT_CREATE_PROFILE_JSON_GENERATED = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.json",
    "protocol": "json.generated",
    "tool": "manage_component (action='create')",
    "note": (
        "Generated JSON profile. Builds a Boomi profile.json component from a "
        "structured field tree (root object → entries → optional nested "
        "object / repeating array nodes). M2 supports four scalar data types: "
        "character, number, datetime, boolean. The builder emits a deterministic "
        "JSONRootValue → JSONObject envelope with per-leaf field indexes that "
        "the direct map builder (protocol='direct') consumes to render "
        "<Mapping fromKey/toKey/fromKeyPath/toKeyPath/fromNamePath/toNamePath/>."
    ),
    "template": {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": "<<profile name>>",
        "folder_path": "<<optional folder>>",
        "description": "<<optional description>>",
        "root": {
            "name": "<<root node name>>",
            "kind": "object",
            "children": [
                {
                    "name": "<<leaf name>>",
                    "kind": "simple",
                    "data_type": "<<character | number | datetime | boolean>>",
                    "required": False,
                },
            ],
        },
    },
    "required": [
        "component_type",
        "profile_type",
        "component_name",
        "root",
    ],
    "defaults": {
        "component_type": "profile.json",
        "profile_type": "json.generated",
    },
    "supported_data_types": ["character", "number", "datetime", "boolean"],
    "supported_kinds": ["simple", "object", "array"],
    "field_tree_rules": [
        "Root must have kind='object'.",
        "Simple leaves carry a data_type and may NOT declare children.",
        "Object and array nodes require non-empty children and may NOT carry data_type.",
        "Reserved characters '/', '[', ']' are not allowed in node names "
        "(they form logical path segments and array repetition markers).",
        "Boomi JSON profiles synthesize a JSONObject wrapper named 'Object' "
        "inside the root and inside every JSONArrayElement; logical leaf "
        "paths use 'Root/list[]/key' but the emitted namePath is "
        "'Root/Object/list/Array/list/Object/key'.",
    ],
    "depends_on_requirements": [
        "No external dependencies. Generated profiles are self-contained.",
        "Downstream transform.map components reference this profile via "
        "'$ref:KEY' in their source_profile_id / target_profile_id and add "
        "the key to their depends_on.",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ],
    "error_codes": {
        "UNSUPPORTED_PROFILE_GENERATION_MODE": (
            "profile_type is not 'json.generated'"
        ),
        "PROFILE_FIELD_VALIDATION_FAILED": (
            "shape / cross-field issue in the field tree"
        ),
        "PROFILE_GENERATION_VALIDATION_FAILED": (
            "malformed root or node validated by profile_from_json_schema"
        ),
        "DUPLICATE_PROFILE_FIELD_PATH": (
            "two siblings inside an object/array share the same name"
        ),
        "UNSUPPORTED_PROFILE_FIELD_TYPE": (
            "data_type outside character/number/datetime/boolean"
        ),
        "INVALID_PROFILE_FIELD_PATH": (
            "node name contains '/', '[' or ']' (reserved path characters)"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a key in the config dict matches a secret-shaped substring"
        ),
    },
    "gotchas": [
        (
            "Boolean leaves emit an empty <DataFormat/> tag (matches live "
            "Boomi JSON profile reference). Other types use ProfileCharacterFormat / "
            "ProfileNumberFormat / ProfileDateFormat children."
        ),
        (
            "Arrays always wrap the array element's fields in a synthetic "
            "JSONObject (matches the live profile.json envelope). Arrays of "
            "pure scalars are not modeled in M2; wrap in an object with one "
            "scalar entry to express the same shape."
        ),
    ],
    "recommended_workflow": [
        "1. Declare the JSON field tree (root object, entries, optional nested object/array).",
        "2. Plan this profile component standalone or alongside a transform.map.",
        "3. Apply — the builder emits deterministic dense keys (starting at 1) "
        "and exposes a per-leaf path index for downstream maps.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned "
        "JSONProfile/DataElements subtree is replaced while JSONProfile "
        "siblings such as tagLists and any unknown future sections survive. "
        "Direct manage_component action='update' with config.xml remains a "
        "full XML replacement (no preservation)."
    ),
    "example": {
        "key": "request_json_profile",
        "type": "profile.json",
        "action": "create",
        "name": "<<profile display name>>",
        "config": {
            "component_type": "profile.json",
            "profile_type": "json.generated",
            "component_name": "<<profile display name>>",
            "root": {
                "name": "<<root name>>",
                "kind": "object",
                "children": [
                    {
                        "name": "<<scalar leaf>>",
                        "kind": "simple",
                        "data_type": "<<character | number | datetime | boolean>>",
                    },
                ],
            },
        },
        "_example_note": (
            "Placeholder values only. Replace the angle-bracket markers with "
            "your task-specific tree. No canned JSON payloads are shipped here."
        ),
    },
    "out_of_scope": {
        "inferred_from_sample_json": (
            "Inferring the field tree from a sample JSON document is available "
            "via infer_profile_fields(source_type='profile_from_sample_json') — "
            "read-only discovery (issue #47). It returns a builder-ready "
            "profile.json contract; ambiguous fields are flagged for confirmation."
        ),
        "edi_flatfile_profiles": (
            "EDI and flat-file profile families are deferred to later issues."
        ),
    },
}


_COMPONENT_CREATE_PROFILE_XML_GENERATED = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.xml",
    "protocol": "xml.generated",
    "tool": "manage_component (action='create')",
    "note": (
        "Generated XML profile (element-only). Builds a Boomi profile.xml "
        "component from a structured element tree where every node has "
        "kind='element'. Each element either contains children (structural) "
        "or carries a data_type (leaf). M2 supports four leaf data types: "
        "character, number, datetime, boolean (boolean stores as character "
        "format, mirroring live Boomi XML profile shape). Element-only — "
        "attributes, namespaces, and schema imports are rejected with "
        "UNSUPPORTED_XML_PROFILE_FEATURE — use the raw-XML escape hatch for "
        "those. To build this element-only tree from an existing XSD or sample "
        "XML, run infer_profile_fields (source_type='profile_from_xsd' | "
        "'profile_from_sample_xml', issue #47) first; it covers the "
        "namespace-less element-only subset only."
    ),
    "template": {
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "component_name": "<<profile name>>",
        "folder_path": "<<optional folder>>",
        "description": "<<optional description>>",
        "root": {
            "name": "<<root element name>>",
            "kind": "element",
            "min_occurs": 1,
            "max_occurs": 1,
            "children": [
                {
                    "name": "<<row element>>",
                    "kind": "element",
                    "max_occurs": -1,
                    "children": [
                        {
                            "name": "<<leaf>>",
                            "kind": "element",
                            "data_type": "<<character | number | datetime | boolean>>",
                        },
                    ],
                },
            ],
        },
    },
    "required": [
        "component_type",
        "profile_type",
        "component_name",
        "root",
    ],
    "defaults": {
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "min_occurs_root": 1,
        "max_occurs_root": 1,
        "min_occurs_child": 0,
        "max_occurs_child": 1,
    },
    "supported_data_types": ["character", "number", "datetime", "boolean"],
    "supported_kinds": ["element", "attribute"],
    "field_tree_rules": [
        "Nodes use kind='element' or kind='attribute'. Attributes are leaf-only "
        "and emit as <XMLAttribute> before sibling elements; namespaces attach "
        "via a node-level 'namespace': {uri, prefix?} field.",
        "An element with child ELEMENTS is structural (no data_type); an element "
        "with no child elements is a leaf (data_type required) and may still "
        "carry attribute children.",
        "max_occurs accepts a positive integer or -1 (unbounded). min_occurs "
        "is a non-negative integer.",
        "Reserved characters '/', '[', ']' are not allowed in node names.",
        "Repeating elements (max_occurs != 1) append '[]' to the logical "
        "path segment that their descendants use (matches the JSON profile "
        "convention).",
    ],
    "unsupported_features": [
        "attributes",
        "namespaces",
        "namespace_uri",
        "xsd",
        "schema_import",
    ],
    "unsupported_features_note": (
        "These raw config KEYS are not how XML structure is expressed: add "
        "attributes with kind='attribute' child nodes and namespaces with a "
        "node-level 'namespace': {uri, prefix?} field (both fully supported, "
        "emitting <XMLAttribute> / <XMLNamespace>+useNamespace). To build the "
        "tree from an XSD or sample, use infer_profile_fields (profile_from_xsd "
        "/ profile_from_sample_xml), which handle namespaces and attributes. "
        "Constructs none of these support — mixed content, choice/all/any/group, "
        "schema imports — require the raw-XML escape hatch (config={'xml': '...'})."
    ),
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ],
    "error_codes": {
        "UNSUPPORTED_PROFILE_GENERATION_MODE": (
            "profile_type is not 'xml.generated'"
        ),
        "UNSUPPORTED_XML_PROFILE_FEATURE": (
            "config carries an attribute/namespace/schema-import key not "
            "supported by element-only M2 generation"
        ),
        "PROFILE_FIELD_VALIDATION_FAILED": (
            "shape / cross-field issue in the element tree"
        ),
        "PROFILE_GENERATION_VALIDATION_FAILED": (
            "malformed root or node validated by profile_from_xml_schema"
        ),
        "DUPLICATE_PROFILE_FIELD_PATH": (
            "two sibling elements share the same name"
        ),
        "UNSUPPORTED_PROFILE_FIELD_TYPE": (
            "data_type outside character/number/datetime/boolean"
        ),
        "INVALID_PROFILE_FIELD_PATH": (
            "element name contains '/', '[' or ']' (reserved path characters)"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a key in the config dict matches a secret-shaped substring"
        ),
    },
    "depends_on_requirements": [
        "No external dependencies. Generated profiles are self-contained.",
    ],
    "recommended_workflow": [
        "1. Declare the XML element tree (root element, nested elements, leaves with data_type).",
        "2. Plan this profile component standalone or alongside a transform.map.",
        "3. Apply — the builder emits deterministic dense keys and exposes a "
        "per-leaf path index for downstream maps.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned "
        "XMLProfile/DataElements subtree is replaced while XMLProfile "
        "siblings such as Namespaces, ProfileProperties extras, tagLists, "
        "and any unknown future sections survive. Direct manage_component "
        "action='update' with config.xml remains a full XML replacement "
        "(no preservation)."
    ),
    "example": {
        "key": "shipping_xml_profile",
        "type": "profile.xml",
        "action": "create",
        "name": "<<profile display name>>",
        "config": {
            "component_type": "profile.xml",
            "profile_type": "xml.generated",
            "component_name": "<<profile display name>>",
            "root": {
                "name": "<<root>>",
                "kind": "element",
                "min_occurs": 1,
                "max_occurs": 1,
                "children": [
                    {
                        "name": "<<leaf>>",
                        "kind": "element",
                        "data_type": "<<character | number | datetime | boolean>>",
                    },
                ],
            },
        },
        "_example_note": (
            "Placeholder values only. Replace the angle-bracket markers with "
            "your task-specific element tree. No canned XML envelopes are "
            "shipped here."
        ),
    },
    "out_of_scope": {
        "inferred_from_xsd": (
            "Inferring the element tree from an XSD or sample XML is available "
            "via infer_profile_fields(source_type='profile_from_xsd') and "
            "infer_profile_fields(source_type='profile_from_sample_xml') — "
            "read-only discovery (issue #47). Both now support XML namespaces "
            "(targetNamespace / namespaced sample tags) and attributes; mixed "
            "content, choice/all/any/group, and foreign-namespace type refs "
            "still fail with actionable unsupported-shape errors."
        ),
        "attributes_and_namespaces": (
            "Element attributes and namespaces ARE supported: attributes infer "
            "as kind='attribute' nodes (emit <XMLAttribute>) and namespaces as a "
            "node-level 'namespace' field (emit <XMLNamespace> + useNamespace). "
            "Mixed content remains deferred to the raw-XML escape hatch."
        ),
    },
}


_COMPONENT_CREATE_TRANSFORM_MAP_DIRECT = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "transform.map",
    "protocol": "direct",
    "tool": "build_integration (action='plan' | 'apply')",
    "tool_note": (
        "Structured transform.map creation goes through build_integration so "
        "the map builder can compute source/target profile field indexes "
        "from in-spec '$ref:KEY' profile components. manage_component "
        "(action='create') only dispatches profile builders today — it does "
        "not understand structured field_mappings. To author a single map "
        "outside an integration spec, fall back to the raw-XML escape hatch "
        "via manage_component config={'xml': '<bns:Component ...>...'}"
    ),
    "note": (
        "Direct profile-to-profile transform.map. Renders one <Mapping/> per "
        "source/target leaf pair. M2 is direct-only — function (#40), script "
        "(#41), XSLT (#42), lookup, expression, and default routes are "
        "rejected at plan time with structured pointers. Source and target "
        "profile references must point at in-spec profile.json / profile.xml / "
        "profile.db components via '$ref:KEY'; literal existing-profile UUIDs "
        "are rejected with MAP_PROFILE_INDEX_UNAVAILABLE (indexing live "
        "existing-profile XML remains separate future work; infer_profile_fields "
        "infers only from supplied artifacts, not live profile XML)."
    ),
    "template": {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": "<<map name>>",
        "folder_path": "<<optional folder>>",
        "description": "<<optional description>>",
        "source_profile_id": "$ref:<<source profile key>>",
        "source_profile_type": "<<profile.db | profile.json | profile.xml>>",
        "target_profile_id": "$ref:<<target profile key>>",
        "target_profile_type": "<<profile.db | profile.json | profile.xml>>",
        "field_mappings": [
            {
                "source_path": "<<source logical path>>",
                "target_path": "<<target logical path>>",
            },
        ],
    },
    "required": [
        "component_type",
        "map_type",
        "component_name",
        "source_profile_id",
        "source_profile_type",
        "target_profile_id",
        "target_profile_type",
        "field_mappings",
    ],
    "defaults": {
        "component_type": "transform.map",
        "map_type": "direct",
    },
    "supported_map_types": ["direct", "function", "map_function"],
    "unsupported_routes": {
        "functions": (
            "Raw <Functions> XML is not accepted; switch to "
            "map_type='function' (#40) and declare structured function_mappings."
        ),
        "function_mappings": (
            "function_mappings belong to map_type='function' (#40); they are "
            "rejected on direct maps."
        ),
        "scripts": (
            "Switch to map_type='script' and declare script_mappings[] "
            "referencing a script.mapping component (#41 shipped)."
        ),
        "xslt": "#42 (XSLT deferred decision; unsupported in M2)",
        "default_values": (
            "Switch to map_type='function' and declare "
            "function_mappings[].function_type='default_value' (#40)."
        ),
        "lookup": (
            "Switch to map_type='function' and declare "
            "function_mappings[].function_type='simple_lookup' (#40)."
        ),
        "expression": (
            "Inline Boomi expressions are not a structured primitive. Use a "
            "native function via map_type='function' (#40), or wrap the logic "
            "in a script.mapping component called via map_type='script' (#41)."
        ),
    },
    "unsupported_routes_note": (
        "Direct maps are profile-to-profile only. Function-class routes "
        "(default/lookup/standard function primitives) are supported via "
        "map_type='function' (#40); reusable script-based transforms are "
        "supported via map_type='script' (#41). XSLT remains tracked by #42."
    ),
    "depends_on_requirements": [
        "Include source_profile_id's $ref key in depends_on so the source "
        "profile component runs first.",
        "Include target_profile_id's $ref key in depends_on so the target "
        "profile component runs first.",
        "Both profiles must be in-spec — literal existing-profile UUIDs "
        "produce MAP_PROFILE_INDEX_UNAVAILABLE because issue #26 does not "
        "parse arbitrary Boomi profile XML (live-profile-XML indexing remains "
        "separate future work; not infer_profile_fields).",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ],
    "error_codes": {
        "MAP_PROFILE_REF_REQUIRED": (
            "source_profile_id or target_profile_id missing / blank"
        ),
        "MAP_PROFILE_INDEX_UNAVAILABLE": (
            "literal existing-profile UUID supplied without an in-spec "
            "generated profile component to index (separate future work; infer_profile_fields does not index live existing-profile XML)"
        ),
        "MAP_FIELD_NOT_FOUND": (
            "source_path or target_path is not declared in the corresponding "
            "profile's field index"
        ),
        "DUPLICATE_TARGET_MAPPING": (
            "two field_mappings entries bind the same target_path"
        ),
        "UNSUPPORTED_TRANSFORM_ROUTE": (
            "config declares a function/script/xslt/lookup/expression/default "
            "route deferred to #40/#41/#42"
        ),
        "PROFILE_FIELD_NOT_MAPPABLE": (
            "source_path or target_path resolves to a structural node "
            "(object/array/non-leaf element)"
        ),
        "PROFILE_FIELD_VALIDATION_FAILED": (
            "shape / cross-field issue in the map config"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a key in the config dict matches a secret-shaped substring"
        ),
    },
    "gotchas": [
        (
            "$ref:KEY tokens are resolved at apply time. The plan-time "
            "validator uses the referenced component's config to compute "
            "the field index, then re-checks every source_path / target_path "
            "against the index. Renaming a referenced profile component "
            "after planning requires a fresh plan."
        ),
        (
            "Boomi maps reject duplicate target bindings — only one direct "
            "mapping may write each destination leaf. To fan in multiple "
            "sources, use a map_function (issue #40) when it lands."
        ),
        (
            "Source and target profile types may differ "
            "(profile.db→profile.json, profile.json→profile.xml, etc.). "
            "The builder reads the matching profile builder's "
            "build_field_index() to render fromKeyPath/toKeyPath consistently."
        ),
    ],
    "recommended_workflow": [
        "1. Create the source profile component (profile.db / profile.json / profile.xml).",
        "2. Create the target profile component (profile.db / profile.json / profile.xml).",
        "3. Plan this map with source_profile_id='$ref:<src key>', "
        "target_profile_id='$ref:<tgt key>', and depends_on=[<src>, <tgt>].",
        "4. Apply — $ref tokens resolve to real UUIDs, and the map XML is "
        "emitted with deterministic fromKey/toKey path references.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned <Map> "
        "subtree (mappings, functions, defaults) is replaced while "
        "bns:encryptedValues entries and unknown bns:Component-level "
        "children/attributes survive. Direct manage_component "
        "action='update' with config.xml remains a full XML replacement "
        "(no preservation)."
    ),
    "example": {
        "key": "db_to_json_map",
        "type": "transform.map",
        "action": "create",
        "name": "<<map display name>>",
        "depends_on": ["<<source profile key>>", "<<target profile key>>"],
        "config": {
            "component_type": "transform.map",
            "map_type": "direct",
            "component_name": "<<map display name>>",
            "source_profile_id": "$ref:<<source profile key>>",
            "source_profile_type": "<<profile.db | profile.json | profile.xml>>",
            "target_profile_id": "$ref:<<target profile key>>",
            "target_profile_type": "<<profile.db | profile.json | profile.xml>>",
            "field_mappings": [
                {
                    "source_path": "<<source logical path>>",
                    "target_path": "<<target logical path>>",
                },
            ],
        },
        "_example_note": (
            "Placeholder values only. Replace angle-bracket markers with "
            "task-specific keys / paths. No canned mappings are shipped here."
        ),
    },
    "out_of_scope": {
        "map_function_advanced": (
            "The first M2 standard function set (date_format, default_value, "
            "trim/left_trim/right_trim, uppercase/lowercase, append/prepend/"
            "replace/remove, simple_lookup, sequential_value, math) is "
            "supported on map_type='function' (#40). Standalone reusable "
            "transform.function components and chained multi-step function "
            "graphs remain future work."
        ),
        "xslt": (
            "XSLT support is explicitly deferred and out of M2 "
            "(issue #42). It is not a planned M2 builder; reopen only "
            "for XML-heavy migration, SOAP/XML-to-XML scenarios, or "
            "imported integration assets that already ship XSLT "
            "stylesheets."
        ),
        "existing_profile_index_discovery": (
            "Indexing arbitrary existing-profile XML to support literal-UUID "
            "profile refs remains separate future work (not infer_profile_fields, which infers from supplied artifacts only)."
        ),
    },
}


_COMPONENT_CREATE_TRANSFORM_MAP_FUNCTION = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "transform.map",
    "protocol": "function",
    "tool": "build_integration (action='plan' | 'apply')",
    "tool_note": (
        "Structured transform.map creation goes through build_integration so "
        "the map builder can compute source/target profile field indexes "
        "from in-spec '$ref:KEY' profile components. manage_component "
        "(action='create') only dispatches profile builders today — it does "
        "not understand structured function_mappings. To author a single map "
        "outside an integration spec, fall back to the raw-XML escape hatch "
        "via manage_component config={'xml': '<bns:Component ...>...'}"
    ),
    "note": (
        "Structured map-function transform.map. Each entry in function_mappings "
        "declares one mapped output via {function_type, inputs, target_path, "
        "parameters}. M2.6a supports a 14-family allow-list: date_format, "
        "default_value, trim, left_trim, right_trim, uppercase, lowercase, "
        "append, prepend, replace, remove, simple_lookup, sequential_value, "
        "math. Mixed maps may also declare direct field_mappings alongside "
        "function_mappings. Source/target profile refs follow the same "
        "'$ref:KEY' rule as direct maps; literal existing-profile UUIDs are "
        "rejected with MAP_PROFILE_INDEX_UNAVAILABLE (separate future work; infer_profile_fields does not index live existing-profile XML). "
        "Reusable script-based transforms (Groovy / JavaScript) ship in "
        "map_type='script' (#41). XSLT (#42), standalone reusable "
        "transform.function components, and chained multi-step function "
        "graphs remain out of scope."
    ),
    "template": {
        "component_type": "transform.map",
        "map_type": "function",
        "component_name": "<<map name>>",
        "folder_path": "<<optional folder>>",
        "description": "<<optional description>>",
        "source_profile_id": "$ref:<<source profile key>>",
        "source_profile_type": "<<profile.db | profile.json | profile.xml>>",
        "target_profile_id": "$ref:<<target profile key>>",
        "target_profile_type": "<<profile.db | profile.json | profile.xml>>",
        "field_mappings": [
            {
                "source_path": "<<optional direct source path>>",
                "target_path": "<<optional direct target path>>",
            },
        ],
        "function_mappings": [
            {
                "function_type": "<<one of the supported_function_types keys>>",
                "inputs": ["<<source logical path>>"],
                "target_path": "<<target logical path>>",
                "parameters": {
                    "<<parameter key>>": "<<parameter value placeholder>>",
                },
            },
        ],
    },
    "required": [
        "component_type",
        "map_type",
        "component_name",
        "source_profile_id",
        "source_profile_type",
        "target_profile_id",
        "target_profile_type",
        "function_mappings",
    ],
    "optional": [
        "field_mappings",
        "folder_path",
        "description",
    ],
    "defaults": {
        "component_type": "transform.map",
        "map_type": "function",
    },
    "supported_map_types": ["function", "map_function"],
    "supported_function_types": {
        "date_format": {
            "mapped_inputs": 1,
            "required_parameters": ["input_format", "output_format"],
            "optional_parameters": [],
            "note": (
                "Inputs[0] = source date string. parameters.input_format and "
                "parameters.output_format follow Boomi date pattern syntax."
            ),
        },
        "default_value": {
            "mapped_inputs": 0,
            "required_parameters": ["value"],
            "optional_parameters": [],
            "note": (
                "Emits a <Default toKey value/> entry inside <Defaults> rather "
                "than a <FunctionStep>; parameters.value is the literal "
                "default written to target_path."
            ),
        },
        "trim": {
            "mapped_inputs": 1,
            "required_parameters": [],
            "optional_parameters": [],
            "note": "Whitespace trim (TrimWhitespace).",
        },
        "left_trim": {
            "mapped_inputs": 1,
            "required_parameters": ["fix_to_length"],
            "optional_parameters": [],
            "note": "Truncate to first parameters.fix_to_length characters.",
        },
        "right_trim": {
            "mapped_inputs": 1,
            "required_parameters": ["fix_to_length"],
            "optional_parameters": [],
            "note": "Truncate to last parameters.fix_to_length characters.",
        },
        "uppercase": {
            "mapped_inputs": 1,
            "required_parameters": [],
            "optional_parameters": [],
            "note": "StringToUpper.",
        },
        "lowercase": {
            "mapped_inputs": 1,
            "required_parameters": [],
            "optional_parameters": [],
            "note": "StringToLower.",
        },
        "append": {
            "mapped_inputs": 1,
            "required_parameters": ["value"],
            "optional_parameters": ["fix_to_length"],
            "note": (
                "Append parameters.value to source string. "
                "parameters.fix_to_length is optional pad/fix length."
            ),
        },
        "prepend": {
            "mapped_inputs": 1,
            "required_parameters": ["value"],
            "optional_parameters": ["fix_to_length"],
            "note": "Prepend parameters.value to source string.",
        },
        "replace": {
            "mapped_inputs": 1,
            "required_parameters": ["search", "replacement"],
            "optional_parameters": [],
            "note": (
                "Search-and-replace on the source string. parameters.search "
                "may be a regex pattern; parameters.replacement is the "
                "replacement string."
            ),
        },
        "remove": {
            "mapped_inputs": 1,
            "required_parameters": ["value"],
            "optional_parameters": [],
            "note": "Remove all occurrences of parameters.value.",
        },
        "simple_lookup": {
            "mapped_inputs": 1,
            "required_parameters": ["rows"],
            "optional_parameters": [],
            "note": (
                "parameters.rows is a non-empty list of {ref1, ref2} (or "
                "{from, to}) entries. The lookup table is task-authored — no "
                "canned reference data is shipped here. Boomi stores the "
                "table internally via a CrossRefTableObj wrapper; the "
                "builder handles that encoding."
            ),
        },
        "sequential_value": {
            "mapped_inputs": 0,
            "required_parameters": ["key_name"],
            "optional_parameters": ["fix_to_length", "batch_size"],
            "note": (
                "Source-free counter. parameters.key_name is an arbitrary "
                "unique name that identifies the counter (the runtime "
                "stores the latest value here for cross-execution "
                "continuation). fix_to_length zero-pads the result to a "
                "fixed width. batch_size reserves a block of sequence "
                "values in memory (default 1). Boomi stores all three as "
                "Input default attributes on the FunctionStep, not on the "
                "<SequentialValue/> Configuration block."
            ),
        },
        "math": {
            "mapped_inputs": "1 or 2 depending on parameters.operation",
            "required_parameters": ["operation"],
            "optional_parameters": ["precision"],
            "applicability": (
                "parameters.precision is only valid when operation == "
                "'set_precision'; supplying it with any other operation "
                "fails plan-time with MAP_FUNCTION_PARAMETER_INVALID."
            ),
            "supported_operations": [
                "add",
                "subtract",
                "multiply",
                "divide",
                "set_precision",
                "ceil",
                "floor",
                "abs",
            ],
            "note": (
                "operation dispatches to one of MathAdd/MathSubtract/"
                "MathMultiply/MathDivide/MathSetPrecision/MathCeil/MathFloor/"
                "MathABS. add/subtract/multiply/divide take 2 mapped inputs; "
                "set_precision takes 1 mapped input plus parameters.precision; "
                "ceil/floor/abs take 1 mapped input."
            ),
        },
    },
    "unsupported_routes": {
        "functions": (
            "Raw <Functions> XML escape hatch is not accepted; use the "
            "structured function_mappings contract instead."
        ),
        "function_steps": (
            "Raw <FunctionStep> XML escape hatch is not accepted; use the "
            "structured function_mappings contract instead."
        ),
        "scripts": (
            "Switch to map_type='script' and declare script_mappings[] "
            "referencing a script.mapping component (#41 shipped)."
        ),
        "map_scripts": (
            "Switch to map_type='script' and declare script_mappings[] "
            "referencing a script.mapping component (#41 shipped)."
        ),
        "xslt": "#42 (XSLT deferred decision; unsupported in M2)",
        "xslt_source": "#42 (XSLT deferred decision; unsupported in M2)",
        "expression": (
            "Inline Boomi expressions are not a structured primitive. Use a "
            "native function via map_type='function' (#40), or wrap the "
            "logic in a script.mapping component called via "
            "map_type='script' (#41)."
        ),
        "default_values": (
            "Use function_mappings[].function_type='default_value' instead "
            "of the raw <Defaults> escape hatch."
        ),
        "lookup": (
            "Use function_mappings[].function_type='simple_lookup' instead "
            "of the raw lookup escape hatch."
        ),
    },
    "unsupported_routes_note": (
        "Function-map authors go through the structured registry. Raw "
        "<Functions>/<FunctionStep> XML, raw <Defaults>, raw lookup blocks, "
        "scripts, XSLT, and free-form expressions all fail plan-time with "
        "UNSUPPORTED_TRANSFORM_ROUTE."
    ),
    "depends_on_requirements": [
        "Include source_profile_id's $ref key in depends_on so the source "
        "profile component runs first.",
        "Include target_profile_id's $ref key in depends_on so the target "
        "profile component runs first.",
        "Both profiles must be in-spec — literal existing-profile UUIDs "
        "produce MAP_PROFILE_INDEX_UNAVAILABLE (indexing live existing-profile "
        "XML remains separate future work; not infer_profile_fields).",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ],
    "error_codes": {
        "MAP_PROFILE_REF_REQUIRED": (
            "source_profile_id or target_profile_id missing / blank, or a "
            "$ref target was not declared in depends_on"
        ),
        "MAP_PROFILE_INDEX_UNAVAILABLE": (
            "literal existing-profile UUID supplied without an in-spec "
            "generated profile component to index (separate future work; infer_profile_fields does not index live existing-profile XML)"
        ),
        "MAP_FIELD_NOT_FOUND": (
            "function_mappings[].inputs[] or .target_path is not declared in "
            "the corresponding profile's field index"
        ),
        "DUPLICATE_TARGET_MAPPING": (
            "two entries (from function_mappings + field_mappings combined) "
            "bind the same target_path"
        ),
        "UNSUPPORTED_TRANSFORM_ROUTE": (
            "config declares a raw <Functions>/<Defaults>/<Lookup> escape "
            "hatch, scripts, XSLT, or expressions"
        ),
        "UNSUPPORTED_MAP_FUNCTION_TYPE": (
            "function_type is not in the supported 14-family allow-list"
        ),
        "MAP_FUNCTION_INPUT_COUNT_MISMATCH": (
            "function_mappings[].inputs count does not match the family's "
            "mapped-input rule"
        ),
        "MAP_FUNCTION_PARAMETER_MISSING": (
            "a required parameter for the function family is missing or empty"
        ),
        "MAP_FUNCTION_PARAMETER_INVALID": (
            "a parameter value fails type / enum validation for the family"
        ),
        "UNSUPPORTED_MATH_OPERATION": (
            "parameters.operation for the math family is not in the supported "
            "operation set"
        ),
        "PROFILE_FIELD_NOT_MAPPABLE": (
            "an input or target path resolves to a structural node "
            "(object/array/non-leaf element)"
        ),
        "PROFILE_FIELD_VALIDATION_FAILED": (
            "shape / cross-field issue in the map config"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a key in the config dict (including inside function_mappings or "
            "parameters) matches a secret-shaped substring"
        ),
    },
    "gotchas": [
        (
            "Each function_mapping produces ONE target output in M2.6a. "
            "Multi-output graphs (StringSplit, user-defined functions) and "
            "chained function steps are future work."
        ),
        (
            "Mapping order is deterministic: direct field_mappings first, "
            "then for each function in declaration order — its source-to-"
            "input mappings, then its output-to-target mapping. Function IDs "
            "and positions match the 1-based index of function_mappings."
        ),
        (
            "default_value entries do NOT emit a <FunctionStep>; they emit "
            "<Default toKey value/> inside the map's <Defaults> block. "
            "parameters.value is the literal written verbatim to the target "
            "leaf."
        ),
        (
            "simple_lookup rows are task-authored — provide your own "
            "{ref1, ref2} (or {from, to}) entries. No canned reference data "
            "is shipped here."
        ),
        (
            "Boomi maps reject duplicate target bindings — only one entry "
            "(direct or function) may write each destination leaf."
        ),
    ],
    "recommended_workflow": [
        "1. Create the source profile component (profile.db / profile.json / profile.xml).",
        "2. Create the target profile component (profile.db / profile.json / profile.xml).",
        "3. Plan this map with map_type='function', source_profile_id='$ref:<src key>', "
        "target_profile_id='$ref:<tgt key>', depends_on=[<src>, <tgt>], "
        "and one entry per transformed output in function_mappings.",
        "4. Apply — $ref tokens resolve to real UUIDs, function steps are "
        "emitted with deterministic IDs, and mapping order is stable "
        "across repeated builds.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned <Map> "
        "subtree (mappings, FunctionSteps, defaults) is replaced while "
        "bns:encryptedValues and unknown bns:Component-level children/"
        "attributes survive. Direct manage_component action='update' with "
        "config.xml remains a full XML replacement (no preservation)."
    ),
    "example": {
        "key": "db_to_json_function_map",
        "type": "transform.map",
        "action": "create",
        "name": "<<map display name>>",
        "depends_on": ["<<source profile key>>", "<<target profile key>>"],
        "config": {
            "component_type": "transform.map",
            "map_type": "function",
            "component_name": "<<map display name>>",
            "source_profile_id": "$ref:<<source profile key>>",
            "source_profile_type": "<<profile.db | profile.json | profile.xml>>",
            "target_profile_id": "$ref:<<target profile key>>",
            "target_profile_type": "<<profile.db | profile.json | profile.xml>>",
            "function_mappings": [
                {
                    "function_type": "<<one supported function family>>",
                    "inputs": ["<<source logical path>>"],
                    "target_path": "<<target logical path>>",
                    "parameters": {
                        "<<parameter key>>": "<<parameter value placeholder>>",
                    },
                },
            ],
        },
        "_example_note": (
            "Placeholder values only. Replace angle-bracket markers with "
            "task-specific keys / paths / parameters. No canned mappings or "
            "lookup rows are shipped here."
        ),
    },
    "out_of_scope": {
        "standalone_transform_function_authoring_surface": (
            "There is no first-class authoring surface for standalone "
            "transform.function components in the function-map route. "
            "#41 introduces a script-wrapper specialization (auto-synth "
            "or caller-declared) for bridging maps to script.mapping "
            "components; a general-purpose transform.function builder "
            "for non-script userdefined function graphs remains future "
            "work."
        ),
        "chained_function_graphs": (
            "Wiring multi-step function pipelines (function-A.output -> "
            "function-B.input) remains future work; M2.6a is one function "
            "per target output."
        ),
        "xslt": (
            "XSLT support is explicitly deferred and out of M2 "
            "(issue #42). It is not a planned M2 builder; reopen only "
            "for XML-heavy migration, SOAP/XML-to-XML scenarios, or "
            "imported integration assets that already ship XSLT "
            "stylesheets."
        ),
        "existing_profile_index_discovery": (
            "Indexing arbitrary existing-profile XML to support literal-UUID "
            "profile refs remains separate future work (not infer_profile_fields, which infers from supplied artifacts only)."
        ),
    },
}


_COMPONENT_CREATE_SCRIPT_MAPPING = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "script.mapping",
    "tool": "manage_component (action='create')",
    "tool_note": (
        "Reusable script.mapping components are standalone — they can be "
        "created via manage_component directly, or declared as an in-spec "
        "component in build_integration alongside the transform.map that "
        "calls them via map_type='script' (#41)."
    ),
    "note": (
        "Wraps a caller-authored Boomi Map Script in a structured component "
        "with declared <Input> and <Output> variables. Boomi sets mapped "
        "input values before the script runs; the script must assign each "
        "<Output> variable before returning. The component is referenced "
        "from a transform.map via map_type='script' and "
        "script_mappings[].script_component_id."
    ),
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned "
        "<MappingScript> subtree (script body, declared Input/Output "
        "ports, language/preserveOrder/useCache attrs) is replaced while "
        "bns:encryptedValues, unknown bns:Component-level children, and "
        "any unknown <bns:object> siblings survive. Direct manage_component "
        "action='update' with config.xml remains a full XML replacement."
    ),
    "template": {
        "component_type": "script.mapping",
        "component_name": "<<script display name>>",
        "folder_path": "<<optional folder>>",
        "description": "<<optional description>>",
        "language": "<<groovy2 | groovy | javascript>>",
        "script_body": "<<caller-authored script body>>",
        "inputs": [
            {
                "name": "<<inputVarName>>",
                "data_type": "<<character | date | integer | float>>",
            },
        ],
        "outputs": [
            {
                "name": "<<outputVarName>>",
            },
        ],
        "preserve_order": True,
        "use_cache": True,
    },
    "required": [
        "component_type",
        "component_name",
        "language",
        "script_body",
        "inputs",
        "outputs",
    ],
    "optional": [
        "folder_path",
        "description",
        "preserve_order",
        "use_cache",
    ],
    "defaults": {
        "component_type": "script.mapping",
        "preserve_order": True,
        "use_cache": True,
    },
    "supported_languages": ["groovy", "groovy2", "javascript"],
    "supported_input_data_types": [
        "character",
        "date",
        "integer",
        "float",
    ],
    "output_data_type_inference_note": (
        "Output entries declare only 'name' — Boomi infers each output's "
        "data type from the value the script assigns at runtime. "
        "Supplying 'data_type' on an output is rejected as misleading."
    ),
    "variable_name_rule": (
        "Input and output variable names share one namespace inside the "
        "script body. Names must match ^[A-Za-z_][A-Za-z0-9_]*$ and must "
        "be unique across inputs + outputs."
    ),
    "indexing_rule": (
        "<Input> entries receive 1-based dataType/index/name attributes in "
        "declaration order. <Output> entries continue monotonically — the "
        "first Output's index is len(inputs) + 1. Live Boomi exports use "
        "exactly this pattern (verified across two work-account references)."
    ),
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ],
    "error_codes": {
        "SCRIPT_MAPPING_VALIDATION_FAILED": (
            "shape / unknown-key / type-check failure not covered by a "
            "more specific code"
        ),
        "SCRIPT_MAPPING_BODY_REQUIRED": (
            "script_body missing or blank"
        ),
        "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED": (
            "language not in (groovy, groovy2, javascript)"
        ),
        "SCRIPT_MAPPING_VARIABLE_INVALID": (
            "input/output variable name invalid, duplicate, or carries "
            "an unsupported data_type"
        ),
        "UNSUPPORTED_TRANSFORM_ROUTE": (
            "raw <Functions>/<scripts>/<xslt>/<expression> escape-hatch "
            "keys are not accepted in a script.mapping config"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a dict key inside the config matches a secret-shaped name"
        ),
    },
    "gotchas": [
        (
            "Boomi's Custom script docs note: character inputs are passed "
            "as empty strings for null/omitted source values; date, "
            "integer, and float inputs can be null. Script authors must "
            "handle nulls explicitly for non-character inputs."
        ),
        (
            "preserveOrder='true' tells Boomi to set mapped inputs in the "
            "declared order before running the script — important when "
            "the script computes outputs that depend on input order."
        ),
        (
            "Output values' types are inferred from what the script "
            "assigns. Returning the wrong type (e.g. a non-numeric "
            "string when the target field is numeric) surfaces at apply "
            "time, not at component create time."
        ),
    ],
    "example": {
        "key": "<<script_key>>",
        "type": "script.mapping",
        "action": "create",
        "name": "<<script display name>>",
        "config": {
            "component_type": "script.mapping",
            "component_name": "<<script display name>>",
            "language": "groovy2",
            "script_body": "<<caller-authored script body>>",
            "inputs": [
                {"name": "<<inputVarName>>", "data_type": "character"},
            ],
            "outputs": [
                {"name": "<<outputVarName>>"},
            ],
            "preserve_order": True,
            "use_cache": True,
        },
        "_example_note": (
            "Placeholder values only. Replace angle-bracket markers with "
            "task-specific keys / variable names / script body. No canned "
            "Groovy or JavaScript business logic is shipped here."
        ),
    },
    "out_of_scope": {
        "script_processing": (
            "Process-level Groovy / JavaScript (script.processing, Data "
            "Process custom scripting) is not a map primitive and is "
            "explicitly NOT a fallback for unsupported script-map "
            "requests."
        ),
        "standalone_transform_function_authoring_surface": (
            "There is no first-class authoring surface for standalone "
            "transform.function components beyond the script-wrapper "
            "specialization #41 ships. The integration builder "
            "auto-synthesizes wrappers from in-spec script.mapping refs "
            "and accepts caller-declared transform.function components "
            "for existing-script reuse, but a general-purpose "
            "transform.function builder (for non-script userdefined "
            "function graphs) remains future work."
        ),
        "discovered_runtime_typing": (
            "The builder does not introspect the script body to infer "
            "input or output runtime types. Authors declare data_type "
            "on inputs; output types come from the assigned value at "
            "run time."
        ),
    },
}


_COMPONENT_CREATE_TRANSFORM_MAP_SCRIPT = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "transform.map",
    "protocol": "script",
    "tool": "build_integration (action='plan' | 'apply')",
    "tool_note": (
        "Script-route transform.map creation goes through build_integration "
        "so the map builder can compute source/target profile field indexes "
        "from in-spec '$ref:KEY' profile components and resolve any "
        "'$ref:KEY' script_component_id references via depends_on. "
        "manage_component (action='create') only dispatches profile builders "
        "today — it does not understand structured script_mappings."
    ),
    "note": (
        "In-map calls to one or more reusable script.mapping components. "
        "Each entry in script_mappings declares one userdefined "
        "<FunctionStep> with {script_component_id, inputs, outputs}. inputs "
        "map source-profile paths to script input variables; outputs map "
        "script output variables to target-profile paths. Mixed maps may "
        "also declare direct field_mappings alongside script_mappings. "
        "Source/target profile refs follow the same '$ref:KEY' rule as "
        "direct and function maps; literal existing-profile UUIDs are "
        "rejected with MAP_PROFILE_INDEX_UNAVAILABLE (separate future work; infer_profile_fields does not index live existing-profile XML)."
    ),
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned <Map> "
        "subtree (mappings, FunctionSteps including userdefined script "
        "refs, defaults) is replaced while bns:encryptedValues and unknown "
        "bns:Component-level children/attributes survive. Direct "
        "manage_component action='update' with config.xml remains a full "
        "XML replacement (no preservation)."
    ),
    "template": {
        "component_type": "transform.map",
        "map_type": "script",
        "component_name": "<<map name>>",
        "folder_path": "<<optional folder>>",
        "description": "<<optional description>>",
        "source_profile_id": "$ref:<<source profile key>>",
        "source_profile_type": "<<profile.db | profile.json | profile.xml>>",
        "target_profile_id": "$ref:<<target profile key>>",
        "target_profile_type": "<<profile.db | profile.json | profile.xml>>",
        "field_mappings": [
            {
                "source_path": "<<optional direct source path>>",
                "target_path": "<<optional direct target path>>",
            },
        ],
        "script_mappings": [
            {
                "script_slot": "<<task-authored slot name>>",
                "script_component_id": "$ref:<<script.mapping key>>",
                "language": "<<groovy2 | groovy | javascript (informational)>>",
                "cache_enabled": False,
                "inputs": [
                    {
                        "source_path": "<<source logical path>>",
                        "input_name": "<<matches script.mapping <Input name>>>",
                    },
                ],
                "outputs": [
                    {
                        "output_name": "<<matches script.mapping <Output name>>>",
                        "target_path": "<<target logical path>>",
                    },
                ],
            },
        ],
    },
    "required": [
        "component_type",
        "map_type",
        "component_name",
        "source_profile_id",
        "source_profile_type",
        "target_profile_id",
        "target_profile_type",
        "script_mappings",
    ],
    "optional": [
        "field_mappings",
        "folder_path",
        "description",
    ],
    "defaults": {
        "component_type": "transform.map",
        "map_type": "script",
    },
    "supported_map_types": ["script", "map_script"],
    "script_component_id_rule": (
        "Each script_mappings entry references a script.mapping or a "
        "transform.function wrapper via '$ref:<key>' pointing at an in-spec "
        "component. Literal componentId values are NOT accepted at this "
        "level — Boomi requires the map FunctionStep id to point at a "
        "transform.function wrapper, which the system can only synthesize "
        "from in-spec components. For '$ref:<script_key>' against an "
        "in-spec script.mapping, the plan automatically synthesizes a "
        "transform.function wrapper that bridges the map to the "
        "script.mapping; the synthesized wrapper applies in topological "
        "order before the calling map and is visible as a first-class "
        "component in the plan output. To reuse an EXISTING Boomi "
        "script.mapping, declare a transform.function wrapper as an "
        "in-spec component (component_type='transform.function' with "
        "script_component_id referencing the existing script.mapping by "
        "literal UUID or by another $ref) and reference that wrapper "
        "from the map."
    ),
    "in_map_xml_shape_note": (
        "Each script call emits a userdefined <FunctionStep "
        "category='userdefined' type='userdefined' id='<wrapper_componentId>'> "
        "with an empty <Configuration/>. Boomi's live shape REQUIRES the "
        "userdefined id to point at a transform.function wrapper component, "
        "NOT at the script.mapping directly — the wrapper internally "
        "references the script.mapping via <Configuration><Scripting "
        "componentId='...' useComponent='true'>. The integration builder "
        "auto-synthesizes the wrapper from the referenced script.mapping's "
        "structure, so callers only declare the script.mapping + reference "
        "it via '$ref:<script_key>'."
    ),
    "unsupported_routes": {
        "functions": (
            "Raw <Functions> XML escape hatch is not accepted; use the "
            "structured script_mappings contract instead."
        ),
        "function_steps": (
            "Raw <FunctionStep> XML escape hatch is not accepted; use the "
            "structured script_mappings contract instead."
        ),
        "function_mappings": (
            "Native map-function primitives belong to map_type='function' "
            "(#40); split function + script work across separate maps or "
            "use one map_type per map."
        ),
        "scripts": (
            "Raw <scripts> XML is not accepted; reference a reusable "
            "script.mapping component via script_mappings[].script_component_id."
        ),
        "map_scripts": (
            "Raw <map_scripts> XML is not accepted; reference a reusable "
            "script.mapping component via script_mappings[].script_component_id."
        ),
        "xslt": "#42 (XSLT deferred decision; unsupported in M2)",
        "xslt_source": "#42 (XSLT deferred decision; unsupported in M2)",
        "expression": (
            "Inline Boomi expressions are not a structured primitive. "
            "Wrap the logic in a script.mapping component."
        ),
        "default_values": (
            "Use map_type='function' and declare "
            "function_mappings[].function_type='default_value' (#40)."
        ),
        "lookup": (
            "Use map_type='function' and declare "
            "function_mappings[].function_type='simple_lookup' (#40)."
        ),
    },
    "unsupported_routes_note": (
        "Script-map authors go through the structured script_mappings "
        "contract. Raw XML escape hatches and route classes that belong "
        "to other map types reject with structured errors."
    ),
    "depends_on_requirements": [
        "Include source_profile_id's $ref key in depends_on so the source "
        "profile component runs first.",
        "Include target_profile_id's $ref key in depends_on so the target "
        "profile component runs first.",
        "Include every script_mappings[].script_component_id's $ref key in "
        "depends_on so each referenced script.mapping component runs before "
        "this map.",
        "Both profiles must be in-spec — literal existing-profile UUIDs "
        "produce MAP_PROFILE_INDEX_UNAVAILABLE (separate future work; infer_profile_fields does not index live existing-profile XML).",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
        "api_key",
        "credentials",
        "authorization",
        "bearer",
    ],
    "error_codes": {
        "MAP_PROFILE_REF_REQUIRED": (
            "source_profile_id or target_profile_id missing / blank, or a "
            "$ref target isn't declared in depends_on"
        ),
        "MAP_PROFILE_INDEX_UNAVAILABLE": (
            "literal existing-profile UUID supplied without an in-spec "
            "generated profile component to index (separate future work; infer_profile_fields does not index live existing-profile XML)"
        ),
        "MAP_FIELD_NOT_FOUND": (
            "a script input's source_path or output's target_path is not "
            "declared in the corresponding profile's field index"
        ),
        "DUPLICATE_TARGET_MAPPING": (
            "two entries — across field_mappings and script_mappings "
            "outputs — bind the same target_path"
        ),
        "UNSUPPORTED_TRANSFORM_ROUTE": (
            "config declares a function/script/xslt/lookup/expression/"
            "default route that doesn't belong to map_type='script'"
        ),
        "SCRIPT_MAPPING_REF_REQUIRED": (
            "script_mappings[].script_component_id is missing, or a "
            "$ref script key is not declared in depends_on"
        ),
        "PROFILE_FIELD_NOT_MAPPABLE": (
            "source_path or target_path resolves to a structural node "
            "(object/array/non-leaf element)"
        ),
        "PROFILE_FIELD_VALIDATION_FAILED": (
            "shape / cross-field issue in the map config"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a key in the config dict matches a secret-shaped substring"
        ),
    },
    "gotchas": [
        (
            "$ref:KEY tokens are resolved at apply time. Renaming a "
            "referenced profile or script.mapping component after planning "
            "requires a fresh plan."
        ),
        (
            "Each script.mapping output has its own port key; multi-output "
            "scripts emit one Mapping row per output. The script_mappings "
            "entry must list every output you want to bind to a "
            "target_path — outputs you omit are dropped."
        ),
        (
            "Cross-list duplicate-target detection runs across "
            "field_mappings AND every script_mappings[].outputs[]; the "
            "destination leaf may receive at most one mapping total."
        ),
        (
            "input_name / output_name strings must match the corresponding "
            "<Input name> / <Output name> declared inside the referenced "
            "script.mapping component for Boomi to bind values at run time."
        ),
    ],
    "recommended_workflow": [
        "1. Create the source profile component (profile.db / profile.json / profile.xml).",
        "2. Create the target profile component (profile.db / profile.json / profile.xml).",
        "3. Create the script.mapping component(s) the map will call.",
        "4. Plan this map with source_profile_id, target_profile_id, and "
        "every script_mappings[].script_component_id referenced via "
        "'$ref:KEY'; declare all three in depends_on.",
        "5. Apply — $ref tokens resolve to real UUIDs and the map XML "
        "emits with deterministic FunctionStep IDs and Mapping rows.",
    ],
    "example": {
        "key": "<<map_key>>",
        "type": "transform.map",
        "action": "create",
        "name": "<<map display name>>",
        "depends_on": [
            "<<source profile key>>",
            "<<target profile key>>",
            "<<script.mapping key>>",
        ],
        "config": {
            "component_type": "transform.map",
            "map_type": "script",
            "component_name": "<<map display name>>",
            "source_profile_id": "$ref:<<source profile key>>",
            "source_profile_type": "<<profile.db | profile.json | profile.xml>>",
            "target_profile_id": "$ref:<<target profile key>>",
            "target_profile_type": "<<profile.db | profile.json | profile.xml>>",
            "script_mappings": [
                {
                    "script_slot": "<<task-authored slot name>>",
                    "script_component_id": "$ref:<<script.mapping key>>",
                    "inputs": [
                        {
                            "source_path": "<<source logical path>>",
                            "input_name": "<<matches script <Input name>>>",
                        },
                    ],
                    "outputs": [
                        {
                            "output_name": "<<matches script <Output name>>>",
                            "target_path": "<<target logical path>>",
                        },
                    ],
                },
            ],
        },
        "_example_note": (
            "Placeholder values only. Replace angle-bracket markers with "
            "task-specific keys / paths / variable names. No canned script "
            "wiring is shipped here."
        ),
    },
    "out_of_scope": {
        "script_processing_fallback": (
            "Process-level Groovy / JavaScript (script.processing, Data "
            "Process custom scripting) is NOT used as a fallback for "
            "unsupported map-script requests."
        ),
        "standalone_transform_function_authoring_surface": (
            "There is no first-class authoring surface for standalone "
            "transform.function components beyond the script-wrapper "
            "specialization #41 ships. Plan-time synthesis auto-creates "
            "wrappers from in-spec script.mapping refs, and callers can "
            "declare their own transform.function components for "
            "existing-script reuse, but a general-purpose "
            "transform.function builder (for non-script userdefined "
            "function graphs) remains future work."
        ),
        "chained_script_graphs": (
            "Wiring one script's output into another script's input via a "
            "single FunctionStep chain remains future work; declare each "
            "as a separate script_mappings entry instead."
        ),
        "xslt": (
            "XSLT support is explicitly deferred and out of M2 "
            "(issue #42). It is not a planned M2 builder; reopen only "
            "for XML-heavy migration, SOAP/XML-to-XML scenarios, or "
            "imported integration assets that already ship XSLT "
            "stylesheets."
        ),
        "existing_profile_index_discovery": (
            "Indexing arbitrary existing-profile XML to support literal-UUID "
            "profile refs remains separate future work (not infer_profile_fields, which infers from supplied artifacts only)."
        ),
    },
}


_COMPONENT_CREATE_CONNECTOR_ACTION_REST_OPERATION = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-action",
    "protocol": "rest.operation",
    "boomi_subtype": "officialboomi-X3979C-rest-prod",
    "public_aliases": ["rest", "rest_client", "officialboomi-X3979C-rest-prod"],
    "tool": "manage_connector (action='create')",
    "note": (
        "Boomi REST Client operation. Wraps a single REST call in an "
        "Operation envelope with GenericOperationConfig "
        "customOperationType=<method> and operationType=EXECUTE. All 8 "
        "REST methods are buildable: GET, PATCH, PUT, POST, DELETE, HEAD, "
        "OPTIONS, TRACE. The connection is bound at the process connector "
        "step, not in the operation XML — connection_ref_key is plan-only "
        "metadata for dependency ordering."
    ),
    "template": {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "execute",
        "component_name": "<<operation name>>",
        "folder_name": "<<folder>>",
        "description": "<<optional description>>",
        "connection_ref_key": "<<rest connection key>>",
        "method": "PATCH",
        "path": "/<<endpoint path>>",
        "query_parameters": {},
        "request_headers": {},
        "request_profile_type": "json",
        "request_profile_id": "$ref:<<request profile key>>",
        "response_profile_type": "json",
        "response_profile_id": "$ref:<<response profile key>>",
        "return_application_errors": True,
        "track_response": True,
        # follow_redirects intentionally omitted: the template defaults
        # method=PATCH, and the verified PATCH live export does NOT carry a
        # followRedirects field. Document the field via
        # follow_redirects_values / follow_redirects_emission_rule below
        # instead. GET callers may set it explicitly.
        "payload_source_ref_key": "<<payload source key>>",
        "credential_ref": "credential://<<vendor>>/<<role>>",
    },
    "required": [
        "component_type",
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "method",
        "path",
    ],
    "defaults": {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "execute",
        "folder_name": "Home",
        "request_profile_type": "xml",
        "response_profile_type": "xml",
        "return_application_errors": True,
        "track_response": True,
    },
    "supported_operation_modes": ["execute"],
    "supported_methods": [
        "GET", "PATCH", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "TRACE",
    ],
    "unverified_pending_methods": [],
    "follow_redirects_values": ["NONE", "STRICT", "LAX"],
    "follow_redirects_emission_rule": {
        "default_none_methods": ["GET", "POST", "HEAD", "DELETE"],
        "omit_methods": ["PATCH", "PUT", "OPTIONS", "TRACE"],
        "explicit_values_always_emit": True,
        "summary": (
            "Four verbs (GET/POST/HEAD/DELETE) emit a followRedirects "
            "field with value='NONE' by default. Four verbs "
            "(PATCH/PUT/OPTIONS/TRACE) OMIT the field entirely when the "
            "caller doesn't supply follow_redirects. Explicit "
            "NONE/STRICT/LAX values are always emitted regardless of "
            "method. Verified per-method against live RenEra exports."
        ),
    },
    "field_method_dependency_map": {
        "summary": (
            "Machine-readable map of which operation fields are "
            "independent of method (work with any of the 8 supported "
            "verbs) vs method-tied. The only method-tied behavior is "
            "the default-emission rule for follow_redirects; every "
            "other input field is method-orthogonal."
        ),
        "independent": [
            "path",
            "query_parameters",
            "request_headers",
            "request_profile_id",
            "response_profile_id",
            "request_profile_type",
            "response_profile_type",
            "return_application_errors",
            "track_response",
        ],
        "method_tied": {
            "follow_redirects_default": {
                "emit_NONE": ["GET", "POST", "HEAD", "DELETE"],
                "omit": ["PATCH", "PUT", "OPTIONS", "TRACE"],
                "explicit_values_always_emit": True,
            },
        },
    },
    "query_parameters_status": "plain_buildable",
    "request_headers_status": "plain_buildable",
    "customproperties_shape": {
        "summary": (
            "Both query_parameters and request_headers accept a flat JSON "
            "object whose keys and values are non-secret strings. The "
            "builder emits one `<properties key=... value=.../>` child per "
            "entry inside the `<customProperties>` container. Insertion "
            "order is preserved. Verified against live REST Query Param "
            "GET (9ede2c08) and REST Headers GET (4986d5eb) — only the "
            "plain entries are emitted; encrypted entries are rejected."
        ),
        "plain_examples": {
            "query_parameters": {"limit": "100", "offset": "0", "filter": "active=true"},
            "request_headers": {"Accept": "application/json", "Cache-Control": "no-cache"},
        },
        "rejected_secret_shaped_keys": (
            "authorization, x-api-key, x-auth-token, api-key, api_key, "
            "bearer, token, password, secret, credential, client-secret. "
            "Case-insensitive whole-key match — rejected with "
            "REST_SECRET_VALUE_FORBIDDEN."
        ),
        "rejected_secret_shaped_values": (
            "JWT-shaped strings (eyJ... prefix), long base64-shaped values "
            "(40+ chars of [A-Za-z0-9+/=] with no whitespace), and any "
            "value starting with the literal `[encrypted]` marker."
        ),
        "rejected_encrypted_marker": (
            "A dict whose 'encrypted' key is True triggers "
            "UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY. Encrypted "
            "customProperty emission requires a secret-safe write path "
            "that does not exist yet."
        ),
    },
    "depends_on_requirements": [
        "Include connection_ref_key in depends_on so the REST connector-settings runs first.",
        "When request_profile_id uses '$ref:KEY', include KEY in depends_on too.",
        "When response_profile_id uses '$ref:KEY', include KEY in depends_on too.",
        "When payload_source_ref_key is supplied, include that key in depends_on.",
    ],
    "forbidden_secret_fields": [
        "password",
        "password_ref",
        "secret",
        "token",
        "access_token",
        "client_secret",
    ],
    "credential_note": (
        "Bearer-style and API-key-style headers MUST NOT be placed in "
        "request_headers entries. The builder rejects secret-shaped keys "
        "(Authorization, X-API-Key, Bearer, Token, Password, etc.) with "
        "REST_SECRET_VALUE_FORBIDDEN. Model token-based authentication on "
        "the CONNECTION (auth='OAUTH2') and let Boomi inject the "
        "Authorization header using the encrypted credential store. "
        "Plain non-secret customProperty values (Accept, Content-Type, "
        "User-Agent, limit, filter, etc.) are accepted via Phase 6."
    ),
    "error_codes": {
        "UNSUPPORTED_REST_OPERATION_MODE": "operation_mode is not 'execute'",
        "UNSUPPORTED_REST_METHOD": "method is not one of the 8 buildable REST verbs",
        "UNVERIFIED_REST_XML_VARIANT": "reserved for future methods recognized but not yet buildable (currently no such methods — Phase 5 made all 8 verbs buildable)",
        "REST_CUSTOM_PROPERTY_INVALID": "query_parameters or request_headers entry has non-string key or non-string value",
        "UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY": "query_parameters or request_headers contains an `encrypted=True` marker (Boomi-export-shape forwarded as JSON config)",
        "REST_SECRET_VALUE_FORBIDDEN": "secret-shaped customProperty key (Authorization, X-API-Key, Bearer, etc.) or value (JWT, long base64, [encrypted] prefix)",
        "REST_PATH_REQUIRED": "path absent or empty",
        "REST_CONNECTION_REF_REQUIRED": "connection_ref_key absent or empty",
        "REST_DEPENDENCY_REQUIRED": "connection_ref_key / $ref target / payload_source_ref_key not declared in depends_on",
        "REST_PROFILE_REF_UNRESOLVED": "request_profile_id or response_profile_id $ref token is empty",
        "REST_REF_TYPE_MISMATCH": "connection_ref_key, request_profile_id, or response_profile_id $ref points to a component of the wrong type at plan time (issue #49)",
        "REST_OPERATION_VALIDATION_FAILED": "shape / type / required-field issue",
        "PLAINTEXT_SECRET_REJECTED": "a forbidden secret-shaped key appeared in config",
    },
    "gotchas": [
        (
            "Boomi binds the connection at the process connector step, not "
            "in the operation XML. The builder will NOT emit a connection "
            "ID — connection_ref_key is plan-only metadata for dependency "
            "ordering."
        ),
        (
            "REST Client preserves the path value verbatim in emitted XML, "
            "including any leading slash. Pass the path exactly as it "
            "should appear after the connection's base_url."
        ),
        (
            "followRedirects emission is per-method (Phase 5): four verbs "
            "(GET/POST/HEAD/DELETE) emit value='NONE' by default; four "
            "(PATCH/PUT/OPTIONS/TRACE) omit the field unless "
            "follow_redirects is explicitly supplied. Explicit "
            "NONE/STRICT/LAX values always emit regardless of method."
        ),
        (
            "connection_ref_key, payload_source_ref_key, credential_ref, "
            "and any request body content are plan-only metadata. They "
            "never appear in the emitted operation XML."
        ),
    ],
    "recommended_workflow": [
        "1. Create the REST connector-settings (manage_connector, connector_type=rest).",
        "2. Create the request profile and response profile components (e.g. profile.json) "
        "upstream so the operation can $ref them.",
        "3. Plan this operation with depends_on=[<connection_key>, "
        "<request_profile_key>, <response_profile_key>, <payload_source_key>], "
        "connection_ref_key set, and request_profile_id='$ref:<request_profile_key>'.",
        "4. Apply — $ref tokens are substituted with the created component "
        "IDs via the id_registry.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' now use "
        "read-merge-write semantics (issue #45): the builder-owned "
        "GenericOperationConfig <field id=\"...\"> children and owned "
        "attributes (customOperationType, path, requestProfile/"
        "responseProfile refs) are merged into the live XML, while "
        "unknown field ids, Operation-level Archiving/Tracking/Caching, "
        "and any future siblings survive. UI-added live query parameters "
        "and request headers survive a path-only or method-only update "
        "(Codex r8 P2). Profile bindings travel as a unit: "
        "requestProfileType/responseProfileType follow their "
        "requestProfile/responseProfile id, so a path/method-only update "
        "preserves the live profile type and a binding change applies the "
        "new id+type together. KNOWN LIMITATION (Codex r20 P2): a "
        "type-only update (request_profile_type/response_profile_type "
        "without the matching request_profile_id/response_profile_id) is a "
        "no-op on update — the live type is preserved. The builder always "
        "emits a default profile type, so the merge keys the type on its "
        "id to avoid clobbering live JSON/XML bindings on path-only "
        "updates; supply the profile id to change the type, or use the "
        "raw-XML escape hatch (conditional-emission fix tracked in #50). "
        "KNOWN LIMITATION (Codex r10 P2): an "
        "explicit ``query_parameters={}`` / ``request_headers={}`` to "
        "clear all live custom properties is indistinguishable from "
        "omitting those fields, so the live props survive — use the "
        "raw-XML escape hatch via manage_component to force-clear. Direct "
        "manage_component action='update' with config.xml remains a full "
        "XML replacement."
    ),
    "example": {
        "key": "target_rest_operation",
        "type": "connector-action",
        "action": "create",
        "name": "<<operation name>>",
        "depends_on": [
            "target_rest_connection",
            "target_json_profile",
            "payload_map",
        ],
        "config": {
            "component_type": "connector-action",
            "connector_type": "rest",
            "operation_mode": "execute",
            "component_name": "<<operation name>>",
            "connection_ref_key": "target_rest_connection",
            "method": "PATCH",
            "path": "/<<endpoint path>>",
            "query_parameters": {},
            "request_headers": {},
            "request_profile_type": "json",
            "request_profile_id": "$ref:target_json_profile",
            "response_profile_type": "json",
            "payload_source_ref_key": "payload_map",
            "credential_ref": "credential://<<vendor>>/<<role>>",
        },
        "_example_note": (
            "Placeholder values only. $ref tokens are substituted with "
            "the matching component_id at apply time."
        ),
    },
    "out_of_scope": {
        "process_emission": (
            "Wiring the connection + operation into a runnable process "
            "(retry, DLQ, schedule) is tracked by later M2 issues."
        ),
        "encrypted_custom_properties": (
            "Encrypted query_parameter / request_header entries (Boomi's "
            "`<properties encrypted=\"true\" .../>` shape) remain out of "
            "scope. Caller-supplied configs with the `encrypted=True` "
            "marker return UNSUPPORTED_REST_ENCRYPTED_CUSTOM_PROPERTY. "
            "Lifting this requires a secret-safe write path that does "
            "not exist yet."
        ),
    },
}


_COMPONENT_CREATE_CONNECTOR_ACTION_OVERVIEW = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-action",
    "tool": "manage_connector (action='create')",
    "note": (
        "Connector-action (operation) builders. Available: database.get "
        "(issue #23) and rest.operation (issue #24). Database send/write "
        "is tracked by issue #32."
    ),
    "available_protocols": ["database.get", "rest.operation"],
    "hint": (
        "Re-call get_schema_template(resource_type='component', operation='create', "
        "component_type='connector-action', protocol='<protocol>') for the chosen "
        "protocol's full JSON template."
    ),
    "escape_hatch": (
        "For operations without a builder, use manage_connector action='get' on "
        "an existing connector-action to export its XML, then pass as config.xml "
        "to manage_connector action='create'."
    ),
}


_COMPONENT_SEARCH = {
    "resource_type": "component",
    "operation": "search",
    "tool": "query_components (action='search')",
    "template": {
        "name": "%partial_name% (LIKE pattern, use % wildcard)",
        "type": "process | connector-settings | connector-action | transform.map | profile.xml | ... (see component_types list)",
        "sub_type": "(optional sub-type filter)",
        "component_id": "(optional specific ID)",
        "created_by": "user@example.com",
        "modified_by": "user@example.com",
        "folder_name": "(client-side filter, exact match)",
        "show_all": "false (set true to include deleted/historical)",
    },
}

_COMPONENT_CLONE = {
    "resource_type": "component",
    "operation": "clone",
    "tool": "manage_component (action='clone')",
    "template": {
        "name": "New Component Name (REQUIRED)",
        "folder_name": "(optional) target folder",
        "folder_id": "(optional) target folder ID",
        "description": "(optional) new description",
    },
}

_COMPONENT_COMPARE = {
    "resource_type": "component",
    "operation": "compare_versions",
    "tool": "analyze_component (action='compare_versions')",
    "template": {
        "source_version": 1,
        "target_version": 2,
    },
    "hint": "Version numbers are integers starting at 1. Use query_components get to see current version.",
}

_COMPONENT_SAFE_EDIT = {
    "resource_type": "component",
    "operation": "safe_edit",
    "tools": [
        "prepare_component_edit (read-only preview)",
        "apply_component_edit (confirmed write)",
    ],
    "summary": (
        "Two-phase safe edit of an existing component (M9.7): pull -> structured "
        "patch -> diff -> push. Preview is read-only; the write requires explicit "
        "confirmation and aborts if the component drifted since preview. Raw XML is "
        "rejected — use structured fields so encrypted values and unknown XML are "
        "preserved through the #45/#50 merge."
    ),
    "patch_modes": {
        "metadata_partial": (
            "Partial edit: config holds ONLY metadata fields "
            "(name/component_name, description, folder_name, folder_id). The live "
            "root is smart-merged in place, so you change just those fields and "
            "everything else is preserved verbatim."
        ),
        "structured_body": (
            "Body edit: config must be the COMPLETE structured config the typed "
            "builder for this component_type consumes (same contract as "
            "build_integration's update path) — e.g. a connector edit needs "
            "connector_type plus all required connection fields, a profile edit "
            "needs profile_type plus its fields. The builder rebuilds its owned "
            "subtree from that config; the #45/#50 merge then preserves encrypted "
            "values and any unknown XML outside it. A body config that omits the "
            "type discriminator or a required builder field is rejected (it is NOT "
            "a field-level delta — only metadata fields support partial edits)."
        ),
    },
    # A copy-pasteable, structurally-valid patch (the simplest case: a partial
    # metadata edit; component_type is optional and defaults to the live type).
    # For a body edit, see body_edit_example below.
    "patch_template": {
        "config": {
            "name": "(metadata edit) optional new name",
            "description": "(metadata edit) optional new description",
            "folder_name": "(metadata edit) optional target folder",
            "folder_id": "(metadata edit) optional target folder id",
        },
    },
    # A separate, full-config body edit (NOT a field-level delta): config must be
    # the complete config the typed builder for this component_type consumes.
    "body_edit_example": {
        "component_type": "connector-settings",
        "config": {
            "connector_type": "database  # required type discriminator",
            "component_name": "...  # plus EVERY field the typed builder requires",
            "...": "full structured config (see get_schema_template operation='create' for the type)",
        },
        "map_context": {
            "source_index": "(transform.map body edits only)",
            "target_index": "(transform.map body edits only)",
        },
    },
    "workflow": [
        "1. query_components(action='get', component_id=...) — inspect the live component",
        "2. prepare_component_edit(profile, component_id, patch) — review diff + confirmation_token (no mutation)",
        "3. apply_component_edit(profile, component_id, patch, confirmation_token, confirm_apply=true) — commit",
        "4. analyze_component(action='compare_versions', ...) — confirm what changed",
    ],
    "error_codes": [
        "COMPONENT_EDIT_RAW_XML_UNSUPPORTED",
        "COMPONENT_EDIT_CONFIRMATION_REQUIRED",
        "COMPONENT_EDIT_TOKEN_INVALID",
        "COMPONENT_EDIT_PATCH_MISMATCH",
        "COMPONENT_EDIT_DRIFT_DETECTED",
        "COMPONENT_EDIT_TYPE_MISMATCH",
    ],
    "note": "Rollback / version restore stays deferred (not part of this surface).",
}


# ============================================================================
# Organization Templates
# ============================================================================

_ORGANIZATION_OVERVIEW = {
    "resource_type": "organization",
    "tool": "manage_trading_partner (org_* actions)",
    "available_actions": ["list", "get", "create", "update", "delete"],
    "note": "These map to manage_trading_partner actions: org_list, org_get, org_create, org_update, org_delete",
    "hint": "Use operation='create' for the full create template",
}

_ORGANIZATION_CREATE = {
    "resource_type": "organization",
    "operation": "create",
    "tool": "manage_trading_partner (action='org_create')",
    "template": {
        "component_name": "Acme Corp (REQUIRED)",
        "folder_name": "Home/Organizations",
        "contact_name": "John Doe",
        "contact_email": "john@acme.com",
        "contact_phone": "555-1234",
        "contact_fax": "",
        "contact_url": "",
        "contact_address": "123 Main St",
        "contact_address2": "",
        "contact_city": "New York",
        "contact_state": "NY",
        "contact_country": "USA",
        "contact_postalcode": "10001",
    },
    "required_fields": ["component_name"],
}


# ============================================================================
# Monitoring Templates
# ============================================================================

_MONITORING_OVERVIEW = {
    "resource_type": "monitoring",
    "tool": "monitor_platform",
    "available_actions": ["execution_records", "execution_logs", "execution_artifacts", "audit_logs", "events", "certificates", "throughput", "execution_metrics", "connector_documents", "download_connector_document"],
    "hint": "Use operation='execution_records' or 'audit_logs' etc. for action-specific templates",
}

_MONITORING_EXECUTION_RECORDS = {
    "resource_type": "monitoring",
    "operation": "execution_records",
    "tool": "monitor_platform",
    "template": {
        "start_date": "2025-01-01T00:00:00Z",
        "end_date": "2025-01-31T23:59:59Z",
        "status": "COMPLETE | ERROR | ABORTED | COMPLETE_WARN | INPROCESS",
        "process_name": "(optional filter)",
        "process_id": "(optional filter)",
        "atom_name": "(optional filter)",
        "atom_id": "(optional filter)",
        "execution_id": "(optional specific execution)",
        "limit": 100,
    },
    "enums": {
        "status": ["COMPLETE", "ERROR", "ABORTED", "COMPLETE_WARN", "INPROCESS"],
    },
    "required": "At least one filter field is required",
}

_MONITORING_EXECUTION_LOGS = {
    "resource_type": "monitoring",
    "operation": "execution_logs",
    "tool": "monitor_platform",
    "template": {
        "execution_id": "REQUIRED — from execution_records result",
        "log_level": "ALL | SEVERE | WARNING | INFO | CONFIG | FINE | FINER | FINEST",
        "fetch_content": "true (default) | false (returns URL only)",
    },
    "enums": {
        "log_level": ["SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL"],
    },
}

_MONITORING_EXECUTION_ARTIFACTS = {
    "resource_type": "monitoring",
    "operation": "execution_artifacts",
    "tool": "monitor_platform",
    "template": {
        "execution_id": "REQUIRED — from execution_records result",
        "fetch_content": "true (default) | false (returns URL only)",
    },
}

_MONITORING_AUDIT_LOGS = {
    "resource_type": "monitoring",
    "operation": "audit_logs",
    "tool": "monitor_platform",
    "template": {
        "start_date": "2025-01-01T00:00:00Z (REQUIRED)",
        "end_date": "2025-01-31T23:59:59Z (REQUIRED)",
        "user": "(optional) user@example.com",
        "action": "(optional) Deploy | Create | Update | Delete",
        "type": "(optional) Process | Connection | Environment",
        "level": "(optional) INFO | WARNING | ERROR",
        "source": "(optional) API | UI",
        "limit": 100,
    },
}

_MONITORING_EVENTS = {
    "resource_type": "monitoring",
    "operation": "events",
    "tool": "monitor_platform",
    "template": {
        "start_date": "2025-01-01T00:00:00Z",
        "end_date": "2025-12-31T23:59:59Z",
        "event_level": "(optional) ERROR | WARNING | INFO",
        "event_type": "(optional) process.error",
        "process_name": "(optional filter)",
        "atom_name": "(optional filter)",
        "execution_id": "(optional filter)",
        "limit": 100,
    },
}

_MONITORING_CERTIFICATES = {
    "resource_type": "monitoring",
    "operation": "certificates",
    "tool": "monitor_platform",
    "template": {
        "days_ahead": 30,
        "limit": 50,
    },
}

_MONITORING_THROUGHPUT = {
    "resource_type": "monitoring",
    "operation": "throughput",
    "tool": "monitor_platform",
    "template": {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
        "atom_id": "(optional filter)",
        "limit": 100,
    },
    "required": "At least one filter: start_date, end_date, atom_id",
}

_MONITORING_EXECUTION_METRICS = {
    "resource_type": "monitoring",
    "operation": "execution_metrics",
    "tool": "monitor_platform",
    "template": {
        "start_date": "2025-01-01T00:00:00Z",
        "end_date": "2025-01-31T23:59:59Z",
        "top_failures": 5,
        "limit": 200,
    },
    "required": "Same filters as execution_records",
}

_MONITORING_CONNECTOR_DOCUMENTS = {
    "resource_type": "monitoring",
    "operation": "connector_documents",
    "tool": "monitor_platform",
    "template": {
        "execution_id": "REQUIRED - from execution_records result",
        "connector_type": "(optional filter)",
        "status": "SUCCESS | ERROR",
        "action_type": "(optional filter)",
        "limit": 50,
    },
}

_MONITORING_DOWNLOAD_CONNECTOR_DOCUMENT = {
    "resource_type": "monitoring",
    "operation": "download_connector_document",
    "tool": "monitor_platform",
    "template": {
        "generic_connector_record_id": "REQUIRED - from connector_documents result (id_ field)",
        "fetch_content": True,
    },
    "notes": "Downloads actual document content. Text content returned inline, binary as base64. "
             "Set fetch_content=false for URL-only metadata.",
}


# ============================================================================
# Environment Templates
# ============================================================================

_ENVIRONMENT_OVERVIEW = {
    "resource_type": "environment",
    "note": "Environment management is available via the Boomi SDK. "
            "Use query_components to find existing environments.",
    "hint": "Use operation='create' for the create template",
}

_ENVIRONMENT_CREATE = {
    "resource_type": "environment",
    "operation": "create",
    "template": {
        "name": "Production (REQUIRED)",
        "classification": "TEST | PROD",
    },
    "enums": {
        "classification": ["TEST", "PROD"],
    },
    "sdk_pattern": "sdk.environment.create_environment(EnvironmentModel(name=..., classification=...))",
}


# ============================================================================
# Package Templates
# ============================================================================

_PACKAGE_OVERVIEW = {
    "resource_type": "package",
    "note": "Package and deployment management via Boomi SDK.",
    "available_operations": ["create", "deploy"],
    "hint": "Use operation='create' or operation='deploy' for templates",
}

_PACKAGE_CREATE = {
    "resource_type": "package",
    "operation": "create",
    "template": {
        "component_id": "REQUIRED — ID of component to package",
        "component_type": "REQUIRED — process, certificate, customlibrary, flowservice, processroute, tpgroup, webservice",
        "package_version": "REQUIRED — user-defined version string (e.g. '1.0.0')",
        "notes": "Release notes for this package (optional)",
        "branch_name": "main (optional, defaults to main branch)",
    },
    "sdk_pattern": "sdk.packaged_component.create_packaged_component(...)",
    "tool": "manage_deployment(action='create_package', config='{...}')",
}

_PACKAGE_DEPLOY = {
    "resource_type": "package",
    "operation": "deploy",
    "template": {
        "package_id": "REQUIRED — ID of packaged component",
        "environment_id": "REQUIRED — target environment ID",
        "listener_status": "RUNNING or PAUSED (optional)",
        "notes": "Deployment notes (optional)",
    },
    "sdk_pattern": "sdk.deployed_package.create_deployed_package(...)",
    "tool": "manage_deployment(action='deploy', package_id='...', environment_id='...')",
}


# ============================================================================
# Execution Request Templates
# ============================================================================

_EXECUTION_REQUEST_OVERVIEW = {
    "resource_type": "execution_request",
    "note": "Execute processes on Boomi runtimes.",
    "hint": "Use operation='execute' for the execution template",
}

_EXECUTION_REQUEST_EXECUTE = {
    "resource_type": "execution_request",
    "operation": "execute",
    "template": {
        "process_id": "REQUIRED — process component ID to execute",
        "atom_id": "REQUIRED — runtime/atom ID to execute on",
        "dynamic_properties": {
            "property_name": "property_value",
            "another_property": "another_value",
        },
    },
    "sdk_pattern": "sdk.execution_request.create_execution_request(ExecutionRequest(atom_id=..., process_id=...))",
    "hint": "Use query_components list with type='process' to find process_id.",
}


# ============================================================================
# Generic API Invoker
# ============================================================================


def _truncate_json_response(parsed, max_size):
    """Truncate a parsed JSON response to fit within max_size characters.

    For dict responses containing a list value (common Boomi pattern like
    {'result': [...], 'numberOfResults': N}), removes trailing array elements
    until the serialized result fits.  Root-level arrays are handled the same
    way.  For any other shape that still exceeds max_size, the serialized JSON
    is hard-truncated as a last resort.  Returns (truncated_obj, metadata_dict).
    """
    import json as _json
    meta = {}

    # --- Root-level list ---
    if isinstance(parsed, list) and len(parsed) > 0:
        total_items = len(parsed)
        lo, hi = 0, total_items
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if len(_json.dumps(parsed[:mid])) <= max_size:
                lo = mid
            else:
                hi = mid - 1
        meta["items_returned"] = lo
        meta["items_total"] = total_items
        return parsed[:lo], meta

    # --- Dict with a list field (common Boomi pattern) ---
    if isinstance(parsed, dict):
        list_key = None
        for k, v in parsed.items():
            if isinstance(v, list) and len(v) > 0:
                list_key = k
                break
        if list_key is not None:
            items = parsed[list_key]
            total_items = len(items)
            # Binary search for the largest count that fits
            lo, hi = 0, total_items
            while lo < hi:
                mid = (lo + hi + 1) // 2
                parsed[list_key] = items[:mid]
                if len(_json.dumps(parsed)) <= max_size:
                    lo = mid
                else:
                    hi = mid - 1
            parsed[list_key] = items[:lo]
            meta["items_returned"] = lo
            meta["items_total"] = total_items
            return parsed, meta

    # --- Fallback: hard-truncate serialized JSON ---
    serialized = _json.dumps(parsed)
    if len(serialized) <= max_size:
        return parsed, meta
    meta["note"] = "Response too large to truncate cleanly; data may be incomplete"
    return serialized[:max_size], meta


# Boomi query pagination: "<Object>/query" starts a query, "<Object>/queryMore"
# continues it with a queryToken. Both are POSTs that only read platform state.
_READ_LIKE_POST_SEGMENTS = ("query", "querymore")


def _classify_raw_api_request(method: str, endpoint: str) -> Dict[str, Any]:
    """Classify a raw API call as read vs write for the confirm_write gate.

    DELETE is classified write but exempt from confirm_write — it stays governed
    by the pre-existing confirm_delete gate. PATCH never reaches this helper
    (rejected by the method whitelist).
    """
    method = method.upper()
    path = endpoint.split("?", 1)[0].split("#", 1)[0]
    segments = [seg.lower() for seg in path.strip("/").split("/") if seg]
    last_segment = segments[-1] if segments else ""

    if method == "GET":
        return {
            "class": "read",
            "requires_confirm_write": False,
            "reason": "GET requests are read-only.",
        }
    if method == "POST" and last_segment in _READ_LIKE_POST_SEGMENTS:
        return {
            "class": "read",
            "requires_confirm_write": False,
            "reason": "POST to a */query or */queryMore endpoint only reads platform state.",
        }
    if method == "DELETE":
        return {
            "class": "write",
            "requires_confirm_write": False,
            "reason": "DELETE is governed by confirm_delete, not confirm_write.",
        }
    return {
        "class": "write",
        "requires_confirm_write": True,
        "reason": f"{method} to '{endpoint}' mutates platform state.",
    }


# First endpoint path segment (lowercased) -> safer typed tools for that family.
_TYPED_ALTERNATIVES_BY_SEGMENT = {
    "component": [
        "query_components", "manage_component", "analyze_component",
        "build_integration", "manage_connector",
    ],
    "componentmetadata": [
        "query_components", "manage_component", "analyze_component",
        "build_integration", "manage_connector",
    ],
    # ComponentReference is reference lookup only — steer to the read tools
    # (analyze_component wraps this API for where_used/dependencies).
    "componentreference": ["query_components", "analyze_component"],
    # manage_process is read-only (list/get) and cannot author processes;
    # build_integration is the typed write path for process components.
    "process": ["build_integration"],
    "packagedcomponent": ["manage_deployment", "orchestrate_deploy"],
    "deployedpackage": ["manage_deployment", "orchestrate_deploy"],
    "componentenvironmentattachment": ["manage_deployment", "orchestrate_deploy"],
    "processenvironmentattachment": ["manage_deployment", "orchestrate_deploy"],
    "componentatomattachment": ["manage_deployment", "orchestrate_deploy"],
    "processatomattachment": ["manage_deployment", "orchestrate_deploy"],
    "environment": ["manage_environments"],
    "environmentextensions": ["manage_environments"],
    "environmentmapextension": ["manage_environments"],
    "environmentrole": ["manage_environments"],
    "atom": ["manage_runtimes", "manage_deployment"],
    "cloud": ["manage_runtimes", "manage_deployment"],
    "installertoken": ["manage_runtimes", "manage_deployment"],
    "role": ["manage_account"],
    "branch": ["manage_account"],
    "userrole": ["manage_account"],
    "userfederation": ["manage_account"],
    "ssoconfig": ["manage_account"],
    "folder": ["manage_folders"],
    "sharedwebserver": ["manage_shared_resources"],
    "sharedserverinformation": ["manage_shared_resources"],
    "sharedcommunicationchannelcomponent": ["manage_shared_resources"],
    "processschedules": ["manage_schedules"],
    "processschedulestatus": ["manage_schedules"],
    "executionrequest": ["execute_process", "monitor_platform", "troubleshoot_execution"],
    "executionrecord": ["execute_process", "monitor_platform", "troubleshoot_execution"],
    "listqueues": ["execute_process", "monitor_platform", "troubleshoot_execution"],
    "clearqueue": ["execute_process", "monitor_platform", "troubleshoot_execution"],
    "movequeue": ["execute_process", "monitor_platform", "troubleshoot_execution"],
    "tradingpartnercomponent": ["manage_trading_partner"],
    "tradingpartnerprocessinggroup": ["manage_trading_partner"],
    "integrationpack": ["manage_integration_packs"],
    "integrationpackinstance": ["manage_integration_packs"],
    "publisherintegrationpack": ["manage_integration_packs"],
    "releaseintegrationpack": ["manage_integration_packs"],
    "accountgroup": ["manage_account_groups"],
    "accountgroupaccount": ["manage_account_groups"],
    "listenerstatus": ["manage_listeners"],
}

_DEFAULT_TYPED_ALTERNATIVES = ["list_capabilities", "get_schema_template", "search_boomi_docs"]


def _typed_alternatives_for_endpoint(endpoint: str) -> list:
    """Safer typed tools for the endpoint's object family (first path segment)."""
    path = endpoint.split("?", 1)[0].split("#", 1)[0]
    segments = [seg for seg in path.strip("/").split("/") if seg]
    first_segment = segments[0].lower() if segments else ""
    return list(
        _TYPED_ALTERNATIVES_BY_SEGMENT.get(first_segment, _DEFAULT_TYPED_ALTERNATIVES)
    )


def _raw_write_confirmation_guard(
    endpoint: str, method: str, classification: Dict[str, Any]
) -> Dict[str, Any]:
    """Fail-closed guard response for an unconfirmed raw write (no Boomi call made)."""
    alternatives = _typed_alternatives_for_endpoint(endpoint)
    path = endpoint.split("?", 1)[0].split("#", 1)[0]
    segments = [seg for seg in path.strip("/").split("/") if seg]
    family = segments[0] if segments else "the target object"
    return {
        "_success": False,
        "error": "Raw Boomi API write requires confirm_write=true before invoking the platform.",
        "error_code": RAW_WRITE_CONFIRMATION_REQUIRED,
        "retryable": False,
        "remediation": (
            f"Prefer the typed tools for this endpoint family first: {', '.join(alternatives)}. "
            f"They validate parameters and preserve component XML via read-merge-write. "
            f"Re-call with confirm_write=true only after confirming no typed tool covers this write."
        ),
        "method": method,
        "endpoint": endpoint,
        "classification": classification,
        "confirm_write_required": True,
        "typed_alternatives": alternatives,
        "suggested_searches": {
            "docs": [
                f"search_boomi_docs(query='{family} API {method} reference')",
                f"search_boomi_docs(query='{family} required fields')",
            ],
            "gotchas": [
                f"search_boomi_docs(query='{family} common errors gotchas')",
            ],
        },
    }


def invoke_api(
    boomi_client: Boomi,
    profile: str,
    endpoint: str,
    method: str = "GET",
    payload: str = None,
    content_type: str = "json",
    accept: str = "json",
    confirm_delete: bool = False,
    confirm_write: bool = False,
) -> Dict[str, Any]:
    """Execute arbitrary Boomi API call using SDK's Serializer.

    Uses the same proven Serializer + send_request() pattern from _shared.py.
    Mutating POST/PUT calls require confirm_write=True; DELETE keeps its
    separate confirm_delete gate.
    """
    import json as json_mod

    # --- Validate method ---
    method = method.upper()
    if method not in ("GET", "POST", "PUT", "DELETE"):
        return {
            "_success": False,
            "error": f"Invalid method: {method}",
            "hint": "Valid methods: GET, POST, PUT, DELETE",
        }

    # --- Safety: DELETE confirmation ---
    if method == "DELETE" and not confirm_delete:
        return {
            "_success": False,
            "error": "DELETE operations require explicit confirmation",
            "hint": "Re-call with confirm_delete=true after user confirms the deletion.",
            "endpoint": endpoint,
            "warning": "This operation may be irreversible",
        }

    # --- Safety: write confirmation (mutating POST/PUT) ---
    # Must return before any platform access (boomi_client.account below).
    classification = _classify_raw_api_request(method, endpoint)
    if classification["requires_confirm_write"] and not confirm_write:
        return _raw_write_confirmation_guard(endpoint, method, classification)

    # --- Build URL ---
    # All SDK services share the same base URL (includes accountId) + auth
    svc = boomi_client.account
    base = svc.base_url or Environment.DEFAULT.url
    url = f"{base.rstrip('/')}/{endpoint.lstrip('/')}"

    # --- Normalize + validate content types ---
    accept = accept.lower().strip()
    content_type = content_type.lower().strip()
    ct_map = {
        "json": "application/json",
        "xml":  "application/xml",
    }
    accept_header = ct_map.get(accept)
    content_type_header = ct_map.get(content_type)
    if not accept_header or not content_type_header:
        return {
            "_success": False,
            "error": f"Invalid content type: accept={accept!r}, content_type={content_type!r}",
            "hint": "Valid values: 'json' or 'xml'",
        }

    # --- Parse payload ---
    # The SDK's send_request() JSON-encodes the body, so for JSON payloads
    # we parse the string to a dict to avoid double-encoding.
    # For XML payloads, we pass the raw string.
    body = None
    if method in ("POST", "PUT") and payload:
        if content_type == "json":
            try:
                body = json_mod.loads(payload)
            except (json_mod.JSONDecodeError, TypeError):
                return {
                    "_success": False,
                    "error": "Invalid JSON payload",
                    "hint": "The payload parameter must be a valid JSON string",
                }
        else:
            body = payload

    # --- Build request via Serializer ---
    ser = Serializer(  # sdk-bypass-ok: invoke_boomi_api is the explicit raw escape hatch
        url,
        [svc.get_access_token(), svc.get_basic_auth()],
    )
    ser = ser.add_header("Accept", accept_header)

    # serialize() returns a Request object; set_method/set_body are on Request
    serialized = ser.serialize().set_method(method)

    if body is not None:
        serialized = serialized.set_body(body, content_type_header)

    # --- Execute ---
    # The SDK raises ApiError for non-2xx responses, so we catch it
    # and extract the response details.
    try:
        response, status, _ = svc.send_request(serialized)  # sdk-bypass-ok: invoke_boomi_api raw escape hatch
    except Exception as api_err:
        # Extract status and response body from ApiError
        status = getattr(api_err, "status", 0)
        err_response = getattr(api_err, "response", None)
        err_body = getattr(err_response, "body", None) if err_response else None

        result = {
            "_success": False,
            "status_code": status,
            "method": method,
            "endpoint": endpoint,
            "url": url,
            "profile": profile,
            "error": f"HTTP {status}" if status else str(api_err),
        }
        if err_body:
            result["data"] = err_body if isinstance(err_body, dict) else str(err_body)
        return result

    # --- Parse response ---
    if isinstance(response, dict):
        raw = json_mod.dumps(response)
    elif isinstance(response, bytes):
        raw = response.decode("utf-8", errors="replace")
    elif isinstance(response, str):
        raw = response
    else:
        raw = str(response)

    # --- Response truncation (safety) ---
    MAX_RESPONSE_SIZE = 50000  # characters
    truncated = len(raw) > MAX_RESPONSE_SIZE

    result = {
        "_success": 200 <= status < 300,
        "status_code": status,
        "method": method,
        "endpoint": endpoint,
        "url": url,
        "profile": profile,
    }

    if accept == "json":
        try:
            parsed = json_mod.loads(raw)
            if truncated:
                parsed, trunc_meta = _truncate_json_response(parsed, MAX_RESPONSE_SIZE)
                result["truncated"] = True
                result["total_size"] = len(raw)
                result.update(trunc_meta)
            if truncated and isinstance(parsed, str):
                # Hard-truncated fallback — not valid JSON, use raw_response
                result["raw_response"] = parsed + "... [TRUNCATED]"
            else:
                result["data"] = parsed
        except (json_mod.JSONDecodeError, TypeError):
            if truncated:
                result["truncated"] = True
                result["total_size"] = len(raw)
                result["raw_response"] = raw[:MAX_RESPONSE_SIZE] + "... [TRUNCATED]"
            else:
                result["raw_response"] = raw
    else:
        if truncated:
            result["truncated"] = True
            result["total_size"] = len(raw)
            result["raw_response"] = raw[:MAX_RESPONSE_SIZE] + "... [TRUNCATED]"
        else:
            result["raw_response"] = raw

    if status >= 400:
        result["error"] = f"HTTP {status}"
        if "raw_response" not in result:
            result["raw_response"] = raw[:5000]

    return result


# ============================================================================
# Action Router
# ============================================================================

_VALID_RESOURCE_TYPES = [
    "trading_partner", "process", "component",
    "environment", "package", "execution_request",
    "organization", "folder", "monitoring", "integration",
    "profile_inference",
]


def _authoring_workflow_sequences() -> Dict[str, Any]:
    """Canonical workflow sequences surfaced by both list_capabilities and
    get_schema_template(schema_name='workflow_sequences'). Returns a fresh dict
    per call so per-call filtering never mutates shared state.
    """
    return {
        "discover_components": {
            "description": "Find and understand components in your account",
            "steps": [
                "1. list_boomi_profiles() → find your profile",
                "2. query_components(action='list', config='{\"type\": \"process\"}') → list processes",
                "3. query_components(action='get', component_id='...') → get details",
                "4. analyze_component(action='where_used', component_id='...') → find dependencies",
            ],
        },
        "safe_edit_existing_component": {
            "description": "Safely edit an existing component (M9.7): pull → structured patch → diff → confirmed push, preserving encrypted values and unknown XML; aborts if the component drifted since preview.",
            "steps": [
                "1. query_components(action='get', component_id='...') → inspect the live component and its current version",
                "2. prepare_component_edit(profile='...', component_id='...', patch='{\"config\": {\"name\": \"...\"}}') → review the diff and capture confirmation_token (read-only, no Boomi mutation)",
                "3. apply_component_edit(profile='...', component_id='...', patch='<same patch>', confirmation_token='<token>', confirm_apply=true) → commit the merged XML (aborts on drift / patch change)",
                "4. analyze_component(action='compare_versions', component_id='...', config='{\"source_version\": <base>, \"target_version\": <new>}') → confirm exactly what changed",
            ],
        },
        "create_and_deploy_process": {
            "description": "Author a typed process and deploy it (freeform process JSON authoring has been removed)",
            "steps": [
                "1. list_integration_archetypes() / get_schema_template(resource_type='process', protocol='database_to_api_sync'|'wrapper_subprocess'|'sync_pipeline') → pick a typed process_kind and inspect its schema",
                "2. build_from_archetype(name='...', parameters={...}) → emit IntegrationSpecV1 (or hand-author a process component with config.process_kind)",
                "3. build_integration(action='apply', config='{\"dry_run\": false, \"integration_spec\": <spec>}') → create the process component(s)",
                "4. manage_deployment(action='create_package', config='{\"component_id\":\"...\", \"component_type\":\"process\", \"package_version\":\"1.0\"}') → package it",
                "5. manage_deployment(action='deploy', package_id='<pkg_id>', environment_id='<env_id>') → deploy it",
                "6. execute_process(profile='...', process_id='<proc_id>', environment_id='<env_id>') → run it",
                "7. monitor_platform(action='execution_records', config='{\"execution_id\": \"...\"}') → check status",
            ],
        },
        "build_integration_from_description": {
            "description": "Author an integration: FIRST consult design_doctrine and select patterns by capability_status, THEN prefer V3 archetypes; fall back to direct IntegrationSpecV1 only when no archetype fits.",
            "steps": [
                "1. list_boomi_profiles() → pick the credential profile; pass profile=... to every account-scoped call",
                # Design-consultation step (issue #86). A parsable
                # get_schema_template(...) step so the available_tools filter
                # (_refs_in_steps) tracks it: the design-first authoring workflow
                # genuinely depends on get_schema_template to serve
                # design_doctrine, so the workflow is correctly dropped when that
                # tool is absent rather than advertising an unusable consult.
                # Routing lives in this response payload, never in a tool
                # description (MCP-conformance, issue #86).
                "2. get_schema_template(schema_name='design_doctrine') → consult the design pattern catalog BEFORE choosing an archetype; fetch a specific pattern with get_schema_template(schema_name='design_pattern:<name>'). Also consult get_schema_template(schema_name='account_governance') for folder placement, component naming, and role/write-restriction governance (fetch one with get_schema_template(schema_name='governance_pattern:<name>')). Select entries from BOTH surfaces by capability_status and record each as emittable_today (proceed via the named tool), gated (design around / propose for GUI apply), or guidance_only (GUI/handoff).",
                "3. list_integration_archetypes() → discover archetype catalog (read-only, no Boomi mutation)",
                "4. get_integration_archetype(name='...') → inspect parameter_schema, capability_notes, limitations, examples",
                "5. build_from_archetype(name='...', parameters={...}) → emit IntegrationSpecV1 (no Boomi mutation)",
                "6. build_integration(action='plan', config='{\"integration_spec\": <spec from step 5>, \"conflict_policy\": \"reuse\"}') → preview deterministic plan",
                "7. review_transformation(action='validate_unmapped', config='{\"integration_spec\": <spec from step 5>}') → confirm the transform has no unmapped/invalid mappings BEFORE apply (read-only, no Boomi mutation). Optionally also run review_transformation(action='list_fields'|'mapping_diff') to inspect fields or diff against a prior spec.",
                "8. build_integration(action='apply', config='{\"dry_run\": false, \"integration_spec\": <spec from step 5>, ...}') → execute ordered component creation/update",
                "9. build_integration(action='verify', config='{\"build_id\": \"<uuid-from-apply>\"}') → verify created components and dependencies",
                "10. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=true) → preview package → deploy → runtime-bind → optional schedule/test; re-run with dry_run=false to execute (deployment happens BEFORE any schedule/test).",
            ],
            "fallback": {
                "when": "No archetype fits — e.g., an integration shape not yet covered by the registry.",
                "steps": [
                    "F1. get_schema_template(resource_type='integration', operation='plan') → get raw IntegrationSpecV1 template",
                    "F2. build_integration(action='plan', config='...') → validate the hand-authored spec",
                    "F3. build_integration(action='apply', config='{\"dry_run\": false, ...}') → execute",
                    "F4. build_integration(action='verify', config='{\"build_id\": \"...\"}') → verify",
                    "F5. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=true) → preview the deploy plan; re-run with dry_run=false to package → deploy → bind runtime → optional schedule/test.",
                ],
            },
        },
        "set_up_b2b_trading_partner": {
            "description": "Create a trading partner for EDI/B2B integration",
            "steps": [
                "1. manage_trading_partner(action='list_options') → see available standards/protocols",
                "2. get_schema_template(resource_type='trading_partner', standard='x12') → get template",
                "3. manage_trading_partner(action='create', config='{...}') → create partner",
                "4. manage_trading_partner(action='analyze_usage', resource_id='...') → verify setup",
            ],
        },
        "troubleshoot_failed_execution": {
            "description": "Debug why a process execution failed",
            # Issue #78: the canonical chain routes execution record → logs →
            # artifacts → dependencies → gotcha search. The final gotcha step is
            # stripped in list_capabilities_action when search_boomi_gotchas is
            # not in the live registry (mirrors how research_boomi_docs is
            # filtered there), so the live capability catalog never advertises an
            # unregistered tool while this canonical surface stays complete.
            "steps": [
                "1. monitor_platform(action='execution_records', config='{\"status\": \"ERROR\", \"limit\": 10}') → find failures",
                "2. monitor_platform(action='execution_logs', config='{\"execution_id\": \"...\"}') → get error logs",
                "3. monitor_platform(action='execution_artifacts', config='{\"execution_id\": \"...\"}') → get output docs",
                "4. analyze_component(action='dependencies', component_id='...') → check dependencies",
                "5. search_boomi_gotchas(query='<observed symptom phrase>') → match the failure against known silent-failure modes / field traps when logs and dependencies are inconclusive",
            ],
        },
        "research_boomi_docs": {
            "description": "Look up current Boomi product behavior in the bundled documentation KB",
            "steps": [
                "1. search_boomi_docs(query='...', top_k=5) → find relevant documentation chunks",
                "2. read_boomi_doc_page(page_key='<hit page_key>') → read surrounding page context when needed",
            ],
        },
        "manage_admin_operations": {
            "description": "Account administration — roles, branches, and uncovered APIs",
            "steps": [
                "1. manage_account(action='list_roles') → list roles",
                "2. manage_account(action='manage_role', config='{\"operation\": \"create\", \"name\": \"...\", \"privileges\": [...]}') → create/update/delete roles",
                "3. manage_account(action='list_branches') → list branches",
                "4. manage_account(action='manage_branch', config='{\"operation\": \"create\", \"name\": \"...\"}') → create/delete branches",
                "5. invoke_boomi_api(...) → for remaining uncovered APIs (integration packs, etc.)",
            ],
        },
    }


def _valid_schema_names() -> list:
    """All accepted get_schema_template(schema_name=...) values.

    Best effort on archetype discovery: a registry failure must never break the
    error envelope that calls this for its valid_schema_names listing.
    """
    names = [
        "IntegrationSpecV1",
        "workflow_sequences",
        "design_doctrine",
        "account_governance",
    ]
    names += [f"workflow:{key}" for key in _authoring_workflow_sequences()]
    # design_doctrine / account_governance are stdlib-only static modules —
    # import-safe, so no try/except guard (unlike the archetype registry
    # discovery below).
    names += [f"design_pattern:{name}" for name in valid_design_pattern_names()]
    names += [
        f"governance_pattern:{name}" for name in valid_governance_pattern_names()
    ]
    try:
        # Call-time import — registry discovery imports patterns.archetypes.*,
        # which imports categories.components.builders; keep meta_tools free of
        # that import-order constraint. Pass the package MODULE (resolved
        # relatively), not a hard-coded name, so discovery works from both the
        # boomi_mcp.* and src.boomi_mcp.* namespaces.
        from .. import patterns as patterns_pkg
        from ..patterns import PatternKind, PatternRegistry
        registry = PatternRegistry.from_package(patterns_pkg)
        archetypes = registry.list_patterns(kind=PatternKind.ARCHETYPE)
    except Exception:  # noqa: BLE001 — discovery is advisory here
        return names
    names += [f"archetype:{cls.metadata.name}" for cls in archetypes]
    return names


def _get_authoring_schema_by_name(schema_name: str) -> Dict[str, Any]:
    """Dispatch get_schema_template(schema_name=...) requests (issue #10).

    Read-only reference data — never calls Boomi, never emits raw XML.
    """
    if schema_name == "IntegrationSpecV1":
        return {
            "_success": True,
            "schema_name": "IntegrationSpecV1",
            "json_schema": IntegrationSpecV1.model_json_schema(),
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "tool": "build_integration (action='plan' | 'apply')",
            "hint": (
                "Prefer archetype-first authoring: list_integration_archetypes() → "
                "get_integration_archetype(...) → build_from_archetype(...) emits a "
                "valid IntegrationSpecV1 for you. Hand-author this spec only when "
                "no archetype fits."
            ),
        }

    if schema_name == "workflow_sequences":
        return {
            "_success": True,
            "schema_name": "workflow_sequences",
            "workflow_sequences": _authoring_workflow_sequences(),
            "record_schema": {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "steps": {"type": "array", "items": {"type": "string"}},
                    "fallback": {
                        "type": "object",
                        "properties": {
                            "when": {"type": "string"},
                            "steps": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                },
                "required": ["description", "steps"],
            },
            "raw_xml_exposed": False,
            "boomi_mutation": False,
        }

    if schema_name.startswith("workflow:"):
        wf_name = schema_name[len("workflow:"):]
        sequences = _authoring_workflow_sequences()
        sequence = sequences.get(wf_name)
        if sequence is None:
            return {
                "_success": False,
                "error": f"Unknown workflow sequence: {wf_name}",
                "error_code": WORKFLOW_SEQUENCE_NOT_FOUND,
                "valid_workflows": sorted(sequences),
            }
        return {
            "_success": True,
            "schema_name": schema_name,
            "workflow": sequence,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
        }

    if schema_name == "design_doctrine":
        # Read-only integration-architecture knowledge surface (issue #86).
        # Conceptual decisions only — no XML/attribute mechanics (see the
        # boomi_mcp.kb.design_doctrine module + its token-lint test).
        catalog = get_design_doctrine_catalog()
        return {
            "_success": True,
            "schema_name": "design_doctrine",
            "surface": "design_doctrine",
            "pattern_surface": "get_schema_template(schema_name='design_pattern:<name>')",
            **catalog,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "read_only": True,
        }

    if schema_name.startswith("design_pattern:"):
        pattern_name = schema_name[len("design_pattern:"):]
        entry = get_design_pattern(pattern_name)
        if entry is None:
            return {
                "_success": False,
                "error": f"Unknown design pattern: {pattern_name}",
                "error_code": SCHEMA_NAME_UNSUPPORTED,
                "valid_design_patterns": valid_design_pattern_names(),
            }
        return {
            "_success": True,
            "schema_name": schema_name,
            "design_pattern": entry,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "read_only": True,
        }

    if schema_name == "account_governance":
        # Read-only account/workspace governance knowledge surface (issue #93).
        # Folder placement, naming, role/write-restriction, copy/versioning
        # decisions — never GUI click-paths or folder-API mechanics (see the
        # boomi_mcp.kb.account_governance module + its anti-template test).
        catalog = get_account_governance_catalog()
        return {
            "_success": True,
            "schema_name": "account_governance",
            "surface": "account_governance",
            "pattern_surface": "get_schema_template(schema_name='governance_pattern:<name>')",
            **catalog,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "read_only": True,
        }

    if schema_name.startswith("governance_pattern:"):
        pattern_name = schema_name[len("governance_pattern:"):]
        entry = get_governance_pattern(pattern_name)
        if entry is None:
            return {
                "_success": False,
                "error": f"Unknown governance pattern: {pattern_name}",
                "error_code": SCHEMA_NAME_UNSUPPORTED,
                "valid_governance_patterns": valid_governance_pattern_names(),
            }
        return {
            "_success": True,
            "schema_name": schema_name,
            "governance_pattern": entry,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "read_only": True,
        }

    if schema_name.startswith("archetype:"):
        archetype_name = schema_name[len("archetype:"):]
        # Call-time import — see _valid_schema_names for the rationale (incl.
        # the module-not-name argument to from_package).
        from .. import patterns as patterns_pkg
        from ..patterns import PatternKind, PatternRegistry, PatternRegistryError
        try:
            registry = PatternRegistry.from_package(patterns_pkg)
            cls = registry.get(archetype_name, kind=PatternKind.ARCHETYPE)
        except PatternRegistryError as exc:
            if exc.error_code == PATTERN_NOT_FOUND:
                return {
                    "_success": False,
                    "error": f"Unknown archetype: {archetype_name}",
                    "error_code": SCHEMA_NAME_UNSUPPORTED,
                    "valid_schema_names": _valid_schema_names(),
                }
            return {
                "_success": False,
                "error": exc.error,
                "error_code": SCHEMA_LOOKUP_FAILED,
                "registry_error_code": exc.error_code,
                "context": exc.context,
            }
        return {
            "_success": True,
            "schema_name": schema_name,
            **cls.describe(),
            "raw_xml_exposed": False,
            "boomi_mutation": False,
        }

    return {
        "_success": False,
        "error": f"Unknown schema_name: {schema_name}",
        "error_code": SCHEMA_NAME_UNSUPPORTED,
        "valid_schema_names": _valid_schema_names(),
    }


# Issue #47 — discovery protocol entry for the read-only infer_profile_fields
# tool. Documents the four modes, inputs/outputs, safety flags, error codes, and
# placeholder-only examples (no canned SQL/JSON/XML payloads).
_PROFILE_INFERENCE_TEMPLATE = {
    "resource_type": "profile_inference",
    "tool": "infer_profile_fields(source_type=..., artifact=..., options=...)",
    "note": (
        "Read-only DISCOVERY (issue #47). Turns a caller-supplied DB metadata "
        "summary / sample JSON / XSD / sample XML into an issue-#43 builder-ready "
        "profile-field contract (profile_config + field_index_by_path + "
        "mappable_paths) plus a parallel `fields` list carrying confidence / "
        "ambiguities / confirmation_required. Never calls Boomi, constructs an "
        "SDK client, reads credentials, requires direct JDBC, or echoes sample "
        "VALUES. Ambiguous sample-derived fields are flagged "
        "(confirmation_required=true) and force ready_for_builder=false — confirm "
        "before passing the contract to a profile/map builder."
    ),
    "read_only": True,
    "boomi_mutation": False,
    "raw_xml_exposed": False,
    "supported_source_types": [
        "profile_from_db_metadata",
        "profile_from_sample_json",
        "profile_from_xsd",
        "profile_from_sample_xml",
    ],
    "modes": {
        "profile_from_db_metadata": {
            "input": (
                "artifact = a column summary: a bare list or "
                "{'columns'|'fields'|'result_columns': [{name, "
                "data_type|db_type|jdbc_type|type, nullable?/required?/mandatory?/optional?}]}."
            ),
            "output_profile": "profile.db / database.read",
            "notes": (
                "string→character, numeric→number, date/time/timestamp→datetime; "
                "boolean/bit and unknown non-binary types are ambiguous candidates "
                "(mapped to character, confirmation_required); binary/blob/image/"
                "varbinary are rejected as unsupported."
            ),
        },
        "profile_from_sample_json": {
            "input": "artifact = a JSON string or already-parsed object / array of objects.",
            "output_profile": "profile.json / json.generated",
            "notes": (
                "object roots map directly; array roots wrap in a synthetic root "
                "object with one repeating child. Mixed scalar / null-only / "
                "optional-across-rows leaves are ambiguous; scalar roots, empty / "
                "scalar / heterogeneous arrays are unsupported-shape errors."
            ),
        },
        "profile_from_xsd": {
            "input": "artifact = an XSD document string (conservative same-document subset).",
            "output_profile": "profile.xml / xml.generated",
            "notes": (
                "supports xs:element / complexType / sequence / simpleType "
                "restriction + minOccurs/maxOccurs(unbounded), targetNamespace "
                "(+ elementFormDefault qualified/unqualified) and xs:attribute. "
                "choice/all/any/group/mixed/import/include/extension/list/union/"
                "substitution and foreign-namespace type refs are unsupported; "
                "recursive types fail with PROFILE_INFERENCE_RECURSIVE_XML."
            ),
        },
        "profile_from_sample_xml": {
            "input": "artifact = an XML document string (namespaces + attributes supported).",
            "output_profile": "profile.xml / xml.generated",
            "notes": (
                "repeated siblings become max_occurs=-1 with [] descendant paths; "
                "children missing from some repeated parents become optional. "
                "attributes / mixed content / namespaced tags / same-name-ancestor "
                "recursion are rejected; leaf types are inferred from text without "
                "echoing the text."
            ),
        },
    },
    "options": {
        "component_name": "Optional display name copied into the generated contract.",
        "root_name": "Optional JSON root object name when a synthetic root is needed (default 'Root').",
        "array_item_name": "Optional name for the synthetic repeating child of a root array (default 'items').",
        "datetime_detection": "Optional bool (default true) — conservative ISO-like datetime recognition.",
        "max_input_chars": "Optional input character limit (lowerable; raisable to a hard cap).",
        "max_nodes": "Optional parsed-node limit (lowerable; raisable to a hard cap).",
        "max_fields": "Optional inferred-field limit (lowerable; raisable to a hard cap).",
    },
    "output_shape": {
        "_success": True,
        "read_only": True,
        "boomi_mutation": False,
        "raw_xml_exposed": False,
        "generation_mode": "<<source_type>>",
        "component_type": "<<profile.db | profile.json | profile.xml>>",
        "profile_type": "<<database.read | json.generated | xml.generated>>",
        "profile_config": "<<builder-ready contract — same shape #43 emits>>",
        "field_index_by_path": "<<path -> field metadata (no #47 keys injected)>>",
        "mappable_paths": "<<leaf paths>>",
        "fields": "<<per-path: confidence / ambiguities / confirmation_required>>",
        "ready_for_builder": "<<bool — false if any field needs confirmation>>",
        "issues": "<<advisory warnings/inferences>>",
        "truncated": False,
        "truncation": None,
    },
    "error_codes": [
        "PROFILE_INFERENCE_INVALID_INPUT",
        "PROFILE_INFERENCE_INVALID_SAMPLE",
        "PROFILE_INFERENCE_UNSUPPORTED_SHAPE",
        "PROFILE_INFERENCE_AMBIGUOUS_SHAPE",
        "PROFILE_INFERENCE_INPUT_TOO_LARGE",
        "PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE",
        "PROFILE_INFERENCE_RECURSIVE_XML",
    ],
    "examples": [
        "infer_profile_fields(source_type='profile_from_db_metadata', "
        "artifact={'columns': [{'name': '<<col>>', 'data_type': '<<varchar|int|timestamp>>'}]})",
        "infer_profile_fields(source_type='profile_from_sample_json', artifact='<<sample JSON string>>')",
        "infer_profile_fields(source_type='profile_from_xsd', artifact='<<XSD document string>>', "
        "options='{\"component_name\": \"<<name>>\"}')",
        "infer_profile_fields(source_type='profile_from_sample_xml', artifact='<<sample XML string>>')",
    ],
    "out_of_scope": {
        "existing_profile_index_discovery": (
            "infer_profile_fields does NOT index arbitrary existing live Boomi "
            "profile XML for literal-UUID transform.map refs — that remains "
            "deferred (the transform.map MAP_PROFILE_INDEX_UNAVAILABLE path)."
        ),
        "business_mappings": (
            "Discovery never invents business field mappings, payload templates, "
            "SQL, scripts, or default values from samples."
        ),
    },
}


def get_schema_template_action(
    resource_type: Optional[str] = None,
    operation: Optional[str] = None,
    standard: Optional[str] = None,
    component_type: Optional[str] = None,
    protocol: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Look up and return the appropriate template.

    Two selectors: ``resource_type`` picks a legacy template; ``schema_name``
    picks an authoring schema (issue #10 — IntegrationSpecV1, archetype:<name>,
    workflow_sequences, workflow:<name>). ``schema_name`` takes precedence when
    both are supplied; omitting both returns SCHEMA_SELECTOR_REQUIRED.
    """

    if schema_name:
        return _get_authoring_schema_by_name(schema_name)

    if not resource_type:
        return {
            "_success": False,
            "error": "Provide resource_type or schema_name.",
            "error_code": SCHEMA_SELECTOR_REQUIRED,
            "valid_types": _VALID_RESOURCE_TYPES,
            "valid_schema_names": _valid_schema_names(),
            "hint": (
                "resource_type selects a template (e.g. 'process'); schema_name "
                "selects an authoring schema (e.g. 'IntegrationSpecV1')."
            ),
        }

    registry = {
        "trading_partner": _get_trading_partner_template,
        "process": _get_process_template,
        "integration": _get_integration_template,
        "component": _get_component_template,
        "environment": _get_environment_template,
        "package": _get_package_template,
        "execution_request": _get_execution_request_template,
        "organization": _get_organization_template,
        "folder": _get_folder_template,
        "monitoring": _get_monitoring_template,
        "profile_inference": _get_profile_inference_template,
    }

    handler = registry.get(resource_type)
    if not handler:
        return {
            "_success": False,
            "error": f"Unknown resource_type: {resource_type}",
            "error_code": SCHEMA_LOOKUP_FAILED,
            "valid_types": _VALID_RESOURCE_TYPES,
        }

    result = handler(
        operation=operation,
        standard=standard,
        component_type=component_type,
        protocol=protocol,
    )
    # Make every template-lookup failure branchable by error_code without
    # touching the individual handlers; setdefault keeps pre-existing specific
    # codes (e.g. UNSUPPORTED_DB_OPERATION_MODE) intact.
    if isinstance(result, dict) and result.get("_success") is False:
        result.setdefault("error_code", SCHEMA_LOOKUP_FAILED)
    return result


def _get_profile_inference_template(operation=None, standard=None, component_type=None, protocol=None, **_):
    """Issue #47 read-only profile-inference discovery protocol entry.

    ``protocol`` may name one of the four source types to surface that mode's
    detail; otherwise the full overview is returned.
    """
    result = {"_success": True, **_PROFILE_INFERENCE_TEMPLATE}
    if protocol:
        modes = _PROFILE_INFERENCE_TEMPLATE["modes"]
        if protocol not in modes:
            return {
                "_success": False,
                "error": f"Unknown profile_inference source_type: {protocol}",
                "valid_protocols": list(modes.keys()),
            }
        result["filtered_mode"] = protocol
        result["mode_detail"] = modes[protocol]
    return result


def _get_trading_partner_template(operation=None, standard=None, protocol=None, **_):
    if protocol:
        tpl = _TP_PROTOCOLS.get(protocol)
        if not tpl:
            return {
                "_success": False,
                "error": f"Unknown protocol: {protocol}",
                "valid_protocols": list(_TP_PROTOCOLS.keys()),
            }
        return {"_success": True, **tpl}

    if not operation:
        return {"_success": True, **_TP_OVERVIEW}

    if operation == "create":
        std = standard or "x12"
        tpl = _TP_CREATE.get(std)
        if not tpl:
            return {
                "_success": False,
                "error": f"Unknown standard: {std}",
                "valid_standards": list(_TP_CREATE.keys()),
            }
        return {"_success": True, **tpl}

    if operation == "list":
        return {
            "_success": True,
            "resource_type": "trading_partner",
            "operation": "list",
            "tool": "manage_trading_partner (action='list')",
            "template": {
                "standard": "x12 | edifact | hl7 | rosettanet | tradacoms | odette | custom",
                "classification": "tradingpartner | mycompany",
                "folder_name": "(optional folder filter)",
            },
            "note": "Returns total_count, partners, by_standard (grouped by standard), and summary. "
                    "Results reflect upstream API rows faithfully, including any duplicate component_ids.",
        }

    if operation == "update":
        return {
            "_success": True,
            "resource_type": "trading_partner",
            "operation": "update",
            "tool": "manage_trading_partner (action='update')",
            "note": "Pass only the fields you want to change. "
                    "Use get_schema_template with operation='create' + standard to see all available fields. "
                    "Protocol fields can also be updated.",
            "example": '{"contact_email": "new@acme.com", "http_url": "https://new.acme.com"}',
        }

    return {
        "_success": False,
        "error": f"Unknown trading_partner operation: {operation}",
        "valid_operations": ["create", "list", "update"],
    }


_PROCESS_FLOW_PROTOCOLS = {
    "database_to_api_sync": {
        "resource_type": "process",
        "operation": "create",
        "protocol": "database_to_api_sync",
        "tool": "build_integration (action='plan' | 'apply')",
        "process_kind": "database_to_api_sync",
        "description": (
            "Structured M2.5 process-flow builder that wires a DB Get "
            "source to a REST send target, with an optional passthrough, "
            "Message, or map-reference transform. Routed via build_integration "
            "when an IntegrationSpecV1 component of type='process' carries "
            "config.process_kind='database_to_api_sync'."
        ),
        "required_fields": [
            "source.connector_type",
            "source.connection_id",
            "source.operation_id",
            "source.action_type",
            "target.connector_type",
            "target.connection_id",
            "target.operation_id",
            "target.action_type",
        ],
        "optional_fields": [
            "folder_name",
            "description",
            "transform",
            "transform.mode",
            "transform.message_text",
            "transform.map_ref",
            # Issue #106 M10.2: process-level Data Process shape
            # (transform.mode='dataprocess'). v1 ships only the Custom Scripting
            # operation; the steps list is ordered and each step carries the
            # operation + its script body.
            "transform.label",
            "transform.steps",
            "transform.steps[].operation",
            "transform.steps[].script",
            "transform.steps[].language",
            "transform.steps[].use_cache",
            # Issue #109 M10.5: process-level Document Cache Retrieve shape
            # (transform.mode='doccacheretrieve'). Pulls documents from a Document
            # Cache into the current flow (the read half of Document Cache CRUD,
            # pairing the already-shipped Add to Cache / doccacheload). v1 ships
            # only the all-document retrieve (load_all_documents=true) with the
            # recommended 'stopprocess' empty-cache behavior.
            "transform.document_cache_id",
            "transform.empty_cache_behavior",
            "transform.load_all_documents",
            "reliability",
            "reliability.retry_count",
            # Issue #99 G1: Try/Catch placement scope. "process" (default — the
            # pre-#99 whole-chain wrapper) or "connector" (a Try/Catch per
            # connector: source retry 0, target retry N) so a target retry does
            # not re-run the source read.
            "reliability.try_catch_scope",
            "reliability.dlq",
            "reliability.dlq.mode",
            # Issue #51 M3.R1a / #88 M4.5.3: DLQ catch-path bindings consumed by
            # the verified Try/Catch wrapper (retry_count 0..5). Bind via the
            # *_id field — a literal component id or a $ref:KEY token in
            # depends_on; the bare *_ref_key variant is not resolvable here.
            "reliability.dlq.document_cache_id",
            "reliability.dlq.process_id",
            # Issue #89 M4.5.4: optional Notify on the wired catch leg
            # (catch -> notify -> dlq route -> stop). Requires a wired DLQ.
            "reliability.catch_notify",
            "reliability.catch_notify.message_template",
            "reliability.catch_notify.level",
            # Issue #108 M10.4: optional deliberate Exception (Throw) terminal on
            # the catch leg — the leg ends in a thrown user-defined error (fail or
            # halt) instead of a bare Stop, and needs no DLQ (bare
            # catch -> [notify ->] exception). Composes with catch_notify / a DLQ
            # route: [notify ->] [dlq route ->] exception.
            "reliability.catch_exception",
            "reliability.catch_exception.title",
            "reliability.catch_exception.message_template",
            "reliability.catch_exception.stop_single_document",
            "reliability.catch_exception.parameter_source",
            # Issue #107 M10.3: optional Return Documents terminal. When
            # return_documents.enabled=true the flow ends in a Return Documents
            # shape (subprocess return value) instead of a Stop; the optional
            # label is the Boomi custom label identifying the returned document
            # type(s). Default (absent) keeps the trailing Stop.
            "return_documents",
            "return_documents.enabled",
            "return_documents.label",
            # Issue #112 M10.8: optional Branch (N-way forward fan-out). When
            # branch.enabled is true (default when the block is present), the
            # post-source document fans to N independent target legs — leg 1 is the
            # top-level target, legs 2..N are branch.targets[] — each ending in its
            # own Stop (forward-only, no join/merge; numBranches = 1 + len(targets),
            # in Boomi's 2..25 range). Each leg target is a REST connector binding
            # with the same fields as the top-level target. Default (branch absent
            # or enabled=false) keeps the single-target flow.
            "branch",
            "branch.enabled",
            "branch.targets",
            "branch.targets[].connector_type",
            "branch.targets[].connection_id",
            "branch.targets[].operation_id",
            "branch.targets[].action_type",
            "branch.targets[].label",
            # Issue #92 M4.5.7: declare connection fields as per-environment
            # override points on the deployed process (see notes for the
            # CREATE-only behavior and the connection_id / fields shape).
            "process_extensions",
            "process_extensions.connections",
        ],
        # Issue #28 added primitives that PRODUCE these fields as process
        # fragments (schedule_envelope, run_metadata, dlq_writer,
        # error_classifier). Issue #29 REPRESENTS them as metadata under
        # build_from_archetype's validation_rules.operational_intent.
        # Issue #51 M3.R1a now CONSUMES the dlq_writer fragment: for
        # retry_count == 0, reliability.dlq.mode in {document_cache_ref,
        # error_subprocess_ref} emits a verified Try/Catch + DLQ catch-path
        # (see optional_fields), so reliability.on_failure is no longer
        # deferred. The remaining fields below are still NOT consumed into
        # process XML, so they stay deferred (not optional) — promoting them
        # would repeat the Codex r3 P2 "silently ignored" lie.
        # `produced_by` names the issue-#28 primitive; `represented_by` names
        # where #29 surfaces the field as metadata; `tracked_by` names the
        # issue/milestone that will wire it into the executable process.
        "deferred_fields": [
            {
                "field": "execution.trigger",
                "produced_by": "schedule_envelope primitive (#28)",
                "represented_by": "build_from_archetype operational_intent metadata (#29)",
                "tracked_by": "M3 (deploy + schedule activation)",
            },
            {
                "field": "execution.run_metadata",
                "produced_by": "run_metadata primitive (#28)",
                "represented_by": "build_from_archetype operational_intent metadata (#29)",
                "tracked_by": "#51 (run-metadata / dynamic process-property wiring)",
            },
            {
                "field": "reliability.error_classifier",
                "produced_by": "error_classifier primitive (#28)",
                "represented_by": "build_from_archetype operational_intent metadata (#29)",
                "tracked_by": "#51 follow-up (catch-path classifier wiring)",
            },
        ],
        "supported_transform_modes": ["passthrough", "message", "map_ref", "dataprocess", "doccacheretrieve"],
        "supported_dataprocess_operations": ["custom_scripting"],
        # Issue #109 M10.5: the only live-verified Document Cache Retrieve
        # "If cache is empty" wire value (Stop document execution, recommended).
        # The backward-compat "fail document with errors" behavior is deferred.
        "supported_doccache_retrieve_empty_behaviors": ["stopprocess"],
        # Issue #107 M10.3: the flow terminal. "stop" is the default; with
        # return_documents.enabled=true it is "returndocuments" instead (and no
        # Stop follows — the verifier's RETURN_DOCS_STOP_EXCLUSIVE invariant).
        "supported_terminal_shapes": ["stop", "returndocuments"],
        # Issue #112 M10.8: control-flow shapes the builder can emit. Branch is the
        # N-way forward fan-out (the only emittable control shape today); Decision /
        # Route remain design guidance (not yet builder-emitted).
        "supported_control_shapes": ["branch"],
        "supported_dlq_modes": ["disabled", "document_cache_ref", "error_subprocess_ref"],
        "supported_notify_levels": ["INFO", "WARNING", "ERROR"],
        "supported_connector_action_bindings": {
            "database_source": {
                "connector_type": "database",
                "action_type": "Get",
            },
            "rest_target": {
                "connector_type": "rest | rest_client | officialboomi-X3979C-rest-prod",
                "action_type": "GET | POST | PUT | PATCH | DELETE | HEAD | OPTIONS | TRACE",
            },
        },
        "structured_errors": [
            {"error_code": "PROCESS_KIND_UNSUPPORTED", "field": "process_kind"},
            {"error_code": "PROCESS_KIND_XML_CONFLICT", "field": "config.xml"},
            {"error_code": "PROCESS_NAME_REQUIRED", "field": "name"},
            {"error_code": "PROCESS_NAME_CONFLICT", "field": "name"},
            {"error_code": "MISSING_PROCESS_DEPENDENCY", "field": "depends_on"},
            {"error_code": "PROCESS_CONNECTOR_BINDING_INVALID", "field": "source|target|branch.targets[N]|branch.targets[N].connector_type|branch.targets[N].connection_id|branch.targets[N].operation_id|branch.targets[N].action_type"},
            {"error_code": "PROCESS_REF_TYPE_MISMATCH", "field": "source.connection_id|source.operation_id|target.connection_id|target.operation_id|target.action_type|branch.targets[N].connection_id|branch.targets[N].operation_id|branch.targets[N].action_type"},
            {"error_code": "PROCESS_SHAPE_UNSUPPORTED", "field": "transform.mode"},
            {"error_code": "PROCESS_DATAPROCESS_CONFIG_INVALID", "field": "transform|transform.steps|transform.steps[N].script|transform.steps[N].language|transform.steps[N].use_cache"},
            {"error_code": "PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED", "field": "transform.steps[N].operation"},
            {"error_code": "PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID", "field": "transform|transform.document_cache_id|transform.empty_cache_behavior|transform.load_all_documents|transform.label"},
            {"error_code": "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID", "field": "return_documents|return_documents.enabled|return_documents.label"},
            {"error_code": "PROCESS_RETRY_UNVERIFIED", "field": "reliability.retry_count|reliability.try_catch_scope"},
            {"error_code": "PROCESS_DLQ_BINDING_INVALID", "field": "reliability.dlq|reliability.dlq.mode|reliability.dlq.document_cache_id|reliability.dlq.process_id"},
            {"error_code": "PROCESS_NOTIFY_CONFIG_INVALID", "field": "reliability.catch_notify|reliability.catch_notify.message_template|reliability.catch_notify.level"},
            {"error_code": "PROCESS_EXCEPTION_CONFIG_INVALID", "field": "reliability.catch_exception|reliability.catch_exception.message_template|reliability.catch_exception.title|reliability.catch_exception.stop_single_document|reliability.catch_exception.parameter_source"},
            {"error_code": "PROCESS_PATH_REPLACEMENT_INVALID", "field": "target.dynamic_path|target.dynamic_path.ddp_name|target.dynamic_path.segments"},
            # Issue #112 M10.8: Branch fan-out. BRANCH_OUTPUT_UNSET is the hard error
            # for an enabled branch with no targets (also the verifier's hard error
            # for an unset branch output). PROCESS_BRANCH_CONFIG_INVALID covers a
            # malformed branch block (non-dict / non-bool enabled / unknown key), too
            # many legs (>25), or an unsupported v1 composition (dynamic_path /
            # Try-Catch reliability / return_documents alongside Branch). A malformed
            # branch LEG BINDING (connector_type / connection_id / operation_id /
            # action_type) reuses PROCESS_CONNECTOR_BINDING_INVALID, and a swapped leg
            # $ref reuses PROCESS_REF_TYPE_MISMATCH — both field-scoped to
            # branch.targets[N].* (see those rows above). BRANCH_NUM_BRANCHES_MISMATCH
            # is a graph-verifier WARNING (numBranches vs dragpoint count), never
            # produced by the builder, which derives numBranches from the leg count.
            {"error_code": "BRANCH_OUTPUT_UNSET", "field": "branch.targets"},
            {"error_code": "PROCESS_BRANCH_CONFIG_INVALID", "field": "branch|branch.enabled|branch.targets|branch.targets[N].dynamic_path|reliability|return_documents|target.dynamic_path"},
            {"error_code": "PROCESS_XML_VALIDATION_FAILED", "field": "config"},
            {"error_code": "PROCESS_EXTENSIONS_INVALID", "field": "process_extensions|process_extensions.connections|process_extensions.connections[N].connection_id|process_extensions.connections[N].fields"},
            {"error_code": "PLAINTEXT_SECRET_REJECTED", "field": "<scanned secret field path>"},
        ],
        "notes": [
            "Issue #51 M3.R1a / #88 M4.5.3: retry_count 0..5 with dlq.mode in "
            "{document_cache_ref, error_subprocess_ref} emits a verified "
            "Try/Catch wrapper + DLQ catch-path (shape captured from live Boomi "
            "Try/Catch XML). Bind the catch leg via reliability.dlq.document_cache_id "
            "(or .process_id) — a literal component id or a $ref:KEY token in "
            "depends_on; the bare *_ref_key variant is rejected with "
            "PROCESS_DLQ_BINDING_INVALID on this build path.",
            "retry_count 1..5 is un-gated (issue #88): Boomi Try/Catch applies "
            "platform-controlled timing (count 1 retries immediately; 2..5 use "
            "built-in escalating waits) — there is no caller-selected backoff "
            "field. retry_count > 0 requires a wired Try/Catch catch path — a "
            "supported reliability.dlq.mode OR a reliability.catch_exception throw "
            "terminal (issue #108 M10.4); retry_count outside 0..5 (or > 0 with "
            "neither a DLQ mode nor a catch_exception) returns "
            "PROCESS_RETRY_UNVERIFIED.",
            "Issue #99 G1: reliability.try_catch_scope selects the Try/Catch "
            "placement. 'process' (default) wraps the whole source -> [transform] "
            "-> target chain in ONE Try/Catch, so a target (REST) retry re-runs "
            "the source (DB) read. 'connector' emits a Try/Catch per connector — "
            "the source in its own Try/Catch with retry 0 and the target in its "
            "own Try/Catch with the configured retry, separated by the source so "
            "the target retry does NOT re-execute the source read (keep source "
            "reads idempotent regardless). Each connector gets its own DLQ (and "
            "optional Notify) catch leg. The database_to_api_sync archetype emits "
            "connector scope by default; an unsupported value returns "
            "PROCESS_RETRY_UNVERIFIED.",
            "Issue #100 G4: the DLQ document-cache catch leg captures the failed "
            "document on a best-effort basis — a malformed failed-REST document "
            "can log 'executed with errors' at the doccacheload step. The failure "
            "is never silent (the Notify step logs the real caught error durably), "
            "but for GUARANTEED durable capture of the failed payload use "
            "reliability.dlq.mode='error_subprocess_ref' (a custom error "
            "subprocess) — the formalized durable escape hatch. The Document "
            "Cache is execution-scoped, so cross-run DLQ replay is currently "
            "manual.",
            "Issue #100 G2: per-document dynamic REST path replacement (e.g. PATCH "
            "/clients/{clientId} bound to a mapped field) is supported via "
            "target.send_request.path_replacements: [{name, target_path}]. The "
            "REST Client connector cannot declare in-operation URL path "
            "parameters (that is an HTTP-Client feature), so the path is supplied "
            "at the process connector step's 'Path' dynamic operation property: a "
            "Set Properties shape concatenates the static segments with the mapped "
            "target leaf(s) into a per-endpoint Dynamic Document Property "
            "(DDP_PATH_<RESOURCE>) — document-scoped so each record in a "
            "multi-record run keeps its own path — and the connector Path property "
            "sources it. "
            "Each {name} token in send_request.path must have a matching "
            "replacement whose target_path is a declared simple leaf bound by a "
            "transform output; names must be unique. When path_replacements is "
            "empty the path is sent verbatim (static), byte-for-byte the prior "
            "behavior. Grounded in a live REST Client export.",
            "Issue #89 M4.5.4: reliability.catch_notify (optional) emits a "
            "verified Notify step at the HEAD of the catch leg. It requires a wired "
            "catch path — a supported DLQ (document_cache_ref / error_subprocess_ref) "
            "OR a reliability.catch_exception throw terminal (issue #108 M10.4). The "
            "leg is one of: notify -> dlq route -> catch stop (DLQ, no exception); "
            "notify -> exception (catch_exception, no DLQ); or notify -> dlq route "
            "-> exception (both). message_template "
            "must reference the platform caught-error property "
            "(meta.base.catcherrorsmessage) so the Notify logs the real error; "
            "level is one of supported_notify_levels (INFO/WARNING/ERROR). The "
            "Notify is log-only — email/SMS notification channels and Notify "
            "outside catch paths are out of scope; unsupported config returns "
            "PROCESS_NOTIFY_CONFIG_INVALID.",
            "Issue #108 M10.4: reliability.catch_exception (optional) ends the "
            "Try/Catch catch leg in a deliberate Exception (Throw) terminal — a "
            "user-defined error reported on the Process Reporting page — INSTEAD "
            "of a bare catch-row Stop (the Boomi docs: a Stop is a successful "
            "conclusion; an error path uses an Exception). It needs no DLQ (a bare "
            "catcherrors -> exception is the live 'fail/halt' shape) and composes "
            "with catch_notify and/or a DLQ route: [notify ->] [dlq route ->] "
            "exception; it also un-gates retry_count > 0 without a DLQ. "
            "message_template carries the {1} placeholder bound by parameter_source "
            "(caught_error = the platform Try/Catch error message; current_document "
            "= the current document; none = a static message with no parameter). "
            "stop_single_document=true fails only the reaching document (others "
            "continue); false (default) halts the whole process. The optional title "
            "is the alert subject / process-log title. The Exception is terminal — "
            "no Stop follows it, so the catch leg stays CONTROL_BRANCH_BARE_STOP-"
            "clean. Malformed config returns PROCESS_EXCEPTION_CONFIG_INVALID. "
            "Live-verified against a real work-account process export.",
            "Issue #28 primitives schedule_envelope, run_metadata, and "
            "error_classifier PRODUCE execution/reliability fragments that "
            "ProcessFlowBuilder does not yet consume — see deferred_fields. The "
            "dlq_writer fragment IS now consumed (above).",
            "Map components are referenced by id or $ref token only; map creation "
            "is tracked by issue #26.",
            "Issue #106 M10.2: transform.mode='dataprocess' inserts a "
            "process-level Data Process shape between source and target, carrying "
            "an ordered transform.steps list. v1 supports ONLY the Custom "
            "Scripting operation (transform.steps[].operation='custom_scripting', "
            "language 'groovy2', use_cache true) — the sole live-observed "
            "operation. Every other documented Data Process operation "
            "(Search/Replace, Zip, Unzip, Base64 encode/decode, Split/Combine "
            "Documents, character encoding) is rejected "
            "PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED until it has a "
            "byte-accurate live capture; malformed step config returns "
            "PROCESS_DATAPROCESS_CONFIG_INVALID. Keep the step body minimal — "
            "prefer native components over custom scripts.",
            "Issue #107 M10.3: return_documents.enabled=true ends the flow in a "
            "Return Documents terminal shape (the subprocess return value — it "
            "returns the current documents to the calling source point: the parent "
            "process via a Process Call/Route, or a web-service client) INSTEAD of "
            "the default Stop. No Stop is emitted after Return Documents (the graph "
            "verifier enforces RETURN_DOCS_STOP_EXCLUSIVE: a Return Documents path "
            "must never reach a Stop). The optional return_documents.label is the "
            "Boomi custom label identifying the returned document type(s), used for "
            "Process Call/Route return-path mapping; it is optional (empty in the "
            "live capture). Default (return_documents absent) keeps the trailing "
            "Stop byte-for-byte. Malformed config returns "
            "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID. Live-verified against a real "
            "work-account process export.",
            "Issue #109 M10.5: transform.mode='doccacheretrieve' inserts a "
            "process-level Document Cache Retrieve shape between source and target "
            "that pulls documents from a Document Cache into the current flow — the "
            "READ half of Document Cache CRUD, pairing the already-shipped Add to "
            "Cache (doccacheload) on the DLQ catch leg. It is a normal linear "
            "non-terminal step. Required transform.document_cache_id binds the "
            "Document Cache component (a literal id or a $ref:KEY token in "
            "depends_on). v1 ships ONLY the live-observed all-document retrieve: "
            "transform.load_all_documents must be true (keyed/index retrieval is "
            "deferred pending a byte-accurate live capture) and "
            "transform.empty_cache_behavior defaults to 'stopprocess' (Stop "
            "document execution — the recommended, sole live-verified value; the "
            "backward-compat 'fail document with errors' option is deferred). "
            "Malformed config returns PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID. "
            "Runtime note (live-verified): all-document retrieve initializes only "
            "when the bound Document Cache enforces one index entry per document; "
            "against a cache without that setting the process fails at graph init "
            "('Retrieve all is only supported for document caches which are set to "
            "enforce single document') — a cache-configuration requirement, not a "
            "builder/XML defect. Live-verified against a real work-account process "
            "export and a renera deploy+execute round-trip.",
            "Issue #112 M10.8: an optional branch block fans the post-source "
            "document out to N independent forward legs (an unconditional Branch — "
            "use a Decision/Route for value-comparing selection). Leg 1 is the "
            "top-level target; branch.targets[] are legs 2..N, each a REST connector "
            "binding with the same fields as the top-level target. numBranches is "
            "derived as 1 + len(branch.targets) and must stay in Boomi's 2..25 "
            "range; each leg ends in its own Stop with no join/merge, and the legs "
            "run in sequence (each completes before the next begins — never truly "
            "parallel). An enabled branch with no targets returns BRANCH_OUTPUT_UNSET; "
            "a malformed branch, >25 legs, or an unsupported v1 composition "
            "(dynamic_path, Try/Catch reliability, or return_documents alongside "
            "branch) returns PROCESS_BRANCH_CONFIG_INVALID. BRANCH_NUM_BRANCHES_MISMATCH "
            "is a graph-verifier warning only — the builder always derives numBranches "
            "from the leg count, so it never emits a mismatch. Default (branch absent "
            "or enabled=false) keeps the single-target flow byte-for-byte. "
            "Live-verified against a real work-account process export.",
            "Issue #92 M4.5.7: process_extensions declares connection fields as "
            "per-environment override points so the DEPLOYED process exposes them "
            "via manage_environments(get_extensions) / update_extensions — without "
            "embedding a credential in the connection component. Shape: "
            "{\"connections\": [{\"connection_id\": <same id/$ref the connector "
            "shape binds>, \"connector_type\": \"database\", \"fields\": [{\"id\": "
            "\"password\", \"label\": \"Password\", \"xpath\": <connection-field "
            "xpath>}]}]}. The declaration is emitted on CREATE only: the builder "
            "leaves the override element unowned so UI-populated per-environment "
            "override VALUES survive structured updates. A field is overrideable "
            "at runtime only because the process declares it; a partial_update "
            "success from update_extensions is not proof a field exists as an "
            "override point — verify via get_extensions after deploy. Malformed "
            "shapes return PROCESS_EXTENSIONS_INVALID.",
            "Schedule activation, deployment, and execution remain M3 scope.",
        ],
        "example_component_spec": {
            "key": "main_process",
            "type": "process",
            "action": "create",
            "name": "<<Integration Process Name>>",
            "depends_on": [
                "db_connection",
                "db_query_operation",
                "target_rest_connection",
                "target_rest_operation",
                "dlq_document_cache",
            ],
            "config": {
                "process_kind": "database_to_api_sync",
                "folder_name": "<<Boomi folder path>>",
                "description": "<<optional description>>",
                "source": {
                    "connector_type": "database",
                    "connection_id": "$ref:db_connection",
                    "operation_id": "$ref:db_query_operation",
                    "action_type": "Get",
                    "label": "<<DB extract label>>",
                },
                "transform": {"mode": "passthrough"},
                "target": {
                    "connector_type": "rest",
                    "connection_id": "$ref:target_rest_connection",
                    "operation_id": "$ref:target_rest_operation",
                    "action_type": "PATCH",
                    "label": "<<REST send label>>",
                },
                # Wired DLQ + optional Notify (#51 / #89): the catch leg logs the
                # caught error then routes failed documents to the DLQ cache. The
                # message_template references the platform caught-error property.
                # try_catch_scope='connector' (#99 G1) gives each connector its
                # own Try/Catch so a target retry does not re-run the source read.
                "reliability": {
                    "retry_count": 0,
                    "try_catch_scope": "connector",
                    "dlq": {
                        "mode": "document_cache_ref",
                        "document_cache_id": "$ref:dlq_document_cache",
                    },
                    "catch_notify": {
                        "level": "ERROR",
                        "message_template": "<<caller-authored notify message referencing meta.base.catcherrorsmessage>>",
                    },
                },
                # Issue #92 M4.5.7: declare the DB connection credential fields
                # as per-environment override points (no embedded credential).
                # connection_id reuses the same $ref the source binds, so the
                # override targets the one DB connection; it must appear in
                # depends_on. The archetype emits this by default for create-mode
                # username_password DB sources.
                "process_extensions": {
                    "connections": [
                        {
                            "connection_id": "$ref:db_connection",
                            "connector_type": "database",
                            "fields": [
                                {
                                    "id": "username",
                                    "label": "User",
                                    "xpath": "DatabaseConnectionSettings/@username",
                                },
                                {
                                    "id": "password",
                                    "label": "Password",
                                    "xpath": "DatabaseConnectionSettings/@password",
                                },
                            ],
                        }
                    ]
                },
            },
        },
    },
    "wrapper_subprocess": {
        "resource_type": "process",
        "operation": "create",
        "protocol": "wrapper_subprocess",
        "process_kind": "wrapper_subprocess",
        "summary": (
            "A thin wrapper-parent ('facade') process: start -> Process Call(s) "
            "-> stop (issue #90 M4.5.5). The parent orchestrates child processes "
            "(the logic units) authored in the SAME IntegrationSpecV1 and "
            "referenced by key (subprocess_ref='$ref:KEY'), or existing Boomi "
            "components referenced by id (process_id). No connector source/target "
            "of its own."
        ),
        "required_fields": [
            "process_kind",
            "process_calls",
        ],
        "optional_fields": [
            "folder_name",
            "description",
            "process_calls[].subprocess_ref",
            "process_calls[].process_id",
            "process_calls[].wait",
            "process_calls[].abort_on_error",
            "process_calls[].label",
            # Issue #99 G3: connection env-extension override points surfaced on
            # the wrapper-deployed package. Usually HOISTED automatically from a
            # called child's process_extensions; may also be declared directly.
            "process_extensions",
            "process_extensions.connections",
            # Issue #107 M10.3: a wrapper that is itself a subprocess may end in a
            # Return Documents terminal (subprocess return value) instead of a
            # Stop. Same shape as database_to_api_sync.
            "return_documents",
            "return_documents.enabled",
            "return_documents.label",
        ],
        "supported_terminal_shapes": ["stop", "returndocuments"],
        "field_notes": {
            "process_calls": "Non-empty list; each entry is one standalone Process Call to a child process.",
            "process_calls[].subprocess_ref": "$ref:KEY of an in-spec process component (the child). EXACTLY ONE of subprocess_ref / process_id per entry.",
            "process_calls[].process_id": "Component id of an EXISTING Boomi process (no in-spec child required).",
            "process_calls[].wait": "Wait for the child to finish before continuing (default true).",
            "process_calls[].abort_on_error": "Abort the parent if the child fails (default false — the parent continues, matching the live wrapper exemplar).",
            "process_extensions": "Issue #99 G3: same shape as the database_to_api_sync process_extensions (connections[].connection_id + fields[].{id,label,xpath}). The integration builder HOISTS a called child's process_extensions onto the wrapper automatically so a wrapper-deployed package surfaces the child override points via get_extensions; you rarely declare it by hand.",
            "return_documents": "Issue #107 M10.3: optional {enabled: bool, label?: str}. When enabled=true the wrapper ends in a Return Documents terminal (the subprocess return value) instead of a Stop — use when the wrapper is itself a subprocess that returns documents to its caller. label is the optional Boomi custom label identifying the returned document type(s). Malformed config returns PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID.",
        },
        "structured_errors": [
            {"error_code": "PROCESS_KIND_UNSUPPORTED", "field": "process_kind"},
            {"error_code": "PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID", "field": "return_documents|return_documents.enabled|return_documents.label"},
            {"error_code": "PROCESS_REF_MISSING", "field": "process_calls|process_calls[N]"},
            {"error_code": "PROCESS_REF_AMBIGUOUS", "field": "process_calls[N]"},
            {"error_code": "PROCESS_REF_SELF_REFERENCE", "field": "process_calls[N].subprocess_ref"},
            {"error_code": "PROCESS_REF_NOT_FOUND", "field": "process_calls[N].subprocess_ref"},
            {"error_code": "PROCESS_REF_TYPE_MISMATCH", "field": "process_calls[N].subprocess_ref|process_extensions.connections[N].connection_id"},
            {"error_code": "PROCESS_CALL_CONFIG_INVALID", "field": "process_calls[N].process_id|process_calls[N].wait|process_calls[N].abort_on_error"},
            {"error_code": "PROCESS_EXTENSIONS_INVALID", "field": "process_extensions|process_extensions.connections|process_extensions.connections[N].connection_id|process_extensions.connections[N].fields"},
            {"error_code": "MISSING_PROCESS_DEPENDENCY", "field": "process_extensions"},
            {"error_code": "PROCESS_XML_VALIDATION_FAILED", "field": "config"},
            {"error_code": "PLAINTEXT_SECRET_REJECTED", "field": "<scanned secret field path>"},
        ],
        "notes": [
            "Author the parent and its children as separate process components in "
            "ONE IntegrationSpecV1. The parent's process_calls reference children "
            "by $ref:KEY; the integration builder applies children FIRST (an "
            "implicit parent->child dependency edge is synthesized, so depends_on "
            "need not list them) and substitutes $ref->created id before building "
            "the parent.",
            "Standalone Process Call is transcribed from a live wrapper exemplar: "
            "it runs the child as a separate process and waits for it (wait=true), "
            "and by default does NOT abort the parent on a child failure "
            "(abort_on_error=false).",
            "Parent-redeploy implication: the parent is the release boundary — when "
            "a child subprocess changes, repackage and redeploy the parent so the "
            "deployed wrapper references the intended child implementation.",
            "Issue #99 G3: a called child's connection env-extension overrides "
            "(#92 process_extensions) do NOT surface through a wrapper Process "
            "Call deployment on their own — they only surface when the declaring "
            "process is deployed directly. The integration builder therefore "
            "HOISTS the child's process_extensions.connections onto the wrapper at "
            "plan time (deduped by connection+field; a wrapper-declared override "
            "wins) and adds the connection $ref to the wrapper's depends_on, so a "
            "wrapper-deployed package exposes the override points through "
            "get_extensions / update_extensions. Verify via get_extensions after "
            "deploy — a partial_update success is not proof the override exists.",
            "Invalid references fail at plan time before any Boomi mutation: a "
            "missing/ambiguous target, a self-reference, a key not present in the "
            "spec, or a non-process target each return a structured PROCESS_REF_* "
            "error.",
            "Branch shape and cross-part document-handoff contracts are out of "
            "scope (Branch stays gated; #14 owns composed fanout).",
        ],
        "example_component_spec": {
            "key": "wrapper_parent",
            "type": "process",
            "action": "create",
            "name": "<<Wrapper Parent Process Name>>",
            "depends_on": ["main_logic"],
            "config": {
                "process_kind": "wrapper_subprocess",
                "folder_name": "<<Boomi folder path>>",
                "process_calls": [
                    {
                        "subprocess_ref": "$ref:main_logic",
                        "wait": True,
                        "abort_on_error": False,
                        "label": "<<invoke main-logic subprocess>>",
                    },
                ],
            },
        },
        "example_child_note": (
            "Author the child (e.g. a database_to_api_sync process) as its own "
            "process component keyed 'main_logic' in the same spec; the parent "
            "references it via subprocess_ref='$ref:main_logic'."
        ),
    },
    "sync_pipeline": {
        "resource_type": "process",
        "operation": "create",
        "protocol": "sync_pipeline",
        "tool": "build_integration (action='plan' | 'apply')",
        "process_kind": "sync_pipeline",
        "description": (
            "Verified-linear process builder (issue #70 M5.2). Takes a semantic "
            "M5.1 PipelineSpec (issue #69) stage graph and lowers the all-"
            "'ordering' linear subset — read(db_read) -> [map] -> send(rest_send) "
            "-> stop — into the proven database_to_api_sync source/transform/"
            "target config. It adds NO new shape: the emitted XML is identical to "
            "the equivalent database_to_api_sync process. Routed via "
            "build_integration when a type='process' component carries "
            "config.process_kind='sync_pipeline'."
        ),
        "required_fields": [
            "process_kind",
            "pipeline",
            "pipeline.stages",
            "pipeline.stages[].key",
            "pipeline.stages[].kind",
            "pipeline.stages[].config",
            "pipeline.stages[].config.primitive",
        ],
        "optional_fields": [
            "name",
            "folder_name",
            "description",
            "process_extensions",
            "pipeline.dependencies",
            "pipeline.stages[].config.connector_type",
            "pipeline.stages[].config.action_type",
            "pipeline.stages[].config.connection_id",
            "pipeline.stages[].config.operation_id",
            "pipeline.stages[].config.map_ref",
            "pipeline.stages[].config.label",
        ],
        "supported_stage_kinds": ["read", "map", "send"],
        "supported_edge_kinds": ["ordering"],
        "supported_terminal_shapes": ["stop"],
        "reserved_stage_kinds": {
            "fetch": "rest_fetch REST source — reserved for M5.4 (issue #72).",
            "write": "db_write DB target — reserved for M5.6 (issue #32).",
            "lookup": "reserved (modeled in M5.1 #69, no emitter yet).",
            "combine": "reserved; combine/control-flow emitters owned by M10 (#103).",
            "flow_control": "reserved; Flow Control is M10.7 (#111), owned by M10 (#103).",
            "branch": "no PipelineSpec lowering; Branch shape owned by M10.8 (#112).",
            "decision": "reserved; control-flow emitters owned by M10 (#103).",
            "dataprocess": "no PipelineSpec lowering; Data Process owned by M10.2 (#106).",
            "exception": "no PipelineSpec lowering; Exception/Throw owned by M10.4 (#108).",
            "doccacheretrieve": "no PipelineSpec lowering; Document Cache Retrieve owned by M10.5 (#109).",
        },
        "field_notes": {
            "pipeline": "An M5.1 PipelineSpec: {stages: [...], dependencies: [...]}. Only the verified-linear, all-'ordering' subset is lowered in M5.2.",
            "pipeline.stages[].kind": "One of read/map/send. Every other PipelineStageKind is reserved (see reserved_stage_kinds) and rejected.",
            "pipeline.stages[].config.primitive": "Required discriminator: 'db_read' for a read stage, 'map' for a map stage, 'rest_send' for a send stage. A reserved primitive (rest_fetch/db_write) is rejected with its owning-issue hint.",
            "pipeline.stages[].config": "read/send carry the connector binding (connection_id, operation_id, optional connector_type/action_type/label); map carries map_ref (or map_id). Any other config key — e.g. a gated dynamic_path or reliability sub-block — is rejected (never silently dropped).",
            "pipeline.stages[].config.map_ref": "The map component id or a $ref:KEY token. Its reachability is enforced (MISSING_PROCESS_DEPENDENCY if the $ref key is not in depends_on), but its component TYPE is not type-checked at plan time — matching database_to_api_sync's transform.map_ref. A shared map-ref role check is a future concern.",
            "pipeline.dependencies": "Typed edges; sync_pipeline requires every edge to be edge_kind='ordering' (the default). The chain must be a single read -> [map] -> send path with no fan-out/fan-in.",
            "gated_blocks": "reliability (Try/Catch retry+DLQ), branch, process_calls, and return_documents are GATED for sync_pipeline — it is verified-linear only (M5.2). Use database_to_api_sync (reliability/dynamic path) or wrapper_subprocess (Process Calls) instead.",
        },
        "structured_errors": [
            {"error_code": "PROCESS_KIND_UNSUPPORTED", "field": "process_kind"},
            {"error_code": "SYNC_PIPELINE_CONFIG_INVALID", "field": "config|pipeline|pipeline.stages|pipeline.stages[KEY].config|pipeline.stages[KEY].config.primitive|pipeline.stages[KEY].config.map_ref|pipeline.stages[KEY].component_ref"},
            {"error_code": "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED", "field": "pipeline.dependencies|pipeline.stages"},
            {"error_code": "SYNC_PIPELINE_STAGE_UNSUPPORTED", "field": "pipeline.stages[KEY].kind|pipeline.stages[KEY].config.primitive"},
            {"error_code": "PROCESS_CONNECTOR_BINDING_INVALID", "field": "source.*|target.*"},
            {"error_code": "PROCESS_REF_TYPE_MISMATCH", "field": "source.connection_id|source.operation_id|target.connection_id|target.operation_id"},
            {"error_code": "MISSING_PROCESS_DEPENDENCY", "field": "source|transform|target"},
            {"error_code": "PROCESS_XML_VALIDATION_FAILED", "field": "config"},
            {"error_code": "PLAINTEXT_SECRET_REJECTED", "field": "<scanned secret field path>"},
        ],
        "notes": [
            "sync_pipeline is the M5.2 foundation: it proves the database_to_api_sync "
            "linear core (DB Get source -> optional map -> REST send target -> stop) "
            "can be expressed as a semantic stage graph and lowered to the SAME XML. "
            "The archetype adapter that emits a sync_pipeline directly is M5.3 (#71); "
            "in M5.2 database_to_api_sync is unchanged.",
            "Every binding $ref:KEY token (source/target connection_id + operation_id, "
            "the map map_ref) must be reachable via the component's depends_on, exactly "
            "as for database_to_api_sync — the lowered config funnels through the same "
            "ref-type and reachability checks.",
            "Reserved stage kinds and gated blocks fail at PLAN time before any Boomi "
            "mutation, each with a hint naming the owning issue (#72/#32 for fetch/"
            "write, M10/#103 for control flow).",
        ],
        "example_component_spec": {
            "key": "sync_pipeline_process",
            "type": "process",
            "action": "create",
            "name": "<<Sync Pipeline Process Name>>",
            "depends_on": ["db_conn", "db_op", "field_map", "rest_conn", "rest_op"],
            "config": {
                "process_kind": "sync_pipeline",
                "folder_name": "<<Boomi folder path>>",
                "pipeline": {
                    "stages": [
                        {
                            "key": "source",
                            "kind": "read",
                            "config": {
                                "primitive": "db_read",
                                "connection_id": "$ref:db_conn",
                                "operation_id": "$ref:db_op",
                            },
                        },
                        {
                            "key": "transform",
                            "kind": "map",
                            "config": {
                                "primitive": "map",
                                "map_ref": "$ref:field_map",
                            },
                        },
                        {
                            "key": "target",
                            "kind": "send",
                            "config": {
                                "primitive": "rest_send",
                                "action_type": "<<HTTP method, e.g. POST>>",
                                "connection_id": "$ref:rest_conn",
                                "operation_id": "$ref:rest_op",
                            },
                        },
                    ],
                    "dependencies": [
                        {"from_stage": "source", "to_stage": "transform"},
                        {"from_stage": "transform", "to_stage": "target"},
                    ],
                },
            },
        },
    },
}


def _get_process_template(operation=None, protocol=None, **_):
    if protocol:
        tpl = _PROCESS_FLOW_PROTOCOLS.get(protocol)
        if not tpl:
            return {
                "_success": False,
                "error": f"Unknown process protocol: {protocol}",
                "valid_protocols": list(_PROCESS_FLOW_PROTOCOLS.keys()),
            }
        return {"_success": True, **tpl}

    if not operation:
        return {"_success": True, **_PROCESS_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_PROCESS_CREATE_REMOVED}

    if operation == "list":
        return {"_success": True, **_PROCESS_LIST}

    return {
        "_success": False,
        "error": f"Unknown process operation: {operation}",
        "valid_operations": ["create", "list"],
    }


def _get_integration_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_INTEGRATION_OVERVIEW}

    if operation == "plan":
        return {"_success": True, **_INTEGRATION_PLAN}

    if operation == "apply":
        return {"_success": True, **_INTEGRATION_APPLY}

    if operation == "verify":
        return {"_success": True, **_INTEGRATION_VERIFY}

    return {
        "_success": False,
        "error": f"Unknown integration operation: {operation}",
        "valid_operations": ["plan", "apply", "verify"],
    }


def _get_component_template(operation=None, component_type=None, protocol=None, **_):
    if not operation:
        result = {"_success": True, **_COMPONENT_OVERVIEW}
        if component_type:
            valid = _COMPONENT_OVERVIEW["component_types"]
            if component_type in valid:
                result["filtered_type"] = component_type
                result["hint"] = f"Use operation='create' or 'search' for {component_type}-specific templates"
            else:
                return {
                    "_success": False,
                    "error": f"Unknown component_type: {component_type}",
                    "valid_types": valid,
                }
        return result

    if operation == "create":
        if component_type == "customlibrary":
            return {"_success": True, **_COMPONENT_CREATE_CUSTOMLIBRARY}
        if component_type == "connector-settings":
            if protocol == "database.sqlserver":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_DATABASE_SQLSERVER}
            if protocol == "rest.client":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_REST_CLIENT}
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown connector-settings protocol: {protocol}",
                    "valid_protocols": _COMPONENT_CREATE_CONNECTOR_SETTINGS_OVERVIEW["available_protocols"],
                }
            return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_SETTINGS_OVERVIEW}
        if component_type == "profile.db":
            if protocol == "database.read":
                return {"_success": True, **_COMPONENT_CREATE_PROFILE_DB_DATABASE_READ}
            if protocol == "database.stored_procedure_read":
                return {
                    "_success": True,
                    **_COMPONENT_CREATE_PROFILE_DB_DATABASE_STORED_PROCEDURE_READ,
                }
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown profile.db protocol: {protocol}",
                    "valid_protocols": _COMPONENT_CREATE_PROFILE_DB_OVERVIEW["available_protocols"],
                }
            return {"_success": True, **_COMPONENT_CREATE_PROFILE_DB_OVERVIEW}
        if component_type == "profile.json":
            if protocol == "json.generated":
                return {"_success": True, **_COMPONENT_CREATE_PROFILE_JSON_GENERATED}
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown profile.json protocol: {protocol}",
                    "valid_protocols": ["json.generated"],
                }
            return {"_success": True, **_COMPONENT_CREATE_PROFILE_JSON_GENERATED}
        if component_type == "profile.xml":
            if protocol == "xml.generated":
                return {"_success": True, **_COMPONENT_CREATE_PROFILE_XML_GENERATED}
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown profile.xml protocol: {protocol}",
                    "valid_protocols": ["xml.generated"],
                }
            return {"_success": True, **_COMPONENT_CREATE_PROFILE_XML_GENERATED}
        if component_type == "transform.map":
            if protocol == "direct":
                return {"_success": True, **_COMPONENT_CREATE_TRANSFORM_MAP_DIRECT}
            if protocol in ("function", "map_function"):
                return {"_success": True, **_COMPONENT_CREATE_TRANSFORM_MAP_FUNCTION}
            if protocol in ("script", "map_script"):
                return {"_success": True, **_COMPONENT_CREATE_TRANSFORM_MAP_SCRIPT}
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown transform.map protocol: {protocol}",
                    "valid_protocols": [
                        "direct",
                        "function",
                        "map_function",
                        "script",
                        "map_script",
                    ],
                }
            return {"_success": True, **_COMPONENT_CREATE_TRANSFORM_MAP_DIRECT}
        if component_type == "script.mapping":
            return {"_success": True, **_COMPONENT_CREATE_SCRIPT_MAPPING}
        if component_type == "connector-action":
            if protocol == "database.get":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_DATABASE_GET}
            if protocol == "rest.operation":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_REST_OPERATION}
            if protocol == "database.send":
                # Explicit out-of-scope marker — point callers at issue #32
                # so they don't think this is a typo we'd accept later.
                return {
                    "_success": False,
                    "error_code": "UNSUPPORTED_DB_OPERATION_MODE",
                    "error": "Database Send/write operations are not implemented in issue #23",
                    "hint": (
                        "Database Send (DatabaseSendAction + WriteProfile + "
                        "commit semantics) is tracked by issue #32 (M5.x). "
                        "Use protocol='database.get' for read extractions."
                    ),
                }
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown connector-action protocol: {protocol}",
                    "valid_protocols": _COMPONENT_CREATE_CONNECTOR_ACTION_OVERVIEW["available_protocols"],
                }
            return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_OVERVIEW}
        result = {"_success": True, **_COMPONENT_CREATE}
        if component_type == "process":
            result["recommendation"] = (
                "For processes, prefer build_from_archetype()/build_integration "
                "with a typed config.process_kind "
                "(database_to_api_sync / wrapper_subprocess / sync_pipeline); use config.xml here "
                "only as an explicit raw-XML escape hatch."
            )
        return result

    if operation == "search":
        return {"_success": True, **_COMPONENT_SEARCH}

    if operation == "clone":
        return {"_success": True, **_COMPONENT_CLONE}

    if operation == "compare_versions":
        return {"_success": True, **_COMPONENT_COMPARE}

    if operation == "safe_edit":
        return {"_success": True, **_COMPONENT_SAFE_EDIT}

    return {
        "_success": False,
        "error": f"Unknown component operation: {operation}",
        "valid_operations": ["create", "search", "clone", "compare_versions", "safe_edit"],
    }


def _get_environment_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_ENVIRONMENT_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_ENVIRONMENT_CREATE}

    return {
        "_success": False,
        "error": f"Unknown environment operation: {operation}",
        "valid_operations": ["create"],
    }


def _get_package_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_PACKAGE_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_PACKAGE_CREATE}

    if operation == "deploy":
        return {"_success": True, **_PACKAGE_DEPLOY}

    return {
        "_success": False,
        "error": f"Unknown package operation: {operation}",
        "valid_operations": ["create", "deploy"],
    }


def _get_execution_request_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_EXECUTION_REQUEST_OVERVIEW}

    if operation == "execute":
        return {"_success": True, **_EXECUTION_REQUEST_EXECUTE}

    return {
        "_success": False,
        "error": f"Unknown execution_request operation: {operation}",
        "valid_operations": ["execute"],
    }


def _get_organization_template(operation=None, **_):
    # Strip org_ prefix if caller passes the manage_trading_partner action name
    if operation and operation.startswith("org_"):
        operation = operation[4:]

    if not operation:
        return {"_success": True, **_ORGANIZATION_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_ORGANIZATION_CREATE}

    if operation == "list":
        return {
            "_success": True,
            "resource_type": "organization",
            "operation": "list",
            "tool": "manage_trading_partner (action='org_list')",
            "template": {"folder_name": "Home/Organizations"},
        }

    if operation in ("get", "update", "delete"):
        result = {
            "_success": True,
            "resource_type": "organization",
            "operation": operation,
            "tool": f"manage_trading_partner (action='org_{operation}')",
        }
        if operation == "update":
            result["note"] = "Pass only the fields you want to change."
            result["example"] = '{"contact_email": "new@acme.com", "contact_phone": "555-5678"}'
        return result

    return {
        "_success": False,
        "error": f"Unknown organization operation: {operation}",
        "valid_operations": ["list", "get", "create", "update", "delete"],
    }


def _get_folder_template(operation=None, **_):
    _FOLDER_OVERVIEW = {
        "resource_type": "folder",
        "tool": "manage_folders",
        "description": "Manage folder hierarchy for organizing Boomi components",
        "actions": {
            "list": "List all folders with tree view, optional filters (include_deleted, folder_name, folder_path)",
            "get": "Get single folder by ID (requires folder_id)",
            "create": "Create folder or hierarchy from path like 'Parent/Child/Grand' (requires folder_name in config)",
            "move_component": "Move a component to a different folder (requires component_id, target_folder_id in config)",
            "delete": "Delete an empty folder (requires folder_id)",
            "restore": "Restore a deleted folder by ID (requires folder_id)",
            "contents": "List components and sub-folders in a folder (requires folder_id or folder_name in config)",
        },
        "examples": {
            "list": 'manage_folders(profile="prod", action="list")',
            "list_filtered": 'manage_folders(profile="prod", action="list", config=\'{"folder_name": "Production"}\')',
            "get": 'manage_folders(profile="prod", action="get", folder_id="abc-123")',
            "create_hierarchy": 'manage_folders(profile="prod", action="create", config=\'{"folder_name": "Production/APIs/v2"}\')',
            "move_component": 'manage_folders(profile="prod", action="move_component", config=\'{"component_id": "comp-123", "target_folder_id": "folder-456"}\')',
            "delete": 'manage_folders(profile="prod", action="delete", folder_id="abc-123")',
            "restore": 'manage_folders(profile="prod", action="restore", folder_id="abc-123")',
            "contents": 'manage_folders(profile="prod", action="contents", folder_id="abc-123")',
        },
    }

    if not operation:
        return {"_success": True, **_FOLDER_OVERVIEW}

    if operation == "create":
        return {
            "_success": True,
            "resource_type": "folder",
            "operation": "create",
            "tool": "manage_folders (action='create')",
            "template": {
                "folder_name": "(required) single name or path like 'A/B/C'",
                "parent_folder_id": "(optional) parent folder ID for the top-level folder",
            },
            "notes": [
                "Paths like 'A/B/C' create all missing levels automatically",
                "Existing folders in the path are reused (not duplicated)",
            ],
        }

    if operation == "list":
        return {
            "_success": True,
            "resource_type": "folder",
            "operation": "list",
            "tool": "manage_folders (action='list')",
            "template": {
                "include_deleted": "(optional, default false) include deleted folders",
                "folder_name": "(optional) filter by folder name (case-insensitive contains)",
                "folder_path": "(optional) filter by full path (case-insensitive contains)",
                "tree_view": "(optional, default true) include ASCII tree in response",
            },
        }

    if operation == "contents":
        return {
            "_success": True,
            "resource_type": "folder",
            "operation": "contents",
            "tool": "manage_folders (action='contents')",
            "template": {
                "folder_id": "(required, or use folder_name) folder ID",
                "folder_name": "(alternative to folder_id) folder name to look up",
            },
        }

    if operation in ("move_component", "move"):
        return {
            "_success": True,
            "resource_type": "folder",
            "operation": "move_component",
            "tool": "manage_folders (action='move_component')",
            "template": {
                "component_id": "(required) ID of the component to move",
                "target_folder_id": "(required) destination folder ID",
            },
            "note": "This moves a component into a folder, not a folder into another folder.",
        }

    if operation in ("get", "delete", "restore"):
        return {
            "_success": True,
            "resource_type": "folder",
            "operation": operation,
            "tool": f"manage_folders (action='{operation}')",
            "note": "Requires folder_id parameter",
        }

    return {
        "_success": False,
        "error": f"Unknown folder operation: {operation}",
        "valid_operations": ["list", "get", "create", "move_component", "delete", "restore", "contents"],
    }


def _get_monitoring_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_MONITORING_OVERVIEW}

    templates = {
        "execution_records": _MONITORING_EXECUTION_RECORDS,
        "execution_logs": _MONITORING_EXECUTION_LOGS,
        "execution_artifacts": _MONITORING_EXECUTION_ARTIFACTS,
        "audit_logs": _MONITORING_AUDIT_LOGS,
        "events": _MONITORING_EVENTS,
        "certificates": _MONITORING_CERTIFICATES,
        "throughput": _MONITORING_THROUGHPUT,
        "execution_metrics": _MONITORING_EXECUTION_METRICS,
        "connector_documents": _MONITORING_CONNECTOR_DOCUMENTS,
        "download_connector_document": _MONITORING_DOWNLOAD_CONNECTOR_DOCUMENT,
    }

    tpl = templates.get(operation)
    if not tpl:
        return {
            "_success": False,
            "error": f"Unknown monitoring operation: {operation}",
            "valid_operations": list(templates.keys()),
        }

    return {"_success": True, **tpl}


# ============================================================================
# plan_integration_design — read-only design-brief assembler (issue #94)
# ============================================================================
#
# A DETERMINISTIC join over three EXISTING read-only registries — the archetype
# registry, design_doctrine (#86), and account_governance (#93) — plus a static
# discovery catalog. It parses NO natural language, calls NO LLM / Sampling,
# calls NO Boomi, invents NO archetype, and carries NO canned task->archetype or
# flag->entry table (anti-template rule). The AGENT owns task -> archetype +
# intent_flags; this tool only ASSEMBLES the brief. Sibling of list_capabilities.

# Generic intent token -> related SEARCH tokens. This only widens the search
# vocabulary used to SCORE catalog entries; it names no entry and no archetype,
# so it is NOT a flag->entry or task->archetype map. Tokens are generic
# integration concepts only.
_PLAN_INTENT_KEYWORD_EXPANSIONS: Dict[str, frozenset] = {
    "retry": frozenset({"retry", "reliability", "resilience", "error", "backoff", "transient"}),
    "dlq": frozenset({"dlq", "dead", "letter", "reliability", "error", "quarantine", "queue"}),
    "reliability": frozenset({"reliability", "retry", "dlq", "error", "resilience", "idempotent"}),
    "notify": frozenset({"notify", "notification", "alert", "observability", "monitoring"}),
    "incremental": frozenset({"incremental", "sync", "watermark", "delta", "cdc", "change"}),
    "bidirectional": frozenset({"bidirectional", "sync", "master", "conflict", "reconcile"}),
    "sync": frozenset({"sync", "incremental", "watermark", "delta", "replication"}),
    "async_queue": frozenset({"async", "queue", "messaging", "decoupling", "buffer"}),
    "human_workflow": frozenset({"human", "workflow", "approval", "task", "review"}),
    "migration": frozenset({"migration", "migrate", "cutover", "backfill"}),
    "routing": frozenset({"routing", "route", "fanout", "dispatch", "branch"}),
    "security": frozenset({"security", "auth", "credential", "secret", "encryption"}),
    "testing": frozenset({"testing", "test", "verification", "mock", "stub"}),
}

# Generic decision-point keywords — a parameter_schema field whose leaf name OR
# description contains one is surfaced as a required user decision. Generic
# integration concepts, never archetype-specific names. Tiered so the
# cross-cutting ARCHITECTURAL choices (watermark, dlq, retry, notification, …)
# rank ahead of required connection plumbing, which ranks ahead of generic
# config keywords — otherwise a deep source/target binding subtree would exhaust
# the decision budget before execution/reliability fields are reached.
_PLAN_DESIGN_KEYWORDS = (
    "watermark", "master", "channel", "destination", "queue", "schedule",
    "retry", "dlq", "notify", "notification", "conflict", "reconcile",
)
_PLAN_CONFIG_KEYWORDS = (
    "auth", "credential", "folder", "runtime", "source", "target",
)
# Combined view (kept for any caller/test referencing the full keyword set).
_PLAN_DECISION_KEYWORDS = _PLAN_DESIGN_KEYWORDS + _PLAN_CONFIG_KEYWORDS

# Decision priority tiers (lower sorts first).
_PLAN_DECISION_PRIORITY_DESIGN = 0   # architectural choice (design keyword)
_PLAN_DECISION_PRIORITY_REQUIRED = 1  # required field at its schema level
_PLAN_DECISION_PRIORITY_CONFIG = 2   # generic config keyword only

# Generic decision verbs that mark a doctrine when_to_use clause as actionable.
_PLAN_DECISION_VERBS = ("choose", "select", "configure", "decide", "specify", "set")

_PLAN_DOCTRINE_CAP = 10
_PLAN_GOVERNANCE_CAP = 5
_PLAN_SCHEMA_DECISION_CAP = 25
_PLAN_DOCTRINE_DECISION_CAP = 5
_PLAN_WHEN_TO_USE_TRUNCATE = 200
_PLAN_SCHEMA_MAX_DEPTH = 6
_PLAN_STATUS_RANK = {"emittable_today": 0, "gated": 1, "guidance_only": 2, "na": 3}
_PLAN_FLAG_ALLOWED = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
)


PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "_success": {"type": "boolean"},
        "tool": {"const": "plan_integration_design"},
        "mode": {"enum": ["archetype", "pre_selection", "error"]},
        "archetype": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "intent_flags": {"type": "array", "items": {"type": "string"}},
        "profile": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "missing_inputs": {"type": "array", "items": {"type": "string"}},
        "recommended_doctrine_patterns": {
            "type": "array", "items": {"$ref": "#/$defs/pattern"}
        },
        "recommended_governance_patterns": {
            "type": "array", "items": {"$ref": "#/$defs/pattern"}
        },
        "capability_gaps": {"type": "array", "items": {"$ref": "#/$defs/gap"}},
        "required_user_decisions": {
            "type": "array", "items": {"$ref": "#/$defs/decision"}
        },
        "discovery_steps": {
            "type": "array", "items": {"$ref": "#/$defs/discovery_step"}
        },
        "doctrine_shown": {"type": "integer"},
        "doctrine_total": {"type": "integer"},
        "governance_shown": {"type": "integer"},
        "governance_total": {"type": "integer"},
        "budget_note": {"type": "string"},
        "notes": {"type": "array", "items": {"type": "string"}},
        "read_only": {"type": "boolean"},
        "boomi_mutation": {"type": "boolean"},
        "raw_xml_exposed": {"type": "boolean"},
        "text": {"type": "string"},
        "error": {"type": "string"},
        "error_code": {"type": "string"},
        "valid_archetypes": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "_success", "tool", "mode", "read_only", "boomi_mutation",
        "raw_xml_exposed", "text",
    ],
    "$defs": {
        "pattern": {
            "type": "object",
            "properties": {
                "source": {"enum": ["design_doctrine", "account_governance"]},
                "name": {"type": "string"},
                "category": {"type": "string"},
                "capability_status": {
                    "enum": ["emittable_today", "gated", "guidance_only", "na"]
                },
                "when_to_use": {"type": "string"},
                "cross_refs": {"type": "array", "items": {"type": "string"}},
            },
            "required": [
                "source", "name", "category", "capability_status",
                "when_to_use", "cross_refs",
            ],
        },
        "gap": {
            "type": "object",
            "properties": {
                "source": {"enum": ["design_doctrine", "account_governance"]},
                "name": {"type": "string"},
                "category": {"type": "string"},
                "capability_status": {"enum": ["gated", "guidance_only", "na"]},
            },
            "required": ["source", "name", "category", "capability_status"],
        },
        "decision": {
            "type": "object",
            "properties": {
                "field": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "description": {"type": "string"},
                "from": {"type": "string"},
            },
            "required": ["field", "description", "from"],
        },
        "discovery_step": {
            "type": "object",
            "properties": {
                "tool": {"type": "string"},
                "purpose": {"type": "string"},
                "arguments": {"type": "object", "additionalProperties": True},
            },
            "required": ["tool", "purpose", "arguments"],
        },
    },
}


def _plan_valid_flag(flag: Any) -> bool:
    """A flag is a short ``[A-Za-z0-9_-]`` token — never free text."""
    return isinstance(flag, str) and bool(flag) and all(
        ch in _PLAN_FLAG_ALLOWED for ch in flag
    )


def _plan_tokenize(text: str) -> set:
    """Lowercase, split on non-alphanumerics, drop tokens shorter than 2."""
    out: set = set()
    cur: list = []
    for ch in str(text).lower():
        if ch.isalnum():
            cur.append(ch)
        elif cur:
            token = "".join(cur)
            if len(token) >= 2:
                out.add(token)
            cur = []
    if cur:
        token = "".join(cur)
        if len(token) >= 2:
            out.add(token)
    return out


def _plan_intent_keywords(flags: list) -> set:
    """Expand normalized flags into a generic search-keyword set."""
    keywords: set = set()
    for flag in flags:
        normalized = flag.lower()
        keywords.update(_PLAN_INTENT_KEYWORD_EXPANSIONS.get(normalized, ()))
        keywords.update(_plan_tokenize(normalized))
    return keywords


def _plan_archetype_keywords(cls: Any) -> set:
    """Tokens drawn from archetype metadata — generic, registry-sourced."""
    keywords: set = set()
    md = cls.metadata
    sources = (
        list(md.tags)
        + list(md.use_cases)
        + list(getattr(cls, "capability_notes", []))
        + list(getattr(cls, "limitations", []))
    )
    for item in sources:
        keywords.update(_plan_tokenize(str(item)))
    return keywords


def _plan_entry_haystack(entry: Dict[str, Any]) -> set:
    parts = [entry.get("name", ""), entry.get("category", "")]
    parts.extend(entry.get("cross_refs", []) or [])
    parts.append(entry.get("when_to_use", "") or "")
    return _plan_tokenize(" ".join(parts))


def _plan_score(entry: Dict[str, Any], keywords: set) -> int:
    if not keywords:
        return 0
    return len(keywords & _plan_entry_haystack(entry))


def _plan_project_pattern(entry: Dict[str, Any], source: str) -> Dict[str, Any]:
    wtu = entry.get("when_to_use", "") or ""
    if len(wtu) > _PLAN_WHEN_TO_USE_TRUNCATE:
        wtu = wtu[: _PLAN_WHEN_TO_USE_TRUNCATE - 3].rstrip() + "..."
    return {
        "source": source,
        "name": entry["name"],
        "category": entry.get("category", ""),
        "capability_status": entry.get("capability_status", "na"),
        "when_to_use": wtu,
        "cross_refs": list(entry.get("cross_refs", []) or []),
    }


def _plan_select_doctrine(entries: list, keywords: set) -> list:
    """Score doctrine entries, expand by cross_refs, rank, and cap.

    Returns the raw selected entries (not yet projected), deterministically
    ordered by (score desc, catalog order asc).
    """
    by_name = {e["name"]: (i, e) for i, e in enumerate(entries)}
    candidates: Dict[str, tuple] = {}
    for i, entry in enumerate(entries):
        score = _plan_score(entry, keywords)
        if score > 0:
            candidates[entry["name"]] = (score, i, entry)
    # Expand selected entries through their cross_refs (score may be 0).
    for name in list(candidates):
        _, _, entry = candidates[name]
        for ref in entry.get("cross_refs", []) or []:
            if ref in by_name and ref not in candidates:
                ref_i, ref_entry = by_name[ref]
                candidates[ref] = (_plan_score(ref_entry, keywords), ref_i, ref_entry)
    ranked = sorted(candidates.values(), key=lambda t: (-t[0], t[1]))
    return [entry for _, _, entry in ranked[:_PLAN_DOCTRINE_CAP]]


def _plan_select_governance(entries: list, keywords: set) -> list:
    """Rank governance entries by (capability_status, relevance desc, catalog).

    Capability status leads so the brief surfaces BUILDABLE governance first
    (all emittable_today, then gated, then guidance_only/na); intent relevance
    orders entries WITHIN each status tier. Status-first ranking is deliberate:
    a relevance-first sort could let the 5-item cap fill with high-relevance
    gated entries (e.g. intent_flags=["folder"]) and omit every emittable_today
    naming pattern — the actionable guidance the agent must not miss.
    """
    ranked = sorted(
        ((_PLAN_STATUS_RANK.get(e.get("capability_status"), 9), _plan_score(e, keywords), i, e)
         for i, e in enumerate(entries)),
        key=lambda t: (t[0], -t[1], t[2]),
    )
    return [entry for _, _, _, entry in ranked[:_PLAN_GOVERNANCE_CAP]]


def _plan_resolve_ref(schema_root: Dict[str, Any], node: Any) -> Any:
    """Follow a local ``#/$defs/`` ``$ref`` chain; non-local refs pass through."""
    seen: set = set()
    while isinstance(node, dict) and "$ref" in node:
        ref = node["$ref"]
        if not ref.startswith("#/$defs/") or ref in seen:
            return node
        seen.add(ref)
        node = schema_root.get("$defs", {}).get(ref[len("#/$defs/"):], {})
    return node


def _plan_schema_decisions(schema_root: Dict[str, Any]) -> list:
    """Walk a parameter_schema and emit generic required-decision items.

    A field is surfaced if its dotted path / description contains a generic
    decision keyword (high priority) OR it is required at its level (lower
    priority). The full schema is walked (depth- and count-bounded), then
    keyword-matched decisions are ordered ahead of merely-required ones before
    the budget cap is applied — so the semantically meaningful decisions
    (watermark, dlq, retry, …) survive even when the schema has many required
    leaf fields. No archetype-specific knowledge is used — purely structural.
    """
    candidates: list = []  # (priority, order, item)
    seen_paths: set = set()
    order = 0
    # Hard guard against pathological/recursive schemas (cycles via $ref are
    # already broken by _plan_resolve_ref, but bound total work anyway).
    max_candidates = _PLAN_SCHEMA_DECISION_CAP * 20

    def keyword_text(path: str, pnode: Any, resolved: Any) -> str:
        # Match the LEAF field name (not the full dotted path) plus the
        # description: a path prefix like "source"/"target" must not flood every
        # nested descendant into a keyword hit.
        leaf = path.rsplit(".", 1)[-1].replace("[]", "")
        desc = ""
        if isinstance(pnode, dict):
            desc = pnode.get("description", "") or ""
        if not desc and isinstance(resolved, dict):
            desc = resolved.get("description", "") or ""
        return (leaf + " " + str(desc)).lower()

    def emit(path: str, pnode: Any, resolved: Any, priority: int) -> None:
        nonlocal order
        if path in seen_paths:
            return
        seen_paths.add(path)
        desc = (
            (isinstance(pnode, dict) and pnode.get("description"))
            or (isinstance(resolved, dict) and resolved.get("description"))
            or f"Provide a value for {path}."
        )
        candidates.append((priority, order, {
            "field": path,
            "description": str(desc)[:_PLAN_WHEN_TO_USE_TRUNCATE],
            "from": "archetype_parameter_schema",
        }))
        order += 1

    def recurse(node: Any, path: str, depth: int) -> None:
        if depth > _PLAN_SCHEMA_MAX_DEPTH or len(candidates) >= max_candidates:
            return
        node = _plan_resolve_ref(schema_root, node)
        if not isinstance(node, dict):
            return
        if "anyOf" in node:
            for branch in node["anyOf"]:
                if isinstance(branch, dict) and branch.get("type") == "null":
                    continue
                recurse(branch, path, depth)
            return
        if node.get("type") == "object" or "properties" in node:
            required = set(node.get("required", []) or [])
            for pname, pnode in (node.get("properties") or {}).items():
                child_path = f"{path}.{pname}" if path else pname
                resolved = _plan_resolve_ref(schema_root, pnode)
                text = keyword_text(child_path, pnode, resolved)
                if any(kw in text for kw in _PLAN_DESIGN_KEYWORDS):
                    emit(child_path, pnode, resolved, _PLAN_DECISION_PRIORITY_DESIGN)
                elif pname in required:
                    emit(child_path, pnode, resolved, _PLAN_DECISION_PRIORITY_REQUIRED)
                elif any(kw in text for kw in _PLAN_CONFIG_KEYWORDS):
                    emit(child_path, pnode, resolved, _PLAN_DECISION_PRIORITY_CONFIG)
                recurse(pnode, child_path, depth + 1)
            return
        if node.get("type") == "array" or "items" in node:
            items = node.get("items")
            if isinstance(items, dict):
                recurse(items, path + "[]", depth + 1)

    recurse(schema_root, "", 0)
    candidates.sort(key=lambda t: (t[0], t[1]))
    return [item for _, _, item in candidates[:_PLAN_SCHEMA_DECISION_CAP]]


def _plan_doctrine_decisions(shown_entries: list) -> list:
    """Extract actionable decisions from selected doctrine ``when_to_use`` prose.

    A clause is surfaced only if it contains a generic decision verb — no entry
    is hard-coded; the verb match is purely textual.
    """
    decisions: list = []
    for entry in shown_entries:
        text = (entry.get("when_to_use", "") or "").replace(";", ".").replace("\n", ".")
        for clause in text.split("."):
            cleaned = clause.strip()
            if not cleaned:
                continue
            low = cleaned.lower()
            if any(verb in low for verb in _PLAN_DECISION_VERBS):
                decisions.append({
                    "field": None,
                    "description": cleaned[:_PLAN_WHEN_TO_USE_TRUNCATE],
                    "from": f"design_doctrine:{entry['name']}",
                })
                if len(decisions) >= _PLAN_DOCTRINE_DECISION_CAP:
                    return decisions
    return decisions


def _plan_discovery_steps(profile: Optional[str], pre_selection: bool) -> list:
    steps: list = []
    if pre_selection:
        steps.append({
            "tool": "list_integration_archetypes",
            "purpose": (
                "Browse archetypes, pick one, then call plan_integration_design "
                "again with archetype set for the full brief."
            ),
            "arguments": {},
        })
    steps.append({
        "tool": "list_boomi_profiles",
        "purpose": "List credential profiles; pass profile= to every account-scoped call.",
        "arguments": {},
    })
    query_args: Dict[str, Any] = {"action": "list"}
    if profile:
        query_args["profile"] = profile
    steps.append({
        "tool": "query_components",
        "purpose": "Discover reusable existing connection components before authoring new ones.",
        "arguments": query_args,
    })
    steps.append({
        "tool": "infer_profile_fields",
        "purpose": "Derive builder-ready profile-field contracts from sample data, DB metadata, or XSD.",
        "arguments": {},
    })
    return steps


def _plan_error_envelope(
    error: str,
    error_code: str,
    archetype: Optional[str],
    intent_flags: list,
    profile: Optional[str],
    valid_archetypes: Optional[list] = None,
) -> Dict[str, Any]:
    envelope = {
        "_success": False,
        "tool": "plan_integration_design",
        "mode": "error",
        "archetype": archetype if isinstance(archetype, str) else None,
        "intent_flags": list(intent_flags),
        "profile": profile if isinstance(profile, str) else None,
        "error": error,
        "error_code": error_code,
        "read_only": True,
        "boomi_mutation": False,
        "raw_xml_exposed": False,
        "text": f"plan_integration_design error [{error_code}]: {error}",
    }
    if valid_archetypes is not None:
        envelope["valid_archetypes"] = valid_archetypes
    return envelope


def plan_integration_design_action(
    archetype: Optional[str] = None,
    intent_flags: Optional[list] = None,
    profile: Optional[str] = None,
) -> Dict[str, Any]:
    """Deterministically assemble a budgeted integration design brief.

    A read-only JOIN over the archetype registry, ``design_doctrine`` (#86), and
    ``account_governance`` (#93), plus a static discovery catalog. Parses no free
    text, calls no LLM/Boomi, infers no archetype. Two modes keyed on whether an
    ``archetype`` is supplied (full brief vs pre-selection brief).
    """
    # --- Validate intent_flags (anti-free-text-parsing guard) ----------------
    if intent_flags is None:
        raw_flags: list = []
    elif isinstance(intent_flags, list):
        raw_flags = intent_flags
    else:
        return _plan_error_envelope(
            "intent_flags must be a list of short tokens, not free text.",
            INVALID_INPUT, archetype, [], profile,
        )
    for flag in raw_flags:
        if not _plan_valid_flag(flag):
            return _plan_error_envelope(
                f"Invalid intent flag {flag!r}: flags must be short "
                f"[A-Za-z0-9_-] tokens, not free text.",
                INVALID_INPUT, archetype, [], profile,
            )
    flags = [flag.lower() for flag in raw_flags]

    # --- Validate archetype type --------------------------------------------
    if archetype is not None and not isinstance(archetype, str):
        return _plan_error_envelope(
            "archetype must be an archetype name string or omitted.",
            INVALID_INPUT, None, flags, profile,
        )
    archetype_provided = isinstance(archetype, str) and archetype.strip() != ""

    keywords = _plan_intent_keywords(flags)

    # --- Archetype mode: resolve + schema-derived decisions ------------------
    schema_decisions: list = []
    cls = None
    if archetype_provided:
        # Call-time import — see _valid_schema_names for the rationale.
        from .. import patterns as patterns_pkg
        from ..patterns import PatternKind, PatternRegistry, PatternRegistryError
        registry = PatternRegistry.from_package(patterns_pkg)
        try:
            cls = registry.get(archetype, kind=PatternKind.ARCHETYPE)
        except PatternRegistryError as exc:
            valid = [c.metadata.name for c in registry.list_patterns(kind=PatternKind.ARCHETYPE)]
            return _plan_error_envelope(
                f"Unknown archetype: {archetype}",
                exc.error_code or PATTERN_NOT_FOUND,
                archetype, flags, profile, valid_archetypes=valid,
            )
        keywords |= _plan_archetype_keywords(cls)
        schema_decisions = _plan_schema_decisions(cls.parameter_schema())

    # --- Join over design_doctrine + account_governance ----------------------
    doctrine_catalog = get_design_doctrine_catalog()
    governance_catalog = get_account_governance_catalog()
    doctrine_entries = doctrine_catalog["entries"]
    governance_entries = governance_catalog["entries"]

    selected_doctrine = _plan_select_doctrine(doctrine_entries, keywords)
    selected_governance = _plan_select_governance(governance_entries, keywords)

    recommended_doctrine_patterns = [
        _plan_project_pattern(e, "design_doctrine") for e in selected_doctrine
    ]
    recommended_governance_patterns = [
        _plan_project_pattern(e, "account_governance") for e in selected_governance
    ]

    capability_gaps = [
        {
            "source": p["source"],
            "name": p["name"],
            "category": p["category"],
            "capability_status": p["capability_status"],
        }
        for p in recommended_doctrine_patterns + recommended_governance_patterns
        if p["capability_status"] != "emittable_today"
    ]

    # --- Mode-specific decisions + discovery + missing_inputs ----------------
    notes: list = []
    if archetype_provided:
        mode = "archetype"
        missing_inputs: list = []
        required_user_decisions = (
            schema_decisions + _plan_doctrine_decisions(selected_doctrine)
        )
        discovery_steps = _plan_discovery_steps(profile, pre_selection=False)
    else:
        mode = "pre_selection"
        missing_inputs = ["archetype"]
        required_user_decisions = [{
            "field": None,
            "description": "select an archetype (see list_integration_archetypes)",
            "from": "missing_input:archetype",
        }]
        discovery_steps = _plan_discovery_steps(profile, pre_selection=True)
        notes.append(
            "Pre-selection brief: no archetype supplied, so "
            "parameter-schema-derived decisions are omitted. Select an archetype "
            "and call again for the full brief."
        )

    # --- Budget + summary ----------------------------------------------------
    doctrine_total = doctrine_catalog["entry_count"]
    governance_total = governance_catalog["entry_count"]
    doctrine_shown = len(recommended_doctrine_patterns)
    governance_shown = len(recommended_governance_patterns)
    budget_note = (
        f"showing {doctrine_shown} of {doctrine_total} design_doctrine entries "
        f"and {governance_shown} of {governance_total} account_governance "
        f"entries (relevance- and capability-ranked)."
    )
    text = "\n".join([
        f"Design brief ({mode}) for archetype="
        f"{archetype if archetype_provided else 'none'} intent_flags={flags}.",
        budget_note,
        f"{len(required_user_decisions)} required decision(s), "
        f"{len(capability_gaps)} capability gap(s), "
        f"{len(discovery_steps)} discovery step(s).",
    ])

    return {
        "_success": True,
        "tool": "plan_integration_design",
        "mode": mode,
        "archetype": archetype if archetype_provided else None,
        "intent_flags": flags,
        "profile": profile,
        "missing_inputs": missing_inputs,
        "recommended_doctrine_patterns": recommended_doctrine_patterns,
        "recommended_governance_patterns": recommended_governance_patterns,
        "capability_gaps": capability_gaps,
        "required_user_decisions": required_user_decisions,
        "discovery_steps": discovery_steps,
        "doctrine_shown": doctrine_shown,
        "doctrine_total": doctrine_total,
        "governance_shown": governance_shown,
        "governance_total": governance_total,
        "budget_note": budget_note,
        "notes": notes,
        "read_only": True,
        "boomi_mutation": False,
        "raw_xml_exposed": False,
        "text": text,
    }


def list_capabilities_action(available_tools: set = None) -> Dict[str, Any]:
    """Return full catalog of MCP tools, actions, and workflows.

    Zero API calls — returns static metadata about this MCP server.

    Args:
        available_tools: Optional set of tool names from the live FastMCP registry.
            When provided, the returned catalog is filtered to only tools actually
            registered in the current runtime (e.g., local-only credential tools
            are excluded in production mode).
    """

    tools = {
        # === Category 1: Components (4 tools) ===
        "query_components": {
            "category": "Components",
            "description": "Query Boomi components — all read operations",
            "actions": ["list", "get", "search", "bulk_get"],
            "read_only": True,
            "parameters": {
                "profile": "str (required) — Boomi profile name",
                "action": "str (required) — list | get | search | bulk_get",
                "component_id": "str (optional) — component ID (required for get)",
                "component_ids": "str (optional) — JSON array of IDs for bulk_get (max 5)",
                "config": "JSON str (optional) — action-specific config",
            },
            "examples": [
                'query_components(profile="prod", action="list", config=\'{"type": "process"}\')',
                'query_components(profile="prod", action="get", component_id="abc-123")',
                'query_components(profile="prod", action="search", config=\'{"name": "%Order%", "type": "process"}\')',
            ],
            "sdk_examples_covered": [
                "list_all_components.py",
                "get_component.py",
                "query_process_components.py",
                "bulk_get_components.py",
            ],
        },
        "manage_component": {
            "category": "Components",
            "description": "Manage component lifecycle — create, update, clone, delete",
            "actions": ["create", "update", "clone", "delete"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — create | update | clone | delete",
                "component_id": "str (optional) — required for update/clone/delete",
                "config": "JSON str (optional) — action-specific config (XML for create, fields for update)",
            },
            "examples": [
                'manage_component(profile="prod", action="clone", component_id="abc-123", config=\'{"name": "My Clone"}\')',
                'manage_component(profile="prod", action="create", config=\'{"xml": "<Component>...</Component>"}\')',
            ],
            "sdk_examples_covered": [
                "create_process_component.py",
                "update_component.py",
                "clone_component.py",
                "delete_component.py",
            ],
        },
        "analyze_component": {
            "category": "Components",
            "description": "Analyze component relationships, version diffs, and merge across branches",
            "actions": ["where_used", "dependencies", "compare_versions", "merge"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — where_used | dependencies | compare_versions | merge",
                "component_id": "str (required)",
                "config": "JSON str (optional) — action-specific config. where_used/dependencies type filter: DEPENDENT or INDEPENDENT",
            },
            "examples": [
                'analyze_component(profile="prod", action="where_used", component_id="abc-123")',
                'analyze_component(profile="prod", action="compare_versions", component_id="abc-123", config=\'{"source_version": 1, "target_version": 2}\')',
                'analyze_component(profile="prod", action="merge", component_id="abc-123", config=\'{"source_branch": "dev-id", "target_branch": "main-id"}\')',
            ],
            "sdk_examples_covered": [
                "find_where_used.py",
                "find_what_uses.py",
                "analyze_dependencies.py",
                "compare_component_versions.py",
                "component_diff.py",
                "merge_components.py",
            ],
        },

        "prepare_component_edit": {
            "category": "Components",
            "description": (
                "Safe edit phase 1 (M9.7): pull an existing component, preview a structured patch, "
                "return a diff + confirmation_token. Read-only — no Boomi mutation. Metadata fields "
                "(name/description/folder) support partial edits; body edits require the full typed-builder "
                "config (see get_schema_template operation='create')."
            ),
            "actions": ["prepare"],
            "read_only": True,
            "parameters": {
                "profile": "str (required)",
                "component_id": "str (required)",
                "patch": "JSON str (required) — {\"component_type\"?, \"config\": {...}, \"map_context\"?}; metadata config = partial edit, body config = full builder config; raw config.xml is rejected",
                "max_diff_lines": "int (optional, default 200)",
            },
            "examples": [
                '# Partial metadata edit:',
                'prepare_component_edit(profile="prod", component_id="abc-123", patch=\'{"config": {"name": "Renamed", "description": "updated"}}\')',
                '# Body edit needs the FULL typed-builder config (connector_type + all fields), not a single field:',
                'prepare_component_edit(profile="prod", component_id="abc-123", patch=\'{"component_type": "connector-settings", "config": {"connector_type": "database", "component_name": "...", "driver_id": "mysql", "host": "db.internal", "dbname": "app", "auth_mode": "username_password", "username": "svc", "credential_ref": "..."}}\')',
            ],
        },
        "apply_component_edit": {
            "category": "Components",
            "description": "Safe edit phase 2 (M9.7): commit a previewed structured patch with the confirmation_token. Requires confirm_apply=true; aborts if the component drifted since preview. Preserves encrypted values / unknown XML.",
            "actions": ["apply"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "component_id": "str (required)",
                "patch": "JSON str (required) — the same patch previewed by prepare_component_edit",
                "confirmation_token": "str (required) — from prepare_component_edit",
                "confirm_apply": "bool (required to commit; defaults false)",
                "max_diff_lines": "int (optional, default 200)",
            },
            "examples": [
                'apply_component_edit(profile="prod", component_id="abc-123", patch=\'{"config": {"name": "Renamed"}}\', confirmation_token="<token>", confirm_apply=true)',
            ],
        },

        "manage_connector": {
            "category": "Components",
            "description": "Manage connector components (connections and operations) with catalog discovery and CRUD",
            "actions": ["list_types", "get_type", "list", "get", "create", "update", "delete"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — list_types | get_type | list | get | create | update | delete",
                "component_id": "str (optional) — for get, update, delete",
                "config": "JSON str (optional) — action-specific config/filters",
            },
            "examples": [
                'manage_connector(profile="prod", action="list_types")',
                'manage_connector(profile="prod", action="get_type", config=\'{"connector_type": "officialboomi-X3979C-rest-prod"}\')',
                'manage_connector(profile="prod", action="list", config=\'{"component_type": "connection", "connector_type": "officialboomi-X3979C-rest-prod"}\')',
                'manage_connector(profile="prod", action="get", component_id="abc-123")',
                'manage_connector(profile="prod", action="create", config=\'{"connector_type": "rest", "component_name": "Target REST OAuth2 Connection", "base_url": "https://api.example.com", "auth": "OAUTH2", "oauth2": {"grant_type": "client_credentials", "client_id": "<<client id>>", "client_secret_ref": "credential://<<vendor>>/oauth-client-secret", "access_token_url": "https://api.example.com/oauth/token"}}\')',
                'manage_connector(profile="prod", action="update", component_id="abc-123", config=\'{"description": "Updated description", "folder_name": "Process Library"}\')',
                'manage_connector(profile="prod", action="delete", component_id="abc-123")',
            ],
            "sdk_examples_covered": [
                "query_connectors.py",
                "get_connector.py",
            ],
        },

        # === Category 2: Environments & Runtimes (2 tools) ===
        "manage_environments": {
            "category": "Environments & Runtimes",
            "description": "Manage environments and their configuration extensions",
            "actions": ["list", "get", "create", "update", "delete", "get_extensions", "update_extensions", "query_extensions", "stats", "get_properties", "update_properties", "get_map_extension", "bulk_get_map_extensions", "list_map_udf_summaries", "create_map_udf", "get_map_udf", "update_map_udf", "delete_map_udf", "list_map_external_components", "list_environment_roles", "create_environment_role", "delete_environment_role"],
            "read_only": False,
            "implemented": True,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "resource_id": "str (optional) — environment ID; for get_properties/update_properties this is the atom/runtime ID",
                "config": "JSON str (optional)",
            },
            "sdk_examples_covered": [
                "manage_environments.py",
                "create_environment.py",
                "get_environment.py",
                "list_environments.py",
                "query_environments.py",
                "update_environment.py",
                "delete_environment.py",
                "manage_environment_extensions.py",
                "update_environment_extensions.py",
            ],
        },
        "manage_runtimes": {
            "category": "Environments & Runtimes",
            "description": "Manage Boomi runtimes — cloud attachments, environment bindings, restart, Java upgrades, release schedules, observability, security, and more",
            "actions": [
                "list", "get", "create", "update", "delete", "attach", "detach", "list_attachments",
                "restart", "configure_java", "create_installer_token",
                "available_clouds", "cloud_list", "cloud_get", "cloud_create", "cloud_update", "cloud_delete",
                "diagnostics",
                "get_release_schedule", "create_release_schedule", "update_release_schedule", "delete_release_schedule",
                "get_observability_settings", "update_observability_settings",
                "get_startup_properties", "reset_counters", "purge",
                "get_security_policies", "update_security_policies",
                "get_connector_versions", "offboard_node", "refresh_secrets_manager",
                "get_account_cloud_attachment_properties", "update_account_cloud_attachment_properties",
                "list_account_cloud_attachment_quotas", "get_account_cloud_attachment_quota",
                "create_account_cloud_attachment_quota", "update_account_cloud_attachment_quota",
                "delete_account_cloud_attachment_quota",
                "get_cloud_attachment_properties", "update_cloud_attachment_properties",
            ],
            "read_only": False,
            "implemented": True,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "resource_id": "str (optional) — runtime ID, attachment ID, or cloud ID",
                "environment_id": "str (optional) — for attach/detach/list_attachments",
                "config": "JSON str (optional)",
            },
            "sdk_examples_covered": [
                "manage_runtimes.py",
                "list_runtimes.py",
                "query_runtimes.py",
                "create_environment_atom_attachment.py",
                "detach_runtime_from_environment.py",
                "restart_runtime.py",
                "manage_java_runtime.py",
                "create_installer_token.py",
            ],
        },

        # === Category 3: Deployment & B2B (4 tools) ===
        "manage_deployment": {
            "category": "Deployment & B2B",
            "description": "Manage deployment packages, deploy to environments, and manage component/process attachments. Environment attachments are the supported path; the *_atom attachment actions are deprecated.",
            "actions": [
                "list_packages", "get_package", "create_package", "delete_package",
                "deploy", "undeploy", "list_deployments", "get_deployment",
                "list_component_atom_attachments", "attach_component_atom", "detach_component_atom",
                "list_component_environment_attachments", "attach_component_environment", "detach_component_environment",
                "list_process_atom_attachments", "attach_process_atom", "detach_process_atom",
                "list_process_environment_attachments", "attach_process_environment", "detach_process_environment", "get_package_manifest",
            ],
            "deprecated_actions": {
                "list_component_atom_attachments": {
                    "error_code": "DEPRECATED_ATOM_ATTACHMENT_ACTION",
                    "replacement_actions": ["list_component_environment_attachments", "manage_runtimes(action='list_attachments')"],
                    "note": "Direct atom attachments are deprecated; rejected/empty on environment-enabled accounts.",
                },
                "attach_component_atom": {
                    "error_code": "DEPRECATED_ATOM_ATTACHMENT_ACTION",
                    "replacement_actions": ["attach_component_environment", "manage_runtimes(action='attach')"],
                    "note": "Direct atom attachments are deprecated; rejected/empty on environment-enabled accounts.",
                },
                "detach_component_atom": {
                    "error_code": "DEPRECATED_ATOM_ATTACHMENT_ACTION",
                    "replacement_actions": ["detach_component_environment", "manage_runtimes(action='detach')"],
                    "note": "Direct atom attachments are deprecated; rejected/empty on environment-enabled accounts.",
                },
                "list_process_atom_attachments": {
                    "error_code": "DEPRECATED_ATOM_ATTACHMENT_ACTION",
                    "replacement_actions": ["list_process_environment_attachments", "manage_runtimes(action='list_attachments')"],
                    "note": "Direct atom attachments are deprecated; rejected/empty on environment-enabled accounts.",
                },
                "attach_process_atom": {
                    "error_code": "DEPRECATED_ATOM_ATTACHMENT_ACTION",
                    "replacement_actions": ["attach_process_environment", "manage_runtimes(action='attach')"],
                    "note": "Direct atom attachments are deprecated; rejected/empty on environment-enabled accounts.",
                },
                "detach_process_atom": {
                    "error_code": "DEPRECATED_ATOM_ATTACHMENT_ACTION",
                    "replacement_actions": ["detach_process_environment", "manage_runtimes(action='detach')"],
                    "note": "Direct atom attachments are deprecated; rejected/empty on environment-enabled accounts.",
                },
            },
            "read_only": False,
            "implemented": True,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "package_id": "str (optional) — package ID for get/delete/deploy/get_package_manifest",
                "environment_id": "str (optional) — target env for deploy, filter for list_deployments",
                "resource_id": "str (optional) — attachment ID for detach actions",
                "config": "str (optional) — JSON with action-specific params. undeploy/get_deployment accept deployment_id or package_id+environment_id. list_deployments accepts component_id filter.",
            },
            "sdk_examples_covered": [
                "create_packaged_component.py",
                "get_packaged_component.py",
                "query_packaged_components.py",
                "delete_packaged_component.py",
                "create_deployment.py",
                "query_deployed_packages.py",
                "promote_package_to_environment.py",
            ],
        },
        "orchestrate_deploy": {
            "category": "Deployment & B2B",
            "description": (
                "One-call deployment orchestration: resolve a build_integration(action='apply') "
                "build to its process component, then package -> deploy -> bind the runtime, then "
                "apply the optional schedule override and optional test run. Stages run strictly in "
                "that order (schedules never run before deployment). Returns a single high-level "
                "summary agents can branch on instead of calling each low-level tool. dry_run=true "
                "(the default) previews the plan with no Boomi mutation; dry_run=false executes. "
                "Every stage reuses (never duplicates) existing resources, so a retry after a "
                "partial failure resumes safely. A failed real run returns structured failure "
                "metadata (error_code, failed_stage, prior_stage_summary, next_step) and a "
                "dry-run cleanup PLAN naming exactly what would be undeployed/deleted/detached; "
                "cleanup defaults to no mutation unless cleanup_on_failure=true. Every full "
                "response carries a top-level behavior_verified marker: deploy/test success is not "
                "behavioral correctness — when run_test=true, read the returned log excerpts before "
                "declaring the integration working, and set require_test_logs=true to fail the run "
                "(TEST_LOGS_UNAVAILABLE) when a test ran but its logs were unavailable."
            ),
            "read_only": False,
            "implemented": True,
            "parameters": {
                "profile": "str (required) — only consulted on a real run (dry_run=false)",
                "build_id": "str (required) — build id returned by build_integration(action='apply')",
                "environment_id": "str (required) — target environment id",
                "runtime_id": "str (required) — target runtime (atom) id",
                "dry_run": "bool (optional) — preview only, no Boomi mutation. DEFAULTS TO true",
                "run_test": "bool (optional) — after a real deploy, execute the process and fetch log/artifact diagnostics",
                "config": (
                    "str (optional) — JSON object for the remaining engine inputs. Allowed keys: "
                    "build_id, environment_id, runtime_id, schedule_override, run_test, dry_run, "
                    "package_version, cleanup_on_failure, test_timeout_seconds, "
                    "test_dynamic_properties, test_process_properties, test_log_level, "
                    "test_fetch_logs, test_fetch_artifacts, test_log_fetch_content, "
                    "require_test_logs. Top-level args override matching config values. "
                    "cleanup_on_failure=false (default) plans cleanup on failure; true executes it "
                    "(destructive). require_test_logs=false (default) keeps a failed test-log fetch "
                    "diagnostic-only; true fails the run with TEST_LOGS_UNAVAILABLE."
                ),
            },
            "response_keys": [
                "_success", "build_id", "process_id", "environment_id", "runtime_id",
                "package", "deployment", "runtime_attachment", "schedule", "execution", "logs",
                "cleanup", "summary", "errors", "warnings", "next_steps", "behavior_verified",
                "error_code", "failed_stage", "prior_stage_summary", "next_step",
            ],
            "examples": [
                'orchestrate_deploy(profile="prod", build_id="<uuid-from-apply>", environment_id="env-1", runtime_id="atom-1", dry_run=true)',
                'orchestrate_deploy(profile="prod", build_id="<uuid-from-apply>", environment_id="env-1", runtime_id="atom-1", dry_run=false)',
            ],
        },
        "manage_trading_partner": {
            "category": "Deployment & B2B",
            "description": "Manage B2B/EDI trading partners (all 7 standards), organizations, and processing groups",
            "actions": [
                "list", "get", "create", "update", "delete",
                "analyze_usage", "list_options",
                "org_list", "org_get", "org_create", "org_update", "org_delete",
                "pg_list", "pg_get", "pg_create", "pg_update", "pg_delete",
            ],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "resource_id": "str (optional) — trading partner or org ID",
                "config": "JSON str (optional) — action-specific config",
            },
            "examples": [
                'manage_trading_partner(profile="prod", action="list", config=\'{"standard": "x12"}\')',
                'manage_trading_partner(profile="prod", action="create", config=\'{"component_name": "Acme", "standard": "x12", ...}\')',
                'manage_trading_partner(profile="prod", action="list_options")',
            ],
            "sdk_examples_covered": [
                "create_trading_partner.py",
                "delete_trading_partner.py",
            ],
        },

        # === Category 4: Execution ===
        "manage_process": {
            "category": "Execution",
            "description": "Read-only process component inspection (list/get). Process authoring is typed: use build_from_archetype()/build_integration with config.process_kind.",
            "actions": ["list", "get"],
            "read_only": True,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — list | get",
                "process_id": "str (optional) — required for get",
                "filters": "JSON str (optional) — e.g. {\"folder_name\": \"Home\"}",
            },
            "examples": [
                'manage_process(profile="prod", action="list")',
                'manage_process(profile="prod", action="get", process_id="<component-id>")',
            ],
        },
        # === Category 4a: Integration Authoring (3 tools — V3 archetypes, no Boomi mutation) ===
        "list_integration_archetypes": {
            "category": "Integration Authoring",
            "description": "List V3 integration archetypes from the local pattern registry. Read-only, no Boomi mutation.",
            "actions": ["(single action — lists archetype metadata)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "query": "str (optional) — case-insensitive substring filter over name/description/tags/use_cases/not_for",
                "tags": "list[str] (optional) — tags the archetype must include (subset match)",
            },
            "examples": [
                'list_integration_archetypes()',
                'list_integration_archetypes(query="stub")',
                'list_integration_archetypes(tags=["safe", "no-boomi-mutation"])',
            ],
        },
        "get_integration_archetype": {
            "category": "Integration Authoring",
            "description": "Get an archetype's metadata, parameter_schema, capability_notes, limitations, and non-template examples. Read-only, no Boomi mutation.",
            "actions": ["(single action — returns enriched describe() payload)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "name": "str (required) — archetype name from list_integration_archetypes()",
            },
            "examples": [
                'get_integration_archetype(name="stub_minimal_integration")',
            ],
        },
        "build_from_archetype": {
            "category": "Integration Authoring",
            "description": "Build an IntegrationSpecV1 from an archetype WITHOUT calling Boomi. Pass the returned spec to build_integration(action='plan') to preview steps. The database_to_api_sync archetype emits executable component specs (DB source, JSON transform, REST target, structured process), including a verified Try/Catch + DLQ catch path with platform-timed retry and an optional log-only Notify step (#51/#88/#89); deployment and scheduling remain M3.",
            "actions": ["(single action — emits an IntegrationSpecV1 only)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "name": "str (required) — archetype name from list_integration_archetypes()",
                "parameters": "dict (optional) — values matching the archetype's parameter_schema",
            },
            "examples": [
                'build_from_archetype(name="stub_minimal_integration", parameters={"integration_name": "demo"})',
                'build_from_archetype(name="database_to_api_sync", parameters={...}) → executable spec for build_integration(action=\'plan\')',
            ],
        },
        "review_transformation": {
            "category": "Integration Authoring",
            "description": "Inspect a transform spec BEFORE build_integration(action='apply'): list fields, find unmapped/invalid mappings, diff mappings, generate synthetic test skeletons, and compare expected-vs-actual payloads. Read-only, never calls Boomi, never exposes raw SQL/XML/credentials/script bodies.",
            "actions": ["list_fields", "validate_unmapped", "mapping_diff", "generate_test_payload", "compare_expected_actual"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "action": "str (required) — list_fields | validate_unmapped | mapping_diff | generate_test_payload | compare_expected_actual",
                "config": "JSON str (optional) — {integration_spec, previous_spec, expected_payload, actual_payload, ignored_paths, allow_extra, strict_types}",
            },
            "examples": [
                'review_transformation(action="validate_unmapped", config=\'{"integration_spec": {...}}\')',
                'review_transformation(action="list_fields", config=\'{"integration_spec": {...}}\')',
                'review_transformation(action="mapping_diff", config=\'{"integration_spec": {...}, "previous_spec": {...}}\')',
                'review_transformation(action="compare_expected_actual", config=\'{"expected_payload": {...}, "actual_payload": {...}}\')',
            ],
        },
        "infer_profile_fields": {
            "category": "Integration Authoring",
            "description": (
                "Read-only DISCOVERY (issue #47): infer issue-#43 builder-ready profile-field "
                "contracts from a DB metadata summary, sample JSON, XSD, or sample XML. Never "
                "calls Boomi, reads credentials, requires JDBC, or echoes sample values. Ambiguous "
                "sample-derived fields are flagged confirmation_required=true / ready_for_builder=false."
            ),
            "actions": [
                "profile_from_db_metadata",
                "profile_from_sample_json",
                "profile_from_xsd",
                "profile_from_sample_xml",
            ],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "source_type": "str (required) — one of profile_from_db_metadata | profile_from_sample_json | profile_from_xsd | profile_from_sample_xml",
                "artifact": "dict/list or str (required) — the metadata summary / sample / schema to infer from",
                "options": "JSON str or dict (optional) — {component_name, root_name, array_item_name, datetime_detection, max_input_chars, max_nodes, max_fields}",
            },
            "examples": [
                "infer_profile_fields(source_type=\"profile_from_db_metadata\", artifact={\"columns\": [{\"name\": \"id\", \"data_type\": \"int\"}]})",
                "infer_profile_fields(source_type=\"profile_from_sample_json\", artifact=\"<sample JSON string>\")",
                "infer_profile_fields(source_type=\"profile_from_xsd\", artifact=\"<XSD string>\") → profile.xml contract for build_integration",
            ],
        },
        "build_integration": {
            "category": "Execution",
            "description": (
                "High-level orchestrator for building integrations from component-oriented JSON specs. "
                "action='apply' returns a build_id; hand that build_id to orchestrate_deploy to "
                "package -> deploy -> bind the runtime (then optional schedule/test) in one call. "
                "Use orchestrate_deploy(dry_run=true) to preview that deploy plan, dry_run=false to execute."
            ),
            "actions": ["plan", "apply", "verify"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — plan | apply | verify",
                "config": "JSON str (optional) — IntegrationSpecV1 payload and execution options",
            },
            "examples": [
                'build_integration(profile="prod", action="plan", config=\'{"name":"Order Sync","mode":"lift_shift","components":[{"key":"p1","type":"process","action":"create","name":"Order Process","config":{"process_kind":"wrapper_subprocess","process_calls":[{"process_id":"<existing-process-uuid>"}]}}]}\')',
                'build_integration(profile="prod", action="apply", config=\'{"dry_run":false,"conflict_policy":"reuse","integration_spec":{"name":"Order Sync","mode":"lift_shift","components":[...]}}\')',
                'build_integration(profile="prod", action="verify", config=\'{"build_id":"<uuid>"}\')',
                '# After apply returns build_id: orchestrate_deploy(profile="prod", build_id="<uuid-from-apply>", environment_id="env-1", runtime_id="atom-1", dry_run=true)',
            ],
        },
        "manage_schedules": {
            "category": "Execution",
            "description": "Manage process schedules — cron-based schedules and schedule enable/disable status",
            "actions": ["list", "get", "update", "delete", "list_status", "get_status", "enable", "disable"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — list | get | update | delete | list_status | get_status | enable | disable",
                "resource_id": "str (optional) — base64 schedule ID",
                "config": "JSON str (optional) — process_id, atom_id, cron, max_retry",
            },
            "examples": [
                'manage_schedules(profile="prod", action="list")',
                'manage_schedules(profile="prod", action="update", resource_id="Q1BTLi4u", config=\'{"cron": "0 9 * * *"}\')',
                'manage_schedules(profile="prod", action="list_status")',
                'manage_schedules(profile="prod", action="enable", config=\'{"process_id": "abc-123", "atom_id": "atom-456"}\')',
            ],
            "sdk_examples_covered": [
                "manage_process_schedules.py",
            ],
        },
        "execute_process": {
            "category": "Execution",
            "description": "Execute a Boomi process (sync or async)",
            "actions": ["execute"],
            "read_only": False,
            "implemented": True,
            "parameters": {
                "profile": "str (required)",
                "process_id": "str (required)",
                "environment_id": "str (optional) — required when atom_id not provided (for runtime auto-resolution)",
                "atom_id": "str (optional) — if provided, skips auto-resolution and environment_id is not needed",
                "config": "JSON str (optional) — {wait: bool, timeout: int, dynamic_properties: {}, process_properties: {}, atom_id: str (fallback), environment_id: str (fallback)}",
            },
            "sdk_examples_covered": [
                "execute_process.py",
            ],
        },

        "troubleshoot_execution": {
            "category": "Execution",
            "description": "Troubleshoot failed executions — error details, retry, reprocess, cancel, queue management. "
                           "On the error_details single-execution path, pass config.observed_symptoms (a string or list) "
                           "to route symptoms through the operational-gotcha catalog; when the gotcha KB is enabled and a "
                           "symptom matches, the response carries a gotcha_matches list (id + title + remediation pointer).",
            "actions": ["error_details", "retry", "reprocess", "cancel", "list_queues", "clear_queue", "move_queue"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "execution_id": "str (optional) — required for error_details, retry; optional for reprocess (see process_id, environment_id, config.atom_id)",
                "process_id": "str (optional) — required for reprocess (with environment_id)",
                "environment_id": "str (optional) — required for reprocess (with process_id)",
                "config": "JSON str (optional) — action-specific options (e.g. days, limit, atom_id, queue_name, dest_queue, "
                          "observed_symptoms for error_details gotcha triage)",
            },
            "examples": [
                'troubleshoot_execution(profile="prod", action="error_details", config=\'{"days": 1, "limit": 5}\')',
                'troubleshoot_execution(profile="prod", action="error_details", execution_id="exec-123", config=\'{"observed_symptoms": ["404 on deployed API"]}\')',
                'troubleshoot_execution(profile="prod", action="retry", execution_id="exec-123")',
                'troubleshoot_execution(profile="prod", action="reprocess", execution_id="exec-123")',
                'troubleshoot_execution(profile="prod", action="reprocess", process_id="proc-456", environment_id="env-789")',
                'troubleshoot_execution(profile="prod", action="list_queues", config=\'{"atom_id": "atom-123"}\')',
                'troubleshoot_execution(profile="prod", action="clear_queue", config=\'{"atom_id": "atom-123", "queue_name": "my-queue"}\')',
            ],
        },

        # === Category 5: Monitoring (1 tool) ===
        "monitor_platform": {
            "category": "Monitoring",
            "description": "Monitor executions, logs, artifacts, audit trail, events, certificates, throughput, metrics, connector documents, summaries, counts, API usage, licensing, and EDI records",
            "actions": [
                "execution_records", "execution_logs", "execution_artifacts",
                "audit_logs", "events", "certificates", "throughput",
                "execution_metrics", "connector_documents", "download_connector_document",
                "execution_summary", "document_counts", "execution_counts",
                "api_usage_counts", "connection_licensing_report", "custom_tracked_fields", "edi_connector_records",
            ],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "config": "JSON str (optional) — action-specific filters",
            },
            "examples": [
                'monitor_platform(profile="prod", action="execution_records", config=\'{"execution_id": "exec-123"}\')',
                'monitor_platform(profile="prod", action="audit_logs", config=\'{"start_date": "2025-01-01", "user": "admin@co.com"}\')',
                'monitor_platform(profile="prod", action="events", config=\'{"event_level": "ERROR"}\')',
                'monitor_platform(profile="prod", action="certificates", config=\'{"days_ahead": 30}\')',
                'monitor_platform(profile="prod", action="throughput", config=\'{"start_date": "2025-01-01", "end_date": "2025-01-31"}\')',
                'monitor_platform(profile="prod", action="execution_metrics", config=\'{"start_date": "2025-01-01T00:00:00Z", "top_failures": 5}\')',
                'monitor_platform(profile="prod", action="connector_documents", config=\'{"execution_id": "exec-123"}\')',
                'monitor_platform(profile="prod", action="download_connector_document", config=\'{"generic_connector_record_id": "rec-123"}\')',
            ],
            "sdk_examples_covered": [
                "poll_execution_status.py",
                "get_execution_summary.py",
                "execution_records.py",
                "analyze_execution_metrics.py",
                "download_process_log.py",
                "download_execution_artifacts.py",
                "query_audit_logs.py",
                "query_events.py",
            ],
        },

        # === Category 6: Organization (1 tool) ===
        "manage_folders": {
            "category": "Organization",
            "description": "Manage folder hierarchy for organizing components — CRUD, move_component, tree view, contents",
            "actions": ["list", "get", "create", "move_component", "delete", "restore", "contents"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — list | get | create | move_component | delete | restore | contents",
                "folder_id": "str (optional) — folder ID (required for get, delete, restore, contents)",
                "config": "JSON str (optional) — action-specific config",
            },
            "examples": [
                'manage_folders(profile="prod", action="list")',
                'manage_folders(profile="prod", action="list", config=\'{"include_deleted": true}\')',
                'manage_folders(profile="prod", action="create", config=\'{"folder_name": "Production/APIs/v2"}\')',
                'manage_folders(profile="prod", action="contents", folder_id="abc-123")',
                'manage_folders(profile="prod", action="move_component", config=\'{"component_id": "comp-123", "target_folder_id": "folder-456"}\')',
            ],
            "sdk_examples_covered": [
                "manage_folders.py",
                "folder_structure.py",
            ],
        },

        # === Category 7: Administration (2 tools) ===
        "manage_shared_resources": {
            "category": "Administration",
            "description": "Manage shared web servers, communication channels, and server information on Boomi runtimes",
            "actions": [
                "list_web_servers", "update_web_server", "get_web_server",
                "list_channels", "get_channel", "create_channel", "update_channel", "delete_channel",
                "get_server_info", "update_server_info",
            ],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — list_web_servers | get_web_server | update_web_server | list_channels | get_channel | create_channel | update_channel | delete_channel | get_server_info | update_server_info",
                "resource_id": "str (optional) — atom ID (web server/server info actions) or channel ID (channel actions)",
                "config": "JSON str (optional) — action-specific parameters",
            },
            "examples": [
                'manage_shared_resources(profile="prod", action="list_web_servers", resource_id="<atom_id>")',
                'manage_shared_resources(profile="prod", action="list_channels")',
                'manage_shared_resources(profile="prod", action="create_channel", config=\'{"name": "My Channel", "channel_type": "HTTP"}\')',
            ],
        },
        "manage_account": {
            "category": "Administration",
            "description": "Manage Boomi account administration — roles, branches, user roles, federations, SSO",
            "actions": [
                "list_roles", "manage_role", "list_branches", "manage_branch",
                "list_assignable_roles", "list_user_roles", "assign_user_role", "remove_user_role",
                "list_user_federations", "create_user_federation", "delete_user_federation", "get_sso_config",
            ],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "resource_id": "str (optional) — role, branch, or association ID (ignored by list_assignable_roles)",
                "config": "JSON str (optional) — action-specific config",
            },
            "notes": {
                "remove_user_role": (
                    "Requires config.confirm_remove=true. "
                    "Blocks removal of a user's last remaining role (prevents total access loss). "
                    "Blocks removal of critical roles (Administrator, Standard User, API - Full Access). "
                    "Blocks removal when critical-role lookup fails (fails closed). "
                    "When using resource_id, user_id must also be provided in config."
                ),
            },
            "examples": [
                'manage_account(profile="prod", action="list_roles")',
                'manage_account(profile="prod", action="list_assignable_roles")',
                'manage_account(profile="prod", action="assign_user_role", config=\'{"user_id": "usr-1", "role_id": "role-2"}\')',
                'manage_account(profile="prod", action="remove_user_role", config=\'{"user_id": "usr-1", "role_id": "role-2", "confirm_remove": true}\')',
                'manage_account(profile="prod", action="get_sso_config")',
            ],
        },

        # === Category 8: Documentation ===
        "search_boomi_docs": {
            "category": "Documentation",
            "description": (
                "Search the Boomi documentation knowledge base by semantic "
                "similarity and return top-ranked chunks with inline content."
            ),
            "actions": ["(single action — semantic docs search)"],
            "read_only": True,
            "parameters": {
                "query": "str (required) — factual Boomi docs question or search terms",
                "top_k": "int (optional) — number of chunks to return, capped by server config",
            },
            "examples": [
                'search_boomi_docs(query="Agent step output process property", top_k=5)',
                'search_boomi_docs(query="Tracking Direction Input Documents Output Documents Process Reporting")',
            ],
            "note": (
                "Use this before answering factual Boomi platform behavior, "
                "connector, configuration, deployment/runtime, scripting, EDI/API, "
                "or error-message questions. After a scale-to-zero cold start the "
                "first call may return error 'warming_up' (the KB is still "
                "loading — wait retry_after_seconds and retry the same call) or "
                "'kb_unavailable' (temporarily unavailable — report that rather "
                "than inventing facts; a later retry may succeed)."
            ),
        },
        "read_boomi_doc_page": {
            "category": "Documentation",
            "description": (
                "Read chunks from a Boomi documentation page by page_key after "
                "a search result indicates the page is relevant."
            ),
            "actions": ["(single action — read page chunks)"],
            "read_only": True,
            "parameters": {
                "page_key": "str (required) — page_key returned by search_boomi_docs",
                "max_chunks": "int (optional) — number of chunks to return",
                "start_chunk_index": "int (optional) — pagination start index",
            },
            "examples": [
                'read_boomi_doc_page(page_key="https://help.boomi.com/docs/Atomsphere/Integration/Process%20building/int-Agent_step")',
            ],
            "note": "Use after search_boomi_docs when surrounding page context is needed.",
        },
        "search_boomi_gotchas": {
            "category": "Documentation",
            "description": (
                "Search the curated catalog of Boomi operational gotchas — known "
                "silent-failure modes and field traps not covered by official "
                "documentation. A separate surface from search_boomi_docs."
            ),
            "actions": ["(single action — symptom search / issue_ids exact lookup)"],
            "read_only": True,
            "parameters": {
                "query": "str (optional) — symptom or error search terms",
                "top_k": "int (optional, default 5, clamped 1..10)",
                "issue_ids": "list[str] (optional) — exact gotcha ids; precedence over query",
            },
            "examples": [
                'search_boomi_gotchas(query="listener test mode returns nothing")',
                'search_boomi_gotchas(issue_ids=["process_call_parent_redeploy"])',
            ],
            "note": (
                "Use for symptom-style questions (deployed cleanly but "
                "misbehaves, value silently dropped). Honor each entry's "
                "verification_status; on low_confidence/no_match do not invent "
                "entries."
            ),
        },

        # === Category 9: Meta / Power Tools ===
        "get_schema_template": {
            "category": "Meta Tools",
            "description": "Get example payloads, field descriptions, and enum values for all tools",
            "actions": ["(single action — specify resource_type+operation, or schema_name)"],
            "read_only": True,
            "parameters": {
                "resource_type": "str (optional — or use schema_name) — trading_partner | process | integration | component | environment | etc.",
                "operation": "str (optional) — create | update | list | etc.",
                "standard": "str (optional) — for trading_partner: x12, edifact, hl7, etc.",
                "component_type": "str (optional) — for component: process, connector-settings, transform.map, etc.",
                "protocol": "str (optional) — for trading_partner: http, as2, ftp, sftp, etc.",
                "schema_name": "str (optional) — authoring schema selector: 'IntegrationSpecV1' | "
                               "'archetype:<name>' | 'workflow_sequences' | 'workflow:<name>' | "
                               "'design_doctrine' | 'design_pattern:<name>' | "
                               "'account_governance' | 'governance_pattern:<name>'. "
                               "Takes precedence over resource_type.",
            },
            "examples": [
                'get_schema_template(resource_type="trading_partner", operation="create", standard="x12")',
                'get_schema_template(resource_type="process", protocol="database_to_api_sync")',
                'get_schema_template(resource_type="integration", operation="plan")',
                'get_schema_template(resource_type="trading_partner", protocol="http")',
                'get_schema_template(schema_name="IntegrationSpecV1")',
                'get_schema_template(schema_name="archetype:database_to_api_sync")',
                'get_schema_template(schema_name="workflow_sequences")',
                'get_schema_template(schema_name="account_governance")',
                'get_schema_template(schema_name="governance_pattern:descriptive_unique_component_names")',
            ],
            "note": "No profile needed — returns static reference data. No API calls. "
                    "Omitting both resource_type and schema_name returns SCHEMA_SELECTOR_REQUIRED.",
        },
        "invoke_boomi_api": {
            "category": "Meta Tools",
            "description": "Generic escape hatch — direct access to ANY Boomi REST API endpoint",
            "actions": ["(any HTTP method to any endpoint)"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "endpoint": "str (required) — e.g., 'Role/query', 'Folder/12345', 'Branch'",
                "method": "str (optional, default=GET) — GET | POST | PUT | DELETE",
                "payload": "JSON str (optional) — request body for POST/PUT",
                "content_type": "str (optional, default=json) — json | xml",
                "accept": "str (optional, default=json) — json | xml",
                "confirm_delete": "bool (optional, default=false) — must be true to allow DELETE operations",
                "confirm_write": "bool (optional, default=false) — must be true for mutating POST/PUT; "
                                 "GET and */query, */queryMore POSTs are read-like and need no confirmation",
            },
            "examples": [
                'invoke_boomi_api(profile="prod", endpoint="Role/query", method="POST", payload=\'{"QueryFilter":...}\')',
                'invoke_boomi_api(profile="prod", endpoint="Branch", method="POST", payload=\'{"name":"feature-v2"}\', confirm_write=True)',
                'invoke_boomi_api(profile="prod", endpoint="Component/abc-123", method="GET", accept="xml")',
            ],
            "covers_uncovered_apis": [
                "Queue Management (async)",
                "Secrets Rotation",
                "Document Reprocessing",
            ],
            "note": "Use dedicated tools when available for better parameter validation. "
                    "Mutating POST/PUT requires confirm_write=true; DELETE still requires "
                    "confirm_delete=true. Raw Component XML writes are a full replacement "
                    "(typed tools preserve unknown XML via read-merge-write) — prefer typed tools.",
        },
        "plan_integration_design": {
            "category": "Knowledge / Design",
            "description": (
                "Assemble a budgeted, read-only design brief by joining the "
                "archetype registry, design_doctrine, and account_governance. "
                "Returns recommended patterns with capability_status, capability "
                "gaps, required user decisions (archetype mode), and discovery "
                "steps. Deterministic — no LLM, no Boomi, no free-text parsing."
            ),
            "actions": ["(single action — returns an assembled design brief)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "archetype": "str (optional) — archetype name from list_integration_archetypes(); omit for the pre-selection brief",
                "intent_flags": "list[str] (optional) — short tokens like retry, dlq, incremental, bidirectional, notify (no free text)",
                "profile": "str (optional) — echoed into the suggested discovery-step arguments; no account call is made",
            },
            "examples": [
                'plan_integration_design(intent_flags=["incremental", "retry"])',
                'plan_integration_design(archetype="database_to_api_sync", intent_flags=["incremental", "dlq"])',
            ],
        },
        "list_capabilities": {
            "category": "Meta Tools",
            "description": "This tool — lists all available MCP tools and capabilities",
            "actions": ["(single action — returns full catalog)"],
            "read_only": True,
            "parameters": {},
            "note": "No parameters needed. Returns this catalog.",
        },

        # === Category 9b: Account Group Management ===
        "manage_account_groups": {
            "category": "Administration",
            "description": "Manage account groups — CRUD, account associations, user roles, integration pack sharing",
            "actions": [
                "list", "get", "create", "update", "delete",
                "list_accounts", "add_account", "remove_account",
                "list_user_roles", "assign_user_role", "remove_user_role",
                "list_integration_packs", "share_integration_pack", "unshare_integration_pack",
            ],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "resource_id": "str (optional) — group or association ID",
                "config": "JSON str (optional) — action-specific parameters",
            },
            "examples": [
                'manage_account_groups(profile="prod", action="list")',
                'manage_account_groups(profile="prod", action="create", config=\'{"name": "Team A"}\')',
                'manage_account_groups(profile="prod", action="add_account", config=\'{"account_group_id": "grp-1", "account_id": "acc-2"}\')',
            ],
        },

        # === Category 10: Listener Management ===
        "manage_listeners": {
            "category": "Runtime Operations",
            "description": "Manage Boomi listener processes — status, pause, resume, restart",
            "actions": ["status", "pause", "resume", "restart"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — status | pause | resume | restart",
                "resource_id": "str (required) — container/atom ID",
                "config": "JSON str (optional) — listener_id to target single listener",
            },
            "examples": [
                'manage_listeners(profile="prod", action="status", resource_id="atom-123")',
                'manage_listeners(profile="prod", action="pause", resource_id="atom-123", config=\'{"listener_id": "lid-456"}\')',
                'manage_listeners(profile="prod", action="restart", resource_id="atom-123")',
            ],
        },

        # === Category 11: Integration Pack Management ===
        "manage_integration_packs": {
            "category": "Administration",
            "description": "Manage integration packs — publisher packs, instances, releases, attachments",
            "actions": [
                "list_packs", "get_pack",
                "list_publisher_packs", "get_publisher_pack", "create_publisher_pack",
                "update_publisher_pack", "delete_publisher_pack",
                "list_instances", "install_instance", "uninstall_instance",
                "release_pack", "update_release", "get_release_status",
                "list_atom_attachments", "attach_atom", "detach_atom",
                "list_environment_attachments", "attach_environment", "detach_environment",
            ],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "resource_id": "str (optional) — pack, instance, or attachment ID",
                "config": "JSON str (optional) — action-specific parameters",
            },
            "examples": [
                'manage_integration_packs(profile="prod", action="list_packs")',
                'manage_integration_packs(profile="prod", action="get_pack", resource_id="pack-123")',
                'manage_integration_packs(profile="prod", action="install_instance", config=\'{"integration_pack_id": "pack-123"}\')',
            ],
        },

        # === Credential Management ===
        "list_boomi_profiles": {
            "category": "Credentials",
            "description": "List all saved Boomi credential profiles",
            "actions": ["(single action — returns profile names)"],
            "read_only": True,
            "parameters": {},
            "note": "Call this first to see available profiles.",
        },
        "boomi_account_info": {
            "category": "Credentials",
            "description": "Get Boomi account info from a specific profile",
            "actions": ["(single action — returns account details)"],
            "read_only": True,
            "parameters": {
                "profile": "str (required) — profile name from list_boomi_profiles",
            },
        },
        "set_boomi_credentials": {
            "category": "Credentials",
            "description": "Store Boomi API credentials for local testing (local dev only)",
            "actions": ["(single action — stores credentials)"],
            "read_only": False,
            "local_only": True,
            "parameters": {
                "profile": "str (required) — profile name (e.g. 'production', 'sandbox')",
                "account_id": "str (required) — Boomi account ID",
                "username": "str (required) — Boomi API username (BOOMI_TOKEN.*)",
                "password": "str (required) — Boomi API password/token",
            },
            "note": "Only available in local development mode (BOOMI_LOCAL=true).",
        },
        "delete_boomi_profile": {
            "category": "Credentials",
            "description": "Delete a stored Boomi credential profile (local dev only)",
            "actions": ["(single action — deletes profile)"],
            "read_only": False,
            "local_only": True,
            "parameters": {
                "profile": "str (required) — profile name to delete",
            },
            "note": "Only available in local development mode (BOOMI_LOCAL=true).",
        },
    }

    # --- Filter to live registry when available ---
    if available_tools is not None:
        tools = {k: v for k, v in tools.items() if k in available_tools}

    # --- Build implementation status ---
    implemented = []
    not_implemented = []
    for name, info in tools.items():
        if info.get("implemented", True):  # default True unless explicitly False
            implemented.append(name)
        else:
            not_implemented.append(name)

    # --- Workflow suggestions (canonical source: _authoring_workflow_sequences) ---
    workflows = _authoring_workflow_sequences()

    # Issue #78: the canonical troubleshoot_failed_execution chain ends with a
    # search_boomi_gotchas step. Strip it here when that tool is NOT in the live
    # registry, BEFORE the available_tools filter runs — otherwise _refs_in_steps
    # would see the unregistered "5. search_boomi_gotchas(" reference and drop the
    # whole workflow. When available_tools is None (no live registry to filter
    # against) the canonical 5-step chain is surfaced as-is, matching
    # get_schema_template(schema_name='workflow:troubleshoot_failed_execution').
    if available_tools is not None and "search_boomi_gotchas" not in available_tools:
        tsfe = workflows.get("troubleshoot_failed_execution")
        if tsfe:
            tsfe["steps"] = [
                s for s in tsfe["steps"] if "search_boomi_gotchas(" not in s
            ]

    # --- Filter workflows to only reference tools in the catalog ---
    if available_tools is not None:
        import re
        tool_names = set(tools.keys())

        def _refs_in_steps(steps):
            # Match both numbered ("1. tool(") and prefixed ("F1. tool(") forms.
            refs = set()
            for step in steps:
                m = re.match(r"[A-Z]*\d+\.\s+(\w+)\(", step)
                if m:
                    refs.add(m.group(1))
            return refs

        filtered_workflows = {}
        for wf_key, wf in workflows.items():
            refs = _refs_in_steps(wf.get("steps", []))
            if not (refs <= tool_names):
                # Main chain references unregistered tools; drop the workflow.
                continue

            # Workflow's main chain is intact. If a fallback block exists but
            # references tools that aren't registered, strip just the fallback
            # — agents can still follow the main chain.
            fallback = wf.get("fallback")
            if fallback:
                fb_refs = _refs_in_steps(fallback.get("steps", []))
                if not (fb_refs <= tool_names):
                    wf = {k: v for k, v in wf.items() if k != "fallback"}

            filtered_workflows[wf_key] = wf
        workflows = filtered_workflows

    # --- Coverage stats ---
    coverage = {
        "total_sdk_examples": 67,
        "direct_coverage": 57,
        "direct_coverage_pct": "85%",
        "indirect_via_invoke_boomi_api": 10,
        "indirect_coverage_pct": "15%",
        "total_coverage_pct": "100%",
        "fully_covered_categories": [
            "Discover & Analyze",
            "Create & Modify",
            "Runtime Setup",
            "Package & Deploy",
            "Execute & Test",
            "Version & Compare",
        ],
    }

    hints = {
        "start_here": "Call list_boomi_profiles() first to see available profiles",
        "need_template": "Use get_schema_template() before create/update operations",
        "uncovered_api": "Use invoke_boomi_api() for APIs without dedicated tools (integration packs, secrets rotation, etc.)",
        "profile_required": "Most tools require a 'profile' parameter — get it from list_boomi_profiles()",
        "raw_write_gate": "invoke_boomi_api mutating POST/PUT requires confirm_write=true (enforced); "
                          "blocked calls return error_code=RAW_WRITE_CONFIRMATION_REQUIRED naming typed alternatives",
        "review_logs": "After a test execution, read the execution log excerpts — deploy/test success is not behavioral verification",
        "bounded_retries": "Never retry an unchanged failing call; change one variable at a time and stop after 3-4 rounds",
        "reuse_connections": "Prefer existing secured connection components before authoring new ones",
        "avoid_scripts": "Prefer native steps and typed map rungs over scripts; a ~50+ line script is an escalation signal",
    }
    # Only point at the docs KB when the live runtime actually registers it.
    if available_tools is None or "search_boomi_docs" in available_tools:
        hints["boomi_docs"] = (
            "For factual Boomi product behavior, connector/configuration semantics, "
            "runtime behavior, EDI/API behavior, scripting, or error messages, "
            "start with search_boomi_docs(). On a cold start it may return "
            "'warming_up' (wait retry_after_seconds and retry) or 'kb_unavailable' "
            "(temporarily unavailable — don't invent facts)."
        )
    # Only point at the gotcha KB when the live runtime actually registers it.
    if available_tools is None or "search_boomi_gotchas" in available_tools:
        hints["boomi_gotchas"] = (
            "For 'why did this silently fail / why is this empty / why didn't my "
            "change take effect' operational issues (silent drops, listener "
            "test-mode, tracked-field scope, parent redeploy after a subprocess "
            "change), consult search_boomi_gotchas() — curated failure patterns "
            "with corrected approaches, separate from search_boomi_docs."
        )
    # Only recommend the archetype-first flow when the entry-point tool is
    # actually registered; otherwise the hint points at a tool the catalog
    # doesn't surface.
    if available_tools is None or "list_integration_archetypes" in available_tools:
        hints["prefer_archetypes"] = (
            "For NEW or migrated integration creation, start with "
            "list_integration_archetypes() before "
            "get_schema_template(resource_type='integration') or direct "
            "build_integration authoring."
        )

    # --- Operating doctrine (issue #10 — companion-adoption guidance) ---
    # Text-only guidance for agents; mechanical counterparts live in the M9
    # epic (#78 gotcha KB, #81 test-verification, #82 anti-script lint) and the
    # #13/M7.3 connection-reuse surface.
    operating_doctrine = {
        "profile_first": (
            "Call list_boomi_profiles() first and pass profile=... to every "
            "account-scoped call."
        ),
        "archetype_first": (
            "For component/integration creation, route through archetypes: "
            "list_integration_archetypes → get_integration_archetype → "
            "build_from_archetype → build_integration(plan/apply). Hand-author "
            "IntegrationSpecV1 only when no archetype fits."
        ),
        "typed_tools_before_raw": (
            "Prefer typed tools over invoke_boomi_api. Reaching for the raw API "
            "escape hatch is an anomaly worth flagging to the user — stop and "
            "reconsider the typed path first."
        ),
        "raw_write_gate_enforced": (
            "Mutating raw POST/PUT via invoke_boomi_api REQUIRES "
            "confirm_write=true — this is ENFORCED, not advisory: unconfirmed "
            "writes are blocked with error_code=RAW_WRITE_CONFIRMATION_REQUIRED "
            "and a typed_alternatives list."
        ),
        "reuse_secured_connections": (
            "Prefer existing secured connection components over authoring new "
            "ones — reuse keeps credentials out of the conversation."
        ),
        "review_logs_after_test": (
            "Deploy/test success is not behavioral correctness: a terminal "
            "COMPLETE status alone is not verification. Read the execution log "
            "excerpts before declaring behavior verified (e.g. Groovy compiles "
            "only at first Atom execution). orchestrate_deploy surfaces this as a "
            "top-level behavior_verified marker; set require_test_logs=true to "
            "enforce log retrieval (a missing fetch then fails the run with "
            "TEST_LOGS_UNAVAILABLE rather than passing diagnostic-only)."
        ),
        "bounded_escalation": (
            "Escalation ladder: (1) check the docs KB first — the most common "
            "failure mode is concluding docs don't exist without actually "
            "checking; (2) apply analogous patterns; (3) bounded experimentation "
            "— one variable at a time, never retry unchanged, stop after 3-4 "
            "rounds; (4) structured user handoff: what was tried, what was "
            "learned, what specifically blocks."
        ),
        "repeated_auth_stop": (
            "[companion_unverified] STOP after ~2 consecutive auth/credential "
            "errors — repeated calls with invalid auth risk platform lockout. "
            "No documented lockout policy was found in the official docs KB; "
            "retained as defensive doctrine."
        ),
        "gui_only_boundaries": (
            "Be honest about GUI-only platform surfaces (branded-connector "
            "OAuth authorization, API Gateway policies, Flow UI, MFT portal "
            "config): say exactly which part needs the Boomi GUI and which part "
            "the MCP can build."
        ),
        "no_throwaway_scripts": (
            "Prefer typed transform rungs (direct map → map function → map "
            "script) and native process steps over scripts; a script past ~50 "
            "lines is an escalation signal, not a convenience."
        ),
    }

    # --- Design doctrine (issue #86 — integration-architecture knowledge) ---
    # A compact index only: name / category / capability_status per entry. The
    # full prose lives behind get_schema_template(schema_name='design_doctrine')
    # so list_capabilities stays a budgeted catalog, not a prose dump. Text-only,
    # so it survives available_tools filtering (like operating_doctrine).
    design_doctrine = {
        "entry_count": get_design_doctrine_catalog()["entry_count"],
        "surface": "get_schema_template(schema_name='design_doctrine')",
        "pattern_surface": "get_schema_template(schema_name='design_pattern:<name>')",
        "note": (
            "Integration-architecture decisions (decomposition, reliability, "
            "sync, routing, testing). Consult BEFORE selecting an archetype; "
            "select patterns by capability_status (emittable_today | gated | "
            "guidance_only)."
        ),
        "index": list_design_doctrine_index(),
    }

    # --- Account governance (issue #93 — folder/naming/role governance) ---
    # A compact index only: name / category / capability_status per entry. The
    # full prose lives behind get_schema_template(schema_name='account_governance')
    # so list_capabilities stays a budgeted catalog. Text-only, so it survives
    # available_tools filtering (like operating_doctrine / design_doctrine).
    account_governance = {
        "entry_count": get_account_governance_catalog()["entry_count"],
        "surface": "get_schema_template(schema_name='account_governance')",
        "pattern_surface": "get_schema_template(schema_name='governance_pattern:<name>')",
        "note": (
            "Account/workspace governance decisions (where a component goes, "
            "what it is named, who may edit it). Consult when authoring; select "
            "entries by capability_status (emittable_today names honored by the "
            "build_integration name lint | gated folder/role decisions the user "
            "applies in the GUI | guidance_only | na)."
        ),
        "index": list_account_governance_index(),
    }

    return {
        "_success": True,
        "server_name": "Boomi MCP Server",
        "server_version": "1.3",
        "total_tools": len(tools),
        "implemented_count": len(implemented),
        "not_implemented_count": len(not_implemented),
        "implemented_tools": implemented,
        "not_implemented_tools": not_implemented,
        "tools": tools,
        "workflows": workflows,
        "coverage": coverage,
        "hints": hints,
        "operating_doctrine": operating_doctrine,
        "design_doctrine": design_doctrine,
        "account_governance": account_governance,
    }
