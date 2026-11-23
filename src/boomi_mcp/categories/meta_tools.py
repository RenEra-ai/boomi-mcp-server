"""
Meta tools — schema templates and generic API invoker.

- get_schema_template_action: self-documenting reference data (no API calls)
- invoke_api: generic escape-hatch for any Boomi REST API endpoint
"""

from typing import Dict, Any, List, Optional, Tuple

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
from .components.wss_route_methods import (
    ASC_CROSS_BASE_MEASURED_PRECEDENCE,
    ASC_GET_WITH_INPUT_RULE,
    ASC_METHOD_RULE,
    ASC_SAME_BASE_SHADOWING,
    BARE_METHOD_RULE,
)
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


def _query_components_actions() -> tuple:
    """The component-query router's own action list.

    Imported at call time rather than at module load: a module-load import of
    the components package from here would invert the layering the
    reachability freeze pins.
    """
    from .components.query_components import QUERY_COMPONENTS_ACTIONS

    return QUERY_COMPONENTS_ACTIONS



# ============================================================================
# Contact Fields (shared across trading partners and organizations)
# ============================================================================

# Issue #139D (ADR-001 §5/§9) — the two IntegrationSpecV1 authoring surfaces.
# Published so a caller can discover the strict surface without reading the ADR:
# ADR-001 §9 lets the strict surface ship precisely because it withdraws no V1
# acceptance, so V1 is NOT deprecated and emits no warning.
_AUTHORITY_VERSIONS = {
    "default": "1.0",
    "strict": "1.1",
    "selector_path": "config.integration_spec.version",
    "strict_input_form": "integration_spec",
    "legacy_only_input_forms": ["source_description", "bare_top_level"],
    "v1_deprecated": False,
    "strict_behavior": (
        "Since #159 S3, version='1.1' selects no different behaviour from '1.0': "
        "the strict pipeline-vs-process authority retired with the legacy "
        "process builders, so no serving route raises "
        "LEGACY_ADAPTER_AUTHORITY_CONFLICT any more. An authored top-level "
        "integration_spec.pipeline is an inert analysis view on both versions "
        "and is never executable; build_integration authors a process only from a "
        "canonical ProcessIR unit (integration_spec.processes, or authoring_request "
        "with intent_kind='process_ir'), its only executable process authority."
    ),
    "legacy_behavior": (
        "On version='1.0' (the default) an authored top-level pipeline is "
        "accepted and preserved as an inert echo that drives nothing — "
        "including when it contradicts the authored process."
    ),
}

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
        "build_integration using config.authoring_request: a recipe from "
        "list_integration_archetypes() (intent_kind='recipe') or ProcessIR "
        "units (intent_kind='process_ir'; get_schema_template("
        "schema_name='AuthoringRequestV1'). A legacy config.process_kind body "
        "is refused with PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED."
    ),
}

#: #159 S3: the three legacy process_kind protocol templates are RETIRED. A
#: process body is authored only as a canonical unit, so a template teaching a
#: config.process_kind body would teach a shape admission refuses
#: (PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED). The names stay addressable so a
#: caller who asks gets the removal envelope below instead of "unknown".
_RETIRED_PROCESS_PROTOCOLS: Tuple[str, ...] = (
    "database_to_api_sync",
    "wrapper_subprocess",
    "sync_pipeline",
)

# Legacy freeform process JSON authoring has been removed. The
# operation='create' schema now returns removal guidance instead of a
# shape-graph template, steering callers to canonical authoring units.
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
            "list_integration_archetypes() → get_integration_archetype() → "
            "build_integration(action='plan'|'compile'|'apply') with "
            "config.authoring_request (intent_kind='recipe')"
        ),
        "typed_process_kind": (
            "Retired (#159 S3): a config.process_kind (or process_type / "
            "flow_sequence) body is refused with "
            "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED. Author the process as "
            "authoring_request.intent with intent_kind='process_ir' (one unit per "
            "process: envelope + ProcessIR root), or as a typed unit in "
            "integration_spec.processes; see get_schema_template("
            "schema_name='AuthoringRequestV1'). A flat type='process' row may "
            "only bind an existing process (config.reference_only=true with "
            "config.component_id)."
        ),
        "raw_xml_escape_hatch": (
            "manage_component (type='component', config.xml) for hand-authored "
            "process XML."
        ),
    },
    "retired_process_protocols": list(_RETIRED_PROCESS_PROTOCOLS),
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
            "CONNECTION_ENDPOINT_IP_LITERAL (prefer FQDN), CONNECTION_BASE_URL_HAS_PATH / CONNECTION_CARRIES_PER_CALL_FIELDS (minimal-connection), FOLDER_REQUIRED_ON_CREATE (no folder → wherever the create route defaults), PROPERTY_NAMING (DPP_/DDP_ UPPER_SNAKE), and — when naming.convention=='bracketed' — NAMING_CONVENTION_BRACKETED per-type checks.",
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
        "A process body is authored only as a canonical unit: "
        "config.authoring_request (a recipe from list_integration_archetypes(), "
        "or intent_kind='process_ir' units) or a typed unit in "
        "integration_spec.processes. A flat type='process' row may only bind an "
        "existing process (config.reference_only=true with config.component_id); "
        "a config.process_kind / process_type / flow_sequence body, like freeform "
        "shape-graph process JSON, is refused with "
        "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED before anything is planned or "
        "written.",
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
        # DERIVED from the router, not transcribed: a hand-copied action list is
        # how this catalogue came to advertise ten monitoring actions where the
        # router accepted seventeen.
        "query_components": list(_query_components_actions()),
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
    "note": "Boomi's Component API requires type-specific XML. For processes, prefer build_integration with config.authoring_request (a recipe from list_integration_archetypes() or ProcessIR units); use config.xml here only as an explicit raw-XML escape hatch.",
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
        "   OR for processes: use build_integration with config.authoring_request (a recipe from list_integration_archetypes() or ProcessIR units)",
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
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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


_COMPONENT_CREATE_CONNECTOR_SOAP_CLIENT = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-settings",
    "protocol": "soap.client",
    "boomi_subtype": "wssoapclientsdk",
    "public_aliases": ["soap_client", "web_services_soap_client", "wssoapclientsdk"],
    "tool": "manage_connector (action='create')",
    "note": (
        "Boomi Web Services SOAP Client connector-settings (connection) for "
        "calling outbound SOAP web services (#126). Models the WSDL URL plus a "
        "distinct service endpoint and NETWORK_AUTH transport credentials. Only "
        "NETWORK_AUTH security is structurally buildable for v1; other security "
        "modes and WS-Security return a structured error with a raw-XML "
        "escape-hatch hint. This is the OUTBOUND SOAP CLIENT — not the inbound "
        "Web Services Server. Use connector_type='soap_client'. The password is "
        "never written to XML — supply credential_ref and set the secret via an "
        "environment extension / the Boomi UI."
    ),
    "template": {
        "connector_type": "soap_client",
        "component_name": "<<target SOAP connection>>",
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "wsdl_url": "https://<<host>>/<<service>>.asmx?wsdl",
        "endpoint_url": "https://<<host>>/<<service>>.asmx",
        "security": "NETWORK_AUTH",
        "username": "<<service username>>",
        "credential_ref": "credential://<<vendor>>/<<role>>",
    },
    "required": [
        "connector_type",
        "component_name",
        "wsdl_url",
        "endpoint_url",
        "username",
        "credential_ref",
    ],
    "defaults": {
        "connector_type": "soap_client",
        "security": "NETWORK_AUTH",
        "folder_name": "Home",
    },
    "supported_security_modes": ["NETWORK_AUTH"],
    "independent_options": {
        "client_ssl_alias": (
            "Optional client (private) certificate Boomi component id (GUID) for "
            "mutual TLS. Maps to the connection `clientsslalias` field."
        ),
        "trust_ssl_alias": (
            "Optional trust (public) certificate Boomi component id (GUID). Maps "
            "to the connection `trustsslalias` field."
        ),
    },
    "unsupported": {
        "wss_security_options": (
            "WS-Security is deferred for v1 — a non-empty value returns "
            "UNSUPPORTED_SOAP_WSS_SECURITY. Use the raw-XML escape hatch "
            "(config.xml) if you need WS-Security today."
        ),
        "other_security_modes": (
            "Only NETWORK_AUTH is verified/buildable; other modes return "
            "SOAP_UNSUPPORTED_SECURITY with a raw-XML escape-hatch hint."
        ),
    },
    "extension_placeholder": (
        "wsdl_url / endpoint_url / username may be set to the literal "
        "'SET_BY_EXTENSION' to defer their value to an environment extension."
    ),
    "escape_hatch": (
        "For any unsupported SOAP field, create the connection via the raw-XML "
        "escape hatch: manage_connector(action='create', config={'xml': '...'}) "
        "sourced from a live SOAP Client export."
    ),
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
    "available_protocols": ["database.sqlserver", "rest.client", "soap.client"],
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
# templates. Database Send/write (DatabaseSendAction, WriteProfile, commit
# semantics, JDBC batching, dynamic insert/update/delete) lands in issue #32:
# profile.db protocol='database.write' + connector-action protocol='database.send'.
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
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
    "see_also": {
        "stored_procedure_read": (
            "For procedure-based Read profiles, use "
            "protocol='database.stored_procedure_read' instead — that "
            "template emits statementType='spread', accepts a procedure_name "
            "config key, and supports parameters[].mode='in'/'out'/'in_out'/'return'."
        ),
        "write_profile": (
            "For Database Write profiles (Standard/Dynamic Insert/Update/"
            "Delete, Stored Procedure Write), use protocol='database.write'."
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
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
    },
    "see_also": {
        "select_statement_read": (
            "For Select-statement Read profiles, use "
            "protocol='database.read' instead — that template emits "
            "statementType='select' with caller-authored SQL."
        ),
        "write_profile": (
            "For Database Write profiles (Standard/Dynamic Insert/Update/"
            "Delete, Stored Procedure Write), use protocol='database.write' "
            "(statement_type selects the variant)."
        ),
    },
}


_COMPONENT_CREATE_PROFILE_DB_DATABASE_WRITE = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.db",
    "protocol": "database.write",
    "tool": "manage_component (action='create')",
    "note": (
        "Database (Legacy) Write profile (issue #32). Describes the target "
        "table/column shape (and WHERE keys, for dynamic update/delete) that a "
        "database Send operation (connector-action database.send) writes "
        "through via WriteProfile. statement_type selects the variant; all SQL, "
        "table names, stored-procedure names, fields, and conditions are "
        "task-authored — the builder never generates SQL or canned templates."
    ),
    "template": {
        "component_type": "profile.db",
        "profile_type": "database.write",
        "component_name": "<<write profile name>>",
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "statement_type": "<<standardinsertupdatedelete|dynamicinsert|dynamicupdate|dynamicdelete|storedprocedurewrite>>",
        "table_name": "<<target table (dynamic* only)>>",
        "stored_procedure": "<<procedure name (storedprocedurewrite only)>>",
        "sql": "<<task-authored SQL (standard / storedprocedurewrite only)>>",
        "fields": [
            {
                "name": "<<column_name>>",
                "data_type": "character",
                "mandatory": False,
                "enforce_unique": False,
            },
        ],
        "conditions": [
            {
                "name": "<<where_key_column>>",
                "data_type": "character",
            },
        ],
    },
    "required": [
        "component_type",
        "profile_type",
        "component_name",
        "statement_type",
    ],
    "defaults": {
        "profile_type": "database.write",
        "folder_name": "Home",
    },
    "statement_types": {
        "standardinsertupdatedelete": (
            "Caller supplies the full INSERT/UPDATE/DELETE SQL (with '?' "
            "placeholders) plus fields[] (the '?'-bound columns in order). No "
            "table_name, no conditions."
        ),
        "dynamicinsert": (
            "Boomi builds the INSERT from table_name + fields[]; omit sql. No "
            "conditions."
        ),
        "dynamicupdate": (
            "Boomi builds the UPDATE from table_name + fields[] (SET columns) + "
            "conditions[] (WHERE keys); omit sql."
        ),
        "dynamicdelete": (
            "Boomi builds the DELETE from table_name + conditions[] (WHERE "
            "keys); omit sql and fields."
        ),
        "storedprocedurewrite": (
            "Caller supplies stored_procedure + the '{ call proc(?, ...) }' sql "
            "plus fields[]. Emits XML statementType='spwrite'."
        ),
    },
    "field_shape": {
        "name": {"type": "string", "required": True},
        "data_type": {
            "type": "string",
            "default": "character",
            "supported": ["character", "number", "datetime"],
            "note": (
                "character→ProfileCharacterFormat (dataType attribute omitted), "
                "number→ProfileNumberFormat, datetime→ProfileDateFormat."
            ),
        },
        "mandatory": {"type": "boolean", "default": False},
        "enforce_unique": {"type": "boolean", "default": False},
    },
    "condition_shape": {
        "name": {"type": "string", "required": True},
        "data_type": {
            "type": "string",
            "default": "character",
            "supported": ["character", "number", "datetime"],
        },
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
        "UNSUPPORTED_DB_STATEMENT_TYPE": "statement_type missing or not one of the five supported values",
        "MISSING_DB_SQL": "sql absent for standardinsertupdatedelete / storedprocedurewrite",
        "MISSING_DB_TABLE_NAME": "table_name absent for a dynamic* statement type",
        "MISSING_DB_STORED_PROCEDURE": "stored_procedure absent for storedprocedurewrite",
        "MISSING_DB_FIELDS": "fields missing/empty where the statement type requires them",
        "MISSING_DB_CONDITIONS": "conditions missing/empty for dynamicupdate / dynamicdelete",
        "UNSUPPORTED_DB_PROFILE_FIELD_TYPE": "a field/condition data_type is not character/number/datetime",
        "UNSUPPORTED_DB_PROFILE_MODE": "profile_type is not database.write",
        "DATABASE_OPERATION_VALIDATION_FAILED": "shape / type / cross-field issue (e.g. sql supplied for a dynamic type)",
        "PLAINTEXT_SECRET_REJECTED": "a forbidden secret-shaped key appeared in config",
    },
    "gotchas": [
        (
            "SQL/table/procedure/field/condition names are stored verbatim — "
            "the builder validates none of them and generates no SQL. "
            "Task-authored values only."
        ),
        (
            "dynamicdelete carries conditions only (no fields); standard types "
            "carry fields only (no conditions)."
        ),
    ],
    "recommended_workflow": [
        "1. Pick the statement_type for the write.",
        "2. For standard/storedprocedurewrite: author the SQL and list fields[] in '?' order.",
        "3. For dynamic*: set table_name, list fields[] (SET/INSERT columns) and, for update/delete, conditions[] (WHERE keys).",
        "4. Plan the write profile alongside the matching connector-action "
        "(database.send) — depends_on the write profile key from the operation.",
    ],
    "example": {
        "key": "db_write_profile",
        "type": "profile.db",
        "action": "create",
        "name": "<<write profile name>>",
        "config": {
            "component_type": "profile.db",
            "profile_type": "database.write",
            "component_name": "<<write profile name>>",
            "statement_type": "dynamicinsert",
            "table_name": "<<target table>>",
            "fields": [
                {"name": "<<column_name>>", "data_type": "character"},
            ],
        },
        "_example_note": (
            "Placeholder values only. Do not copy this example as a starting "
            "SQL/table template — author values based on task requirements."
        ),
    },
    "see_also": {
        "send_operation": (
            "Wrap this write profile in a database Send operation via "
            "get_schema_template(resource_type='component', operation='create', "
            "component_type='connector-action', protocol='database.send')."
        ),
        "read_profile": (
            "For read/query profiles, use protocol='database.read' or "
            "protocol='database.stored_procedure_read'."
        ),
    },
}


_COMPONENT_CREATE_PROFILE_DB_OVERVIEW = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "profile.db",
    "tool": "manage_component (action='create')",
    "note": (
        "Database (Legacy) profile builders. Read variants (issue #23): "
        "database.read (Select statement) and database.stored_procedure_read "
        "(Stored Procedure). Write variant (issue #32): database.write "
        "(Standard/Dynamic Insert/Update/Delete, Stored Procedure Write — "
        "statement_type selects the shape)."
    ),
    "available_protocols": [
        "database.read",
        "database.stored_procedure_read",
        "database.write",
    ],
    "hint": (
        "Re-call get_schema_template(resource_type='component', operation='create', "
        "component_type='profile.db', protocol='database.read') for a "
        "Select-statement Read profile, protocol='database.stored_procedure_read' "
        "for a Stored Procedure Read profile, or protocol='database.write' for a "
        "write profile."
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
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
    "other_operation_modes_note": (
        "This template is for Get (read) operations. Database Send/write "
        "operations use operation_mode='send' — request "
        "protocol='database.send' for that template."
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
    "see_also": {
        "database_send": (
            "For Database Send/write (DatabaseSendAction + WriteProfile), use "
            "protocol='database.send'."
        ),
    },
}


_COMPONENT_CREATE_CONNECTOR_ACTION_DATABASE_SEND = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-action",
    "protocol": "database.send",
    "tool": "manage_connector (action='create')",
    "note": (
        "Database Send (write) operation (issue #32). Wraps a previously-"
        "created database Write profile (profile.db, profile_type="
        "'database.write') in a DatabaseSendAction envelope. The connection "
        "itself is bound at the process connector step, not in the operation "
        "XML — connection_ref_key is a plan-only dependency."
    ),
    "template": {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "send",
        "component_name": "<<operation name>>",
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "connection_ref_key": "<<db connection key>>",
        "write_profile_id": "$ref:<<db write profile key>>",
        "commit_option": "commitprofile",
        "batch_count": 0,
        "enable_batching": True,
    },
    "required": [
        "component_type",
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "write_profile_id",
    ],
    "defaults": {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "send",
        "commit_option": "commitprofile",
        "batch_count": 0,
        "enable_batching": True,
        "folder_name": "Home",
    },
    "supported_operation_modes": ["send"],
    "commit_option_values": {
        "commitprofile": "Commit per the write profile (Boomi default).",
        "commitrows": "Commit by row count (paired with batch_count).",
    },
    "depends_on_requirements": [
        "Include connection_ref_key in depends_on (so the connector-settings runs first).",
        "When write_profile_id uses '$ref:KEY', include KEY in depends_on too.",
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
        "UNSUPPORTED_DB_OPERATION_MODE": "operation_mode is not 'send'",
        "MISSING_DB_WRITE_PROFILE_REF": "write_profile_id absent or empty",
        "INVALID_DB_COMMIT_OPTION": "commit_option is not commitprofile/commitrows",
        "INVALID_DB_BATCH_CONFIG": "batch_count is not a non-negative integer, or enable_batching is not a bool",
        "MISSING_DB_DEPENDENCY": "connection_ref_key / $ref target missing from depends_on",
        "DB_REF_TYPE_MISMATCH": "connection_ref_key or write_profile_id $ref points to a component of the wrong type",
        "DATABASE_OPERATION_VALIDATION_FAILED": "shape / type / cross-field issue",
    },
    "gotchas": [
        (
            "Boomi binds the connection at the process connector step, not in "
            "the operation XML. The builder will NOT emit a connection ID — "
            "connection_ref_key is plan-only metadata for dependency ordering."
        ),
        (
            "write_profile_id must reference a database WRITE profile "
            "(profile_type='database.write'); a read profile is rejected with "
            "DB_REF_TYPE_MISMATCH."
        ),
        (
            "commit_option='commitrows' with batch_count=0 is allowed (matches "
            "live exports)."
        ),
    ],
    "recommended_workflow": [
        "1. Create the database connector-settings (manage_connector, connector_type=database).",
        "2. Create the write profile (manage_component, component_type=profile.db, profile_type=database.write).",
        "3. Plan this Send operation with depends_on=[<connection_key>, <write_profile_key>] and write_profile_id='$ref:<write_profile_key>'.",
        "4. Apply — $ref is resolved to the write profile's component_id from the id_registry.",
    ],
    "update_note": (
        "Structured updates via build_integration action='update' use "
        "read-merge-write semantics (issue #45): inside DatabaseSendAction the "
        "merge updates only the owned batchCount/commitOption/enableBatching "
        "attrs and the WriteProfile child, preserving unknown/future attrs or "
        "children on the action element. Operation-level Archiving, Tracking, "
        "Caching, and any unknown future siblings also survive."
    ),
    "example": {
        "key": "db_write_operation",
        "type": "connector-action",
        "action": "create",
        "name": "<<operation name>>",
        "depends_on": ["db_connection", "db_write_profile"],
        "config": {
            "component_type": "connector-action",
            "connector_type": "database",
            "operation_mode": "send",
            "component_name": "<<operation name>>",
            "connection_ref_key": "db_connection",
            "write_profile_id": "$ref:db_write_profile",
            "commit_option": "commitprofile",
            "batch_count": 0,
            "enable_batching": True,
        },
        "_example_note": (
            "Placeholder values only. The write_profile_id $ref is substituted "
            "with the created profile's component_id during apply."
        ),
    },
    "see_also": {
        "write_profile": (
            "Create the referenced write profile via "
            "get_schema_template(resource_type='component', operation='create', "
            "component_type='profile.db', protocol='database.write')."
        ),
        "get_operation": (
            "For Database Get (read) operations, use protocol='database.get'."
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
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        "profile.db components via '$ref:KEY', OR literal existing-profile "
        "UUIDs validated against a supplied/discoverable field index "
        "(index_profile_component + profile_indexes_by_component_id, #95); an "
        "unindexable literal UUID is rejected with MAP_PROFILE_INDEX_UNAVAILABLE."
    ),
    "template": {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": "<<map name>>",
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
    # THIS TEMPLATE's map types, matching its two siblings (the function template
    # lists its own pair, the script template lists its own pair). It previously
    # advertised the function routes as well, which reads as a tool-level route
    # list but is not one — it omits the two script routes, and the same payload's
    # own `note` says "M2 is direct-only" while its `unsupported_routes` refuses
    # the very key those routes require. Measured: building from this template's
    # own `required` set and setting `map_type` to either function value is
    # REFUSED. A caller obeying the served list inside the served contract got a
    # refusal, which is what makes this served text wrong rather than merely terse.
    "supported_map_types": ["direct"],
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
        "Both profiles are in-spec '$ref:KEY' OR literal existing-profile "
        "UUIDs with a supplied/discoverable field index; an unindexable "
        "literal UUID produces MAP_PROFILE_INDEX_UNAVAILABLE (index it with "
        "index_profile_component and pass via profile_indexes_by_component_id, #95).",
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
            "literal existing-profile UUID with no in-spec generated profile "
            "component and no supplied/discoverable field index — index it with "
            "index_profile_component and pass it via "
            "profile_indexes_by_component_id (#95)"
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
        "parameters}. A 20-family allow-list: date_format, "
        "default_value, trim, left_trim, right_trim, uppercase, lowercase, "
        "append, prepend, replace, remove, simple_lookup, sequential_value, "
        "math, plus the native ProcessProperty families "
        "dynamic_process_property_get, dynamic_process_property_set, "
        "document_property_get, document_property_set, "
        "defined_process_property_get, defined_process_property_set. The three "
        "'set' property families are side-effecting — they omit target_path "
        "(no output leaf). Mixed maps may also declare direct field_mappings "
        "alongside function_mappings. Source/target profile refs follow the same "
        "'$ref:KEY' rule as direct maps; literal existing-profile UUIDs are "
        "validated against a supplied or discoverable field index (index_profile_component + profile_indexes_by_component_id, #95), else rejected with MAP_PROFILE_INDEX_UNAVAILABLE. "
        "Reusable script-based transforms (Groovy / JavaScript) ship in "
        "map_type='script' (#41). XSLT (#42), standalone reusable "
        "transform.function components, and chained multi-step function "
        "graphs remain out of scope."
    ),
    "template": {
        "component_type": "transform.map",
        "map_type": "function",
        "component_name": "<<map name>>",
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        "folder_id",
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
        "dynamic_process_property_get": {
            "mapped_inputs": 0,
            "required_parameters": ["property_name"],
            "optional_parameters": ["default_value"],
            "target_path": "required (reads the DPP into the target leaf)",
            "note": (
                "Get Dynamic Process Property (PropertyGet). Reads the dynamic "
                "process property named parameters.property_name; the optional "
                "parameters.default_value is used when the DPP is unset. No "
                "mapped source input."
            ),
        },
        "dynamic_process_property_set": {
            "mapped_inputs": 1,
            "required_parameters": ["property_name"],
            "optional_parameters": [],
            "target_path": "omitted (side-effecting setter — no output leaf)",
            "note": (
                "Set Dynamic Process Property (PropertySet). inputs[0] is the "
                "source value written to the DPP named parameters.property_name. "
                "Side-effecting: omit target_path."
            ),
        },
        "document_property_get": {
            "mapped_inputs": 0,
            "required_parameters": ["document_property_name"],
            "optional_parameters": [],
            "target_path": "required (reads the DDP into the target leaf)",
            "note": (
                "Get Document Property (DocumentPropertyGet). Reads the dynamic "
                "document property named parameters.document_property_name (the "
                "bare name — the builder adds the 'dynamicdocument.' prefix). No "
                "mapped source input."
            ),
        },
        "document_property_set": {
            "mapped_inputs": 1,
            "required_parameters": ["document_property_name"],
            "optional_parameters": [],
            "target_path": "omitted (side-effecting setter — no output leaf)",
            "note": (
                "Set Document Property (DocumentPropertySet). inputs[0] is the "
                "source value written to the DDP named "
                "parameters.document_property_name (bare name). Side-effecting: "
                "omit target_path."
            ),
        },
        "defined_process_property_get": {
            "mapped_inputs": 0,
            "required_parameters": [
                "process_property_component_id",
                "process_property_component_name",
                "process_property_key",
                "process_property_name",
            ],
            "optional_parameters": [],
            "target_path": "required (reads the property into the target leaf)",
            "note": (
                "Get Process Property (DefinedProcessPropertyGet), a "
                "component-backed Process Property. "
                "parameters.process_property_component_id MUST be a "
                "'$ref:<key>' to an in-spec processproperty component declared "
                "in depends_on; the other three parameters identify the "
                "component name and the specific property (key + name). No "
                "mapped source input."
            ),
        },
        "defined_process_property_set": {
            "mapped_inputs": 1,
            "required_parameters": [
                "process_property_component_id",
                "process_property_component_name",
                "process_property_key",
                "process_property_name",
            ],
            "optional_parameters": [],
            "target_path": "omitted (side-effecting setter — no output leaf)",
            "note": (
                "Set Process Property (DefinedProcessPropertySet), a "
                "component-backed Process Property. inputs[0] is the source "
                "value written to the property. "
                "parameters.process_property_component_id MUST be a "
                "'$ref:<key>' to an in-spec processproperty component declared "
                "in depends_on. Side-effecting: omit target_path."
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
        "Both profiles are in-spec '$ref:KEY' OR literal existing-profile "
        "UUIDs with a supplied/discoverable field index; an unindexable "
        "literal UUID produces MAP_PROFILE_INDEX_UNAVAILABLE (index it with "
        "index_profile_component and pass via profile_indexes_by_component_id, #95).",
        "For defined_process_property_get/set, include the Process Property "
        "component's $ref key (from parameters.process_property_component_id) "
        "in depends_on so the processproperty component runs before the map "
        "(else MAP_FUNCTION_COMPONENT_REF_REQUIRED).",
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
            "literal existing-profile UUID with no in-spec generated profile "
            "component and no supplied/discoverable field index — index it with "
            "index_profile_component and pass it via "
            "profile_indexes_by_component_id (#95)"
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
            "function_type is not in the supported 20-family allow-list"
        ),
        "MAP_FUNCTION_COMPONENT_REF_REQUIRED": (
            "a defined_process_property_get/set function's "
            "parameters.process_property_component_id is missing, not a "
            "'$ref:KEY', absent from depends_on, or resolves to a component "
            "that is not a processproperty"
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
            "Each function_mapping produces ONE target output, EXCEPT the three "
            "side-effecting 'set' property families "
            "(dynamic_process_property_set, document_property_set, "
            "defined_process_property_set) which produce NO output and omit "
            "target_path. Multi-output graphs (StringSplit, user-defined "
            "functions) and chained function steps are future work."
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


# Issue #122 M11.3 (epic #118): typed documentcache create template.
_COMPONENT_CREATE_DOCUMENT_CACHE = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "documentcache",
    "tool": "manage_component (action='create')",
    "tool_note": (
        "Standalone Document Cache components can be created via "
        "manage_component directly, or declared as an in-spec component in "
        "build_integration alongside the cache_put / cache_get flow-sequence "
        "steps and map-level document_cache_joins entries that reference it "
        "via $ref (#122)."
    ),
    "note": (
        "Defines the cache's profile binding plus index/key structure. Cache "
        "steps and map joins reference this component by id — they never "
        "define structure inline. cacheKey elementKey values reference the "
        "bound profile's element keys."
    ),
    "update_note": (
        "Structured updates use read-merge-write (issue #45 semantics): the "
        "builder-owned <DocumentCache> subtree is replaced while "
        "bns:encryptedValues and unknown siblings survive."
    ),
    "template": {
        "component_type": "documentcache",
        "component_name": "<<cache display name>>",
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "profile_type": "<<profile.json | profile.xml>>",
        "profile_id": "<<profile componentId or $ref:KEY>>",
        "enforce_single_lucene": True,
        "indexes": [
            {
                "index_id": 1,
                "index_name": "<<by key name>>",
                "keys": [
                    {
                        "id": 2,
                        "element_key": "<<profile element key>>",
                        "name": "<<element display, e.g. ID (Root/Object/ID)>>",
                    },
                ],
            },
        ],
    },
    "required": [
        "component_type",
        "component_name",
        "profile_type",
        "profile_id",
        "indexes",
    ],
    "optional": ["folder_id", "folder_path", "description", "enforce_single_lucene"],
    "defaults": {"component_type": "documentcache", "enforce_single_lucene": True},
    "supported_profile_types": ["profile.json", "profile.xml"],
    "gated_profile_types_note": (
        "profile.none / profile.flatfile / profile.db / profile.edi are "
        "companion-documented but have no live-captured wire shape (#119 "
        "census) — gated until a disposable-account round-trip verifies them."
    ),
    "gated_key_kinds_note": (
        "Cache keys support the live-verified profile-element form only; "
        "kind='document_property' (DocumentPropertyKeyConfig) is gated for "
        "the same evidence reason."
    ),
    "id_zero_gotcha": (
        "index_id and cacheKey id MUST be non-zero: 0 is accepted by the API "
        "but silently indexes nothing at runtime (nothing retrievable, no "
        "error)."
    ),
    "raw_xml_exposed": False,
    "error_codes": {
        "DOCUMENT_CACHE_VALIDATION_FAILED": "shape / unknown-key / type-check failure",
        "DOCUMENT_CACHE_NAME_REQUIRED": "component_name missing/blank",
        "DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED": "profile_type outside the live-verified v1 set",
        "DOCUMENT_CACHE_PROFILE_REQUIRED": "profile_id missing for a profiled cache",
        "DOCUMENT_CACHE_INDEX_REQUIRED": "indexes list missing or empty",
        "DOCUMENT_CACHE_INDEX_INVALID": "index_id zero/duplicate, index_name missing, or keys empty",
        "DOCUMENT_CACHE_KEY_INVALID": "cacheKey id zero/duplicate or element_key/name missing",
        "DOCUMENT_CACHE_KEY_KIND_GATED": "kind='document_property' requested (gated, #119)",
        "DOCUMENT_CACHE_RAW_XML_UNSUPPORTED": "raw DocumentCache subtree keys are not accepted",
        "PLAINTEXT_SECRET_REJECTED": "a dict key inside the config matches a secret-shaped name",
    },
    "example": {
        "key": "<<cache_key>>",
        "type": "documentcache",
        "action": "create",
        "name": "<<cache display name>>",
        "config": {
            "component_type": "documentcache",
            "component_name": "<<cache display name>>",
            "profile_type": "profile.json",
            "profile_id": "$ref:<<response_profile_key>>",
            "indexes": [
                {
                    "index_id": 1,
                    "index_name": "by id",
                    "keys": [
                        {"id": 2, "element_key": "<<element key>>", "name": "<<element display>>"},
                    ],
                },
            ],
        },
        "_example_note": "Placeholder values only.",
    },
}


# Issue #131 M11.7 (epic #118): typed processproperty create template.
_COMPONENT_CREATE_PROCESS_PROPERTY = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "processproperty",
    "tool": "manage_component (action='create')",
    "tool_note": (
        "Standalone Process Property components can be created via "
        "manage_component directly, or declared as an in-spec component in "
        "build_integration alongside the transform.map (map_type='function') "
        "whose defined_process_property_get/set functions reference it via "
        "parameters.process_property_component_id='$ref:<key>' (#131)."
    ),
    "note": (
        "Declares the component's Defined Process Property slots. Each "
        "property carries an EXPLICIT stable UUID 'key' (v1 contract): the "
        "same UUID is emitted into the component XML and handed to the "
        "referencing map function as parameters.process_property_key — no "
        "generated-key token mechanism exists. 'name' becomes the property "
        "<label> (the display name the map function's property_name shows)."
    ),
    "update_note": (
        "Structured updates use read-merge-write (issue #45 semantics): the "
        "builder-owned <DefinedProcessProperties> subtree is REPLACED as a "
        "whole while bns:encryptedValues, bns:processOverrides, and unknown "
        "siblings survive. WARNING: v1 always emits empty <allowedValues/>, "
        "so a structured update of a legacy component clears any populated "
        "allowedValueSet entries — use the raw-XML escape hatch to preserve "
        "them until constrained value sets ship."
    ),
    "template": {
        "component_type": "processproperty",
        "component_name": "<<component display name>>",
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "properties": [
            {
                "key": "<<stable UUID, e.g. 0e89ebf1-cd46-46df-904e-94c7e7ade31e>>",
                "name": "<<property label>>",
                "type": "<<string | number | boolean | date | password>>",
                "default_value": "<<optional default>>",
                "help_text": "<<optional operator help>>",
                "persisted": False,
            },
        ],
    },
    "required": ["component_type", "component_name", "properties"],
    "optional": ["folder_id", "folder_path", "description"],
    "property_required": ["key", "name", "type"],
    "property_optional": ["default_value", "help_text", "persisted"],
    "defaults": {"component_type": "processproperty", "persisted": False},
    "supported_property_types": ["string", "number", "boolean", "date", "password"],
    "property_type_evidence_note": (
        "The allow-list is the exact platform XSD enumeration, live-verified "
        "2026-07-03 via a renera create probe: [string, number, boolean, "
        "date, password]. 'hidden' and 'character' are rejected — the UI's "
        "'Hidden' data type serializes as XML token 'password'."
    ),
    "password_default_note": (
        "type='password' (the UI's 'Hidden' data type) is accepted only with "
        "an omitted or empty default_value: Boomi stores Defined Process "
        "Property defaults in PLAINTEXT component XML (a password default "
        "round-trips in cleartext and is NOT moved into bns:encryptedValues; "
        "live-verified 2026-07-03). Supply real values via environment "
        "extensions / runtime overrides. Non-empty password defaults are "
        "rejected with PLAINTEXT_SECRET_REJECTED and redacted in plan echoes."
    ),
    "map_function_cross_link": (
        "transform.map map_type='function' defined_process_property_get/set "
        "reference this component: parameters.process_property_component_id="
        "'$ref:<component key>' (declared in the map's depends_on) plus "
        "parameters.process_property_key=<the property 'key' UUID declared "
        "here> and parameters.process_property_name=<the property 'name'>."
    ),
    "no_encrypted_field_note": (
        "There is NO per-property encrypted flag — no live capture carries "
        "one. Component-level <bns:encryptedValues/> is emitted empty and "
        "preserved on update."
    ),
    "raw_xml_exposed": False,
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
        "PROCESS_PROPERTY_VALIDATION_FAILED": (
            "shape / unknown-key / type-check failure not covered by a more "
            "specific code (including the rejected 'encrypted' and "
            "'allowed_values' property keys)"
        ),
        "PROCESS_PROPERTY_NAME_REQUIRED": "component_name or a property name missing/blank",
        "PROCESS_PROPERTY_PROPERTY_REQUIRED": "properties list missing or empty",
        "PROCESS_PROPERTY_KEY_REQUIRED": "a property is missing its explicit stable UUID key",
        "PROCESS_PROPERTY_KEY_INVALID": "a property key is not a lowercase canonical UUID",
        "PROCESS_PROPERTY_TYPE_UNSUPPORTED": (
            "property type not in (string, number, boolean, date, password); "
            "'hidden' is rejected — the UI's 'Hidden' serializes as 'password'"
        ),
        "PROCESS_PROPERTY_DUPLICATE_KEY": "two properties share a key UUID",
        "PROCESS_PROPERTY_DUPLICATE_NAME": "two properties share a label",
        "PROCESS_PROPERTY_DEFAULT_INVALID": "default_value / help_text is not a string",
        "PROCESS_PROPERTY_RAW_XML_UNSUPPORTED": (
            "raw DefinedProcessProperties subtree keys are not accepted; the "
            "raw escape hatch is config.xml"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a dict key inside the config matches a secret-shaped name, or a "
            "type='password' property carries a non-empty default_value "
            "(plaintext-XML policy)"
        ),
    },
    "example": {
        "key": "<<props_key>>",
        "type": "processproperty",
        "action": "create",
        "name": "<<component display name>>",
        "config": {
            "component_type": "processproperty",
            "component_name": "<<component display name>>",
            "properties": [
                {
                    "key": "<<stable UUID>>",
                    "name": "<<property label>>",
                    "type": "string",
                    "default_value": "",
                    "persisted": False,
                },
            ],
        },
        "_example_note": (
            "Placeholder values only. Generate a UUID per property once and "
            "keep it stable across updates — map functions reference it."
        ),
    },
}


# Issue #133 M6.1: typed webservice (API Service Component) create template.
_COMPONENT_CREATE_API_SERVICE = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "webservice",
    "tool": "manage_component (action='create')",
    "tool_note": (
        "API Service Components (ASC, type='webservice') can be created via "
        "manage_component directly (routes take LITERAL process component "
        "UUIDs), or declared as an in-spec component in build_integration "
        "with routes[].process='$ref:<process key>' (the process key MUST "
        "also appear in depends_on so the ASC applies after its route "
        "processes and the $ref resolves). Listener archetypes emit one "
        "automatically via asc_wrapper.enabled=true (#133)."
    ),
    "note": (
        "On an apiType='advanced' Shared Web Server, routes exist ONLY "
        "through a deployed ASC (served under /ws/rest/..., case-VERBATIM); "
        "bare /ws/simple WSS routes 404 there (live-confirmed). On "
        "basic/intermediate the inverse holds — an ASC deploys clean but "
        "does not serve. Every route's process must have a WSS Listen start "
        "(actionType='Listen', connector subType 'wss'). Empty-string route "
        "override attributes mean INHERIT from the linked WSS Listen "
        "operation (per attribute); the all-inherit effective path is "
        "/ws/rest/{WSS-op objectName}. Deploy does NOT cascade: the ASC and "
        "each route process deploy independently to the same environment "
        "(orchestrate_deploy's listener_verify handles both)."
    ),
    "update_note": (
        "Structured updates use subtree_merge preservation: the builder owns "
        "urlPath and ALL SEVEN child blocks (restApi/soapApi/odataApi/"
        "metaInfo/profileOverrides/capturedHeaders/apiRoles) for the "
        "platform's exact XSD ordering — but profileOverrides CONTENT is "
        "never authored: the builder's empty placeholder yields to a "
        "UI/platform-populated live block, which survives structured updates "
        "in its canonical slot (drive profiles from the WSS operation)."
    ),
    "template": {
        "component_type": "webservice",
        "component_name": "<<component display name>>",
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "base_url_path": "<<optional first /ws/rest/<base> segment; '' contributes none>>",
        "title": "<<optional metaInfo title (display; defaults to component_name)>>",
        "version": "<<optional metaInfo version (display; default 1.0.0)>>",
        "routes": [
            {
                "process": "<<'$ref:<process key>' (build_integration) or a process component UUID (manage_component)>>",
                "http_method": "<<'' inherit (" + ASC_METHOD_RULE + ") | GET | POST | PUT | DELETE | PATCH>>",
                "url_path": "<<optional trailing path segment; '' contributes none>>",
                "object_name": "<<'' inherits the WSS operation objectName>>",
                "input_type": "<<'' inherit | none|singledata|singlejson|multijson|singlexml|multixml>>",
                "output_type": "<<'' inherit | same vocabulary>>",
                "description": "<<optional route description>>",
            },
        ],
    },
    "required": ["component_type", "component_name", "routes"],
    "optional": ["folder_id", "folder_path", "description", "base_url_path", "title", "version"],
    "route_required": ["process"],
    "route_optional": [
        "http_method",
        "url_path",
        "object_name",
        "input_type",
        "output_type",
        "input_profile_key",
        "description",
    ],
    "defaults": {"component_type": "webservice", "version": "1.0.0"},
    "effective_path_formula": (
        "/ws/rest/<base_url_path>/<objectName>/<urlPath> with EMPTY segments "
        "omitted and casing preserved verbatim (unlike bare /ws/simple, which "
        "sentence-cases the objectName). Live-confirmed: empty base + "
        "all-inherit route serves /ws/rest/{WSS-op objectName}."
    ),
    "collision_note": (
        "Same base (live-proven 2026-07-05): " + ASC_SAME_BASE_SHADOWING
        + ". Give every ASC a distinct base_url_path (the default '' collides "
        "with any other default-base ASC). Different bases (#158, measured in "
        "both deploy orders): " + ASC_CROSS_BASE_MEASURED_PRECEDENCE
        + ". orchestrate_deploy's listener_verify collision-scans active "
        "webservice deployments: an equal base refuses LISTENER_ASC_COLLISION; "
        "another ASC's route reaching the listener's path warns "
        "LISTENER_ASC_ROUTE_OVERLAP when the listener's ASC is on the more "
        "specific base, and refuses LISTENER_ASC_COLLISION when the other's is; "
        "never live pre-probes (the cloud perimeter answers a uniform 401 "
        "pre-registration)."
    ),
    "raw_xml_exposed": False,
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
        "API_SERVICE_VALIDATION_FAILED": (
            "shape / unknown-key failure not covered by a more specific code"
        ),
        "API_SERVICE_NAME_REQUIRED": "component_name missing/blank",
        "API_SERVICE_ROUTES_REQUIRED": "routes list missing or empty",
        "API_SERVICE_ROUTE_PROCESS_REQUIRED": (
            "a route is missing its process reference (or the $ref key is "
            "absent from depends_on in build_integration specs)"
        ),
        "API_SERVICE_ROUTE_PROCESS_REF_INVALID": (
            "a route process is neither '$ref:KEY' nor a canonical component "
            "UUID, or an unresolved $ref reached emission"
        ),
        "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN": (
            "a $ref route resolves to a non-process or to a process without "
            "a WSS Listen source binding (deploys clean, 404s at runtime)"
        ),
        "API_SERVICE_DUPLICATE_ROUTE": (
            "two routes are identical or resolve to the same explicit "
            "effective method+path (first-deployed-wins shadowing)"
        ),
        "API_SERVICE_METHOD_UNSUPPORTED": "http_method not in (GET, POST, PUT, DELETE, PATCH, '')",
        "API_SERVICE_TYPE_UNSUPPORTED": "input_type/output_type outside the WSS vocabulary",
        "API_SERVICE_RAW_XML_UNSUPPORTED": (
            "raw webservice subtree keys (restApi/soapApi/odataApi/metaInfo/"
            "apiRoles/...) are not accepted; the raw escape hatch is config.xml"
        ),
        "API_SERVICE_PROFILE_OVERRIDES_UNSUPPORTED": (
            "profileOverrides is never authored programmatically — drive "
            "profiles from the WSS Listen operation"
        ),
        "PLAINTEXT_SECRET_REJECTED": (
            "a dict key inside the config matches a secret-shaped name — "
            "endpoint auth comes from the Shared Web Server, never component XML"
        ),
    },
    "example": {
        "key": "api_service",
        "type": "webservice",
        "action": "create",
        "name": "<<component display name>>",
        "depends_on": ["main_process"],
        "config": {
            "component_type": "webservice",
            "component_name": "<<component display name>>",
            "routes": [
                {
                    "process": "$ref:main_process",
                    "http_method": "",
                    "url_path": "",
                    "object_name": "",
                    "input_type": "",
                    "output_type": "",
                },
            ],
        },
        "_example_note": (
            "All-inherit route: the served path comes from the process's WSS "
            "Listen operation (/ws/rest/{objectName}), and its method: "
            + ASC_METHOD_RULE
            + "."
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
    "authoring_guidance": (
        "For the script-body authoring contract (named input/output variables, "
        "assign-do-not-return, ExecutionUtil dynamic-process-property access, "
        "and the no-document-property-access rule) plus a skeleton, call "
        "get_schema_template(schema_name='script_mapping') and "
        "search_boomi_docs('Boomi map scripting function')."
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
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        "folder_id",
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
    "authoring_guidance": (
        "Each referenced script.mapping body follows the map-script contract — "
        "call get_schema_template(schema_name='script_mapping') for the named "
        "input/output variable model and a skeleton."
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
        "validated against a supplied or discoverable field index (index_profile_component + profile_indexes_by_component_id, #95), else rejected with MAP_PROFILE_INDEX_UNAVAILABLE."
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
        "folder_path": "<<optional folder path; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        "folder_id",
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
        "Both profiles are in-spec '$ref:KEY' OR literal existing-profile "
        "UUIDs with a supplied/discoverable field index; an unindexable "
        "literal UUID produces MAP_PROFILE_INDEX_UNAVAILABLE (index it with "
        "index_profile_component and pass via profile_indexes_by_component_id, #95).",
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
            "literal existing-profile UUID with no in-spec generated profile "
            "component and no supplied/discoverable field index — index it with "
            "index_profile_component and pass it via "
            "profile_indexes_by_component_id (#95)"
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
    "runtime_binding_note": (
        "Issue #96 (M5.4a), Boomi UI verified: the REST Client connector step "
        "exposes EXACTLY ONE dynamic operation property — 'Path'. query_parameters / "
        "request_headers here are STATIC operation customProperties (Key/Value, "
        "optionally Encrypted) with NO per-request dynamic-binding mechanism. A "
        "PER-DOCUMENT path is a process-step concern: author it as the path_binding "
        "of a ProcessIR connector_call or target step (build_integration with "
        "config.authoring_request; see get_schema_template(schema_name="
        "'AuthoringRequestV1')), which emits the connector step's 'Path' dynamic "
        "operation property over this operation's blank path. No public route "
        "accepts a primitive runtime_bindings block (#159 S3 retired it). Vary a "
        "query parameter per request by encoding it in the bound path; request "
        "headers can only be set statically here."
    ),
    "template": {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "execute",
        "component_name": "<<operation name>>",
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
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
        # request_profile_type / response_profile_type are intentionally NOT
        # defaulted (#50): the builder emits requestProfileType /
        # responseProfileType only when the caller explicitly supplies them,
        # so a path-only update preserves the live profile type instead of
        # forcing "xml". They remain documented optional fields in the
        # template/example and field_method_dependency_map.
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
        "(Codex r8 P2). Profile bindings AND profile types are now each "
        "conditionally emitted by the builder (#50), so all four attrs "
        "merge additively: a path/method-only update (none of them "
        "supplied) preserves the live profile id+type; a type-only update "
        "(request_profile_type/response_profile_type without the matching "
        "id) now applies the new type while keeping the live id; and "
        "supplying request_profile_id+request_profile_type applies both. "
        "Setting request_profile_type/response_profile_type='none' unbinds "
        "the profile: the new 'none' type applies AND the live profile id "
        "is cleared (no stale id is left dangling beside a 'none' type). "
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


_COMPONENT_CREATE_CONNECTOR_ACTION_SOAP_OPERATION = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-action",
    "protocol": "soap.operation",
    "boomi_subtype": "wssoapclientsdk",
    "public_aliases": ["soap_client", "web_services_soap_client", "wssoapclientsdk"],
    "tool": "manage_connector (action='create')",
    "note": (
        "Boomi Web Services SOAP Client operation (#126). Wraps a single SOAP "
        "call in an Operation envelope with GenericOperationConfig "
        "operationType=EXECUTE. The SOAP Client exposes ONLY the EXECUTE action "
        "(there is no GET/SEND/verb split) — operation_mode must be 'execute'. "
        "Request/response profiles are XML-only. The connection is bound at the "
        "process connector step, not in the operation XML — connection_ref_key is "
        "plan-only metadata for dependency ordering. The operation's WSDL "
        "metadata is caller-provided WSDL-derived data (WSDL discovery is out of "
        "scope, M7); NO canned SOAP envelope or request payload is accepted — the "
        "request body comes from the surrounding map/pipeline stage via the XML "
        "request profile."
    ),
    "template": {
        "component_type": "connector-action",
        "connector_type": "soap_client",
        "operation_mode": "execute",
        "component_name": "<<operation name>>",
        "folder_name": "<<optional folder name; metadata only — this platform ignores it on create>>",
        "folder_id": "<<optional folder id; the ONE spelling that places>>",
        "description": "<<optional description>>",
        "connection_ref_key": "<<soap connection key>>",
        "request_profile_type": "xml",
        "request_profile_id": "$ref:<<request profile key>>",
        "response_profile_type": "xml",
        "response_profile_id": "$ref:<<response profile key>>",
        "expose_request_envelope": True,
        "expose_response_envelope": False,
        "wsdl_metadata": {
            "operation_name": "<<WSDL operation name>>",
            "soap_action": "<<SOAP action URI>>",
            "metadata_connection_url": "https://<<host>>/<<service>>.asmx",
            "service_name": "<<WSDL service name>>",
            "service_namespace": "<<service namespace URI>>",
            "port_name": "<<WSDL port name>>",
            "binding_style": "document",
            "binding_use": "literal",
            "binding_protocol": "soap_1_1",
            "operation_style": "document",
            "operation_use": "literal",
            "input_message_name": "<<input message name>>",
            "input_message_namespace": "<<input message namespace>>",
            "output_message_name": "<<output message name>>",
            "output_message_namespace": "<<output message namespace>>",
            "input_parameters": [
                {"name": "<<param>>", "element_name": "<<element>>", "element_ns": "<<namespace>>"}
            ],
            "output_parameters": [
                {"name": "<<param>>", "element_name": "<<element>>", "element_ns": "<<namespace>>"}
            ],
            "rpc_optional_parameters": True,
            "using_envelope": True,
        },
    },
    "required": [
        "connector_type",
        "operation_mode",
        "component_name",
        "connection_ref_key",
        "request_profile_id",
        "response_profile_id",
        "wsdl_metadata",
    ],
    "defaults": {
        "connector_type": "soap_client",
        "operation_mode": "execute",
        "request_profile_type": "xml",
        "response_profile_type": "xml",
        "expose_request_envelope": True,
        "expose_response_envelope": False,
        "return_application_errors": False,
        "track_response": False,
        "object_type_id": "<<defaults to wsdl_metadata.operation_name>>",
        "object_type_name": "<<defaults to wsdl_metadata.operation_name>>",
    },
    "constraints": {
        "operation_mode": "Must be 'execute' (SOAP Client is EXECUTE-only).",
        "profile_types": "request/response profiles are XML-only.",
        "no_canned_payload": (
            "Fields like soap_body / soap_envelope / raw_envelope / request_payload "
            "/ headers are rejected (SOAP_UNSUPPORTED_FIELD). The request body is "
            "supplied by the map/pipeline stage via the request profile."
        ),
    },
    "escape_hatch": (
        "For any unsupported SOAP operation field, create the operation via the "
        "raw-XML escape hatch (manage_connector action='create' config={'xml': "
        "'...'}) sourced from a live SOAP Client operation export."
    ),
}


_COMPONENT_CREATE_CONNECTOR_ACTION_OVERVIEW = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-action",
    "tool": "manage_connector (action='create')",
    "note": (
        "Connector-action (operation) builders. Available: database.get "
        "(issue #23), database.send (issue #32), rest.operation (issue #24), "
        "and soap.operation (issue #126, SOAP Client EXECUTE)."
    ),
    "available_protocols": ["database.get", "database.send", "rest.operation", "soap.operation"],
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
        "canonical_process_update": (
            "#159: patch.authoring_request (alone — never with config or map_context) "
            "is an AuthoringRequestV1 with intent_kind='process_ir' and exactly one "
            "unit whose envelope has action='update' and component_id equal to the "
            "edited process. Every intent.components row binds an existing component "
            "by explicit id (the row's component_id + action='create' under "
            "conflict_policy='reuse'; a non-connector may instead use "
            "config.reference_only=true + config.component_id) and is never written. The request is compiled like build_integration(action='compile'), "
            "every dependency id is read and type-checked, and the materialized process "
            "is merged with the process preservation policy (overrides, encrypted "
            "values, unknown XML and folder kept). The token also binds the compile "
            "hash and each dependency's live identity; apply recomputes them and "
            "re-reads the process before its single write."
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
        # #159 canonical_process_update admission and binding refusals.
        "COMPONENT_EDIT_AUTHORING_REQUEST_INVALID",
        "COMPONENT_EDIT_TARGET_MISMATCH",
        "COMPONENT_EDIT_SUPPORTING_WRITE_REFUSED",
        "COMPONENT_EDIT_BINDING_REQUIRED",
        "COMPONENT_EDIT_DEPENDENCY_UNVERIFIED",
        "PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID",
        "AUTHORING_APPLY_VALIDATION_REQUIRED",
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

# `available_actions` is filled in at access time from the router's dispatch table —
# see `_monitoring_overview()`. It was a hand-written list of ten while the router
# dispatched eighteen, and it stayed stale through the slice that added an action AND
# through the sweep that was supposed to have caught it. Deriving it here is the third
# attempt at that sweep; the first two each fixed one copy and declared completeness.
_MONITORING_OVERVIEW = {
    "resource_type": "monitoring",
    "tool": "monitor_platform",
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
                "1. list_integration_archetypes() → pick an executable recipe (recipe_id + exact recipe_version); get_integration_archetype(name='<recipe_id>', recipe_version='<version>') returns its input_schema (or author ProcessIR units against get_schema_template(schema_name='AuthoringRequestV1'))",
                "2. build_integration(action='compile', config='{\"authoring_request\": {\"contract_version\": \"2\", \"intent\": {\"intent_kind\": \"recipe\", ...}}}') → compile hash + capability revision (no Boomi mutation)",
                "3. build_integration(action='apply', config='{\"dry_run\": false, \"authoring_request\": {..., \"expected_capability_revision\": \"...\", \"expected_compile_hash\": \"...\"}}') → create the process component(s)",
                "4. manage_deployment(action='create_package', config='{\"component_id\":\"...\", \"component_type\":\"process\", \"package_version\":\"1.0\"}') → package it",
                "5. manage_deployment(action='deploy', package_id='<pkg_id>', environment_id='<env_id>') → deploy it",
                "6. execute_process(profile='...', process_id='<proc_id>', environment_id='<env_id>') → run it",
                "7. monitor_platform(action='execution_records', config='{\"execution_id\": \"...\"}') → check status",
            ],
        },
        "build_integration_from_description": {
            "description": "Author an integration: FIRST consult design_doctrine and select patterns by capability_status, THEN prefer an executable recipe; fall back to direct ProcessIR units only when no recipe fits.",
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
                "2. get_schema_template(schema_name='design_doctrine') → consult the design pattern catalog BEFORE choosing a recipe; fetch a specific pattern with get_schema_template(schema_name='design_pattern:<name>'). Also consult get_schema_template(schema_name='account_governance') for folder placement, component naming, and role/write-restriction governance (fetch one with get_schema_template(schema_name='governance_pattern:<name>')). Select entries from BOTH surfaces by capability_status and record each as emittable_today (proceed via the named tool), gated (design around / propose for GUI apply), or guidance_only (GUI/handoff). For cache / dynamic-property / state-handoff intents also consult get_schema_template(schema_name='cache_property_authoring') (per-term capability_status + provenance, #124) — plan-time lineage validation rejects unprovable cache/property reads before any mutation.",
                "3. list_integration_archetypes() → discover the executable recipe catalog: recipe_id + exact recipe_version per row (read-only, no Boomi mutation)",
                "4. get_integration_archetype(name='<recipe_id>', recipe_version='<version>') → inspect the descriptor (capability_notes, limitations, examples), input_schema and, where declared, component_material_schema",
                # Connection-reuse discovery (issue #83, M7.3). Interposed BEFORE
                # the first build_integration call so callers check for an
                # existing secured connection first and feed the returned
                # reference_only / conflict_policy='reuse' binding into the
                # authoring request — keeping credentials out of the conversation. A
                # parsable suggest_connection_reuse(...) step so the available_tools
                # filter tracks it; it is stripped (see below) when that tool is
                # not registered so the core authoring chain still surfaces.
                "5. suggest_connection_reuse(profile='...', connector_type='...', endpoint_hint='...') → check for an EXISTING secured connection to reuse (reference_only + conflict_policy='reuse') before authoring a new one; feed any returned reuse binding into the recipe input (binding_mode='reuse') or the request's base_components (read-only, no Boomi mutation)",
                "6. build_integration(action='plan', config='{\"authoring_request\": {\"contract_version\": \"2\", \"intent\": {\"intent_kind\": \"recipe\", \"integration_name\": \"...\", \"invocations\": [{\"recipe_id\": \"...\", \"recipe_version\": \"...\", \"invocation_id\": \"...\", \"raw_input\": {...}, \"component_material\": {...}}]}}}') → semantic validation, gaps and the authoring_result.integration_spec_preview (no Boomi mutation)",
                "7. review_transformation(action='validate_unmapped', config='{\"integration_spec\": <authoring_result.integration_spec_preview from step 6>}') → confirm the transform has no unmapped/invalid mappings BEFORE apply (read-only, no Boomi mutation). Optionally also run review_transformation(action='list_fields'|'mapping_diff') to inspect fields or diff against a prior spec.",
                "8. build_integration(action='compile', config='{\"authoring_request\": <request from step 6>}') → capability revision + compile hash (no Boomi mutation)",
                "9. build_integration(action='apply', config='{\"dry_run\": false, \"authoring_request\": {<request from step 6>, \"expected_capability_revision\": \"...\", \"expected_compile_hash\": \"...\"}}') → execute ordered component creation/update",
                "10. build_integration(action='verify', config='{\"build_id\": \"<uuid-from-apply>\"}') → verify created components and dependencies",
                "11. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=true) → preview package → deploy → runtime-bind → optional schedule/test; re-run with dry_run=false to execute (deployment happens BEFORE any schedule/test).",
            ],
            "fallback": {
                "when": "No recipe fits — e.g., an integration shape not yet covered by the recipe registry.",
                "steps": [
                    "F1. get_schema_template(schema_name='AuthoringRequestV1') → the exact request schema; author ProcessIR units (intent_kind='process_ir') against get_schema_template(schema_name='ProcessIRV1')",
                    "F2. build_integration(action='plan', config='{\"authoring_request\": ...}') → validate the hand-authored units, then action='compile' for the compile hash",
                    "F3. build_integration(action='apply', config='{\"dry_run\": false, \"authoring_request\": {..., \"expected_capability_revision\": \"...\", \"expected_compile_hash\": \"...\"}}') → execute",
                    "F4. build_integration(action='verify', config='{\"build_id\": \"...\"}') → verify",
                    "F5. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=true) → preview the deploy plan; re-run with dry_run=false to package → deploy → bind runtime → optional schedule/test.",
                ],
            },
        },
        "compose_multi_target_integration": {
            "description": (
                "Compose a multi-target integration (M8 / issue #14): one DB "
                "source feeding a shared transform that fans out to 2..25 REST "
                "targets via a Branch — the boomi.compose.db_rest_fanout recipe, "
                "for a shape that exceeds one linear recipe. Links may stage the "
                "fan-out through a Document Cache (M8.1 / #132: "
                "handoff.mode='document_cache' lowers onto M11 cache_put/"
                "cache_get; write-before-read leg order is validated at "
                "plan time, before any Boomi mutation)."
            ),
            "steps": [
                "1. get_integration_archetype(name='boomi.compose.db_rest_fanout') → inspect the fan-out input_schema (source, transform, targets, links, handoff modes) and its component_material_schema; for document_cache handoffs also consult get_schema_template(schema_name='cache_property_authoring')",
                "2. build_integration(action='plan', config='{\"authoring_request\": {\"contract_version\": \"2\", \"intent\": {\"intent_kind\": \"recipe\", \"integration_name\": \"...\", \"invocations\": [{\"recipe_id\": \"boomi.compose.db_rest_fanout\", \"recipe_version\": \"2.0.0\", \"invocation_id\": \"...\", \"raw_input\": {...}, \"component_material\": {...}}]}}}') → preview the deterministic plan (invalid contract links fail here, before any Boomi mutation)",
                "3. build_integration(action='compile', config='{\"authoring_request\": <request from step 2>}') → capability revision + compile hash (no Boomi mutation)",
                "4. build_integration(action='apply', config='{\"dry_run\": false, \"authoring_request\": {<request from step 2>, \"expected_capability_revision\": \"...\", \"expected_compile_hash\": \"...\"}}') → execute ordered component creation",
                "5. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=true) → preview package → deploy → runtime-bind; re-run with dry_run=false to execute",
            ],
        },
        "build_and_verify_http_listener": {
            "description": (
                "Author an inbound HTTP (Web Services Server) listener integration "
                "and verify the deployed route (M6 / #12; ASC M6.1 / #133). The "
                "runtime's Shared Web Server apiType decides the publish pattern: "
                "bare WSS /ws/simple serves basic/intermediate; 'advanced' serves "
                "ONLY /ws/rest routes through an API Service Component "
                "(asc_wrapper.enabled=true — bare WSS deploys clean there but "
                "404s). ASC deploy does NOT cascade: the ASC and the listener "
                "process each deploy to the environment (orchestrate_deploy "
                "handles both). Listener processes have NO Test mode — "
                "verification is the orchestrate_deploy listener_verify stage "
                "(apiType preflight, deploy-both check, collision scan, "
                "authenticated live probe, execution readback). ListenerStatus is "
                "NOT a WSS/ASC verification signal — it stays empty for serving "
                "WSS/ASC routes (live-proven)."
            ),
            "steps": [
                "1. manage_shared_resources(action='get_server_info', resource_id='<runtime-id>') → read apiType FIRST (it selects the publish pattern) and whether auth requires a Basic token",
                "2. get_integration_archetype(name='boomi.archetype.http_listener_to_db') or get_integration_archetype(name='boomi.archetype.http_listener_to_rest') → inspect the listener recipe's input_schema and component_material_schema (object_name, operation_type, JSON payload profile, optional inbound_validation, publish_mode / api_service)",
                "3. build_integration(action='plan', config='{\"authoring_request\": {\"contract_version\": \"2\", \"intent\": {\"intent_kind\": \"recipe\", \"integration_name\": \"...\", \"invocations\": [{\"recipe_id\": \"boomi.archetype.http_listener_to_db\", \"recipe_version\": \"1.0.0\", \"invocation_id\": \"...\", \"raw_input\": {...}, \"component_material\": {...}}]}}}') → apiType basic/intermediate: default raw_input.listener.publish_mode='bare_wss'; apiType advanced: set publish_mode='api_service' AND a unique component_material.api_service.base_url_path (one deployed ASC serves per base urlPath — a same-base ASC deployed earlier shadows the whole component); previews the deterministic plan (ASC route dependencies validated here)",
                "4. build_integration(action='compile', config='{\"authoring_request\": <request from step 3>}') → capability revision + compile hash (no Boomi mutation)",
                "5. build_integration(action='apply', config='{\"dry_run\": false, \"authoring_request\": {<request from step 3>, \"expected_capability_revision\": \"...\", \"expected_compile_hash\": \"...\"}}') → create the components",
                "6. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=false, config='{\"listener_test_payload\": \"<json>\", \"listener_base_url\": \"<override when the runtime url is not reachable from here>\"}') → deploy the process (+ the ASC in api_service mode) + listener_verify (tier preflight, active-deployment checks, collision scan, authenticated probe of the /ws/simple or /ws/rest endpoint, execution-record readback)",
                "7. monitor_platform(action='execution_records', config='{\"process_id\": \"...\"}') → inspect the triggered execution — an HTTP 2xx ack does NOT imply process success (live-proven: 200 with an ERROR execution)",
            ],
        },
        "set_up_b2b_trading_partner": {
            "description": "Create a trading partner for EDI/B2B integration",
            "steps": [
                "1. manage_trading_partner(action='list_options') → see available standards/protocols",
                # `operation='create'` is required: without it the trading-partner
                # handler ignores `standard` and serves the generic overview, so
                # the step returned 607 bytes of catalog instead of the 1290-byte
                # X12 create template it promises.
                "2. get_schema_template(resource_type='trading_partner', operation='create', standard='x12') → get template",
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


def _authoring_actions() -> tuple:
    """The ``build_integration`` action set, from the one contract registry.

    Falls back to the legacy three only if the contract cannot be imported: a
    catalog that advertised ``compile`` on a runtime whose dispatcher could not
    serve it would be worse than one that under-reports.
    """
    try:
        from ..authoring.contract import AUTHORING_ACTIONS

        return tuple(AUTHORING_ACTIONS)
    except Exception:  # noqa: BLE001 — the catalog is advisory here
        return ("plan", "apply", "verify")


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
        "script_dataprocess",
        "script_mapping",
        "cache_property_authoring",
        "process_property",
        "document_cache",
        "api_service",
        # Issue #145 M12.10 — typed recipe contributions.
        "recipe_contributions",
        "recipe_registry",
        # Issue #146 M12.11 — the typed MCP authoring surface. Sourced from the
        # ONE contract registry rather than listed here, so this catalog and the
        # dispatcher below cannot advertise different selectors.
    ]
    try:
        # Call-time import, same constraint as the archetype/recipe discovery
        # below: the authoring contract reaches the pattern registry, which
        # imports categories.components.builders.
        from ..authoring.contract import authoring_schema_selectors

        names += list(authoring_schema_selectors())
        names.append("authoring_workflow")
    except Exception:  # noqa: BLE001 — discovery is advisory here
        pass
    names += [f"workflow:{key}" for key in _authoring_workflow_sequences()]
    # design_doctrine / account_governance are stdlib-only static modules —
    # import-safe, so no try/except guard (unlike the recipe registry below).
    names += [f"design_pattern:{name}" for name in valid_design_pattern_names()]
    names += [
        f"governance_pattern:{name}" for name in valid_governance_pattern_names()
    ]
    # #159 S3: the archetype:<name> selectors retired; nothing here reaches the
    # archetype pattern registry any more.
    try:
        # Best-effort: a registry construction failure must not break the error
        # envelope that calls this function purely to LIST valid names.
        from ..recipes import production_registry

        names += [
            f"recipe:{d.recipe_id}@{d.recipe_version}"
            for d in production_registry().descriptors()
        ]
    except Exception:  # noqa: BLE001 — discovery is advisory here
        return names
    return names


# Read-only Groovy custom-scripting authoring contracts (script affordance work).
# Surfaced at the emit point so an agent authoring a script body gets the
# mandatory idioms (the canonical loop + storeStream + DDP/DPP) and a docs
# pointer here, not only from parametric memory. Skeletons are plain strings
# (no fenced blocks, no raw component XML) and the related_gotchas ids are
# wired to the kb.operational_gotchas catalog.
_SCRIPT_DATAPROCESS_AUTHORING_SCHEMA = {
    "surface": "data_process custom_scripting step (transform.steps[].script)",
    "tool": (
        "build_integration — a ProcessIR data_process custom_scripting step in "
        "config.authoring_request"
    ),
    "language": "groovy2",
    "docs": "search_boomi_docs('Boomi Custom Scripting dataContext')",
    "skeleton": (
        "import com.boomi.execution.ExecutionUtil\n"
        "\n"
        "for (int i = 0; i < dataContext.getDataCount(); i++) {\n"
        "    InputStream is = dataContext.getStream(i)\n"
        "    Properties props = dataContext.getProperties(i)\n"
        "    // read a dynamic document property (note the required prefix):\n"
        "    //   props.getProperty('document.dynamic.userdefined.DDP_NAME')\n"
        "    // write one only when the value is not null:\n"
        "    //   if (v != null) props.setProperty('document.dynamic.userdefined.DDP_NAME', v)\n"
        "    // read/write a dynamic process property:\n"
        "    //   ExecutionUtil.getDynamicProcessProperty('DPP_NAME')\n"
        "    //   ExecutionUtil.setDynamicProcessProperty('DPP_NAME', String.valueOf(n), false)\n"
        "    dataContext.storeStream(is, props)  // required, or the document is dropped\n"
        "}\n"
    ),
    "rules": [
        "Process every document: loop over dataContext.getDataCount(), reading "
        "getStream(i) and getProperties(i).",
        "Call dataContext.storeStream(is, props) for every document you keep — "
        "an unstored document is silently dropped; storing N streams emits N "
        "documents.",
        "Prefix dynamic document property keys with "
        "document.dynamic.userdefined. (case-sensitive).",
        "Reach dynamic process properties and the execution logger through "
        "ExecutionUtil; property values are strings, so convert numbers "
        "explicitly.",
        "Never pass null to props.setProperty — it throws a NullPointerException "
        "at assignment.",
        "Use ByteArrayInputStream to emit rewritten content; the runtime is "
        "sandboxed (no external network calls — use a connector step instead).",
        "Prefer native components over scripting; a split or combine is a native "
        "Data Process operation, never a reason to script.",
    ],
    "related_gotchas": [
        "groovy_dataprocess_storestream_required",
        "groovy_props_setproperty_null_npe",
        "groovy_ddp_prefix_required",
        "groovy_compiles_first_execution",
    ],
}

_SCRIPT_MAPPING_AUTHORING_SCHEMA = {
    "surface": (
        "script.mapping component body (script_body) referenced by a map "
        "Scripting function"
    ),
    "tool": (
        "manage_component(action='create', config='{\"component_type\": "
        "\"script.mapping\", \"component_name\": \"...\", \"language\": "
        "\"groovy2\", \"script_body\": \"...\", \"inputs\": [{\"name\": "
        "\"...\", \"data_type\": \"character\"}], \"outputs\": [{\"name\": "
        "\"...\"}]}')"
    ),
    "language": "groovy2",
    "docs": "search_boomi_docs('Boomi map scripting function')",
    "skeleton": (
        "// Inputs and outputs are pre-declared variables — do not redeclare them.\n"
        "// Read each input and assign each output; a returned value is ignored.\n"
        "outputVar = (inputVar ?: '').trim()\n"
        "// A dynamic process property is available here:\n"
        "//   import com.boomi.execution.ExecutionUtil\n"
        "//   ExecutionUtil.setDynamicProcessProperty('DPP_NAME', String.valueOf(n), false)\n"
        "// Dynamic DOCUMENT properties are NOT accessible in this body —\n"
        "// use the Get Document Property / Set Document Property function steps.\n"
    ),
    "rules": [
        "Inputs and outputs are pre-declared variables — read inputs, assign "
        "outputs, and do not redeclare them.",
        "Bind a result by assigning the named output variable; a 'return' value "
        "is ignored.",
        "The dataContext loop and storeStream are not map-script concepts.",
        "A dynamic process property via ExecutionUtil is accessible; dynamic "
        "document properties are NOT accessible in the body — use the Get and "
        "Set Document Property function steps in the map.",
        "Character inputs are never null (an empty string for a missing value); "
        "date, integer, and float inputs can be null, so guard them.",
    ],
    "related_gotchas": [
        "groovy_ddp_prefix_required",
        "groovy_compiles_first_execution",
    ],
    "follow_up": (
        "Native Get/Set Dynamic Process Property and Get/Set Document Property "
        "map functions are now emittable via map_type='function' — use "
        "function_type 'dynamic_process_property_get' / "
        "'dynamic_process_property_set' / 'document_property_get' / "
        "'document_property_set' (and the component-backed "
        "'defined_process_property_get' / 'defined_process_property_set') in "
        "the structured function_mappings contract."
    ),
}


# M11.1 (issue #120, epic #118): the cache/property authoring vocabulary
# surface. Read-only. capability_status is the honesty contract: every term
# starts 'reserved_not_executable' and is flipped to 'executable' ONLY by the
# child that ships its emitter/builder (#121 set_ddp/set_dpp, #131
# processproperty component, #122 cache_put/cache_get/cache_join); terms whose
# wire shape lacks live/corroborated evidence stay 'gated_no_verified_wire_shape'
# (#119 census records each gate outcome).
_CACHE_PROPERTY_AUTHORING_TERMS: Dict[str, Dict[str, str]] = {
    "set_ddp": {
        "provenance": "live_verified",
        "meaning": "Set a Dynamic Document Property (per-document scope) via a Set Properties step",
        "capability_status": "executable",
        "owning_issue": "#121 (M11.2)",
        "surface": "build_integration process config flow_sequence[].kind='set_ddp' (name + source_values)",
    },
    "set_dpp": {
        "provenance": "live_verified",
        "meaning": "Set a Dynamic Process Property (execution scope; optional persist) via a Set Properties step",
        "capability_status": "executable",
        "owning_issue": "#121 (M11.2)",
        "surface": "build_integration process config flow_sequence[].kind='set_dpp' (name + source_values + persist?)",
    },
    "get_property": {
        "provenance": "live_verified",
        "meaning": "Read a property value into a flow (map function today; Set Properties definedparameter source once verified)",
        "capability_status": "executable",
        "owning_issue": "#121 (M11.2) / #131 (M11.7)",
        "surface": (
            "build_integration transform.map map_type='function' "
            "defined_process_property_get referencing an in-spec "
            "processproperty component ($ref + explicit property key). The "
            "Set Properties definedparameter source stays gated (no verified "
            "wire shape, #119 census)."
        ),
    },
    "set_process_property": {
        "provenance": "live_verified",
        "meaning": "Write a Process Property component slot at runtime (definedprocess.<componentId>@<propertyKey>)",
        "capability_status": "executable",
        "owning_issue": "#121 (M11.2) / #131 (M11.7)",
        "surface": (
            "build_integration transform.map map_type='function' "
            "defined_process_property_set referencing an in-spec "
            "processproperty component. The Set Properties shape write "
            "variant (propertyId='definedprocess.<id>@<key>') stays gated "
            "(companion-only wire shape, #119 census)."
        ),
    },
    "cache_put": {
        "provenance": "live_verified",
        "meaning": "Write current documents into a Document Cache (success-path Add to Cache)",
        "capability_status": "executable",
        "owning_issue": "#122 (M11.3)",
        "surface": "build_integration process config flow_sequence[].kind='cache_put' (document_cache_id)",
    },
    "cache_get": {
        "provenance": "live_verified",
        "meaning": "Retrieve documents from a Document Cache (all-document; keyed mode gated on live evidence)",
        "capability_status": "executable",
        "owning_issue": "#122 (M11.3)",
        "surface": (
            "build_integration process config flow_sequence[].kind='cache_get' "
            "(document_cache_id + empty_cache_behavior). Keyed retrieval "
            "(doc_cache_index / cache_key_values) stays gated — no live "
            "capture (#119 census)."
        ),
    },
    "cache_join": {
        "provenance": "live_verified",
        "meaning": "Join cached documents into a map by cache index/key (DocumentCacheJoins)",
        "capability_status": "executable",
        "owning_issue": "#122 (M11.3)",
        "surface": (
            "build_integration transform.map config document_cache_joins[] "
            "(document_cache_id + cache_index + join_id + src_parent_key + "
            "key_values) — live-captured wire shape (#119 census Outcome A)."
        ),
    },
    "processproperty_component": {
        "provenance": "live_verified",
        "meaning": "Typed create/update of a standalone Process Property component (definedProcessProperty slots)",
        "capability_status": "executable",
        "owning_issue": "#131 (M11.7)",
        "surface": (
            "manage_component(action='create', config={component_type:"
            "'processproperty', ...}) or an in-spec build_integration "
            "component; template at get_schema_template("
            "resource_type='component', component_type='processproperty', "
            "operation='create')"
        ),
    },
    "documentcache_component": {
        "provenance": "live_verified",
        "meaning": "Typed create/update of a Document Cache component (profile/index/key modeling)",
        "capability_status": "executable",
        "owning_issue": "#122 (M11.3)",
        "surface": (
            "manage_component(action='create', config={component_type:"
            "'documentcache', ...}) or an in-spec build_integration component; "
            "template at get_schema_template(resource_type='component', "
            "component_type='documentcache', operation='create')"
        ),
    },
}


def cache_property_authoring_contract() -> Dict[str, Any]:
    """The CONTRACT half of the cache/property surface — manifest-free (#146).

    Extracted so one body feeds both the served payload and
    ``schema_revision``. Before this split the selector's payload carried no
    recognised schema body at all, so its digest was permanently unavailable and
    a change to the state-visibility model moved no revision.

    Manifest-free on purpose, exactly like ``authoring_workflow_contract``: the
    served payload embeds the revision, so fingerprinting the served payload
    would recurse into the manifest being built.
    """
    from ..models.cache_property_models import PROPERTY_SOURCE_FIELD_CONTRACT

    return {
        "terms": _CACHE_PROPERTY_AUTHORING_TERMS,
        "source_value_contract": {
            value_type: {
                "required": list(required),
                "optional": list(optional),
            }
            for value_type, (required, optional) in sorted(
                PROPERTY_SOURCE_FIELD_CONTRACT.items()
            )
        },
        "scopes": {
            "ddp": "per-document; copies with the document across branch legs; not visible to sibling-leg documents",
            "dpp": "per-execution; visible across branch legs; last-write-wins so write order matters",
            "processproperty": "component-backed deploy-time defaults; runtime reads via map functions (#131)",
            "documentcache": "execution-scoped store; write (cache_put/doccacheload) must precede read (cache_get/cache_join)",
        },
        # #146: the same four scopes, GENERATED from the authorities that enforce
        # them, beside the legacy prose rather than replacing it.
        #
        # The prose above stays byte-identical for compatibility, but it was an
        # unpinned second copy of the state-visibility model — the exact drift
        # this contract exists to prevent. These rows come from the lineage
        # traversal's own descriptor (and, for the process property, from the
        # model that owns it), so they cannot say something the code does not do,
        # and a parity test asserts the prose agrees with them.
        "canonical_scopes": _canonical_scope_rows(),
        # Per-TERM canonical state, beside each term's own `capability_status`.
        # `canonical_scopes` are visibility facts, not capability states — they
        # answer "where is this value visible", not "may I author this".
        "canonical_term_states": _canonical_term_states(),
        # Issue #124 M11.5: the honesty ledger — what each provenance label
        # means and which shapes remain gated, sourced from the #119 census.
        "provenance_labels": {
            "live_verified": "wire shape captured from live account components/processes",
            "docs_corroborated": "official docs + companion agree; no byte-level live capture",
            "companion_unverified": "companion-only claim; treat as hypothesis",
        },
        "evidence_gates": {
            "keyed_cache_get": "gated — no live capture of a populated key-values retrieve",
            "definedparameter_source": "gated — property-component read inside a property step is companion-only",
            "set_process_property_step": "gated — property-component write inside a property step is companion-only",
            "document_property_cache_key": "gated — document-property cache key kind has no live capture",
            "non_profiled_caches": "gated — only profiled JSON/XML caches are live-verified",
        },
        "hint": (
            "Vocabulary only: a term listed here is NOT executable until its "
            "capability_status says 'executable'. Attempting a reserved term in "
            "build_integration fails structural validation in the process "
            "builders, before any Boomi mutation. Provenance labels what the "
            "evidence actually is — never treat a companion_unverified claim "
            "as authoritative."
        ),
    }


def _canonical_term_states() -> List[Dict[str, Any]]:
    """Each term's own capability_status, mapped to the canonical vocabulary.

    Derived from the term table and the published mapping, so a term flipped to
    executable moves both at once.
    """
    from ..authoring.process_ir_projection import state_mappings

    mapping = {
        row.source_state: row.canonical_state
        for row in state_mappings()
        if row.source_vocabulary == "cache_property_authoring"
    }
    rows = []
    for term, body in sorted(_CACHE_PROPERTY_AUTHORING_TERMS.items()):
        source_state = body.get("capability_status", "")
        if source_state not in mapping:
            # LOUD, like the projector's own `_canonical_state`. Defaulting to
            # "unsupported" would publish "never" for a state whose real meaning
            # nobody decided — and would let a served term cite a source state
            # that is absent from the published `state_mappings`, so a caller
            # could not look it up either.
            raise KeyError(
                f"cache/property term {term!r} has capability_status "
                f"{source_state!r}, which the published state mapping does not "
                f"cover; add it to the mapping rather than defaulting."
            )
        rows.append(
            {
                "term": term,
                "source_state": source_state,
                "canonical_state": mapping[source_state],
            }
        )
    return rows


def _canonical_scope_rows() -> List[Dict[str, Any]]:
    """The four state scopes, generated from the authorities that enforce them.

    Fetched through the authoring projection rather than read off the compiler
    directly. A serving module reaching into the compiler's validation package
    would be a THIRD wiring site into a package whose call sites are
    deliberately enumerated and asserted, and the whole point of #146's seam is
    that exactly one module does that reading.
    """
    from ..authoring.process_ir_projection import canonical_state_scope_rows

    return canonical_state_scope_rows()


def _cache_property_authoring_schema() -> Dict[str, Any]:
    """The read-only cache_property_authoring vocabulary surface (#120)."""
    return {
        "_success": True,
        "schema_name": "cache_property_authoring",
        "surface": "cache_property_authoring",
        "read_only": True,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
        "epic": "#118 (M11: Cache + Dynamic Properties Authoring)",
        **cache_property_authoring_contract(),
    }


def _recipe_contributions_schema() -> Dict[str, Any]:
    """The closed four-type contribution contract (issue #145 M12.10)."""
    from ..models.recipe_contributions import (
        RECIPE_COMPONENT_TYPES,
        RECIPE_CONTRIBUTION_KINDS,
        recipe_contribution_v1_json_schema,
    )

    return {
        "_success": True,
        "schema_name": "recipe_contributions",
        "surface": "typed executable recipe contributions",
        "read_only": True,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
        "contribution_kinds": list(RECIPE_CONTRIBUTION_KINDS),
        "component_types": list(RECIPE_COMPONENT_TYPES),
        "json_schema": recipe_contribution_v1_json_schema(),
        "authoring_note": (
            "These are the ONLY values a registered recipe may return. They "
            "carry opaque references and closed enumerations — never "
            "configuration, credentials, headers, SQL, raw XML, executable "
            "code, JSON pointers, or caller-authored graph edges. Recipe output "
            "is validated exactly as direct ProcessIR/topology/component "
            "authoring and cannot be exempted."
        ),
        "doctrine_note": (
            "Advisory doctrine may RECOMMEND a recipe by exact reference; it can "
            "never be parsed or executed as one. Registry construction rejects an "
            "advisory entry that declares an executor, so no advisory descriptor "
            "a caller can reach carries one."
        ),
    }


def _recipe_registry_schema() -> Dict[str, Any]:
    """The live registry snapshot plus the expectation/skew contract."""
    from ..recipes import production_registry
    from ..recipes.contracts import ExpectedRecipeRegistryV1

    snapshot = production_registry().snapshot()
    # #159 S3: described for exactly the entry kinds the LIVE registry carries.
    # The compatibility adapters retired, so a hand-kept description of a kind
    # no registration has would advertise an entry a caller can never see.
    live_kinds = {str(entry.get("entry_kind", "")) for entry in snapshot.get("entries", ())}
    entry_kinds = {
        "executable_recipe": "registered code that may emit any declared contribution type",
        "constraint_only": "registered code that may emit ConstraintRequirement only",
        "advisory": (
            "a doctrine pointer. Registry construction rejects an advisory "
            "entry that declares an executor, an input schema or outputs, so "
            "no advisory descriptor a caller can reach carries one — and "
            "doctrine prose is never parsed into a recipe id"
        ),
        "compatibility_adapter": "a legacy surface that projects into an exact recipe version",
    }
    return {
        "_success": True,
        "schema_name": "recipe_registry",
        "surface": "recipe registry provenance and skew",
        "read_only": True,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
        "snapshot": snapshot,
        "expected_registry_schema": ExpectedRecipeRegistryV1.model_json_schema(),
        "entry_kinds": {
            kind: text for kind, text in entry_kinds.items() if kind in live_kinds
        },
        "skew_note": (
            "Equal recipe versions are NOT proof of equal code. Supply "
            "implementation_sha256 in expected_recipe_registry entries to have "
            "the live implementation hash compared; a difference reports "
            "'mismatch' even when the versions agree. An expectation carrying "
            "neither registry_revision nor source_revision AND lacking an "
            "implementation_sha256 on some entry is a VERSION-ONLY comparison: "
            "it reports 'unknown' with a reason, never 'match', because it "
            "cannot establish parity. A difference found is still 'mismatch' "
            "however partial the comparison was."
        ),
    }


def _recipe_descriptor_schema(reference: str) -> Dict[str, Any]:
    """One descriptor, addressed as ``recipe:<id>@<version>`` or ``recipe:<id>``."""
    from ..recipes import RecipeError, production_registry

    recipe_id, _, version = reference.partition("@")
    try:
        descriptor = production_registry().resolve(recipe_id, version or None)
    except RecipeError:
        return {
            "_success": False,
            "error": f"No registered recipe matches {reference!r}.",
            "error_code": SCHEMA_NAME_UNSUPPORTED,
            "valid_schema_names": _valid_schema_names(),
        }

    payload: Dict[str, Any] = {
        "_success": True,
        "schema_name": f"recipe:{descriptor.recipe_id}@{descriptor.recipe_version}",
        "surface": "recipe descriptor",
        "read_only": True,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
    }
    # #159 S3: ONE projection, shared with get_integration_archetype — the
    # descriptor, its input schema and (for the registrations that declare the
    # closed companion channel) the component-material schema.
    from .integration_authoring import recipe_descriptor_projection

    payload.update(
        recipe_descriptor_projection(production_registry(), descriptor)
    )
    return payload


def _authoring_contract_schema(
    schema_name: str, *, query: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """Serve one #146 authoring selector from the runtime models themselves.

    Generated, never hand-copied: a hand-written schema is a second source of
    truth for what the wrapper enforces, and the two drift silently. Accepts an
    optional ``@<version>`` suffix; an unknown version is
    ``AUTHORING_SCHEMA_VERSION_UNAVAILABLE`` with the served versions listed, and
    a bare selector stays valid exactly as before.

    ``query`` (#146 amendment) carries the retrieval filters for the one
    selector that is QUERIED rather than fetched whole. The schema is always
    returned alongside the page, so a caller who filters still learns the shape
    of what came back.
    """
    from ..authoring.contract import (
        get_authoring_revisions,
        projection_query_builder,
        schema_builder,
        schema_version_for,
        supported_schema_versions,
    )
    from ..authoring.revisions import sha256_fingerprint
    from ..errors import AUTHORING_SCHEMA_VERSION_UNAVAILABLE

    selector, _, requested_version = schema_name.partition("@")
    builder = schema_builder(selector)
    if builder is None:
        return None

    served_version = schema_version_for(selector)
    if requested_version and requested_version != served_version:
        return {
            "_success": False,
            "error_code": AUTHORING_SCHEMA_VERSION_UNAVAILABLE,
            "schema_name": schema_name,
            "requested_version": requested_version,
            "supported_versions": list(supported_schema_versions(selector)),
            "error": (
                f"Schema '{selector}' is not served at version "
                f"'{requested_version}' by this server."
            ),
            "suggestion": (
                "Call list_capabilities() to discover the served schema and "
                "capability revisions, then request a supported version."
            ),
        }

    revisions = get_authoring_revisions()
    json_schema = builder()
    payload = {
        "_success": True,
        "schema_name": selector,
        "surface": "MCP authoring contract (issue #146 M12.11)",
        "read_only": True,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
        "schema_version": served_version,
        "schema_hash": sha256_fingerprint(json_schema),
        "revision_binding": revisions,
        "json_schema": json_schema,
    }

    query_builder = projection_query_builder(selector)
    if query_builder is None:
        if query:
            # A filter on a selector that has nothing to filter is a caller
            # mistake worth naming: silently ignoring it would return the whole
            # schema and let them believe the filter applied.
            return _authoring_filter_error(schema_name, sorted(query))
        return payload

    from ..authoring.process_ir_projection import ProcessIRAuthoringQueryError

    try:
        page = query_builder(**(query or {}))
    except ProcessIRAuthoringQueryError as exc:
        return _authoring_filter_value_error(schema_name, exc)
    except Exception as exc:  # noqa: BLE001
        # An AUTHORITY this projection reads failed to build. Reported as a
        # typed unavailable answer, exactly as `list_capabilities` already
        # reports the same condition — never as a raw traceback across the MCP
        # boundary, and never as a short catalog. A registry that cannot build
        # used to be swallowed into an empty tuple here, so a dead recipe
        # registry served the contract with ZERO recipe links and `_success:
        # true`: a caller could not tell "this construct links no recipe" from
        # "the registry that knows died". Serving less than the contract claims
        # is worse than saying it is unavailable.
        return _authoring_source_unavailable(schema_name, exc)

    # RE-VALIDATE what is about to be served. The page model makes all seven
    # page-level rules correct by construction — FOUR by `Literal`
    # (`contract_version`, `diagnostic_label_legend` and the two
    # `unlisted_*_state`) and THREE by a registry validator (`state_mappings`,
    # `facets`, `catalog_entry_count`) — but the builder hands back a plain
    # dict, and a dict is editable by every layer between here and the caller.
    # Round-tripping it through the model extends that guarantee past the model
    # layer. (Two earlier versions of this comment miscounted these sets; the
    # count is asserted in the tests rather than trusted here.)
    #
    # Per verb: a rule BLANKED or FALSIFIED downstream is refused, and an
    # unexpected key INJECTED is refused (`extra='forbid'`). DROPPED splits —
    # the three registry-validated rules are declared REQUIRED and are refused,
    # while the four `Literal` rules are restored from their defaults, which is
    # safe only because for them the default IS the correct value. That split
    # is load-bearing: pydantic does not run `AfterValidator` on a default, so
    # while `state_mappings` still carried one it was served EMPTY when
    # dropped.
    payload["contract_page"] = _revalidated_contract_page(page)
    return payload


def _revalidated_contract_page(page: Dict[str, Any]) -> Dict[str, Any]:
    """Round-trip a served contract page through the model that declares it."""
    from ..models.process_ir_authoring import ProcessIRAuthoringContractPageV1

    return ProcessIRAuthoringContractPageV1.model_validate(page).model_dump(
        mode="json"
    )


def _authoring_filter_error(schema_name: str, fields: List[str]) -> Dict[str, Any]:
    from ..errors import INVALID_INPUT

    return {
        "_success": False,
        "error_code": INVALID_INPUT,
        "schema_name": schema_name,
        "invalid_parameters": list(fields),
        "error": (
            "Retrieval filters apply only to schema_name='process_ir_authoring'."
        ),
        "suggestion": (
            "Drop the filter arguments, or request "
            "get_schema_template(schema_name='process_ir_authoring', ...)."
        ),
    }


def _authoring_source_unavailable(schema_name: str, exc: Any) -> Dict[str, Any]:
    """The selector cannot be built because an authority it reads failed.

    Carries the exception TYPE and not its text: an authority's failure message
    is not a published contract and could name a path or a value.
    """
    from ..errors import AUTHORING_SCHEMA_SOURCE_UNAVAILABLE

    return {
        "_success": False,
        "error_code": AUTHORING_SCHEMA_SOURCE_UNAVAILABLE,
        "schema_name": schema_name,
        "status": "unavailable",
        "error": (
            f"The '{schema_name}' authoring contract cannot be built: an "
            f"authority it derives from is unavailable ({type(exc).__name__})."
        ),
        "retryable": True,
    }


def _authoring_filter_value_error(schema_name: str, exc: Any) -> Dict[str, Any]:
    """One bounded envelope naming the field AND the values that would work.

    The allowed list is truncated rather than omitted when it is long: an error
    that says only "invalid" makes the caller guess, and an error that returns
    every value turns a mistake into a payload.
    """
    from ..errors import INVALID_INPUT

    allowed = list(getattr(exc, "allowed", ()) or ())
    rule = getattr(exc, "rule", "") or ""
    envelope = {
        "_success": False,
        "error_code": INVALID_INPUT,
        "schema_name": schema_name,
        "invalid_parameter": getattr(exc, "field", ""),
        "error": f"Invalid value for '{getattr(exc, 'field', '')}'.",
    }
    if rule:
        # A range or a companion requirement, not an enumeration. Publishing an
        # empty allowed-values list beside "filter with a published value" was
        # worse than saying nothing — for a cursor the rejected value IS
        # published, so the advice sent the caller in a circle.
        envelope["rule"] = rule
        envelope["suggestion"] = rule
    else:
        envelope["allowed_values"] = allowed[:50]
        envelope["allowed_value_count"] = len(allowed)
        envelope["suggestion"] = (
            "Fetch the facets with a bare "
            "get_schema_template(schema_name='process_ir_authoring') call, then "
            "filter with a published value."
        )
    return envelope


def _authoring_workflow_schema() -> Dict[str, Any]:
    """The eight-step typed authoring sequence, and what each phase may do.

    Published as a schema selector because the phase boundary is the contract
    this issue adds: which calls may mutate is not a detail a caller should have
    to infer from a tool name.
    """
    from ..authoring.contract import (
        authoring_workflow_contract,
        build_authoring_contract_manifest,
    )

    # The CONTRACT body is manifest-free; the manifest only decorates it with the
    # revision. Reading the whole thing off the manifest made the digest of this
    # selector recurse into the manifest that was computing it — 143 levels of
    # full-bundle recomputation on a cold cache, and an "unavailable" hash on any
    # runtime whose recursion limit bit first.
    manifest = build_authoring_contract_manifest()
    return {
        **authoring_workflow_contract(),
        "_success": True,
        "schema_name": "authoring_workflow",
        "surface": "MCP authoring workflow (issue #146 M12.11)",
        "revision_binding": {
            "contract_version": manifest["contract_version"],
            "schema_revision": manifest["schema_revision"],
            "capability_revision": manifest["capability_revision"],
            "compiler_revision": manifest["compiler_revision"],
        },
        "authoring_note": (
            "Plan and compile perform ZERO remote mutation. Apply cannot proceed "
            "from a fatal validation report or a stale/mismatched revision "
            "binding. Legacy plan/apply/verify requests are unchanged and need "
            "no binding."
        ),
    }


def _get_authoring_schema_by_name(
    schema_name: str, *, query: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Dispatch get_schema_template(schema_name=...) requests (issue #10).

    Read-only reference data — never calls Boomi, never emits raw XML.
    """
    if schema_name == "authoring_workflow":
        if query:
            return _authoring_filter_error(schema_name, sorted(query))
        return _authoring_workflow_schema()

    # Issue #146 M12.11. Checked before the named selectors below so an
    # "@version" suffix reaches the version check rather than falling through to
    # the unknown-schema envelope.
    _authoring = _authoring_contract_schema(schema_name, query=query)
    if _authoring is not None:
        return _authoring

    # Every remaining selector serves a whole schema, so a filter on one is the
    # same caller mistake as a filter on an authoring selector without a
    # projection — reported the same way rather than ignored.
    if query:
        return _authoring_filter_error(schema_name, sorted(query))

    if schema_name == "recipe_contributions":
        return _recipe_contributions_schema()

    if schema_name == "recipe_registry":
        return _recipe_registry_schema()

    if schema_name.startswith("recipe:"):
        return _recipe_descriptor_schema(schema_name[len("recipe:"):])

    if schema_name == "cache_property_authoring":
        return _cache_property_authoring_schema()

    if schema_name == "document_cache":
        # Issue #122 M11.3: read-only authoring reference for Document Cache
        # component/index/key modeling and the join/read/write coupling.
        return {
            "_success": True,
            "schema_name": "document_cache",
            "surface": "document_cache",
            "read_only": True,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "create_template_pointer": (
                "get_schema_template(resource_type='component', "
                "operation='create', component_type='documentcache')"
            ),
            "coupling": (
                "cache_put / cache_get flow-sequence steps and map-level "
                "document_cache_joins entries reference the cache component "
                "by id ($ref for in-spec caches). Joins additionally bind "
                "cache_index to a CacheIndex indexId and each key_values "
                "entry's cache_key_id to a cacheKey id — plan-time validation "
                "cross-checks those against the in-spec cache config."
            ),
            "scope_note": (
                "Caches are execution-scoped: a cache_get / cache_join needs "
                "an upstream cache_put (or DLQ doccacheload) in an earlier "
                "reachable path of the same run unless the cache is "
                "explicitly documented as externally populated."
            ),
            "gates": (
                "Keyed cache_get, profile.none / flatfile / db / edi caches, "
                "and document-property cache keys are gated pending live "
                "wire-shape captures (#119 census)."
            ),
        }

    if schema_name == "process_property":
        # Issue #131 M11.7: read-only authoring reference for the Process
        # Property component <-> map-function key/label coupling.
        return {
            "_success": True,
            "schema_name": "process_property",
            "surface": "process_property",
            "read_only": True,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "create_template_pointer": (
                "get_schema_template(resource_type='component', "
                "operation='create', component_type='processproperty')"
            ),
            "key_label_coupling": (
                "Each definedProcessProperty carries an explicit stable UUID "
                "'key' plus a 'name' (the <label>). A referencing map "
                "function must pass process_property_component_id "
                "('$ref:<component key>' declared in depends_on), "
                "process_property_key (that UUID), and process_property_name "
                "(that label). Keys are caller-supplied in v1 — keep them "
                "stable across updates or map references break."
            ),
            "scopes_note": (
                "Process Property component values are deploy-time defaults "
                "readable at runtime (execution scope) — use them for "
                "operator-tunable settings; use set_dpp for run-computed "
                "state and set_ddp for per-document state."
            ),
        }

    if schema_name == "api_service":
        # Issue #133 M6.1: read-only authoring reference for the API Service
        # Component (webservice) <-> WSS Listen process coupling and the
        # apiType tier dispatch.
        return {
            "_success": True,
            "schema_name": "api_service",
            "surface": "api_service",
            "read_only": True,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "create_template_pointer": (
                "get_schema_template(resource_type='component', "
                "operation='create', component_type='webservice')"
            ),
            "tier_dispatch": (
                "SharedServerInformation.apiType selects the publish pattern: "
                "basic/intermediate serve bare WSS /ws/simple routes (no ASC); "
                "'advanced' serves ONLY /ws/rest routes through a deployed "
                "ASC (bare WSS deploys clean there but 404s — live-confirmed). "
                "Read apiType first via manage_shared_resources "
                "get_server_info; listener archetypes emit the ASC via "
                "asc_wrapper.enabled=true."
            ),
            "route_coupling": (
                "Each restApi route references a process with a WSS Listen "
                "start; empty-string override attributes inherit from that "
                "process's WSS Listen operation per attribute (all-inherit "
                "effective path: /ws/rest/{WSS-op objectName}, case-verbatim). "
                "In build_integration specs the route process is "
                "'$ref:<process key>' and MUST appear in the ASC's depends_on."
            ),
            "deploy_note": (
                "Deploy does NOT cascade: the ASC and every route process "
                "each need an active deployment in the same environment. "
                "orchestrate_deploy packages/deploys both and verifies via "
                "authenticated probe + execution-record readback — "
                "ListenerStatus stays empty for WSS/ASC routes and HTTP 200 "
                "does not imply process success (both live-proven)."
            ),
        }

    if schema_name == "IntegrationSpecV1":
        return {
            "_success": True,
            "schema_name": "IntegrationSpecV1",
            "json_schema": IntegrationSpecV1.model_json_schema(),
            "authority_versions": _AUTHORITY_VERSIONS,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "tool": "build_integration (action='plan' | 'apply')",
            "hint": (
                "Prefer config.authoring_request: a recipe from "
                "list_integration_archetypes() → get_integration_archetype(...), "
                "or ProcessIR units (get_schema_template("
                "schema_name='AuthoringRequestV1')). In this spec a process body "
                "is authored only as a typed unit in integration_spec.processes; "
                "a flat type='process' component may only bind an existing "
                "process, and a config.process_kind body is refused with "
                "PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED."
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
        # #159 S3: retired with the archetype build route. Value-free: the
        # requested name is not echoed, and nothing reaches the archetype
        # pattern registry.
        return {
            "_success": False,
            "error": (
                "The archetype:<name> selectors are retired: no route authors an "
                "archetype any more."
            ),
            "error_code": SCHEMA_NAME_UNSUPPORTED,
            "use_instead": (
                "get_schema_template(schema_name='recipe:<recipe_id>@<version>') "
                "for an executable recipe's descriptor and input schema; "
                "list_integration_archetypes() lists the recipe ids and versions."
            ),
            "valid_schema_names": _valid_schema_names(),
        }

    if schema_name == "script_dataprocess":
        # Read-only Groovy authoring contract for the Data Process Custom
        # Scripting body (no component XML emitted — guidance only).
        return {
            "_success": True,
            "schema_name": "script_dataprocess",
            **_SCRIPT_DATAPROCESS_AUTHORING_SCHEMA,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "read_only": True,
        }

    if schema_name == "script_mapping":
        # Read-only Groovy authoring contract for the map Scripting function /
        # script.mapping body (no component XML emitted — guidance only).
        return {
            "_success": True,
            "schema_name": "script_mapping",
            **_SCRIPT_MAPPING_AUTHORING_SCHEMA,
            "raw_xml_exposed": False,
            "boomi_mutation": False,
            "read_only": True,
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
    authoring_entry_id: Optional[str] = None,
    node_kind: Optional[str] = None,
    category: Optional[str] = None,
    capability_id: Optional[str] = None,
    workflow_stage: Optional[str] = None,
    after_entry_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Look up and return the appropriate template.

    Two selectors: ``resource_type`` picks a legacy template; ``schema_name``
    picks an authoring schema (issue #10 — IntegrationSpecV1, recipe:<recipe_id>,
    workflow_sequences, workflow:<name>). ``schema_name`` takes precedence when
    both are supplied; omitting both returns SCHEMA_SELECTOR_REQUIRED.

    The seven trailing parameters (#146 amendment) filter
    ``schema_name='process_ir_authoring'`` and are rejected anywhere else. They
    are TRAILING and OPTIONAL: every existing call keeps its exact meaning, and
    a caller who supplies none gets byte-identical behaviour to before.
    """
    query = {
        key: value
        for key, value in (
            ("authoring_entry_id", authoring_entry_id),
            ("node_kind", node_kind),
            ("category", category),
            ("capability_id", capability_id),
            ("workflow_stage", workflow_stage),
            ("after_entry_id", after_entry_id),
            ("limit", limit),
        )
        if value is not None and value != ""
    }

    if schema_name:
        return _get_authoring_schema_by_name(schema_name, query=query)

    if query:
        # A filter with no ``schema_name`` cannot mean anything: the legacy
        # ``resource_type`` templates have nothing to filter, and guessing that
        # the caller meant the authoring selector would answer a question they
        # did not ask.
        return _authoring_filter_error("", sorted(query))

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


_PROCESS_PROTOCOL_REMOVED = {
    "resource_type": "process",
    "operation": "create",
    "removed": True,
    "message": (
        "The legacy process_kind protocol templates are retired: a "
        "config.process_kind / process_type / flow_sequence process body is "
        "refused with PROCESS_COMPONENT_AUTHORING_UNIT_REQUIRED before anything "
        "is planned or written."
    ),
    "use_instead": {
        "authoring_request": (
            "build_integration(action='plan'|'compile'|'apply', "
            "config={'authoring_request': ...}) — the request schema is "
            "get_schema_template(schema_name='AuthoringRequestV1')."
        ),
        "recipe": (
            "list_integration_archetypes() → get_integration_archetype(name="
            "'<recipe_id>') for a recipe's input schema (intent_kind='recipe')."
        ),
        "process_unit": (
            "intent_kind='process_ir': one unit per process (envelope + ProcessIR "
            "root); the root schema is get_schema_template(schema_name='ProcessIRV1') "
            "and the behavioural contract "
            "get_schema_template(schema_name='process_ir_authoring')."
        ),
    },
}


#: An unknown process protocol (QA-159-S3-r1-02). Value-free — the caller's string is
#: never echoed — and no protocol is presented as VALID: every process protocol is
#: retired, so the names are listed only as retired (each still answers with the
#: removal envelope above).
_PROCESS_PROTOCOL_UNKNOWN = {
    "_success": False,
    "error": (
        "Unknown process protocol. No process protocol template is served: the "
        "legacy process_kind protocols are retired."
    ),
    "field": "protocol",
    "hint": (
        "Author processes with build_integration(config={'authoring_request': ...}); "
        "the request schema is get_schema_template(schema_name='AuthoringRequestV1'). "
        "Omit protocol for the process overview."
    ),
}


def _get_process_template(operation=None, protocol=None, **_):
    if protocol:
        if protocol not in _RETIRED_PROCESS_PROTOCOLS:
            return {
                **_PROCESS_PROTOCOL_UNKNOWN,
                "retired_process_protocols": list(_RETIRED_PROCESS_PROTOCOLS),
            }
        # Value-free: the same removal envelope for every retired protocol.
        return {"_success": True, **_PROCESS_PROTOCOL_REMOVED}

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
                if component_type in ("script.mapping", "transform.map"):
                    # Surface the map-script authoring pointer on the discovery
                    # call too (no operation), mirroring the process side's
                    # dataprocess_authoring — so an agent is nudged to the
                    # script_mapping contract before it picks operation='create'.
                    result["authoring_guidance"] = (
                        "Authoring a script body? Call "
                        "get_schema_template(schema_name='script_mapping') for the "
                        "map-script contract (named input/output variables, "
                        "assign-do-not-return, ExecutionUtil dynamic-process-"
                        "property access, and the no-document-property-access "
                        "rule) plus a skeleton, and "
                        "search_boomi_docs('Boomi map scripting function')."
                    )
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
            if protocol == "soap.client":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_SOAP_CLIENT}
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
            if protocol == "database.write":
                return {"_success": True, **_COMPONENT_CREATE_PROFILE_DB_DATABASE_WRITE}
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
        if component_type == "processproperty":
            # Issue #131 M11.7: typed template replaces the generic raw-XML
            # fallback, making the list_capabilities advertisement truthful.
            return {"_success": True, **_COMPONENT_CREATE_PROCESS_PROPERTY}
        if component_type == "documentcache":
            # Issue #122 M11.3: same truthful-advertisement upgrade.
            return {"_success": True, **_COMPONENT_CREATE_DOCUMENT_CACHE}
        if component_type in ("webservice", "api_service", "api.service"):
            # Issue #133 M6.1: typed API Service Component template replaces
            # the generic raw-XML fallback.
            return {"_success": True, **_COMPONENT_CREATE_API_SERVICE}
        if component_type == "connector-action":
            if protocol == "database.get":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_DATABASE_GET}
            if protocol == "database.send":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_DATABASE_SEND}
            if protocol == "rest.operation":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_REST_OPERATION}
            if protocol == "soap.operation":
                return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_ACTION_SOAP_OPERATION}
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
                "For processes, prefer build_integration with "
                "config.authoring_request (a recipe from "
                "list_integration_archetypes() or ProcessIR units); use config.xml "
                "here only as an explicit raw-XML escape hatch."
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


def _monitoring_templates():
    """Action name -> its request template. The ONLY definition of this map.

    The overview derives its `actions_with_templates` from this rather than
    carrying a second list, which is how the served overview came to advertise ten
    actions while the router dispatched eighteen.
    """
    return {
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


def _monitoring_overview():
    """The overview, with its action list DERIVED from the router.

    Two lists are served, and they are different facts: every action the tool
    dispatches, and the subset that has a request template. Collapsing them — which
    the hand-written version did by listing only the templated ones — told a caller
    that eight real actions did not exist.
    """
    templated = sorted(_monitoring_templates())
    return {
        **_MONITORING_OVERVIEW,
        "available_actions": _monitoring_actions(),
        "actions_with_templates": templated,
        "actions_without_templates": [
            a for a in _monitoring_actions() if a not in set(templated)
        ],
    }


def _get_monitoring_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_monitoring_overview()}

    templates = _monitoring_templates()

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
    # Issue #124 M11.5: cache/property/state intents route to the M11 doctrine
    # entries (state_scope_selection, caching_lookup_join, ...).
    "cache": frozenset({"cache", "caching", "lookup", "join", "enrichment", "handoff", "scope"}),
    "enrichment": frozenset({"enrichment", "enrich", "lookup", "cache", "join", "property"}),
    "state": frozenset({"state", "property", "scope", "handoff", "execution", "persist", "watermark"}),
    "property": frozenset({"property", "properties", "state", "scope", "handoff", "configuration"}),
    "branch_handoff": frozenset({"branch", "handoff", "cache", "property", "scope", "state"}),
    "join": frozenset({"join", "joins", "cache", "lookup", "merge", "enrichment"}),
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
        # #146 amendment adds "process_ir_pre_selection". ADDITIVE to the enum;
        # `required` below is deliberately untouched, because adding a required
        # property is a breaking change for every caller validating against this
        # schema — and this is the one tool in the repo that declares one.
        "mode": {
            "enum": [
                "archetype",
                "pre_selection",
                "process_ir_pre_selection",
                "error",
            ]
        },
        "supported_process_ir_constructs": {"type": "array"},
        "process_ir_capability_gaps": {"type": "array"},
        "authoring_contract_query": {"type": "object"},
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
        # Issue #145: EXACT recipe references only. Never a contribution, patch,
        # or input payload — an advisory surface must not hand back executable
        # material.
        "recommended_recipes": {
            "type": "array", "items": {"$ref": "#/$defs/recipe_ref"}
        },
        # #159 S3: recipe mode only — the same recipe_descriptor_projection
        # get_integration_archetype serves (descriptor, input_schema and, where
        # declared, component_material_schema). Schemas and documentation only.
        "recipe": {"type": "object", "additionalProperties": True},
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
        # Issue #146: ADVISORY typed hand-off. Deliberately NOT in `required` —
        # adding a required property is a breaking change for every existing
        # caller validating against this schema, and neither field is something
        # a caller must have.
        "revision_binding": {"type": "object", "additionalProperties": True},
        "typed_next_steps": {
            "type": "array", "items": {"$ref": "#/$defs/typed_next_step"}
        },
    },
    "required": [
        "_success", "tool", "mode", "read_only", "boomi_mutation",
        "raw_xml_exposed", "text",
    ],
    "$defs": {
        "recipe_ref": {
            "type": "object",
            "properties": {
                "recipe_id": {"type": "string"},
                "recipe_version": {"type": "string"},
                "entry_kind": {
                    "enum": [
                        "executable_recipe", "constraint_only", "advisory",
                        "compatibility_adapter",
                    ]
                },
                "adapter_target": {
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "recipe_id": {"type": "string"},
                                "recipe_version": {"type": "string"},
                            },
                            "required": ["recipe_id", "recipe_version"],
                        },
                        {"type": "null"},
                    ]
                },
            },
            "required": ["recipe_id", "recipe_version", "entry_kind"],
        },
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
        # Issue #146: a POINTER to the typed surface — a tool, an action and a
        # schema selector. Never a payload: this tool is advisory, and handing
        # back executable material is the boundary it exists to hold.
        "typed_next_step": {
            "type": "object",
            "properties": {
                "order": {"type": "integer"},
                "tool": {"type": "string"},
                "action": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "schema_selector": {"type": "string"},
                "why": {"type": "string"},
            },
            "required": ["order", "tool", "action", "schema_selector", "why"],
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


def _plan_recipe_keywords(descriptor: Dict[str, Any]) -> set:
    """Tokens drawn from a recipe descriptor — generic, registry-sourced."""
    keywords: set = set()
    sources = (
        [str(descriptor.get("recipe_id", "")).replace(".", " ")]
        + list(descriptor.get("capability_notes") or [])
        + list(descriptor.get("limitations") or [])
        + list(descriptor.get("not_for") or [])
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
                "Browse the executable recipes, pick one, then call "
                "plan_integration_design again with archetype set to its "
                "recipe_id for the full brief."
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


def _plan_process_ir_discovery_steps(profile: Optional[str]) -> list:
    """Discovery for a caller authoring ProcessIR directly (#146 amendment).

    Deliberately NOT the archetype path with one step swapped. Archetype
    discovery is the wrong first move here — there is no archetype to pick — so
    the sequence starts at the revision surface, then the grammar, then the
    behaviour, and only then reaches the account for the components the
    document will reference.
    """
    steps: list = [
        {
            "tool": "list_capabilities",
            "purpose": (
                "Read the served actions, selectors and the schema/capability/"
                "compiler revisions this runtime publishes."
            ),
            "arguments": {},
        },
        {
            "tool": "get_schema_template",
            "purpose": "Fetch the strict ProcessIRV1 grammar (what a document may contain).",
            "arguments": {"schema_name": "ProcessIRV1"},
        },
        {
            "tool": "get_schema_template",
            "purpose": (
                "Fetch the behavioural authoring contract (what the constructs "
                "MEAN): facets first, then filter by node_kind, category, "
                "capability_id or workflow_stage."
            ),
            "arguments": {"schema_name": "process_ir_authoring"},
        },
        {
            "tool": "list_boomi_profiles",
            "purpose": "List credential profiles; pass profile= to every account-scoped call.",
            "arguments": {},
        },
    ]
    query_args: Dict[str, Any] = {"action": "list"}
    if profile:
        query_args["profile"] = profile
    steps.append({
        "tool": "query_components",
        "purpose": (
            "Resolve the connection, operation, map, profile and cache "
            "components the ProcessIR document will reference by opaque id."
        ),
        "arguments": query_args,
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


#: The one value ``authoring_mode`` accepts. A CLOSED set, not a free string:
#: this tool's whole contract is that it parses no free text, and an open mode
#: argument would be free text wearing a parameter's clothes.
_PLAN_AUTHORING_MODES: Tuple[str, ...] = ("process_ir",)


def _plan_process_ir_block() -> Dict[str, Any]:
    """The three direct-ProcessIR fields, all derived from the served contract.

    Bounded on purpose. ``supported_process_ir_constructs`` is the node
    vocabulary with a contract id per kind — enough to choose constructs and
    know where to read more — not the entries themselves; those stay behind
    ``get_schema_template``, which is what makes the retrieval budgeted rather
    than dumped into every advisory response.

    Gaps carry the AUTHORITY'S state verbatim, and the published apply refusal
    is one of them: a caller must be able to see, before authoring, that a
    direct ProcessIR intent plans and compiles but never applies.
    """
    from ..authoring.contract import AUTHORING_CAPABILITY_REGISTRY
    from ..authoring.process_ir_projection import build_process_ir_authoring_entries

    entries = build_process_ir_authoring_entries()
    # `state` is the field the plan asked for and the one a caller acts on:
    # "may I author this construct?". `category` answers a different question
    # and was published in its place. Both ship now — a node entry carries no
    # capability state of its own, so it is `supported` unless a capability row
    # says otherwise, and that derivation is stated rather than assumed.
    gated_by_capability = {
        related.split(".", 1)[1]: entry.canonical_state
        for entry in entries
        if entry.entry_type == "capability" and entry.canonical_state != "supported"
        for related in (entry.contract_entry_id,)
    }
    constructs = []
    for entry in entries:
        if entry.entry_type != "node":
            continue
        # WITH IDENTITY. `["gated","gated","unsupported"]` named nothing and
        # repeated itself, so a caller could not tell which capability was
        # gated — while `process_ir_capability_gaps` in the same payload
        # carries ids. Same shape here.
        blocking = sorted(
            (
                {
                    "capability_id": capability,
                    "state": gated_by_capability[capability],
                    "contract_entry_id": related,
                }
                for related in entry.related_entry_ids
                if related.startswith("capability.")
                for capability in (related.split(".", 1)[1],)
                if capability in gated_by_capability
            ),
            key=lambda row: row["capability_id"],
        )
        constructs.append(
            {
                "kind": entry.subject,
                # A node is authorable; where a capability it depends on is not
                # yet available, the strictest of those states is surfaced.
                "state": "supported",
                "category": entry.category,
                "related_capability_gaps": blocking,
                "contract_entry_id": entry.contract_entry_id,
            }
        )
    gaps = [
        {
            "capability_id": entry.capability_id,
            "state": entry.canonical_state,
            "source_state": entry.source_state,
            "contract_entry_id": entry.contract_entry_id,
        }
        for entry in entries
        if entry.entry_type == "capability" and entry.canonical_state != "supported"
    ]
    # #153 (M12.15): published as a GAP only while it IS one. The state is read
    # from the registry — it always was — but the row used to be appended
    # unconditionally, so a supported capability would have been advertised in a
    # list whose every other member is unsupported. The condition is what makes
    # the list mean what its name says.
    materialization = AUTHORING_CAPABILITY_REGISTRY.get(
        "authoring.typed_apply.process_materialization"
    )
    if materialization is not None and materialization[0] != "supported":
        gaps.append(
            {
                "capability_id": "authoring.typed_apply.process_materialization",
                "state": materialization[0],
                "source_state": materialization[0],
                "contract_entry_id": None,
                "note": (
                    "A direct ProcessIR intent is plan/compile-only: nothing on a "
                    "production path materializes a ProcessIR root, so apply is "
                    "refused rather than silently producing a different artifact."
                ),
            }
        )
    return {
        "supported_process_ir_constructs": constructs,
        "process_ir_capability_gaps": gaps,
        "authoring_contract_query": {
            "tool": "get_schema_template",
            "schema_name": "process_ir_authoring",
            "filters": [
                "authoring_entry_id",
                "node_kind",
                "category",
                "capability_id",
                "workflow_stage",
                "after_entry_id",
                "limit",
            ],
            "hint": (
                "A bare call returns the facets with zero entries. Filter by "
                "node_kind while authoring, and by the diagnostic code's entry id "
                "while repairing."
            ),
        },
    }


def plan_integration_design_action(
    archetype: Optional[str] = None,
    intent_flags: Optional[list] = None,
    profile: Optional[str] = None,
    authoring_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Deterministically assemble a budgeted integration design brief.

    A read-only JOIN over the recipe registry, ``design_doctrine`` (#86), and
    ``account_governance`` (#93), plus a static discovery catalog. Parses no free
    text, calls no LLM/Boomi, infers no archetype.

    THREE modes now (#146 amendment). The first two are keyed on whether an
    ``archetype`` is supplied (full brief vs pre-selection brief) and are
    unchanged. The third, ``authoring_mode="process_ir"``, is the direct
    ProcessIR pre-selection brief: a caller who intends to author ProcessIR
    directly is no longer told the archetype is a missing input, because for
    them it is not one.

    ``authoring_mode`` must be requested EXPLICITLY. It is never inferred from
    ``intent_flags`` — flags are relevance hints, and letting a hint switch a
    response mode is how a caller ends up with a brief they did not ask for.
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

    # --- Validate authoring_mode (#146 amendment) ----------------------------
    process_ir_mode = False
    if authoring_mode is not None and str(authoring_mode).strip() != "":
        requested = str(authoring_mode).strip()
        if requested not in _PLAN_AUTHORING_MODES:
            return _plan_error_envelope(
                f"Invalid authoring_mode {requested!r}: expected one of "
                f"{list(_PLAN_AUTHORING_MODES)}.",
                INVALID_INPUT, archetype, flags, profile,
            )
        if archetype_provided:
            # Not a merge of two briefs. An archetype brief answers "configure
            # THIS pattern"; a direct-ProcessIR brief answers "author a process
            # from the node vocabulary". Silently preferring one would give the
            # caller a brief for a question they did not ask.
            return _plan_error_envelope(
                "authoring_mode='process_ir' plans direct ProcessIR authoring and "
                "takes no archetype. Omit the archetype, or drop authoring_mode "
                "for the archetype brief.",
                INVALID_INPUT, archetype, flags, profile,
            )
        process_ir_mode = True

    keywords = _plan_intent_keywords(flags)

    # --- Recipe mode: resolve + schema-derived decisions ---------------------
    # #159 S3: `archetype` names an executable recipe_id (the archetype pattern
    # registry retired from every served surface). The brief serves the same
    # recipe_descriptor_projection get_integration_archetype serves.
    schema_decisions: list = []
    recipe_projection: Optional[Dict[str, Any]] = None
    descriptor = None
    if archetype_provided:
        from ..recipes import RecipeError, production_registry
        from .integration_authoring import (
            list_integration_archetypes_action,
            recipe_descriptor_projection,
        )

        listed = list_integration_archetypes_action()
        valid = [row["recipe_id"] for row in listed.get("recipes", ())]
        try:
            registry = production_registry()
            descriptor = registry.resolve(archetype.strip())
        except RecipeError as exc:
            return _plan_error_envelope(
                "Unknown recipe_id: pass one returned by "
                "list_integration_archetypes().",
                exc.primary_code, archetype, flags, profile, valid_archetypes=valid,
            )
        if descriptor.recipe_id not in valid:
            return _plan_error_envelope(
                "Not an executable recipe: pass a recipe_id returned by "
                "list_integration_archetypes().",
                INVALID_INPUT, archetype, flags, profile, valid_archetypes=valid,
            )
        recipe_projection = recipe_descriptor_projection(registry, descriptor)
        keywords |= _plan_recipe_keywords(recipe_projection["descriptor"])
        schema_decisions = _plan_schema_decisions(
            recipe_projection.get("input_schema") or {}
        )

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
    elif process_ir_mode:
        # The whole point of the mode: an archetype is NOT a missing input for a
        # caller who is authoring ProcessIR directly, so it is neither reported
        # as missing nor offered as a decision, and archetype discovery is
        # replaced by contract discovery.
        mode = "process_ir_pre_selection"
        missing_inputs = []
        required_user_decisions = _plan_doctrine_decisions(selected_doctrine)
        discovery_steps = _plan_process_ir_discovery_steps(profile)
        notes.append(
            "Direct ProcessIR brief: no archetype is required. Author a "
            "ProcessIRV1 document against get_schema_template("
            "schema_name='ProcessIRV1'), consult "
            "get_schema_template(schema_name='process_ir_authoring') for the "
            "behavioural rules, then plan and compile."
        )
        # #153 (§6 review AR1-09): this note used to say "plan and compile
        # only … never applied", which was true before the canonical apply
        # cutover and became served guidance contradicting a supported
        # capability the moment it shipped.
        notes.append(
            "A direct ProcessIR intent is fully appliable: "
            "'authoring.typed_apply.process_materialization' is a supported "
            "capability, so the workflow is plan -> compile -> "
            "build_integration(action='apply') with a mutation attestation and "
            "a separate live-readback attestation recorded per applied root."
        )
    else:
        mode = "pre_selection"
        missing_inputs = ["archetype"]
        required_user_decisions = [{
            "field": None,
            "description": "select an archetype: a recipe_id from list_integration_archetypes()",
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

    # --- Recommended recipes (issue #145) ------------------------------------
    # EXACT references and nothing else. Doctrine text is never parsed, and no
    # contribution, patch, or input payload is returned here: an advisory surface
    # that handed back executable material would be the exact "prose becomes
    # executable" failure this issue exists to prevent. Only an archetype that
    # actually routes through a migrated recipe gets a reference — an unmigrated
    # one is reported honestly by its absence.
    recommended_recipes: list = []
    if descriptor is not None:
        recommended_recipes = [
            {
                "recipe_id": descriptor.recipe_id,
                "recipe_version": descriptor.recipe_version,
                "entry_kind": descriptor.entry_kind,
            }
        ]

    # --- Typed hand-off (issue #146) -----------------------------------------
    # ADVISORY ONLY. This names the exact schema selector and action a caller
    # should use next; it does NOT build an AuthoringRequestV1, because this
    # tool's whole contract is that prose is never turned into executable
    # intent. Deterministically ordered, and additive: nothing here is in the
    # output schema's `required` list, so existing callers are unaffected.
    try:
        from ..authoring.contract import get_authoring_revisions

        revision_binding = get_authoring_revisions()
    except Exception:  # noqa: BLE001 — advisory block, never fatal
        revision_binding = {"status": "unavailable"}

    typed_next_steps = [
        {
            "order": 1,
            "tool": "get_schema_template",
            "action": None,
            "schema_selector": "AuthoringRequestV1",
            "why": "Fetch the exact strict schema for typed authoring intent.",
        },
        {
            "order": 2,
            "tool": "build_integration",
            "action": "plan",
            "schema_selector": "AuthoringPlanResultV1",
            "why": (
                "Validate the typed intent and preview its IntegrationSpecV1 "
                "ComponentPlan. Performs zero remote mutation."
            ),
        },
        {
            "order": 3,
            "tool": "build_integration",
            "action": "compile",
            "schema_selector": "AuthoringCompileResultV1",
            "why": (
                "Canonically compile and obtain the compile hash to bind apply "
                "to. Performs zero remote mutation."
            ),
        },
        {
            "order": 4,
            "tool": "build_integration",
            "action": "apply",
            "schema_selector": "AuthoringRevisionBindingV1",
            "why": (
                "Materialize, carrying expected_capability_revision and "
                "expected_compile_hash. This is the first mutating phase."
            ),
        },
    ]

    # --- Direct-ProcessIR additions (#146 amendment) -------------------------
    # All THREE fields are optional in the output schema and absent from every
    # other mode, so nothing an existing caller reads changes shape.
    process_ir_block: Dict[str, Any] = {}
    if process_ir_mode:
        # #153 (M12.15): apply is a REAL phase for a direct ProcessIR intent, so
        # it is no longer stripped from the sequence and compile no longer
        # describes itself as a dead end. The compile hash is a binding to spend
        # again, which is exactly what it was always for.
        #
        # The filtering that used to happen here is DELETED rather than inverted
        # to a condition: a conditional that re-added `apply` would need to know
        # when materialization is supported, which is a fact the capability
        # registry already owns and the surrounding sequence already reflects.
        process_ir_block = _plan_process_ir_block()

    return {
        "_success": True,
        "tool": "plan_integration_design",
        "mode": mode,
        **process_ir_block,
        "revision_binding": revision_binding,
        "typed_next_steps": typed_next_steps,
        "recommended_recipes": recommended_recipes,
        **({"recipe": recipe_projection} if recipe_projection is not None else {}),
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



def _monitoring_actions():
    """The monitoring router's action names, read from its dispatch table.

    Imported lazily: this module is large and imported early, and the catalog only
    needs the names when it is actually asked to enumerate. The dependency points
    one way — the monitoring router knows nothing about this catalog.
    """
    from boomi_mcp.categories.monitoring import _MONITORING_ACTIONS

    return list(_MONITORING_ACTIONS)

def list_capabilities_action(
    available_tools: set = None,
    expected_capability_revision: str = None,
) -> Dict[str, Any]:
    """Return full catalog of MCP tools, actions, and workflows.

    Zero API calls — returns static metadata about this MCP server.

    Args:
        available_tools: Optional set of tool names from the live FastMCP registry.
            When provided, the returned catalog is filtered to only tools actually
            registered in the current runtime (e.g., local-only credential tools
            are excluded in production mode).
        expected_capability_revision: Optional revision a caller believes this
            server is running (issue #146). Supplying it turns
            ``authoring_contract.capability_comparison`` into a real comparison;
            omitting it yields ``not_requested`` — never a claim of parity the
            caller did not ask for. The live catalog is returned either way, so a
            mismatch is diagnosable rather than merely reported.
    """

    tools = {
        # === Category 1: Components (4 tools) ===
        "query_components": {
            "category": "Components",
            "description": "Query Boomi components — all read operations",
            "actions": list(_query_components_actions()),
            "read_only": True,
            "parameters": {
                "profile": "str (required) — Boomi profile name",
                "action": "str (required) — "
                          + " | ".join(_query_components_actions()),
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
                "config": "JSON str (optional) — action-specific config (XML for create, fields for update)."
                          "#188: every process body written is laid out with Boomi's own Arrange (Auto Format) layout; root key 'layout' = 'arrange' (default) | 'preserve' (keep coordinates); results carry layout {applied, mode, reason, columns, rows, estimated_extent, warnings}; PROCESS_LAYOUT_* codes refuse before any write. Metadata-only updates never move shapes.",
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
                "config": "JSON str (optional) — action-specific config. where_used/dependencies type filter: DEPENDENT or INDEPENDENT."
                          " merge: #188: every process body written is laid out with Boomi's own Arrange (Auto Format) layout; root key 'layout' = 'arrange' (default) | 'preserve' (keep coordinates); results carry layout {applied, mode, reason, columns, rows, estimated_extent, warnings}; PROCESS_LAYOUT_* codes refuse before any write. The merge diff shows the submitted bytes.",
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
                "patch": "JSON str (required) — {\"component_type\"?, \"config\": {...}, \"map_context\"?, \"layout\"?}; metadata config = partial edit, body config = full builder config; raw config.xml is rejected."
                         " #188: every process body written is laid out with Boomi's own Arrange (Auto Format) layout; patch.layout = 'arrange' (default) | 'preserve' (keep coordinates); results carry layout {applied, mode, reason, columns, rows, estimated_extent, warnings}; PROCESS_LAYOUT_* codes refuse before any write. The diff and token cover the arranged bytes; metadata patches never move shapes."
                         " #159: {\"authoring_request\": {...}} (alone) is the canonical process update — one process_ir unit, action='update', envelope.component_id = component_id; supporting components bound by explicit id, never written (see get_schema_template(resource_type='component', operation='safe_edit')).",
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
                "patch": "JSON str (required) — the same patch previewed by prepare_component_edit (#188: apply writes exactly the previewed bytes; a pre-#188 token or a changed patch.layout is refused, COMPONENT_EDIT_TOKEN_INVALID; #159: a canonical patch.authoring_request is recompiled and its dependencies re-read, and any change since prepare refuses with COMPONENT_EDIT_DRIFT_DETECTED)",
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
                "config": "JSON str (optional) — action-specific config/filters."
                          " create/update with config.xml of a process component: #188: every process body written is laid out with Boomi's own Arrange (Auto Format) layout; root key 'layout' = 'arrange' (default) | 'preserve' (keep coordinates); results carry layout {applied, mode, reason, columns, rows, estimated_extent, warnings}; PROCESS_LAYOUT_* codes refuse before any write.",
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
            "description": "Read-only process component inspection (list/get). Process authoring is typed: use build_integration with config.authoring_request (a recipe from list_integration_archetypes() or ProcessIR units).",
            "actions": ["list", "get"],
            "read_only": True,
            "parameters": {
                # #146 amendment: advertised, not only accepted.
                "config": "str (optional) — JSON string; reserved for future process actions, ignored by list/get",
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
        # === Category 4a: Integration Authoring (recipe discovery — #159 S3; no Boomi mutation) ===
        "list_integration_archetypes": {
            "category": "Integration Authoring",
            "description": "List the default executable recipes from the typed recipe registry (one descriptor per recipe_id with its exact recipe_version), plus the registry snapshot and an optional skew comparison. Read-only, no Boomi mutation.",
            "actions": ["(single action — lists executable recipe descriptors)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                # #146 amendment: advertised, not only accepted.
                "expected_recipe_registry": "dict (optional) — the recipe-registry snapshot OBJECT you expect (the wrapper declares an object; a JSON string is rejected by schema validation before the action runs); supplying it turns recipe_registry_skew into a real comparison instead of not_requested",
                "query": "str (optional) — case-insensitive substring filter over recipe_id",
            },
            "examples": [
                'list_integration_archetypes()',
                'list_integration_archetypes(query="listener")',
            ],
        },
        "get_integration_archetype": {
            "category": "Integration Authoring",
            "description": "Get one executable recipe by recipe_id (and optional exact recipe_version): its descriptor, input_schema and, where declared, component_material_schema — the same projection as get_schema_template(schema_name='recipe:<id>@<version>'). next_tool is build_integration. Read-only, no Boomi mutation.",
            "actions": ["(single action — returns the recipe descriptor projection)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                # #146 amendment: advertised, not only accepted.
                "recipe_version": "str (optional) — EXACT version to pin; omit for the recipe's default. An empty, unknown or retired version is RECIPE_VERSION_UNAVAILABLE",
                "name": "str (required) — recipe_id from list_integration_archetypes()",
            },
            "examples": [
                'get_integration_archetype(name="boomi.archetype.stub_minimal")',
                'get_integration_archetype(name="boomi.compose.db_rest_fanout", recipe_version="2.0.0")',
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
        "import_integration_draft": {
            "category": "Integration Authoring",
            "description": (
                "Read-only MIGRATION DISCOVERY (issue #48): convert a structured "
                "migration description or source-tool export summary into a semantic "
                "pipeline_draft (PipelineSpec vocabulary), the closest EXISTING "
                "preset with derived preset_parameters, and — only when zero blocking gaps "
                "remain — a canonical authoring_request_draft (an AuthoringRequestV1: one "
                "pinned typed-recipe invocation, or one direct ProcessIR unit for REST -> "
                "REST) for build_integration(action='plan', config={'authoring_request': "
                "...}). Never calls Boomi or reads credentials. "
                "Response keys: pipeline_draft, selected_preset, selected_recipe_ref, "
                "preset_parameters, authoring_request_draft, confirmed_facts, "
                "inferred_assumptions, gaps, ready_for_build, next_steps. Blocking gaps "
                "suppress the draft; "
                "product/version identifiers stay input_provenance only (no preset forks); "
                "literal profile UUIDs require a #95 index_profile_component field index."
            ),
            "actions": [
                "generic_integration_description",
                "source_tool_export_summary",
            ],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "source_type": "str (required) — generic_integration_description | source_tool_export_summary",
                "artifact": "dict or JSON-object str (required) — the structured migration description (free text rejected)",
                "options": "dict or JSON str (optional) — {integration_name, component_prefix}",
            },
            "examples": [
                "import_integration_draft(source_type=\"generic_integration_description\", artifact={\"source\": {\"protocol\": \"rest\", ...}, \"target\": {...}, \"mappings\": [...]})",
                "import_integration_draft(source_type=\"source_tool_export_summary\", artifact={\"product\": \"<tool>\", \"flow\": {...}}) → drafts + structured gaps",
            ],
        },
        "suggest_connection_reuse": {
            "category": "Integration Authoring",
            "description": (
                "Read-only CONNECTION-REUSE DISCOVERY (issue #83, M7.3): finds and "
                "ranks EXISTING connector-settings components for safe reuse so you "
                "wire a reused secured connection — keeping credentials out of the "
                "conversation — instead of authoring a new one. Ranks by connector "
                "subtype match, endpoint/host hint match, folder locality, and name "
                "similarity. Reads component XML only to extract NON-secret endpoint "
                "context; never returns credential material (encrypted values, "
                "usernames, passwords, OAuth/tokens stay opaque) and never mutates "
                "Boomi. Each candidate carries IntegrationSpecV1-compatible reuse "
                "bindings — a reference_only config (by component_id) plus an "
                "exact-name fallback paired with conflict_policy='reuse'. Response "
                "keys: candidates[] (component_id, name, folder, subtype, score, "
                "why_matched, safe_context, paired_actions, reference), "
                "total_matched, candidates_scanned, enrichment_capped."
            ),
            "actions": ["(single action — ranks reusable connections)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "profile": "str (required) — Boomi profile name",
                "connector_type": "str (required) — connector family/subtype: 'database' | 'rest' | 'rest_client' | 'soap_client' | a raw Boomi subType",
                "purpose": "str (optional) — free-text intent for folder-locality and name-similarity ranking",
                "endpoint_hint": "str (optional) — host or base URL to rank by endpoint match",
                "top_k": "int (optional) — max candidates (clamped 1..25; default 5)",
            },
            "examples": [
                "suggest_connection_reuse(profile=\"prod\", connector_type=\"database\", purpose=\"orders warehouse\", endpoint_hint=\"db.prod.acme.com\")",
                "suggest_connection_reuse(profile=\"prod\", connector_type=\"rest\", endpoint_hint=\"https://api.acme.com\") → reference_only + conflict_policy='reuse' bindings",
            ],
        },
        "search_marketplace_recipes": {
            "category": "Integration Authoring",
            "description": (
                "Read-only MARKETPLACE RECIPE SEARCH (issue #84, M7.4): queries "
                "Boomi's PUBLIC, UNAUTHENTICATED GraphQL catalog (POST "
                "platform.boomi.com/graphql) and returns PUBLISHED Recipe listings "
                "as reference patterns — no profile, no credentials, no Boomi "
                "mutation, and NO install path (install is deliberately deferred). "
                "Each result: slug, name, description, tags[{id,name,category_code}], "
                "install_count, artifact_source_id. Recipes are reference patterns "
                "to review and adapt, not installable/production-ready components "
                "here. Response keys: recipes[], total_count, returned_count. "
                "Endpoint or GraphQL failures return a structured "
                "MARKETPLACE_GRAPHQL_UNAVAILABLE envelope (recipes=[])."
            ),
            "actions": ["(single action — searches published Marketplace recipes)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "query": "str (optional) — free-text search term (GraphQL searchTerm)",
                "tags": "list[str] (optional) — solution tag names (any-match; the Recipe asset-type tag is always required)",
                "top_k": "int (optional) — max recipes (clamped 1..25; default 10; first page only)",
            },
            "examples": [
                "search_marketplace_recipes(query=\"salesforce order sync\")",
                "search_marketplace_recipes(tags=[\"Salesforce\", \"ServiceNow\"], top_k=5)",
            ],
        },
        "index_profile_component": {
            "category": "Integration Authoring",
            "description": (
                "Read-only EXISTING-PROFILE INDEX DISCOVERY (issue #95, M7.5): "
                "fetches a live profile component (profile.json / profile.xml / "
                "profile.db) and parses its exported XML into the canonical "
                "field_index_by_path — platform keys, key paths, name paths, "
                "mappable flags, and profile_component_type — so a transform.map "
                "that references a LITERAL existing-profile UUID can be validated "
                "against real fields. Structural containers are reported "
                "non-mappable (rejected as map endpoints before mutation). The "
                "default response is the normalized index, NOT raw XML "
                "(include_raw_xml=true to expose it). Never mutates. Supply the "
                "returned index to build_integration via "
                "profile_indexes_by_component_id, or let build_integration "
                "discover it. Separate surface from infer_profile_fields (which "
                "stays artifact-based and never calls Boomi)."
            ),
            "actions": ["(single action — indexes one profile component)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "profile": "str (required) — Boomi profile name",
                "component_id": "str (required) — UUID of a profile.json / profile.xml / profile.db component",
                "include_raw_xml": "bool (optional) — also return exported XML under raw_xml (default false)",
            },
            "examples": [
                "index_profile_component(profile=\"prod\", component_id=\"<profile-uuid>\")",
                "index_profile_component(profile=\"prod\", component_id=\"<profile-uuid>\", include_raw_xml=True)",
            ],
        },
        "discover_openapi_spec": {
            "category": "Integration Authoring",
            "description": (
                "Read-only OPENAPI/SWAGGER DISCOVERY (issue #13, M7): parses an "
                "OpenAPI 2.0 (Swagger) or 3.x JSON spec into a bounded summary — "
                "operations[] (path, method, parameters, request/response schemas), "
                "schemas[], and sanitized servers[] — so the LLM can inspect an API "
                "before filling archetype parameters. No profile, no credentials, "
                "no Boomi mutation; the raw spec is never echoed. Supply EXACTLY "
                "ONE of spec_url or artifact. JSON only — YAML returns "
                "OPENAPI_UNSUPPORTED_FORMAT. URL mode is SSRF-guarded, sends no "
                "auth/cookies and follows no redirects (401/403 -> "
                "OPENAPI_AUTH_FAILURE; download private specs and pass via "
                "artifact); external $refs are never fetched. Response keys: "
                "operations[], schemas[], servers[], counts, truncated, warnings."
            ),
            "actions": ["(single action — summarizes an OpenAPI/Swagger spec)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "spec_url": "str (optional) — HTTP(S) URL of a JSON OpenAPI document (URL mode)",
                "artifact": "dict | str (optional) — a parsed OpenAPI dict or JSON string (artifact mode); supply exactly one of spec_url/artifact",
                "options": "dict | str (optional) — max_input_chars, max_nodes, max_fields (clamped to hard caps)",
            },
            "examples": [
                "discover_openapi_spec(spec_url=\"https://<host>/openapi.json\")",
                "discover_openapi_spec(artifact=\"{\\\"openapi\\\": \\\"3.0.0\\\", ...}\")",
            ],
        },
        "discover_soap_wsdl": {
            "category": "Integration Authoring",
            "description": (
                "Read-only WSDL DISCOVERY (issue #13, M7): parses a WSDL 1.1 "
                "document (SOAP 1.1 and 1.2 bindings) into a bounded summary — "
                "services[]/ports, bindings[] with operations (soapAction, "
                "input/output/fault messages), messages[]/parts, and reported "
                "imports[] (fetched=false, never retrieved). No profile, no "
                "credentials, no Boomi mutation; the raw WSDL is never echoed and "
                "DOCTYPE/ENTITY is rejected (XXE-safe). Supply EXACTLY ONE of "
                "wsdl_url or artifact. URL mode is SSRF-guarded, sends no "
                "auth/cookies and follows no redirects (401/403 -> "
                "WSDL_AUTH_FAILURE; download private WSDLs and pass via artifact). "
                "Response keys: services[], bindings[], messages[], imports[], "
                "counts, truncated, warnings."
            ),
            "actions": ["(single action — summarizes a WSDL 1.1 document)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "wsdl_url": "str (optional) — HTTP(S) URL of a WSDL document (URL mode)",
                "artifact": "str (optional) — the WSDL XML text (artifact mode); supply exactly one of wsdl_url/artifact",
                "options": "dict | str (optional) — max_input_chars, max_nodes, max_fields (clamped to hard caps)",
            },
            "examples": [
                "discover_soap_wsdl(wsdl_url=\"https://<host>/service?wsdl\")",
                "discover_soap_wsdl(artifact=\"<definitions ...>...</definitions>\")",
            ],
        },
        "discover_odata_metadata": {
            "category": "Integration Authoring",
            "description": (
                "Read-only ODATA METADATA DISCOVERY (issue #13, M7): fetches and "
                "parses an OData v2 or v4 EDMX $metadata document into a bounded "
                "summary — schemas[], entity_types[] (keys, properties, "
                "navigation), and entity_sets[] (with v4 navigation bindings). No "
                "profile, no credentials, no Boomi mutation; the raw metadata is "
                "never echoed and DOCTYPE/ENTITY is rejected (XXE-safe). URL-only: "
                "fetches the EXACT metadata_url (no /$metadata inference). "
                "SSRF-guarded, sends no auth/cookies and follows no redirects "
                "(401/403 -> ODATA_AUTH_FAILURE; protected endpoints are out of "
                "scope — no auth-forwarding). Response keys: entity_types[], "
                "entity_sets[], schemas[], counts, truncated, warnings."
            ),
            "actions": ["(single action — summarizes an OData EDMX $metadata document)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "metadata_url": "str (required) — HTTP(S) URL of the OData EDMX $metadata document",
                "options": "dict | str (optional) — max_input_chars, max_nodes, max_fields (clamped to hard caps)",
            },
            "examples": [
                "discover_odata_metadata(metadata_url=\"https://<host>/service/$metadata\")",
            ],
        },
        "discover_db_schema": {
            "category": "Integration Authoring",
            "description": (
                "Read-only DB SCHEMA DISCOVERY (issue #13, M7): summarizes a "
                "normalized information-schema JSON artifact into a bounded "
                "relational topology — tables[] with columns, primary/foreign-key "
                "constraints, and indexes. Artifact-only: it NEVER opens a "
                "JDBC/network connection, calls Boomi, or reads credentials. "
                "Actual column default VALUES are never returned (only "
                "default_present) to avoid leaking secrets/customer data. "
                "Discovery-only, NOT builder-ready — to turn a single "
                "query/result column list into a Boomi profile use "
                "infer_profile_fields(source_type='profile_from_db_metadata') "
                "instead; this tool never calls it, creates a profile, picks a "
                "table, or emits SQL. Response keys: tables[] "
                "(columns/constraints/indexes), counts, truncated, warnings."
            ),
            "actions": ["(single action — summarizes a DB information-schema artifact)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "artifact": "dict | str (required) — normalized information-schema snapshot (or JSON-object string); requires a non-empty columns[] list, tables[] optional (derived when omitted)",
                "options": "dict | str (optional) — max_input_chars, max_nodes, max_fields (clamped to hard caps)",
            },
            "examples": [
                "discover_db_schema(artifact={\"columns\": [{\"table_name\": \"orders\", \"column_name\": \"id\", \"data_type\": \"int\"}]})",
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
            # Sourced from the ONE contract registry (issue #146) so this
            # catalog, the dispatcher, the workflow schema and the wrapper
            # docstring cannot advertise different action lists — which is the
            # class of drift this milestone exists to close.
            "actions": list(_authoring_actions()),
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — plan | compile | apply | verify",
                "config": (
                    "JSON str (optional) — IntegrationSpecV1 payload and execution "
                    "options. Issue #146 adds ONE opt-in typed key at the config "
                    "root: 'authoring_request' (an AuthoringRequestV1 — get its "
                    "exact schema from "
                    "get_schema_template(schema_name='AuthoringRequestV1')). The "
                    "binding values 'expected_capability_revision', "
                    "'expected_plan_hash' and 'expected_compile_hash' are fields "
                    "INSIDE that object, not siblings of it — placed at the config "
                    "root they are ignored and a typed apply fails with "
                    "AUTHORING_APPLY_VALIDATION_REQUIRED. A typed apply REQUIRES "
                    "expected_capability_revision and expected_compile_hash inside "
                    "authoring_request; legacy requests need none of them and are "
                    "unchanged. Issue #157: governance (name or component_prefix, "
                    "description, folder_name fan-out, process_extensions, watermark, "
                    "recorded_intents) is authored on each unit's envelope and "
                    "resolved before fingerprinting; recipe intents take "
                    "process_envelopes; flows is derived-only on both compiling "
                    "intents. #188 adds one write-time control at the config root, "
                    "'layout' = 'arrange' (default) | 'preserve': consumed before any "
                    "action (never part of a plan, fingerprint or compile hash), it lays "
                    "out every process apply writes with Boomi's own Arrange layout and "
                    "is reported per process as results.<key>.layout. "
                    "PROCESS_LAYOUT_MODE_INVALID refuses the whole call before any write; "
                    "the other PROCESS_LAYOUT_* codes refuse that one process before it is "
                    "submitted, while components written earlier in the same apply stay "
                    "accounted for in partial_results (read mutation_status)."
                ),
            },
            "authority_versions": _AUTHORITY_VERSIONS,
            "examples": [
                'build_integration(profile="prod", action="plan", config=\'{"authoring_request":{"contract_version":"2","intent":{"intent_kind":"recipe","integration_name":"Order Sync","invocations":[{"recipe_id":"<recipe_id from list_integration_archetypes>","recipe_version":"<exact version>","invocation_id":"main","raw_input":{...},"component_material":{...}}]}}}\')',
                'build_integration(profile="prod", action="compile", config=\'{"authoring_request":{...same request...}}\')',
                'build_integration(profile="prod", action="apply", config=\'{"dry_run":false,"authoring_request":{...same request..., "expected_capability_revision":"sha256:...","expected_compile_hash":"sha256:..."}}\')',
                'build_integration(profile="prod", action="verify", config=\'{"build_id":"<uuid>"}\')',
                '# A flat process row only binds an existing process: {"key":"p1","type":"process","action":"create","config":{"reference_only":true,"component_id":"<existing-process-uuid>"}}',
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
            # DERIVED from the router's dispatch table. This was a hand-written
            # copy, and it went stale the moment an action was added — which is
            # exactly how it was found: a slice added `execution_connectors`,
            # swept the copy in the router's own error envelope, and missed this
            # one, so the discovery surface silently regressed from complete to
            # incomplete while every test stayed green.
            "actions": list(_monitoring_actions()),
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
                "similarity and return top-ranked chunks with inline content. "
                "Hits may include supplemental companion_reference material, not "
                "only official documentation — every hit carries source_type and "
                "verification_status."
            ),
            "actions": ["(single action — semantic docs search)"],
            "read_only": True,
            "parameters": {
                "query": "str (required) — factual Boomi docs question or search terms",
                "top_k": "int (optional) — number of chunks to return, capped by server config",
                "source_type": (
                    "str (optional) — opt-in filter restricting results to one "
                    "corpus: 'official' or 'companion_reference'. Leave unset "
                    "for normal searches; invalid values return "
                    "invalid_source_type, unindexed values return "
                    "source_type_unavailable"
                ),
            },
            "examples": [
                'search_boomi_docs(query="Agent step output process property", top_k=5)',
                'search_boomi_docs(query="Tracking Direction Input Documents Output Documents Process Reporting")',
                'search_boomi_docs(query="map function configuration", source_type="companion_reference")',
            ],
            "note": (
                "Use this before answering factual Boomi platform behavior, "
                "connector, configuration, deployment/runtime, scripting, EDI/API, "
                "or error-message questions. Reuse exact identifiers already "
                "supplied by the user, returned by a prior result, or exposed by "
                "the selected component or capability schema. Never introduce an "
                "identifier solely from this guidance. Honor each hit's "
                "verification_status: "
                "treat companion_unverified results as implementation context or a "
                "hypothesis, not as authoritative official documentation. After a "
                "scale-to-zero cold start the "
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
                "a search result indicates the page is relevant. Pages may be "
                "official docs or supplemental companion_reference material — "
                "every chunk carries source_type and verification_status."
            ),
            "actions": ["(single action — read page chunks)"],
            "read_only": True,
            "parameters": {
                "page_key": (
                    "str (required) — page_key returned by search_boomi_docs "
                    "(a help.boomi.com URL for official docs, or a companion://… "
                    "key for supplemental content)"
                ),
                "max_chunks": "int (optional) — number of chunks to return",
                "start_chunk_index": "int (optional) — pagination start index",
            },
            "examples": [
                'read_boomi_doc_page(page_key="https://help.boomi.com/docs/Atomsphere/Integration/Process%20building/int-Agent_step")',
                'read_boomi_doc_page(page_key="companion://OfficialBoomi/boomi-integration/references/components/map_component.md")',
            ],
            "note": (
                "Use after search_boomi_docs when surrounding page context is "
                "needed. Honor the chunks' verification_status: companion_unverified "
                "content is supplemental implementation context, not authoritative "
                "official documentation."
            ),
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
                               "'recipe:<recipe_id>@<version>' | 'workflow_sequences' | 'workflow:<name>' | "
                               "'design_doctrine' | 'design_pattern:<name>' | "
                               "'account_governance' | 'governance_pattern:<name>' | "
                               "'script_dataprocess' | 'script_mapping'. "
                               "'process_ir_authoring' (the #146 behavioural contract). "
                               "Takes precedence over resource_type.",
                # #146 amendment. The catalog is what a client ENUMERATES, so a
                # parameter documented only in the wrapper docstring is one
                # nobody discovers. All seven apply to 'process_ir_authoring'
                # and are INVALID_INPUT elsewhere.
                "authoring_entry_id": "str (optional) — exact process_ir_authoring entry id; an unknown id returns zero entries, not an error",
                "node_kind": "str (optional) — filter the authoring contract to one ProcessIR node kind",
                "category": "str (optional) — filter the authoring contract by entry domain",
                "capability_id": "str (optional) — filter the authoring contract to one capability row",
                "workflow_stage": "str (optional) — discover | plan | author | compile | repair",
                "after_entry_id": "str (optional) — pagination cursor; requires another filter",
                "limit": "int (optional) — 1-50, default 20; a 64 KiB entries budget applies on top",
            },
            "examples": [
                'get_schema_template(resource_type="trading_partner", operation="create", standard="x12")',
                'get_schema_template(schema_name="AuthoringRequestV1")',
                'get_schema_template(resource_type="integration", operation="plan")',
                'get_schema_template(resource_type="trading_partner", protocol="http")',
                'get_schema_template(schema_name="IntegrationSpecV1")',
                'get_schema_template(schema_name="recipe:boomi.archetype.database_to_api_sync@1.0.0")',
                'get_schema_template(schema_name="workflow_sequences")',
                'get_schema_template(schema_name="account_governance")',
                'get_schema_template(schema_name="governance_pattern:descriptive_unique_component_names")',
                'get_schema_template(schema_name="script_dataprocess")',
                'get_schema_template(schema_name="script_mapping")',
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
                "recipe registry, design_doctrine, and account_governance. "
                "Returns recommended patterns with capability_status, capability "
                "gaps, required user decisions (archetype mode), and discovery "
                "steps. Deterministic — no LLM, no Boomi, no free-text parsing."
            ),
            "actions": ["(single action — returns an assembled design brief)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "archetype": "str (optional) — a recipe_id from list_integration_archetypes() (e.g. boomi.archetype.database_to_api_sync); omit for the pre-selection brief",
                "intent_flags": "list[str] (optional) — short tokens like retry, dlq, incremental, bidirectional, notify (no free text)",
                "profile": "str (optional) — echoed into the suggested discovery-step arguments; no account call is made",
                "authoring_mode": "str (optional) — 'process_ir' returns the direct ProcessIR brief and requires no archetype; must be requested explicitly, intent_flags never selects it",
            },
            "examples": [
                'plan_integration_design(intent_flags=["incremental", "retry"])',
                'plan_integration_design(archetype="boomi.archetype.database_to_api_sync", intent_flags=["incremental", "dlq"])',
            ],
        },
        "list_capabilities": {
            "category": "Meta Tools",
            "description": "This tool — lists all available MCP tools and capabilities",
            "actions": ["(single action — returns full catalog)"],
            "read_only": True,
            "parameters": {
                # It was documented as taking none while accepting one. A
                # caller reading the catalog could not discover the only way to
                # get a real revision comparison instead of `not_requested`.
                "expected_capability_revision": "str (optional) — the revision you believe this server runs; supplying it turns authoring_contract.capability_comparison into a real match/mismatch instead of not_requested",
            },
            "note": "One optional parameter. Returns this catalog.",
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

    # Issue #83 (M7.3): the build_integration_from_description chain interposes a
    # suggest_connection_reuse step. Strip it here when that tool is NOT in the
    # live registry, BEFORE the available_tools filter runs — otherwise
    # _refs_in_steps would see the unregistered "5. suggest_connection_reuse("
    # reference and drop the whole core authoring workflow. When available_tools
    # is None the full chain is surfaced as-is. (A numbering gap after the strip
    # is acceptable — same precedent as the search_boomi_gotchas strip above.)
    if available_tools is not None and "suggest_connection_reuse" not in available_tools:
        bifd = workflows.get("build_integration_from_description")
        if bifd:
            bifd["steps"] = [
                s for s in bifd["steps"] if "suggest_connection_reuse(" not in s
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
        # Names the tool and its requirement rather than showing an empty call.
        # `get_schema_template()` with no argument returns SCHEMA_SELECTOR_REQUIRED,
        # so a reader who pasted the parenthesised form got an error; the guard
        # classifies a no-argument form as a MENTION, and this wording removes
        # the ambiguity instead of relying on that reading.
        "need_template": (
            "Call get_schema_template with a resource_type or schema_name before "
            "create/update operations"
        ),
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
            "list_integration_archetypes() (the executable recipe catalog) "
            "before authoring ProcessIR units directly in "
            "build_integration(config.authoring_request)."
        )
    # Issue #83 (M7.3): upgrade the generic reuse hint to name the discovery tool
    # only when it is actually registered — otherwise keep the tool-agnostic form.
    if available_tools is None or "suggest_connection_reuse" in available_tools:
        hints["reuse_connections"] = (
            "Prefer existing secured connection components before authoring new "
            "ones — call suggest_connection_reuse(profile=..., connector_type=...) "
            "to find and rank reusable candidates (returns reference_only / "
            "conflict_policy='reuse' bindings; credentials never leave Boomi)."
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
            "For component/integration creation, route through recipes: "
            "list_integration_archetypes → get_integration_archetype → "
            "build_integration(plan/compile/apply) with config.authoring_request "
            "(intent_kind='recipe'). Author ProcessIR units "
            "(intent_kind='process_ir') only when no recipe fits."
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
            "ones — reuse keeps credentials out of the conversation. Call "
            "suggest_connection_reuse(profile=..., connector_type=...) to find "
            "and rank reusable candidates; wire the returned reference_only / "
            "conflict_policy='reuse' binding instead of re-entering credentials."
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

    # --- Typed recipe registry (issue #145 — provenance + skew) ---
    # A live snapshot, not a text index: the whole point is that a caller can
    # compare what is RUNNING against what their checkout has, and a summary
    # they cannot hash would defeat that. Best-effort, like the doctrine blocks
    # above — a registry construction failure must not break the catalog.
    try:
        from ..recipes import production_registry

        recipe_registry = production_registry().snapshot()
    except Exception:  # noqa: BLE001 — the catalog is advisory here
        recipe_registry = {
            "schema_version": "1",
            "status": "unavailable",
        }

    # --- MCP authoring contract (issue #146 — revisions + honest drift) ---
    # The live service once reported four archetypes while the checkout had six,
    # and no client could tell. This block publishes the runtime's own archetype
    # list, served selectors, action set and revisions, so a caller can COMPARE
    # rather than assume. Same best-effort discipline as the blocks above.
    try:
        from ..authoring.contract import (
            build_authoring_contract_manifest,
            compare_capability_revision,
        )

        authoring_contract = dict(build_authoring_contract_manifest())
        authoring_contract["capability_comparison"] = compare_capability_revision(
            expected_capability_revision
        )
    except Exception:  # noqa: BLE001 — the catalog is advisory here
        authoring_contract = {"manifest_version": "1", "status": "unavailable"}

    return {
        "_success": True,
        "server_name": "Boomi MCP Server",
        "server_version": "1.3",
        "recipe_registry": recipe_registry,
        "authoring_contract": authoring_contract,
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
