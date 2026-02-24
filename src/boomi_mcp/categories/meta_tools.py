"""
Meta tools — schema templates and generic API invoker.

- get_schema_template_action: self-documenting reference data (no API calls)
- invoke_api: generic escape-hatch for any Boomi REST API endpoint
"""

from typing import Dict, Any, Optional

from boomi import Boomi
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment


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
    "available_actions": ["list", "get", "create", "update", "delete"],
    "config_format": "YAML (config_yaml parameter)",
    "shape_types": ["start", "stop", "return", "message", "map", "connector", "decision", "branch", "note", "documentproperties"],
    "hint": "Use operation='create' for a full YAML template",
}

_PROCESS_CREATE = {
    "resource_type": "process",
    "operation": "create",
    "single_process_template": (
        'name: "My Process Name"\n'
        'folder_name: "Home"\n'
        'description: "Optional description"\n'
        'shapes:\n'
        '  - type: start\n'
        '    name: start\n'
        '  - type: message\n'
        '    name: log_msg\n'
        '    config:\n'
        '      message_text: "Process started"\n'
        '  - type: map\n'
        '    name: transform\n'
        '    config:\n'
        '      map_id: "existing-map-component-id"\n'
        '  - type: connector\n'
        '    name: get_data\n'
        '    config:\n'
        '      connector_id: "connector-component-id"\n'
        '      operation: "Get"\n'
        '      object_type: "Object"\n'
        '  - type: decision\n'
        '    name: check_result\n'
        '    config:\n'
        '      expression: "document property equals value"\n'
        '  - type: branch\n'
        '    name: parallel_work\n'
        '    config:\n'
        '      num_branches: 2\n'
        '  - type: stop\n'
        '    name: end\n'
    ),
    "multi_component_template": (
        'components:\n'
        '  - name: "Transform Map"\n'
        '    type: map\n'
        '    dependencies: []\n'
        '  - name: "Main Process"\n'
        '    type: process\n'
        '    dependencies: ["Transform Map"]\n'
        '    config:\n'
        '      name: "Main Process"\n'
        '      shapes:\n'
        '        - type: start\n'
        '          name: start\n'
        '        - type: map\n'
        '          name: transform\n'
        '          config:\n'
        '            map_ref: "Transform Map"\n'
        '        - type: stop\n'
        '          name: end\n'
    ),
    "shape_reference": {
        "start": {"required": True, "position": "first", "config": "none"},
        "stop": {"position": "last", "config": {"continue_": "true|false"}},
        "return": {"position": "last", "config": {"label": "text"}},
        "message": {"config": {"message_text": "REQUIRED"}},
        "map": {"config": {"map_id": "existing map component ID", "map_ref": "name in multi-component YAML"}},
        "connector": {"config": {"connector_id": "REQUIRED", "operation": "Get|Send", "object_type": "REQUIRED"}},
        "decision": {"config": {"expression": "REQUIRED"}},
        "branch": {"config": {"num_branches": "REQUIRED (integer >= 2)"}},
        "note": {"config": {"note_text": "documentation text", "created_by": "author"}},
        "documentproperties": {"config": {}},
    },
    "process_level_attributes": {
        "allow_simultaneous": "false (default)",
        "enable_user_log": "false (default)",
        "process_log_on_error_only": "false (default)",
        "purge_data_immediately": "false (default)",
        "update_run_dates": "true (default)",
        "workload": "general | high | low (default: general)",
    },
}

_PROCESS_LIST = {
    "resource_type": "process",
    "operation": "list",
    "filters_param": "filters (JSON string)",
    "template": '{"folder_name": "Home"}',
    "available_filters": ["folder_name"],
}


# ============================================================================
# Component Templates
# ============================================================================

_COMPONENT_OVERVIEW = {
    "resource_type": "component",
    "tools": {
        "query_components": ["list", "get", "search", "bulk_get"],
        "manage_component": ["create", "update", "clone", "delete"],
        "analyze_component": ["where_used", "dependencies", "compare_versions"],
    },
    "component_types": [
        "process", "map", "connector", "profile", "certificate",
        "tradingpartner", "tpgroup", "tporganization",
        "xslt", "scriptprocessing", "scriptmapping", "crossref",
        "documentcache", "customlibrary", "flowservice",
        "webservice", "webserviceexternal",
        "connectoraction", "connectorsettings",
        "profiledb", "profileedi", "profileflatfile", "profilexml", "profilejson",
        "processproperty", "processroute", "queue",
    ],
}

_COMPONENT_CREATE = {
    "resource_type": "component",
    "operation": "create",
    "note": "Boomi's Component API requires type-specific XML. For processes, use manage_process with config_yaml instead.",
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
        "   OR for processes: use manage_process with config_yaml (YAML is simpler)",
    ],
}

_COMPONENT_SEARCH = {
    "resource_type": "component",
    "operation": "search",
    "tool": "query_components (action='search')",
    "template": {
        "name": "%partial_name% (LIKE pattern, use % wildcard)",
        "type": "process | connector | map | ...",
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
    "available_actions": ["execution_records", "execution_logs", "execution_artifacts", "audit_logs", "events"],
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
        "classification": "TEST | PRODUCTION",
    },
    "enums": {
        "classification": ["TEST", "PRODUCTION"],
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
        "version": "1.0.0 (optional, auto-generated if omitted)",
        "notes": "Release notes for this package",
        "branch": "main (optional, defaults to main branch)",
    },
    "sdk_pattern": "sdk.packaged_component.create_packaged_component(...)",
}

_PACKAGE_DEPLOY = {
    "resource_type": "package",
    "operation": "deploy",
    "template": {
        "package_id": "REQUIRED — ID of packaged component",
        "environment_id": "REQUIRED — target environment ID",
        "notes": "Deployment notes",
    },
    "sdk_pattern": "sdk.deployment.create_deployment(...)",
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

def invoke_api(
    boomi_client: Boomi,
    profile: str,
    endpoint: str,
    method: str = "GET",
    payload: str = None,
    content_type: str = "json",
    accept: str = "json",
    confirm_delete: bool = False,
) -> Dict[str, Any]:
    """Execute arbitrary Boomi API call using SDK's Serializer.

    Uses the same proven Serializer + send_request() pattern from _shared.py.
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

    # --- Build URL ---
    # All SDK services share the same base URL (includes accountId) + auth
    svc = boomi_client.account
    base = svc.base_url or Environment.DEFAULT.url
    url = f"{base.rstrip('/')}/{endpoint.lstrip('/')}"

    # --- Content types ---
    ct_map = {
        "json": "application/json",
        "xml":  "application/xml",
    }
    accept_header = ct_map.get(accept, "application/json")
    content_type_header = ct_map.get(content_type, "application/json")

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
    ser = Serializer(
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
        response, status, _ = svc.send_request(serialized)
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
    if isinstance(response, bytes):
        raw = response.decode("utf-8")
    elif isinstance(response, str):
        raw = response
    elif isinstance(response, dict):
        # SDK may return parsed dict directly for JSON responses
        return {
            "_success": 200 <= status < 300,
            "status_code": status,
            "method": method,
            "endpoint": endpoint,
            "url": url,
            "profile": profile,
            "data": response,
        }
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

    if truncated:
        result["truncated"] = True
        result["total_size"] = len(raw)

    if accept == "json":
        try:
            parsed = json_mod.loads(raw[:MAX_RESPONSE_SIZE] if truncated else raw)
            result["data"] = parsed
        except (json_mod.JSONDecodeError, TypeError):
            result["raw_response"] = raw[:MAX_RESPONSE_SIZE]
    else:
        result["raw_response"] = raw[:MAX_RESPONSE_SIZE]

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
    "organization", "monitoring",
]


def get_schema_template_action(
    resource_type: str,
    operation: Optional[str] = None,
    standard: Optional[str] = None,
    component_type: Optional[str] = None,
    protocol: Optional[str] = None,
) -> Dict[str, Any]:
    """Look up and return the appropriate template."""

    registry = {
        "trading_partner": _get_trading_partner_template,
        "process": _get_process_template,
        "component": _get_component_template,
        "environment": _get_environment_template,
        "package": _get_package_template,
        "execution_request": _get_execution_request_template,
        "organization": _get_organization_template,
        "monitoring": _get_monitoring_template,
    }

    handler = registry.get(resource_type)
    if not handler:
        return {
            "_success": False,
            "error": f"Unknown resource_type: {resource_type}",
            "valid_types": _VALID_RESOURCE_TYPES,
        }

    return handler(
        operation=operation,
        standard=standard,
        component_type=component_type,
        protocol=protocol,
    )


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
        "_success": True,
        "resource_type": "trading_partner",
        "operation": operation,
        "hint": f"See manage_trading_partner tool docstring for '{operation}' action details",
    }


def _get_process_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_PROCESS_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_PROCESS_CREATE}

    if operation == "list":
        return {"_success": True, **_PROCESS_LIST}

    return {
        "_success": True,
        "resource_type": "process",
        "operation": operation,
        "hint": f"See manage_process tool docstring for '{operation}' action details",
    }


def _get_component_template(operation=None, component_type=None, **_):
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
        result = {"_success": True, **_COMPONENT_CREATE}
        if component_type == "process":
            result["recommendation"] = "For processes, use manage_process with config_yaml instead of raw XML."
        return result

    if operation == "search":
        return {"_success": True, **_COMPONENT_SEARCH}

    if operation == "clone":
        return {"_success": True, **_COMPONENT_CLONE}

    if operation == "compare_versions":
        return {"_success": True, **_COMPONENT_COMPARE}

    return {
        "_success": True,
        "resource_type": "component",
        "operation": operation,
        "hint": f"See query_components or manage_component tool docstring for '{operation}' action details",
    }


def _get_environment_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_ENVIRONMENT_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_ENVIRONMENT_CREATE}

    return {
        "_success": True,
        "resource_type": "environment",
        "operation": operation,
        "hint": "Environment management is available via the Boomi SDK",
    }


def _get_package_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_PACKAGE_OVERVIEW}

    if operation == "create":
        return {"_success": True, **_PACKAGE_CREATE}

    if operation == "deploy":
        return {"_success": True, **_PACKAGE_DEPLOY}

    return {
        "_success": True,
        "resource_type": "package",
        "operation": operation,
        "hint": "See package/deployment SDK patterns",
    }


def _get_execution_request_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_EXECUTION_REQUEST_OVERVIEW}

    if operation == "execute":
        return {"_success": True, **_EXECUTION_REQUEST_EXECUTE}

    return {
        "_success": True,
        "resource_type": "execution_request",
        "operation": operation,
        "hint": "Use operation='execute' for the execution template",
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


def _get_monitoring_template(operation=None, **_):
    if not operation:
        return {"_success": True, **_MONITORING_OVERVIEW}

    templates = {
        "execution_records": _MONITORING_EXECUTION_RECORDS,
        "execution_logs": _MONITORING_EXECUTION_LOGS,
        "execution_artifacts": _MONITORING_EXECUTION_ARTIFACTS,
        "audit_logs": _MONITORING_AUDIT_LOGS,
        "events": _MONITORING_EVENTS,
    }

    tpl = templates.get(operation)
    if not tpl:
        return {
            "_success": False,
            "error": f"Unknown monitoring operation: {operation}",
            "valid_operations": list(templates.keys()),
        }

    return {"_success": True, **tpl}
