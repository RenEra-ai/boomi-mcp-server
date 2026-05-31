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
    "available_actions": ["list", "get", "create", "update", "delete"],
    "config_format": "JSON (config parameter)",
    "shape_types": ["start", "stop", "return", "message", "map", "connector", "decision", "branch", "note", "documentproperties"],
    "hint": "Use operation='create' for a full JSON template",
}

_PROCESS_CREATE = {
    "resource_type": "process",
    "operation": "create",
    "single_process_template": {
        "name": "My Process Name",
        "folder_name": "Home",
        "description": "Optional description",
        "shapes": [
            {"type": "start", "name": "start"},
            {"type": "message", "name": "log_msg", "config": {"message_text": "Process started"}},
            {"type": "map", "name": "transform", "config": {"map_id": "existing-map-component-id"}},
            {
                "type": "connector",
                "name": "get_data",
                "config": {"connector_id": "connector-component-id", "operation": "Get", "object_type": "Object"}
            },
            {"type": "decision", "name": "check_result", "config": {"expression": "document property equals value"}},
            {"type": "branch", "name": "parallel_work", "config": {"num_branches": 2}},
            {"type": "stop", "name": "end"},
        ],
    },
    "multi_component_template": {
        "components": [
            {"name": "Transform Map", "type": "map", "dependencies": []},
            {
                "name": "Main Process",
                "type": "process",
                "dependencies": ["Transform Map"],
                "config": {
                    "name": "Main Process",
                    "shapes": [
                        {"type": "start", "name": "start"},
                        {"type": "map", "name": "transform", "config": {"map_ref": "Transform Map"}},
                        {"type": "stop", "name": "end"},
                    ],
                },
            },
        ],
    },
    "shape_reference": {
        "start": {"required": True, "position": "first", "config": "none"},
        "stop": {"position": "last", "config": {"continue_": "true|false"}},
        "return": {"position": "last", "config": {"label": "text"}},
        "message": {"config": {"message_text": "REQUIRED"}},
        "map": {"config": {"map_id": "existing map component ID", "map_ref": "name in multi-component JSON"}},
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
# Integration Builder Templates
# ============================================================================

_INTEGRATION_OVERVIEW = {
    "resource_type": "integration",
    "tool": "build_integration",
    "available_actions": ["plan", "apply", "verify"],
    "config_format": "JSON (config parameter)",
    "conflict_policy": ["reuse", "clone", "fail"],
    "hint": "Use operation='plan' for full IntegrationSpecV1 templates and routing behavior.",
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
                {
                    "key": "order_process",
                    "type": "process",
                    "action": "create",
                    "name": "Order Sync Process",
                    "depends_on": ["rest_connection"],
                    "config": {
                        "name": "Order Sync Process",
                        "shapes": [
                            {"type": "start", "name": "start"},
                            {
                                "type": "connector",
                                "name": "get_orders",
                                "config": {
                                    "connector_id": "$ref:rest_connection",
                                    "operation": "Get",
                                    "object_type": "orders",
                                },
                            },
                            {"type": "stop", "name": "end"},
                        ],
                    },
                },
            ],
        },
    },
    "notes": [
        "You can also provide integration_spec directly instead of source_description.",
        "plan is read-only and returns deterministic execution order with endpoint routes.",
        "Dependency tokens in config can reference previous components with $ref:<component_key>.",
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
    "note": "Boomi's Component API requires type-specific XML. For processes, use manage_process with config (JSON object) instead.",
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
        "   OR for processes: use manage_process with config (JSON object)",
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
        "UNSUPPORTED_XML_PROFILE_FEATURE; use the raw-XML escape hatch or "
        "wait for issue #47 (XSD/sample-XML inference)."
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
    "supported_kinds": ["element"],
    "field_tree_rules": [
        "Every node must use kind='element' (M2 is element-only).",
        "Element with children = structural (no data_type); element without "
        "children = leaf (data_type required).",
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
        "Element-only generation. For complex XML profiles (attributes, "
        "namespaces, schema imports), use the raw-XML escape hatch "
        "(config={'xml': '...'}) or wait for issue #47."
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
            "read-only discovery (issue #47). Both target the element-only "
            "namespace-less subset; namespaces/attributes/mixed content fail "
            "with actionable unsupported-shape errors."
        ),
        "attributes_and_namespaces": (
            "Element attributes, mixed content, and namespace declarations "
            "are deferred; use the raw-XML escape hatch for now."
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
        "are rejected with MAP_PROFILE_INDEX_UNAVAILABLE (issue #47 owns "
        "existing-profile schema discovery)."
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
        "parse arbitrary Boomi profile XML (issue #47 owns that path).",
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
            "generated profile component to index (deferred to #47)"
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
            "profile refs is tracked by issue #47."
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
        "rejected with MAP_PROFILE_INDEX_UNAVAILABLE (deferred to #47). "
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
        "produce MAP_PROFILE_INDEX_UNAVAILABLE (issue #47 owns existing-"
        "profile schema discovery).",
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
            "generated profile component to index (deferred to #47)"
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
            "profile refs is tracked by issue #47."
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
        "rejected with MAP_PROFILE_INDEX_UNAVAILABLE (deferred to #47)."
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
        "produce MAP_PROFILE_INDEX_UNAVAILABLE (deferred to #47).",
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
            "generated profile component to index (deferred to #47)"
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
            "profile refs is tracked by issue #47."
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
                "restriction + minOccurs/maxOccurs(unbounded). choice/all/any/"
                "attributes/mixed/import/include/extension/list/union/substitution "
                "are unsupported; target/qualified namespaces fail; recursive types "
                "fail with PROFILE_INFERENCE_RECURSIVE_XML."
            ),
        },
        "profile_from_sample_xml": {
            "input": "artifact = an XML document string (element-only).",
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
            "valid_types": _VALID_RESOURCE_TYPES,
        }

    return handler(
        operation=operation,
        standard=standard,
        component_type=component_type,
        protocol=protocol,
    )


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
            "reliability",
            "reliability.retry_count",
            "reliability.dlq",
            "reliability.dlq.mode",
        ],
        # Issue #28 added primitives that PRODUCE these fields as process
        # fragments (schedule_envelope, run_metadata, dlq_writer,
        # error_classifier). Issue #29 now REPRESENTS them as metadata under
        # build_from_archetype's validation_rules.operational_intent — but
        # ProcessFlowBuilder still does NOT consume them into process XML, so
        # they remain deferred here (not optional). Promoting them to
        # optional_fields would repeat the Codex r3 P2 "silently ignored"
        # lie. `produced_by` names the issue-#28 primitive; `represented_by`
        # names where #29 surfaces the field as metadata; `tracked_by` names
        # the issue/milestone that will wire it into the executable process
        # (M3 schedule activation; #51 verified Try/Catch retry/DLQ +
        # dynamic operation-property wiring).
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
                "field": "reliability.on_failure",
                "produced_by": "dlq_writer / error_classifier primitives (#28)",
                "represented_by": "build_from_archetype operational_intent metadata (#29)",
                "tracked_by": "#51",
            },
        ],
        "supported_transform_modes": ["passthrough", "message", "map_ref"],
        "supported_dlq_modes": ["disabled", "document_cache_ref", "error_subprocess_ref"],
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
            {"error_code": "PROCESS_CONNECTOR_BINDING_INVALID", "field": "source|target"},
            {"error_code": "PROCESS_REF_TYPE_MISMATCH", "field": "source.connection_id|source.operation_id|target.connection_id|target.operation_id|target.action_type"},
            {"error_code": "PROCESS_SHAPE_UNSUPPORTED", "field": "transform.mode"},
            {"error_code": "PROCESS_RETRY_UNVERIFIED", "field": "reliability.retry_count|reliability.dlq.mode"},
            {"error_code": "PROCESS_DLQ_BINDING_INVALID", "field": "reliability.dlq|reliability.dlq.mode"},
            {"error_code": "PROCESS_XML_VALIDATION_FAILED", "field": "config"},
            {"error_code": "PLAINTEXT_SECRET_REJECTED", "field": "<scanned secret field path>"},
        ],
        "notes": [
            "retry_count > 0 and dlq.mode != 'disabled' return PROCESS_RETRY_UNVERIFIED "
            "for now; the verified Try/Catch wrapper lands in issue #51 after live "
            "Try/Catch XML is captured.",
            "Issue #28 primitives (schedule_envelope, run_metadata, dlq_writer, "
            "error_classifier) PRODUCE execution/reliability fragments, but "
            "ProcessFlowBuilder does not yet consume them — see deferred_fields.",
            "Map components are referenced by id or $ref token only; map creation "
            "is tracked by issue #26.",
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
                "reliability": {
                    "retry_count": 0,
                    "dlq": {"mode": "disabled"},
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
        return {"_success": True, **_PROCESS_CREATE}

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
            result["recommendation"] = "For processes, use manage_process with config (JSON object) instead of raw XML."
        return result

    if operation == "search":
        return {"_success": True, **_COMPONENT_SEARCH}

    if operation == "clone":
        return {"_success": True, **_COMPONENT_CLONE}

    if operation == "compare_versions":
        return {"_success": True, **_COMPONENT_COMPARE}

    return {
        "_success": False,
        "error": f"Unknown component operation: {operation}",
        "valid_operations": ["create", "search", "clone", "compare_versions"],
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

        # === Category 3: Deployment & B2B (3 tools) ===
        "manage_deployment": {
            "category": "Deployment & B2B",
            "description": "Manage deployment packages, deploy to environments, and manage component/process attachments",
            "actions": [
                "list_packages", "get_package", "create_package", "delete_package",
                "deploy", "undeploy", "list_deployments", "get_deployment",
                "list_component_atom_attachments", "attach_component_atom", "detach_component_atom",
                "list_component_environment_attachments", "attach_component_environment", "detach_component_environment",
                "list_process_atom_attachments", "attach_process_atom", "detach_process_atom",
                "list_process_environment_attachments", "attach_process_environment", "detach_process_environment", "get_package_manifest",
            ],
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
            "description": "Manage process components with JSON-based configuration and scheduling",
            "actions": ["list", "get", "create", "update", "delete"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "process_id": "str (optional)",
                "config": "JSON str (optional) — process definition with shapes",
                "filters": "JSON str (optional)",
            },
            "examples": [
                'manage_process(profile="prod", action="list")',
                'manage_process(profile="prod", action="create", config=\'{"name":"My Process","shapes":[{"type":"start","name":"start"},{"type":"stop","name":"end"}]}\')',
            ],
            "sdk_examples_covered": [
                "create_process_component.py",
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
            "description": "Build an IntegrationSpecV1 from an archetype WITHOUT calling Boomi. Pass the returned spec to build_integration(action='plan') to preview steps. The database_to_api_sync archetype emits executable component specs (DB source, JSON transform, REST target, structured process); deployment/scheduling and verified retry/DLQ remain M3 / #51.",
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
            "description": "High-level orchestrator for building integrations from component-oriented JSON specs",
            "actions": ["plan", "apply", "verify"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required) — plan | apply | verify",
                "config": "JSON str (optional) — IntegrationSpecV1 payload and execution options",
            },
            "examples": [
                'build_integration(profile="prod", action="plan", config=\'{"name":"Order Sync","mode":"lift_shift","components":[{"key":"p1","type":"process","action":"create","name":"Order Process","config":{"name":"Order Process","shapes":[{"type":"start","name":"start"},{"type":"stop","name":"end"}]}}]}\')',
                'build_integration(profile="prod", action="apply", config=\'{"dry_run":false,"conflict_policy":"reuse","integration_spec":{"name":"Order Sync","mode":"lift_shift","components":[...]}}\')',
                'build_integration(profile="prod", action="verify", config=\'{"build_id":"<uuid>"}\')',
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
            "description": "Troubleshoot failed executions — error details, retry, reprocess, cancel, queue management",
            "actions": ["error_details", "retry", "reprocess", "cancel", "list_queues", "clear_queue", "move_queue"],
            "read_only": False,
            "parameters": {
                "profile": "str (required)",
                "action": "str (required)",
                "execution_id": "str (optional) — required for error_details, retry; optional for reprocess (see process_id, environment_id, config.atom_id)",
                "process_id": "str (optional) — required for reprocess (with environment_id)",
                "environment_id": "str (optional) — required for reprocess (with process_id)",
                "config": "JSON str (optional) — action-specific options (e.g. days, limit, atom_id, queue_name, dest_queue)",
            },
            "examples": [
                'troubleshoot_execution(profile="prod", action="error_details", config=\'{"days": 1, "limit": 5}\')',
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

        # === Category 9: Meta / Power Tools ===
        "get_schema_template": {
            "category": "Meta Tools",
            "description": "Get example payloads, field descriptions, and enum values for all tools",
            "actions": ["(single action — specify resource_type and operation)"],
            "read_only": True,
            "parameters": {
                "resource_type": "str (required) — trading_partner | process | integration | component | environment | etc.",
                "operation": "str (optional) — create | update | list | etc.",
                "standard": "str (optional) — for trading_partner: x12, edifact, hl7, etc.",
                "component_type": "str (optional) — for component: process, connector-settings, transform.map, etc.",
                "protocol": "str (optional) — for trading_partner: http, as2, ftp, sftp, etc.",
            },
            "examples": [
                'get_schema_template(resource_type="trading_partner", operation="create", standard="x12")',
                'get_schema_template(resource_type="process", operation="create")',
                'get_schema_template(resource_type="integration", operation="plan")',
                'get_schema_template(resource_type="trading_partner", protocol="http")',
            ],
            "note": "No profile needed — returns static reference data. No API calls.",
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
            },
            "examples": [
                'invoke_boomi_api(profile="prod", endpoint="Role/query", method="POST", payload=\'{"QueryFilter":...}\')',
                'invoke_boomi_api(profile="prod", endpoint="Branch", method="POST", payload=\'{"name":"feature-v2"}\')',
                'invoke_boomi_api(profile="prod", endpoint="Component/abc-123", method="GET", accept="xml")',
            ],
            "covers_uncovered_apis": [
                "Queue Management (async)",
                "Secrets Rotation",
                "Document Reprocessing",
            ],
            "note": "Use dedicated tools when available for better parameter validation. "
                    "DELETE operations are blocked by safety feature.",
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

    # --- Workflow suggestions ---
    workflows = {
        "discover_components": {
            "description": "Find and understand components in your account",
            "steps": [
                "1. list_boomi_profiles() → find your profile",
                "2. query_components(action='list', config='{\"type\": \"process\"}') → list processes",
                "3. query_components(action='get', component_id='...') → get details",
                "4. analyze_component(action='where_used', component_id='...') → find dependencies",
            ],
        },
        "create_and_deploy_process": {
            "description": "Build a process from scratch and deploy it",
            "steps": [
                "1. get_schema_template(resource_type='process', operation='create') → get JSON template",
                "2. manage_process(action='create', config='...') → create process",
                "3. manage_deployment(action='create_package', config='{\"component_id\":\"...\", \"component_type\":\"process\", \"package_version\":\"1.0\"}') → package it",
                "4. manage_deployment(action='deploy', package_id='<pkg_id>', environment_id='<env_id>') → deploy it",
                "5. execute_process(profile='...', process_id='<proc_id>', environment_id='<env_id>') → run it",
                "6. monitor_platform(action='execution_records', config='{\"execution_id\": \"...\"}') → check status",
            ],
        },
        "build_integration_from_description": {
            "description": "Author an integration: prefer V3 archetypes; fall back to direct IntegrationSpecV1 only when no archetype fits.",
            "steps": [
                "1. list_integration_archetypes() → discover archetype catalog (read-only, no Boomi mutation)",
                "2. get_integration_archetype(name='...') → inspect parameter_schema, capability_notes, limitations, examples",
                "3. build_from_archetype(name='...', parameters={...}) → emit IntegrationSpecV1 (no Boomi mutation)",
                "4. build_integration(action='plan', config='{\"integration_spec\": <spec from step 3>, \"conflict_policy\": \"reuse\"}') → preview deterministic plan",
                "5. review_transformation(action='validate_unmapped', config='{\"integration_spec\": <spec from step 3>}') → confirm the transform has no unmapped/invalid mappings BEFORE apply (read-only, no Boomi mutation). Optionally also run review_transformation(action='list_fields'|'mapping_diff') to inspect fields or diff against a prior spec.",
                "6. build_integration(action='apply', config='{\"dry_run\": false, \"integration_spec\": <spec from step 3>, ...}') → execute ordered component creation/update",
                "7. build_integration(action='verify', config='{\"build_id\": \"<uuid-from-apply>\"}') → verify created components and dependencies",
            ],
            "fallback": {
                "when": "No archetype fits — e.g., an integration shape not yet covered by the registry.",
                "steps": [
                    "F1. get_schema_template(resource_type='integration', operation='plan') → get raw IntegrationSpecV1 template",
                    "F2. build_integration(action='plan', config='...') → validate the hand-authored spec",
                    "F3. build_integration(action='apply', config='{\"dry_run\": false, ...}') → execute",
                    "F4. build_integration(action='verify', config='{\"build_id\": \"...\"}') → verify",
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
            "steps": [
                "1. monitor_platform(action='execution_records', config='{\"status\": \"ERROR\", \"limit\": 10}') → find failures",
                "2. monitor_platform(action='execution_logs', config='{\"execution_id\": \"...\"}') → get error logs",
                "3. monitor_platform(action='execution_artifacts', config='{\"execution_id\": \"...\"}') → get output docs",
                "4. analyze_component(action='dependencies', component_id='...') → check dependencies",
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
    }
