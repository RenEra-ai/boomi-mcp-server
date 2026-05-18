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
                    "key": "http_connection",
                    "type": "connector-settings",
                    "action": "create",
                    "name": "Order API Connection",
                    "config": {
                        "connector_type": "http",
                        "component_name": "Order API Connection",
                        "url": "https://api.example.com/orders",
                        "auth_type": "NONE",
                    },
                },
                {
                    "key": "order_process",
                    "type": "process",
                    "action": "create",
                    "name": "Order Sync Process",
                    "depends_on": ["http_connection"],
                    "config": {
                        "name": "Order Sync Process",
                        "shapes": [
                            {"type": "start", "name": "start"},
                            {
                                "type": "connector",
                                "name": "get_orders",
                                "config": {
                                    "connector_id": "$ref:http_connection",
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


_COMPONENT_CREATE_CONNECTOR_DATABASE_SQLSERVER = {
    "resource_type": "component",
    "operation": "create",
    "component_type": "connector-settings",
    "protocol": "database.sqlserver",
    "tool": "manage_connector (action='create')",
    "note": (
        "Database (Legacy) connector for Microsoft SQL Server. Dispatched through "
        "the builder registry (CONNECTOR_BUILDERS['database']) so callers pass JSON "
        "config — not raw XML. The builder emits <DatabaseConnectionSettings> with "
        "Boomi's default <WriteOptions> and <AdapterPoolInfo> blocks."
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
    "supported_driver_ids": ["sqlserver", "microsoft_jdbc", "jtds"],
    "recognized_driver_ids": ["sqlserver", "microsoft_jdbc", "jtds", "custom"],
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
            "note": "Legacy jTDS driver. Pre-loaded in Boomi runtime; no TLS by default.",
        },
        "custom": {
            "shape": "custom_url",
            "buildable": False,
            "unsupported_error_code": "UNSUPPORTED_DB_DRIVER_SHAPE",
            "unsupported_reason": (
                "Custom driver XML emission is deferred until a verified live "
                "Boomi Custom connection export is available. Use reuse mode "
                "on an existing Boomi component or the raw-XML escape hatch "
                "(config.xml=...) in the meantime."
            ),
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
        "urlFormat=jdbc:jtds:sqlserver://{0}:{1}/{2}{3}). Postgres/Oracle/MySQL are "
        "deliberately unsupported in M2.2 and return error_code=UNSUPPORTED_DB_DRIVER."
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
    ],
    "recommended_workflow": [
        "1. manage_connector list_types — confirm 'database' appears.",
        "2. manage_connector create with the JSON config above (credential_ref is opaque; no password).",
        "3. Set the password in the Boomi UI (or update via raw XML with pre-encrypted ciphertext).",
        "4. Test the connection from the UI (Connection Test) against an online runtime.",
        "5. Deploy via manage_deployment.",
    ],
    "update_note": (
        "Field-level update via JSON config is not yet supported for database "
        "connectors (HTTP only). Use manage_connector update with config.xml=... "
        "to replace the XML, or edit in the UI. Raw-XML escape hatch remains "
        "unchanged in M2.2."
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
    "available_protocols": ["database.sqlserver"],
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
        "integration": _get_integration_template,
        "component": _get_component_template,
        "environment": _get_environment_template,
        "package": _get_package_template,
        "execution_request": _get_execution_request_template,
        "organization": _get_organization_template,
        "folder": _get_folder_template,
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


def _get_process_template(operation=None, **_):
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
            if protocol:
                return {
                    "_success": False,
                    "error": f"Unknown connector-settings protocol: {protocol}",
                    "valid_protocols": _COMPONENT_CREATE_CONNECTOR_SETTINGS_OVERVIEW["available_protocols"],
                }
            return {"_success": True, **_COMPONENT_CREATE_CONNECTOR_SETTINGS_OVERVIEW}
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
                'manage_connector(profile="prod", action="get_type", config=\'{"connector_type": "http"}\')',
                'manage_connector(profile="prod", action="list", config=\'{"component_type": "connection", "connector_type": "http"}\')',
                'manage_connector(profile="prod", action="get", component_id="abc-123")',
                'manage_connector(profile="prod", action="create", config=\'{"connector_type": "http", "component_name": "My HTTP", "url": "https://api.example.com", "auth_type": "NONE"}\')',
                'manage_connector(profile="prod", action="update", component_id="abc-123", config=\'{"url": "https://new-url.com"}\')',
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
            "description": "Build an IntegrationSpecV1 from an archetype WITHOUT calling Boomi. Pass the returned spec to build_integration(action='plan') to preview steps.",
            "actions": ["(single action — emits an IntegrationSpecV1 only)"],
            "read_only": True,
            "no_boomi_mutation": True,
            "parameters": {
                "name": "str (required) — archetype name from list_integration_archetypes()",
                "parameters": "dict (optional) — values matching the archetype's parameter_schema",
            },
            "examples": [
                'build_from_archetype(name="stub_minimal_integration", parameters={"integration_name": "demo"})',
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

        # === Category 8: Meta / Power Tools ===
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

        # === Category 8b: Account Group Management ===
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

        # === Category 9: Listener Management ===
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

        # === Category 10: Integration Pack Management ===
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
                "5. build_integration(action='apply', config='{\"dry_run\": false, \"integration_spec\": <spec from step 3>, ...}') → execute ordered component creation/update",
                "6. build_integration(action='verify', config='{\"build_id\": \"<uuid-from-apply>\"}') → verify created components and dependencies",
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
