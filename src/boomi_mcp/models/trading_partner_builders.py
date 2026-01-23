#!/usr/bin/env python3
"""
Trading Partner JSON Model Builders

This module provides helper functions to build nested Boomi Trading Partner JSON models
from flat parameters. It maps the 70+ parameters currently supported in the XML implementation
to the nested JSON structure required by the Boomi API.

Usage:
    from trading_partner_builders import build_trading_partner_model

    tp_model = build_trading_partner_model(
        component_name="My Partner",
        standard="x12",
        classification="tradingpartner",
        folder_name="Home",
        contact_email="partner@example.com",
        isa_id="MYISAID",
        communication_protocols=["http", "as2"],
        http_url="https://partner.example.com/api",
        as2_url="https://partner.example.com/as2"
    )
"""

from typing import Dict, Any, List, Optional
from boomi.models import (
    TradingPartnerComponent,
    TradingPartnerComponentClassification,
    TradingPartnerComponentStandard,
    ContactInfo,
    PartnerCommunication,
    PartnerInfo,
)


# ============================================================================
# Contact Information Builder
# ============================================================================

def build_contact_info(**kwargs) -> Optional[ContactInfo]:
    """
    Build ContactInfo model from flat parameters.

    Args:
        contact_name: Contact person name
        contact_email: Email address
        contact_phone: Phone number
        contact_fax: Fax number
        contact_address: Street address line 1
        contact_address2: Street address line 2
        contact_city: City
        contact_state: State/province
        contact_country: Country
        contact_postalcode: Postal/zip code

    Returns:
        ContactInfo object if any contact fields provided, None otherwise
    """
    # Extract contact fields from kwargs
    contact_fields = {
        'address1': kwargs.get('contact_address', ''),
        'address2': kwargs.get('contact_address2', ''),
        'city': kwargs.get('contact_city', ''),
        'contact_name': kwargs.get('contact_name', ''),
        'country': kwargs.get('contact_country', ''),
        'email': kwargs.get('contact_email', ''),
        'fax': kwargs.get('contact_fax', ''),
        'phone': kwargs.get('contact_phone', ''),
        'postalcode': kwargs.get('contact_postalcode', ''),
        'state': kwargs.get('contact_state', '')
    }

    # Return None if all fields are empty
    if not any(contact_fields.values()):
        return None

    return ContactInfo(**contact_fields)


# ============================================================================
# Communication Protocol Builders
# ============================================================================

def build_disk_communication_options(**kwargs):
    """Build Disk protocol communication options.

    Args:
        disk_get_directory: Directory to read files from
        disk_send_directory: Directory to write files to
        disk_file_filter: File filter pattern (default: *)
        disk_filter_match_type: Filter type - wildcard or regex (default: wildcard)
        disk_delete_after_read: Delete files after reading (true/false)
        disk_max_file_count: Maximum files to retrieve per poll
        disk_create_directory: Create directory if not exists (true/false)
        disk_write_option: Write option - unique, over, append, abort (default: unique)

    Returns dict (not SDK model) - for consistency with other builders
    """
    get_dir = kwargs.get('disk_get_directory')
    send_dir = kwargs.get('disk_send_directory')
    file_filter = kwargs.get('disk_file_filter', '*')
    filter_match_type = kwargs.get('disk_filter_match_type')
    delete_after_read = kwargs.get('disk_delete_after_read')
    max_file_count = kwargs.get('disk_max_file_count')
    create_directory = kwargs.get('disk_create_directory')
    write_option = kwargs.get('disk_write_option')

    if not get_dir and not send_dir:
        return None

    result = {'@type': 'DiskCommunicationOptions'}

    if get_dir:
        get_options = {
            '@type': 'DiskGetOptions',
            'fileFilter': file_filter,
            'getDirectory': get_dir
        }
        if filter_match_type:
            get_options['filterMatchType'] = filter_match_type
        if delete_after_read is not None:
            get_options['deleteAfterRead'] = str(delete_after_read).lower() == 'true'
        if max_file_count is not None:
            get_options['maxFileCount'] = int(max_file_count)
        result['DiskGetOptions'] = get_options

    if send_dir:
        send_options = {
            '@type': 'DiskSendOptions',
            'sendDirectory': send_dir
        }
        if create_directory is not None:
            send_options['createDirectory'] = str(create_directory).lower() == 'true'
        if write_option:
            send_options['writeOption'] = write_option
        result['DiskSendOptions'] = send_options

    return result


def build_ftp_communication_options(**kwargs):
    """Build FTP protocol communication options.

    Args:
        ftp_host: FTP server hostname (required)
        ftp_port: FTP server port (default: 21)
        ftp_username: FTP username
        ftp_password: FTP password
        ftp_remote_directory: Remote directory path
        ftp_ssl_mode: SSL mode - NONE, EXPLICIT, IMPLICIT (default: NONE)
        ftp_connection_mode: Connection mode - ACTIVE, PASSIVE (default: PASSIVE)
        ftp_transfer_type: Transfer type - ascii, binary (default: binary)
        ftp_get_action: Get action - actionget, actiongetdelete, actiongetmove
        ftp_send_action: Send action - actionputrename, actionputappend, actionputerror, actionputoverwrite
        ftp_max_file_count: Maximum files to retrieve per poll
        ftp_file_to_move: Directory to move files after get (when action=actiongetmove)
        ftp_move_to_directory: Directory to move files after send
        ftp_client_ssl_alias: Client SSL certificate alias for mutual TLS

    Returns dict (not SDK model) - API accepts minimal structure
    """
    host = kwargs.get('ftp_host')
    if not host:
        return None

    port = int(kwargs.get('ftp_port', 21))
    username = kwargs.get('ftp_username', '')
    password = kwargs.get('ftp_password', '')
    remote_directory = kwargs.get('ftp_remote_directory')
    ssl_mode = kwargs.get('ftp_ssl_mode', 'NONE')
    connection_mode = kwargs.get('ftp_connection_mode', 'passive')

    # New parameters
    transfer_type = kwargs.get('ftp_transfer_type')
    get_action = kwargs.get('ftp_get_action')
    send_action = kwargs.get('ftp_send_action')
    max_file_count = kwargs.get('ftp_max_file_count')
    file_to_move = kwargs.get('ftp_file_to_move')
    move_to_directory = kwargs.get('ftp_move_to_directory')
    client_ssl_alias = kwargs.get('ftp_client_ssl_alias')

    # Build FTP settings
    ftp_settings = {
        'host': host,
        'port': port,
        'user': username,
        'password': password,
        'connectionMode': connection_mode.lower()  # SDK expects lowercase: 'active' or 'passive'
    }

    # Add SSL options if not NONE or if client SSL alias is specified
    # SDK expects lowercase: 'none', 'explicit', 'implicit'
    ssl_options = {}
    if ssl_mode and ssl_mode.lower() != 'none':
        ssl_options['sslmode'] = ssl_mode.lower()
    if client_ssl_alias:
        # clientSSLCertificate must be an object with 'alias' field
        ssl_options['clientSSLCertificate'] = {'alias': client_ssl_alias}
        ssl_options['useClientAuthentication'] = True

    if ssl_options:
        ftp_settings['FTPSSLOptions'] = ssl_options

    result = {'FTPSettings': ftp_settings}

    # Build get options
    get_options = {}
    if remote_directory:
        get_options['remoteDirectory'] = remote_directory
    if transfer_type:
        get_options['transferType'] = transfer_type.lower()  # 'ascii' or 'binary'
    if get_action:
        get_options['ftpAction'] = get_action.lower()  # 'actionget', 'actiongetdelete', 'actiongetmove'
    if max_file_count:
        get_options['maxFileCount'] = int(max_file_count)
    if file_to_move:
        get_options['fileToMove'] = file_to_move

    if get_options:
        get_options['useDefaultGetOptions'] = False
        result['FTPGetOptions'] = get_options

    # Build send options
    send_options = {}
    if remote_directory:
        send_options['remoteDirectory'] = remote_directory
    if transfer_type:
        send_options['transferType'] = transfer_type.lower()
    if send_action:
        send_options['ftpAction'] = send_action.lower()  # 'actionputrename', 'actionputappend', etc.
    if move_to_directory:
        send_options['moveToDirectory'] = move_to_directory

    if send_options:
        send_options['useDefaultSendOptions'] = False
        result['FTPSendOptions'] = send_options

    return result


def build_sftp_communication_options(**kwargs):
    """Build SFTP protocol communication options.

    Args:
        sftp_host: SFTP server hostname (required)
        sftp_port: SFTP server port (default: 22)
        sftp_username: SFTP username
        sftp_password: SFTP password
        sftp_remote_directory: Remote directory path
        sftp_ssh_key_auth: Enable SSH key authentication (true/false)
        sftp_known_host_entry: Known hosts entry for server verification
        sftp_ssh_key_path: Path to SSH private key file
        sftp_ssh_key_password: Password for encrypted SSH private key
        sftp_dh_key_max_1024: Limit DH key size to 1024 bits for legacy servers (true/false)
        sftp_get_action: Get action - actionget, actiongetdelete, actiongetmove
        sftp_send_action: Send action - actionputrename, actionputappend, actionputerror, actionputoverwrite
        sftp_max_file_count: Maximum files to retrieve per poll
        sftp_file_to_move: Directory to move files after get (when action is actiongetmove)
        sftp_move_to_directory: Directory to move files after operation
        sftp_move_force_override: Force overwrite when moving files (true/false)
        sftp_proxy_enabled: Enable proxy connection (true/false)
        sftp_proxy_host: Proxy server hostname
        sftp_proxy_port: Proxy server port
        sftp_proxy_user: Proxy username
        sftp_proxy_password: Proxy password
        sftp_proxy_type: Proxy type - ATOM, HTTP, SOCKS4, SOCKS5

    Returns dict (not SDK model) - API accepts minimal structure
    """
    host = kwargs.get('sftp_host')
    if not host:
        return None

    port = int(kwargs.get('sftp_port', 22))
    username = kwargs.get('sftp_username', '')
    password = kwargs.get('sftp_password', '')
    remote_directory = kwargs.get('sftp_remote_directory')
    ssh_key_auth = kwargs.get('sftp_ssh_key_auth')
    known_host_entry = kwargs.get('sftp_known_host_entry')
    ssh_key_path = kwargs.get('sftp_ssh_key_path')
    ssh_key_password = kwargs.get('sftp_ssh_key_password')
    dh_key_max_1024 = kwargs.get('sftp_dh_key_max_1024')
    get_action = kwargs.get('sftp_get_action')
    send_action = kwargs.get('sftp_send_action')
    max_file_count = kwargs.get('sftp_max_file_count')
    file_to_move = kwargs.get('sftp_file_to_move')
    move_to_directory = kwargs.get('sftp_move_to_directory')
    move_force_override = kwargs.get('sftp_move_force_override')
    proxy_enabled = kwargs.get('sftp_proxy_enabled')
    proxy_host = kwargs.get('sftp_proxy_host')
    proxy_port = kwargs.get('sftp_proxy_port')
    proxy_user = kwargs.get('sftp_proxy_user')
    proxy_password = kwargs.get('sftp_proxy_password')
    proxy_type = kwargs.get('sftp_proxy_type')

    # Build SFTP settings
    sftp_settings = {
        'host': host,
        'port': port,
        'user': username,
        'password': password
    }

    # Add SSH options if specified
    ssh_options = {}
    if ssh_key_auth is not None:
        ssh_options['sshkeyauth'] = str(ssh_key_auth).lower() == 'true'
    if known_host_entry:
        ssh_options['knownHostEntry'] = known_host_entry
    if ssh_key_path:
        ssh_options['sshkeypath'] = ssh_key_path
    if ssh_key_password:
        ssh_options['sshkeypassword'] = ssh_key_password
    if dh_key_max_1024 is not None:
        ssh_options['dhKeySizeMax1024'] = str(dh_key_max_1024).lower() == 'true'

    if ssh_options:
        sftp_settings['SFTPSSHOptions'] = ssh_options

    # Add proxy settings if specified
    if proxy_enabled is not None or proxy_host:
        proxy_settings = {}
        if proxy_enabled is not None:
            proxy_settings['proxyEnabled'] = str(proxy_enabled).lower() == 'true'
        if proxy_host:
            proxy_settings['host'] = proxy_host
        if proxy_port:
            proxy_settings['port'] = int(proxy_port)
        if proxy_user:
            proxy_settings['user'] = proxy_user
        if proxy_password:
            proxy_settings['password'] = proxy_password
        if proxy_type:
            proxy_settings['type'] = proxy_type.upper()
        sftp_settings['SFTPProxySettings'] = proxy_settings

    result = {'SFTPSettings': sftp_settings}

    # Build get options
    get_options = {}
    if remote_directory:
        get_options['remoteDirectory'] = remote_directory
    if get_action:
        get_options['ftpAction'] = get_action.lower()
    if max_file_count:
        get_options['maxFileCount'] = int(max_file_count)
    if file_to_move:
        get_options['fileToMove'] = file_to_move
    if move_to_directory:
        get_options['moveToDirectory'] = move_to_directory
    if move_force_override is not None:
        get_options['moveToForceOverride'] = str(move_force_override).lower() == 'true'

    if get_options:
        get_options['useDefaultGetOptions'] = False
        result['SFTPGetOptions'] = get_options

    # Build send options
    send_options = {}
    if remote_directory:
        send_options['remoteDirectory'] = remote_directory
    if send_action:
        send_options['ftpAction'] = send_action.lower()
    if move_to_directory:
        send_options['moveToDirectory'] = move_to_directory
    if move_force_override is not None:
        send_options['moveToForceOverride'] = str(move_force_override).lower() == 'true'

    if send_options:
        send_options['useDefaultSendOptions'] = False
        result['SFTPSendOptions'] = send_options

    return result


def build_http_communication_options(**kwargs):
    """Build HTTP protocol communication options.

    Args:
        http_url: HTTP endpoint URL (required)
        http_authentication_type: Authentication type - NONE, BASIC, OAUTH2 (default: NONE)
        http_username: Username for BASIC authentication
        http_password: Password for BASIC authentication
        http_connect_timeout: Connection timeout in milliseconds
        http_read_timeout: Read timeout in milliseconds
        http_client_auth: Enable client SSL authentication (true/false)
        http_trust_server_cert: Trust server certificate (true/false)
        http_client_ssl_alias: Client SSL certificate alias
        http_trusted_cert_alias: Trusted server certificate alias
        http_cookie_scope: Cookie handling - IGNORED, GLOBAL, CONNECTOR_SHAPE
        http_method_type: HTTP method - GET, POST, PUT, DELETE, PATCH (default: POST)
        http_data_content_type: Content type for request data
        http_follow_redirects: Follow redirects (true/false)
        http_return_errors: Return errors in response (true/false)
        http_return_responses: Return response body (true/false)
        http_request_profile: Request profile component ID
        http_request_profile_type: Request profile type - NONE, XML, JSON
        http_response_profile: Response profile component ID
        http_response_profile_type: Response profile type - NONE, XML, JSON
        http_oauth_token_url: OAuth2 token endpoint URL
        http_oauth_client_id: OAuth2 client ID
        http_oauth_client_secret: OAuth2 client secret
        http_oauth_scope: OAuth2 scope

    Returns dict (not SDK model) - API accepts minimal structure
    """
    url = kwargs.get('http_url')
    if not url:
        return None

    # Extract all parameters
    auth_type = kwargs.get('http_authentication_type', 'NONE')
    username = kwargs.get('http_username')
    password = kwargs.get('http_password')
    connect_timeout = kwargs.get('http_connect_timeout')
    read_timeout = kwargs.get('http_read_timeout')
    client_auth = kwargs.get('http_client_auth')
    trust_server_cert = kwargs.get('http_trust_server_cert')
    client_ssl_alias = kwargs.get('http_client_ssl_alias')
    trusted_cert_alias = kwargs.get('http_trusted_cert_alias')
    cookie_scope = kwargs.get('http_cookie_scope')
    method_type = kwargs.get('http_method_type')
    content_type = kwargs.get('http_data_content_type')
    follow_redirects = kwargs.get('http_follow_redirects')
    return_errors = kwargs.get('http_return_errors')
    return_responses = kwargs.get('http_return_responses')
    request_profile = kwargs.get('http_request_profile')
    request_profile_type = kwargs.get('http_request_profile_type')
    response_profile = kwargs.get('http_response_profile')
    response_profile_type = kwargs.get('http_response_profile_type')
    oauth_token_url = kwargs.get('http_oauth_token_url')
    oauth_client_id = kwargs.get('http_oauth_client_id')
    oauth_client_secret = kwargs.get('http_oauth_client_secret')
    oauth_scope = kwargs.get('http_oauth_scope')

    # Build HTTP settings
    http_settings = {
        'url': url,
        'authenticationType': auth_type.upper() if auth_type else 'NONE'
    }

    # Add timeouts if specified
    if connect_timeout:
        http_settings['connectTimeout'] = int(connect_timeout)
    if read_timeout:
        http_settings['readTimeout'] = int(read_timeout)

    # Add cookie scope if specified
    if cookie_scope:
        http_settings['cookieScope'] = cookie_scope.upper()

    # Add BASIC auth credentials if auth type is BASIC
    if auth_type and auth_type.upper() == 'BASIC' and (username or password):
        http_settings['HTTPAuthSettings'] = {
            'user': username or '',
            'password': password or ''
        }

    # Add OAuth2 settings if auth type is OAUTH2
    if auth_type and auth_type.upper() == 'OAUTH2':
        oauth2_settings = {}
        if oauth_token_url:
            oauth2_settings['accessTokenEndpoint'] = {
                'url': oauth_token_url,
                'sslOptions': {}
            }
        if oauth_client_id or oauth_client_secret:
            oauth2_settings['credentials'] = {}
            if oauth_client_id:
                oauth2_settings['credentials']['clientId'] = oauth_client_id
            if oauth_client_secret:
                oauth2_settings['credentials']['clientSecret'] = oauth_client_secret
        if oauth_scope:
            oauth2_settings['scope'] = oauth_scope
        # Default to client_credentials grant type
        oauth2_settings['grantType'] = 'client_credentials'
        if oauth2_settings:
            http_settings['HTTPOAuth2Settings'] = oauth2_settings

    # Add SSL options if specified
    ssl_options = {}
    if client_auth is not None:
        ssl_options['clientauth'] = str(client_auth).lower() == 'true'
    if trust_server_cert is not None:
        ssl_options['trustServerCert'] = str(trust_server_cert).lower() == 'true'
    if client_ssl_alias:
        ssl_options['clientsslalias'] = client_ssl_alias
    if trusted_cert_alias:
        ssl_options['trustedcertalias'] = trusted_cert_alias

    if ssl_options:
        http_settings['HTTPSSLOptions'] = ssl_options

    result = {'HTTPSettings': http_settings}

    # Add send options if method or content type specified
    send_options = {}
    if method_type:
        send_options['methodType'] = method_type.upper()
    if content_type:
        send_options['dataContentType'] = content_type
    if follow_redirects is not None:
        send_options['followRedirects'] = str(follow_redirects).lower() == 'true'
    if return_errors is not None:
        send_options['returnErrors'] = str(return_errors).lower() == 'true'
    if return_responses is not None:
        send_options['returnResponses'] = str(return_responses).lower() == 'true'
    if request_profile:
        send_options['requestProfile'] = request_profile
    if request_profile_type:
        send_options['requestProfileType'] = request_profile_type.upper()
    if response_profile:
        send_options['responseProfile'] = response_profile
    if response_profile_type:
        send_options['responseProfileType'] = response_profile_type.upper()

    if send_options:
        send_options['useDefaultOptions'] = False
        result['HTTPSendOptions'] = send_options
        result['HTTPGetOptions'] = send_options.copy()

    return result


def build_as2_communication_options(**kwargs):
    """Build AS2 protocol communication options.

    Args:
        as2_url: AS2 endpoint URL (required)
        as2_identifier: Local AS2 identifier
        as2_partner_identifier: Partner AS2 identifier
        as2_authentication_type: Authentication type - NONE, BASIC (default: NONE)
        as2_verify_hostname: Verify SSL hostname (true/false)
        as2_username: Username for BASIC authentication
        as2_password: Password for BASIC authentication
        as2_signed: Sign AS2 messages (true/false)
        as2_encrypted: Encrypt AS2 messages (true/false)
        as2_compressed: Compress AS2 messages (true/false)
        as2_encryption_algorithm: Encryption algorithm - tripledes, rc2, aes128, aes192, aes256
        as2_signing_digest_alg: Signing digest algorithm - SHA1, SHA256, SHA384, SHA512
        as2_data_content_type: Content type for AS2 message
        as2_subject: AS2 message subject header
        as2_multiple_attachments: Enable multiple attachments (true/false)
        as2_max_document_count: Maximum documents per message
        as2_attachment_option: Attachment handling - BATCH, DOCUMENT_CACHE
        as2_attachment_cache: Attachment cache component ID
        as2_request_mdn: Request MDN (true/false)
        as2_mdn_signed: Signed MDN (true/false)
        as2_mdn_digest_alg: MDN digest algorithm - SHA1, SHA256, SHA384, SHA512
        as2_synchronous_mdn: Synchronous MDN (true/false, default: true)
        as2_mdn_external_url: External URL for async MDN delivery
        as2_mdn_use_external_url: Use external URL for MDN (true/false)
        as2_mdn_use_ssl: Use SSL for MDN delivery (true/false)
        as2_mdn_client_ssl_cert: Client SSL certificate alias for MDN
        as2_mdn_ssl_cert: Server SSL certificate alias for MDN
        as2_reject_duplicates: Reject duplicate messages (true/false)
        as2_duplicate_check_count: Number of messages to check for duplicates
        as2_legacy_smime: Enable legacy S/MIME compatibility (true/false)

    Returns dict (not SDK model) - API accepts minimal structure
    """
    url = kwargs.get('as2_url')
    if not url:
        return None

    # Basic settings
    auth_type = kwargs.get('as2_authentication_type', 'NONE')
    verify_hostname = kwargs.get('as2_verify_hostname')
    username = kwargs.get('as2_username')
    password = kwargs.get('as2_password')

    # Message options
    signed = kwargs.get('as2_signed')
    encrypted = kwargs.get('as2_encrypted')
    compressed = kwargs.get('as2_compressed')
    encryption_alg = kwargs.get('as2_encryption_algorithm')
    signing_alg = kwargs.get('as2_signing_digest_alg')
    content_type = kwargs.get('as2_data_content_type')
    subject = kwargs.get('as2_subject')
    multiple_attachments = kwargs.get('as2_multiple_attachments')
    max_document_count = kwargs.get('as2_max_document_count')
    attachment_option = kwargs.get('as2_attachment_option')
    attachment_cache = kwargs.get('as2_attachment_cache')

    # MDN options
    request_mdn = kwargs.get('as2_request_mdn')
    mdn_signed = kwargs.get('as2_mdn_signed')
    mdn_digest_alg = kwargs.get('as2_mdn_digest_alg')
    sync_mdn = kwargs.get('as2_synchronous_mdn')
    mdn_external_url = kwargs.get('as2_mdn_external_url')
    mdn_use_external_url = kwargs.get('as2_mdn_use_external_url')
    mdn_use_ssl = kwargs.get('as2_mdn_use_ssl')
    mdn_client_ssl_cert = kwargs.get('as2_mdn_client_ssl_cert')
    mdn_ssl_cert = kwargs.get('as2_mdn_ssl_cert')

    # Partner info
    as2_identifier = kwargs.get('as2_identifier')
    partner_identifier = kwargs.get('as2_partner_identifier')
    reject_duplicates = kwargs.get('as2_reject_duplicates')
    duplicate_check_count = kwargs.get('as2_duplicate_check_count')
    legacy_smime = kwargs.get('as2_legacy_smime')

    # Build AS2 send settings
    send_settings = {
        'url': url,
        'authenticationType': auth_type.upper() if auth_type else 'NONE'
    }

    if verify_hostname is not None:
        send_settings['verifyHostname'] = str(verify_hostname).lower() == 'true'

    # Add BASIC auth if specified (SDK maps auth_settings to AuthSettings)
    if auth_type and auth_type.upper() == 'BASIC' and (username or password):
        send_settings['AuthSettings'] = {
            'user': username or '',
            'password': password or ''
        }

    result = {'AS2SendSettings': send_settings}

    # Build AS2 message options
    message_options = {}
    if signed is not None:
        message_options['signed'] = str(signed).lower() == 'true'
    if encrypted is not None:
        message_options['encrypted'] = str(encrypted).lower() == 'true'
    if compressed is not None:
        message_options['compressed'] = str(compressed).lower() == 'true'
    if encryption_alg:
        # SDK expects format like 'aes-256' not 'aes256'
        alg = encryption_alg.lower()
        # Add hyphen for aes/rc2 formats if missing
        if alg.startswith('aes') and '-' not in alg:
            alg = 'aes-' + alg[3:]  # 'aes256' -> 'aes-256'
        elif alg.startswith('rc2') and '-' not in alg:
            alg = 'rc2-' + alg[3:]  # 'rc2128' -> 'rc2-128'
        message_options['encryptionAlgorithm'] = alg
    if signing_alg:
        message_options['signingDigestAlg'] = signing_alg.upper()
    if content_type:
        message_options['dataContentType'] = content_type
    if subject:
        message_options['subject'] = subject
    if multiple_attachments is not None:
        message_options['multipleAttachments'] = str(multiple_attachments).lower() == 'true'
    if max_document_count:
        message_options['maxDocumentCount'] = int(max_document_count)
    if attachment_option:
        message_options['attachmentOption'] = attachment_option.upper()  # BATCH or DOCUMENT_CACHE
    if attachment_cache:
        message_options['attachmentCache'] = attachment_cache

    # Build AS2 MDN options (note: use JSON key casing like requestMDN, not requestMdn)
    mdn_options = {}
    if request_mdn is not None:
        mdn_options['requestMDN'] = str(request_mdn).lower() == 'true'
    if mdn_signed is not None:
        mdn_options['signed'] = str(mdn_signed).lower() == 'true'
    if mdn_digest_alg:
        mdn_options['mdnDigestAlg'] = mdn_digest_alg.upper()
    if sync_mdn is not None:
        mdn_options['synchronous'] = 'sync' if str(sync_mdn).lower() == 'true' else 'async'
    if mdn_external_url:
        mdn_options['externalURL'] = mdn_external_url
    if mdn_use_external_url is not None:
        mdn_options['useExternalURL'] = str(mdn_use_external_url).lower() == 'true'
    if mdn_use_ssl is not None:
        mdn_options['useSSL'] = str(mdn_use_ssl).lower() == 'true'
    if mdn_client_ssl_cert:
        # Certificate alias format
        mdn_options['mdnClientSSLCert'] = {'alias': mdn_client_ssl_cert}
    if mdn_ssl_cert:
        # Certificate alias format
        mdn_options['mdnSSLCert'] = {'alias': mdn_ssl_cert}

    # Build AS2 partner info
    partner_info = {}
    if partner_identifier:
        partner_info['as2Id'] = partner_identifier
    if reject_duplicates is not None:
        partner_info['rejectDuplicateMessages'] = str(reject_duplicates).lower() == 'true'
    if duplicate_check_count:
        partner_info['messagesToCheckForDuplicates'] = int(duplicate_check_count)
    if legacy_smime is not None:
        partner_info['enabledLegacySMIME'] = str(legacy_smime).lower() == 'true'

    # Build AS2SendOptions
    # IMPORTANT: AS2MDNOptions and AS2MessageOptions are REQUIRED by the API
    # If we're sending AS2SendOptions at all (e.g., with partner_info), we must include them

    has_send_options_content = bool(partner_info or mdn_options or message_options)

    if has_send_options_content:
        # When AS2SendOptions is present, AS2MDNOptions and AS2MessageOptions are required
        send_options = {
            'AS2MDNOptions': mdn_options if mdn_options else {},
            'AS2MessageOptions': message_options if message_options else {}
        }
        if partner_info:
            send_options['AS2PartnerInfo'] = partner_info
        result['AS2SendOptions'] = send_options

    return result


def build_mllp_communication_options(**kwargs):
    """Build MLLP protocol communication options (for HL7 messaging).

    Args:
        mllp_host: MLLP server hostname (required)
        mllp_port: MLLP server port (required)
        mllp_use_ssl: Enable SSL/TLS (true/false)
        mllp_persistent: Use persistent connections (true/false)
        mllp_receive_timeout: Receive timeout in milliseconds
        mllp_send_timeout: Send timeout in milliseconds
        mllp_max_connections: Maximum number of connections
        mllp_inactivity_timeout: Inactivity timeout in seconds (default: 60)
        mllp_max_retry: Maximum retry attempts (1-5)
        mllp_halt_timeout: Halt on timeout (true/false)
        mllp_use_client_ssl: Enable client SSL authentication (true/false)
        mllp_client_ssl_alias: Client SSL certificate alias
        mllp_ssl_alias: Server SSL certificate alias

    Returns dict (not SDK model) - API accepts minimal structure
    """
    host = kwargs.get('mllp_host')
    port = kwargs.get('mllp_port')

    if not host or not port:
        return None

    # Build MLLP send settings with @type for API compatibility
    mllp_settings = {
        '@type': 'MLLPSendSettings',
        'host': host,
        'port': int(port)
    }

    # Add optional settings
    use_ssl = kwargs.get('mllp_use_ssl')
    persistent = kwargs.get('mllp_persistent')
    receive_timeout = kwargs.get('mllp_receive_timeout')
    send_timeout = kwargs.get('mllp_send_timeout')
    max_connections = kwargs.get('mllp_max_connections')
    inactivity_timeout = kwargs.get('mllp_inactivity_timeout')
    max_retry = kwargs.get('mllp_max_retry')
    halt_timeout = kwargs.get('mllp_halt_timeout')
    use_client_ssl = kwargs.get('mllp_use_client_ssl')
    client_ssl_alias = kwargs.get('mllp_client_ssl_alias')
    ssl_alias = kwargs.get('mllp_ssl_alias')

    # Build MLLPSSLOptions with @type
    ssl_options = {
        '@type': 'MLLPSSLOptions',
        'useSSL': str(use_ssl).lower() == 'true' if use_ssl else False
    }
    if use_client_ssl is not None:
        ssl_options['useClientSSL'] = str(use_client_ssl).lower() == 'true'
    if client_ssl_alias:
        ssl_options['clientSSLAlias'] = client_ssl_alias
    if ssl_alias:
        ssl_options['sslAlias'] = ssl_alias

    mllp_settings['MLLPSSLOptions'] = ssl_options

    if persistent is not None:
        mllp_settings['persistent'] = str(persistent).lower() == 'true'
    if receive_timeout:
        mllp_settings['receiveTimeout'] = int(receive_timeout)
    if send_timeout:
        mllp_settings['sendTimeout'] = int(send_timeout)
    if max_connections:
        mllp_settings['maxConnections'] = int(max_connections)
    if inactivity_timeout:
        mllp_settings['inactivityTimeout'] = int(inactivity_timeout)
    if halt_timeout is not None:
        mllp_settings['haltTimeout'] = str(halt_timeout).lower() == 'true'

    # Max retry must be between 1-5 per Boomi API (default to 1)
    if max_retry:
        mllp_settings['maxRetry'] = min(max(int(max_retry), 1), 5)
    else:
        mllp_settings['maxRetry'] = 1  # API requires 1-5, default to 1

    # Standard MLLP delimiters (hex 0B for start, hex 1C hex 0D for end) with @type
    mllp_settings['startBlock'] = {'@type': 'EdiDelimiter', 'delimiterValue': 'bytecharacter', 'delimiterSpecial': '0B'}
    mllp_settings['endBlock'] = {'@type': 'EdiDelimiter', 'delimiterValue': 'bytecharacter', 'delimiterSpecial': '1C'}
    mllp_settings['endData'] = {'@type': 'EdiDelimiter', 'delimiterValue': 'bytecharacter', 'delimiterSpecial': '0D'}

    return {'@type': 'MLLPCommunicationOptions', 'MLLPSendSettings': mllp_settings}


def build_oftp_communication_options(**kwargs):
    """Build OFTP protocol communication options (for ODETTE file transfer).

    Args:
        oftp_host: OFTP server hostname (required)
        oftp_port: OFTP server port (default: 3305)
        oftp_tls: Enable TLS (true/false)
        oftp_ssid_code: ODETTE Session ID code
        oftp_ssid_password: ODETTE Session ID password
        oftp_compress: Enable compression (true/false)
        oftp_ssid_auth: Enable SSID authentication (true/false)
        oftp_sfid_cipher: SFID cipher strength (0=none, 1=3DES, 2=AES-128, 3=AES-192, 4=AES-256)
        oftp_use_gateway: Use OFTP gateway (true/false)
        oftp_use_client_ssl: Use client SSL certificate (true/false)
        oftp_client_ssl_alias: Client SSL certificate alias
        oftp_sfid_sign: Sign files (true/false)
        oftp_sfid_encrypt: Encrypt files (true/false)

    Returns dict (not SDK model) - API accepts minimal structure
    """
    host = kwargs.get('oftp_host')
    if not host:
        return None

    port = int(kwargs.get('oftp_port', 3305))
    tls = kwargs.get('oftp_tls')
    ssid_code = kwargs.get('oftp_ssid_code')
    ssid_password = kwargs.get('oftp_ssid_password')
    compress = kwargs.get('oftp_compress')
    ssid_auth = kwargs.get('oftp_ssid_auth')
    sfid_cipher = kwargs.get('oftp_sfid_cipher')
    use_gateway = kwargs.get('oftp_use_gateway')
    use_client_ssl = kwargs.get('oftp_use_client_ssl')
    client_ssl_alias = kwargs.get('oftp_client_ssl_alias')
    sfid_sign = kwargs.get('oftp_sfid_sign')
    sfid_encrypt = kwargs.get('oftp_sfid_encrypt')

    # Build my partner info (ODETTE partner settings)
    my_partner_info = {'@type': 'OFTPPartnerInfo'}
    if ssid_code:
        my_partner_info['ssidcode'] = ssid_code
    if ssid_password:
        my_partner_info['ssidpswd'] = ssid_password
    if compress is not None:
        my_partner_info['ssidcmpr'] = str(compress).lower() == 'true'
    if sfid_sign is not None:
        my_partner_info['sfidsign'] = str(sfid_sign).lower() == 'true'
    if sfid_encrypt is not None:
        my_partner_info['sfidsec-encrypt'] = str(sfid_encrypt).lower() == 'true'

    # Build defaultOFTPConnectionSettings - Boomi stores values here
    default_settings = {
        '@type': 'DefaultOFTPConnectionSettings',
        'host': host,
        'port': port,
        'myPartnerInfo': my_partner_info
    }

    if tls is not None:
        default_settings['tls'] = str(tls).lower() == 'true'
    if ssid_auth is not None:
        default_settings['ssidauth'] = str(ssid_auth).lower() == 'true'
    if sfid_cipher is not None:
        default_settings['sfidciph'] = int(sfid_cipher)
    if use_gateway is not None:
        default_settings['useGateway'] = str(use_gateway).lower() == 'true'
    if use_client_ssl is not None:
        default_settings['useClientSSL'] = str(use_client_ssl).lower() == 'true'
    if client_ssl_alias:
        default_settings['clientSSLAlias'] = client_ssl_alias

    # Build OFTP connection settings with nested default settings
    connection_settings = {
        '@type': 'OFTPConnectionSettings',
        'defaultOFTPConnectionSettings': default_settings
    }

    return {'@type': 'OFTPCommunicationOptions', 'OFTPConnectionSettings': connection_settings}


class PartnerCommunicationDict(dict):
    """Wrapper to allow dict-based partner communication that serializes correctly.

    Extends dict so that SDK's _define_object and _unmap can iterate over it,
    and provides _map() method for SDK serialization.
    """
    def __init__(self, data: dict):
        super().__init__(data)
        self._data = data

    def _map(self):
        return self._data


def build_partner_communication(**kwargs):
    """
    Build PartnerCommunication from flat protocol parameters.

    Args:
        communication_protocols: Comma-separated list or list of protocols
                                (ftp, sftp, http, as2, mllp, oftp, disk)
        [protocol]_*: Protocol-specific parameters (see individual builders)

    Returns:
        PartnerCommunication object (for Disk) or PartnerCommunicationDict (for others)
    """
    # Parse communication protocols
    protocols = kwargs.get('communication_protocols', [])
    if isinstance(protocols, str):
        protocols = [p.strip().lower() for p in protocols.split(',') if p.strip()]

    if not protocols:
        return None

    # Check if only using Disk
    only_disk = protocols == ['disk']

    if only_disk:
        disk_opts = build_disk_communication_options(**kwargs)
        if disk_opts:
            # Return as PartnerCommunicationDict for consistency
            return PartnerCommunicationDict({'DiskCommunicationOptions': disk_opts})
        return None

    # For other protocols, build as dict (API accepts simpler structure than SDK requires)
    comm_dict = {}

    if 'disk' in protocols:
        disk_opts = build_disk_communication_options(**kwargs)
        if disk_opts:
            comm_dict['DiskCommunicationOptions'] = disk_opts  # Already a dict

    if 'ftp' in protocols:
        ftp_opts = build_ftp_communication_options(**kwargs)
        if ftp_opts:
            comm_dict['FTPCommunicationOptions'] = ftp_opts

    if 'sftp' in protocols:
        sftp_opts = build_sftp_communication_options(**kwargs)
        if sftp_opts:
            comm_dict['SFTPCommunicationOptions'] = sftp_opts

    if 'http' in protocols:
        http_opts = build_http_communication_options(**kwargs)
        if http_opts:
            comm_dict['HTTPCommunicationOptions'] = http_opts

    if 'as2' in protocols:
        as2_opts = build_as2_communication_options(**kwargs)
        if as2_opts:
            comm_dict['AS2CommunicationOptions'] = as2_opts

    if 'mllp' in protocols:
        mllp_opts = build_mllp_communication_options(**kwargs)
        if mllp_opts:
            comm_dict['MLLPCommunicationOptions'] = mllp_opts

    if 'oftp' in protocols:
        oftp_opts = build_oftp_communication_options(**kwargs)
        if oftp_opts:
            comm_dict['OFTPCommunicationOptions'] = oftp_opts

    if not comm_dict:
        return None

    return PartnerCommunicationDict(comm_dict)


# ============================================================================
# Standard-Specific Partner Info Builders
# ============================================================================

def build_x12_partner_info(**kwargs):
    """Build X12-specific partner information

    Maps user-friendly parameters to nested X12PartnerInfo structure:
    - isa_id → IsaControlInfo.interchange_id
    - isa_qualifier → IsaControlInfo.interchange_id_qualifier
    - gs_id → GsControlInfo.applicationcode
    """
    from boomi.models import X12PartnerInfo, X12ControlInfo, IsaControlInfo, GsControlInfo

    isa_id = kwargs.get('isa_id')
    isa_qualifier = kwargs.get('isa_qualifier')
    gs_id = kwargs.get('gs_id')

    if not any([isa_id, isa_qualifier, gs_id]):
        return None

    # Auto-format qualifier if user provides short form (e.g., 'ZZ' -> 'X12IDQUAL_ZZ')
    if isa_qualifier and not isa_qualifier.startswith('X12IDQUAL_'):
        isa_qualifier = f'X12IDQUAL_{isa_qualifier}'

    # Build ISA control info if we have ISA fields
    isa_control_info = None
    if isa_id or isa_qualifier:
        isa_kwargs = {}
        if isa_id:
            isa_kwargs['interchange_id'] = isa_id
        if isa_qualifier:
            isa_kwargs['interchange_id_qualifier'] = isa_qualifier
        isa_control_info = IsaControlInfo(**isa_kwargs)

    # Build GS control info if we have GS fields
    gs_control_info = None
    if gs_id:
        gs_control_info = GsControlInfo(applicationcode=gs_id)

    # Build X12 control info combining ISA and GS
    x12_control_info = None
    if isa_control_info or gs_control_info:
        control_kwargs = {}
        if isa_control_info:
            control_kwargs['isa_control_info'] = isa_control_info
        if gs_control_info:
            control_kwargs['gs_control_info'] = gs_control_info
        x12_control_info = X12ControlInfo(**control_kwargs)

    # Build and return X12PartnerInfo
    if x12_control_info:
        return X12PartnerInfo(x12_control_info=x12_control_info)

    return None


def build_edifact_partner_info(**kwargs):
    """Build EDIFACT-specific partner information.

    Args:
        edifact_interchange_id: Interchange ID (UNB segment)
        edifact_interchange_id_qual: Interchange ID qualifier (e.g., 14 for EAN, ZZ for mutually defined)
        edifact_syntax_id: Syntax identifier (UNOA, UNOB, UNOC, UNOD, UNOE, UNOF)
        edifact_syntax_version: Syntax version (1, 2, 3)
        edifact_test_indicator: Test indicator (1 for test, NA for production)

    Structure: EdifactPartnerInfo → (EdifactControlInfo, EdifactOptions)
    Note: EdifactOptions with delimiters is REQUIRED
    """
    from boomi.models import (
        EdifactPartnerInfo, EdifactControlInfo, EdifactOptions,
        UnbControlInfo, EdiDelimiter, EdiSegmentTerminator
    )
    from boomi.models.unb_control_info import (
        UnbControlInfoInterchangeIdQual, UnbControlInfoSyntaxId,
        UnbControlInfoSyntaxVersion, UnbControlInfoTestIndicator
    )
    from boomi.models.edi_delimiter import DelimiterValue
    from boomi.models.edi_segment_terminator import SegmentTerminatorValue

    interchange_id = kwargs.get('edifact_interchange_id')
    interchange_id_qual = kwargs.get('edifact_interchange_id_qual')
    syntax_id = kwargs.get('edifact_syntax_id')
    syntax_version = kwargs.get('edifact_syntax_version')
    test_indicator = kwargs.get('edifact_test_indicator')

    # Only build if at least one field is provided
    if not any([interchange_id, interchange_id_qual, syntax_id, syntax_version, test_indicator]):
        return None

    # Build UNB control info if any fields provided
    unb_kwargs = {}
    if interchange_id:
        unb_kwargs['interchange_id'] = interchange_id
    if interchange_id_qual:
        # Auto-format qualifier if user provides short form (e.g., '14' -> 'EDIFACTIDQUAL_14')
        if not interchange_id_qual.startswith('EDIFACTIDQUAL_'):
            interchange_id_qual = f'EDIFACTIDQUAL_{interchange_id_qual}'
        unb_kwargs['interchange_id_qual'] = interchange_id_qual
    if syntax_id:
        unb_kwargs['syntax_id'] = syntax_id.upper()  # UNOA, UNOB, etc.
    if syntax_version:
        # Auto-format version (e.g., '3' -> 'EDIFACTSYNTAXVERSION_3')
        if not str(syntax_version).startswith('EDIFACTSYNTAXVERSION_'):
            syntax_version = f'EDIFACTSYNTAXVERSION_{syntax_version}'
        unb_kwargs['syntax_version'] = syntax_version
    if test_indicator:
        # Auto-format (e.g., '1' -> 'EDIFACTTEST_1', 'NA' -> 'EDIFACTTEST_NA')
        if not str(test_indicator).startswith('EDIFACTTEST_'):
            test_indicator = f'EDIFACTTEST_{test_indicator}'
        unb_kwargs['test_indicator'] = test_indicator

    unb_control_info = UnbControlInfo(**unb_kwargs) if unb_kwargs else None
    edifact_control_info = EdifactControlInfo(unb_control_info=unb_control_info) if unb_control_info else EdifactControlInfo()

    # EdifactOptions with REQUIRED delimiters (use EDIFACT defaults)
    # Standard EDIFACT delimiters: + for element, : for composite, ' for segment terminator
    edifact_options = EdifactOptions(
        composite_delimiter=EdiDelimiter(delimiter_value=DelimiterValue.COLONDELIMITED),
        element_delimiter=EdiDelimiter(delimiter_value=DelimiterValue.PLUSDELIMITED),
        segment_terminator=EdiSegmentTerminator(segment_terminator_value=SegmentTerminatorValue.SINGLEQUOTE)
    )

    return EdifactPartnerInfo(
        edifact_control_info=edifact_control_info,
        edifact_options=edifact_options
    )


def build_hl7_partner_info(**kwargs):
    """Build HL7-specific partner information.

    Args:
        hl7_sending_application: Sending application name (MSH-3)
        hl7_sending_facility: Sending facility name (MSH-4)
        hl7_receiving_application: Receiving application name (MSH-5)
        hl7_receiving_facility: Receiving facility name (MSH-6)

    Structure: Hl7PartnerInfo → Hl7ControlInfo → MshControlInfo → HdType
    """
    from boomi.models import Hl7PartnerInfo, Hl7ControlInfo, MshControlInfo, HdType

    sending_app = kwargs.get('hl7_sending_application')
    sending_fac = kwargs.get('hl7_sending_facility')
    receiving_app = kwargs.get('hl7_receiving_application')
    receiving_fac = kwargs.get('hl7_receiving_facility')

    if not any([sending_app, sending_fac, receiving_app, receiving_fac]):
        return None

    # Build MSH control info with HdType objects
    msh_kwargs = {}
    if sending_app:
        msh_kwargs['application'] = HdType(namespace_id=sending_app)
    if sending_fac:
        msh_kwargs['facility'] = HdType(namespace_id=sending_fac)
    # Note: receiving_app and receiving_fac are for the partner (not our MSH)
    # They would be used when building "my company" trading partner

    if not msh_kwargs:
        return None

    msh_control_info = MshControlInfo(**msh_kwargs)
    hl7_control_info = Hl7ControlInfo(msh_control_info=msh_control_info)

    return Hl7PartnerInfo(hl7_control_info=hl7_control_info)


def build_rosettanet_partner_info(**kwargs):
    """Build RosettaNet-specific partner information.

    Args:
        rosettanet_partner_id: Partner ID (DUNS number)
        rosettanet_partner_location: Partner location identifier
        rosettanet_global_usage_code: Test or Production
        rosettanet_supply_chain_code: Supply chain code
        rosettanet_classification_code: Global partner classification code

    Structure: RosettaNetPartnerInfo → RosettaNetControlInfo
    """
    from boomi.models import RosettaNetPartnerInfo, RosettaNetControlInfo
    from boomi.models.rosetta_net_control_info import GlobalUsageCode, PartnerIdType

    partner_id = kwargs.get('rosettanet_partner_id')
    partner_location = kwargs.get('rosettanet_partner_location')
    global_usage_code = kwargs.get('rosettanet_global_usage_code')
    supply_chain_code = kwargs.get('rosettanet_supply_chain_code')
    classification_code = kwargs.get('rosettanet_classification_code')

    if not any([partner_id, partner_location, global_usage_code, supply_chain_code, classification_code]):
        return None

    # Build RosettaNet control info
    ctrl_kwargs = {}
    if partner_id:
        ctrl_kwargs['partner_id'] = partner_id
        ctrl_kwargs['partner_id_type'] = PartnerIdType.DUNS  # Default to DUNS
    if partner_location:
        ctrl_kwargs['partner_location'] = partner_location
    if global_usage_code:
        # Map string to enum
        if global_usage_code.lower() == 'production':
            ctrl_kwargs['global_usage_code'] = GlobalUsageCode.PRODUCTION
        else:
            ctrl_kwargs['global_usage_code'] = GlobalUsageCode.TEST
    if supply_chain_code:
        ctrl_kwargs['supply_chain_code'] = supply_chain_code
    if classification_code:
        ctrl_kwargs['global_partner_classification_code'] = classification_code

    rosettanet_control_info = RosettaNetControlInfo(**ctrl_kwargs)

    return RosettaNetPartnerInfo(rosetta_net_control_info=rosettanet_control_info)


def build_tradacoms_partner_info(**kwargs):
    """Build TRADACOMS-specific partner information.

    Args:
        tradacoms_interchange_id: Interchange ID (STX segment)
        tradacoms_interchange_id_qualifier: Interchange ID qualifier

    Structure: TradacomsPartnerInfo → TradacomsControlInfo → StxControlInfo
    """
    from boomi.models import TradacomsPartnerInfo, TradacomsControlInfo, StxControlInfo

    interchange_id = kwargs.get('tradacoms_interchange_id')
    interchange_id_qualifier = kwargs.get('tradacoms_interchange_id_qualifier')

    if not any([interchange_id, interchange_id_qualifier]):
        return None

    # Build STX control info
    stx_kwargs = {}
    if interchange_id:
        stx_kwargs['interchange_id'] = interchange_id
    if interchange_id_qualifier:
        stx_kwargs['interchange_id_qualifier'] = interchange_id_qualifier

    stx_control_info = StxControlInfo(**stx_kwargs)
    tradacoms_control_info = TradacomsControlInfo(stx_control_info=stx_control_info)

    return TradacomsPartnerInfo(tradacoms_control_info=tradacoms_control_info)


def build_odette_partner_info(**kwargs):
    """Build ODETTE-specific partner information.

    Args:
        odette_interchange_id: Interchange ID (UNB segment)
        odette_interchange_id_qual: Interchange ID qualifier (e.g., 14 for EAN, ZZ for mutually defined)
        odette_syntax_id: Syntax identifier (UNOA, UNOB, UNOC, UNOD, UNOE, UNOF)
        odette_syntax_version: Syntax version (1, 2, 3)
        odette_test_indicator: Test indicator (1 for test, NA for production)

    Structure: OdettePartnerInfo → (OdetteControlInfo, OdetteOptions)
    Note: OdetteOptions with delimiters is REQUIRED (similar to EDIFACT)
    """
    from boomi.models import (
        OdettePartnerInfo, OdetteControlInfo, OdetteOptions,
        OdetteUnbControlInfo, EdiDelimiter, EdiSegmentTerminator
    )
    from boomi.models.edi_delimiter import DelimiterValue
    from boomi.models.edi_segment_terminator import SegmentTerminatorValue

    interchange_id = kwargs.get('odette_interchange_id')
    interchange_id_qual = kwargs.get('odette_interchange_id_qual')
    syntax_id = kwargs.get('odette_syntax_id')
    syntax_version = kwargs.get('odette_syntax_version')
    test_indicator = kwargs.get('odette_test_indicator')

    # Only build if at least one field is provided
    if not any([interchange_id, interchange_id_qual, syntax_id, syntax_version, test_indicator]):
        return None

    # Build ODETTE UNB control info if any fields provided
    unb_kwargs = {}
    if interchange_id:
        unb_kwargs['interchange_id'] = interchange_id
    if interchange_id_qual:
        # Auto-format qualifier if user provides short form (e.g., '14' -> 'ODETTEIDQUAL_14')
        if not interchange_id_qual.startswith('ODETTEIDQUAL_'):
            interchange_id_qual = f'ODETTEIDQUAL_{interchange_id_qual}'
        unb_kwargs['interchange_id_qual'] = interchange_id_qual
    if syntax_id:
        unb_kwargs['syntax_id'] = syntax_id.upper()  # UNOA, UNOB, etc.
    if syntax_version:
        # Auto-format version (e.g., '3' -> 'ODETTESYNTAXVERSION_3')
        if not str(syntax_version).startswith('ODETTESYNTAXVERSION_'):
            syntax_version = f'ODETTESYNTAXVERSION_{syntax_version}'
        unb_kwargs['syntax_version'] = syntax_version
    if test_indicator:
        # Auto-format (e.g., '1' -> 'ODETTETEST_1', 'NA' -> 'ODETTETEST_NA')
        if not str(test_indicator).startswith('ODETTETEST_'):
            test_indicator = f'ODETTETEST_{test_indicator}'
        unb_kwargs['test_indicator'] = test_indicator

    unb_control_info = OdetteUnbControlInfo(**unb_kwargs) if unb_kwargs else None
    odette_control_info = OdetteControlInfo(odette_unb_control_info=unb_control_info) if unb_control_info else OdetteControlInfo()

    # OdetteOptions with REQUIRED delimiters (use ODETTE/EDIFACT defaults)
    # Standard delimiters: + for element, : for composite, ' for segment terminator
    odette_options = OdetteOptions(
        composite_delimiter=EdiDelimiter(delimiter_value=DelimiterValue.COLONDELIMITED),
        element_delimiter=EdiDelimiter(delimiter_value=DelimiterValue.PLUSDELIMITED),
        segment_terminator=EdiSegmentTerminator(segment_terminator_value=SegmentTerminatorValue.SINGLEQUOTE)
    )

    return OdettePartnerInfo(
        odette_control_info=odette_control_info,
        odette_options=odette_options
    )


def build_partner_info(standard: str, **kwargs) -> Optional[PartnerInfo]:
    """
    Build PartnerInfo model based on standard type.

    Args:
        standard: EDI standard (x12, edifact, hl7, rosettanet, custom, tradacoms, odette)
        **kwargs: Standard-specific parameters

    Returns:
        PartnerInfo object with appropriate standard-specific info, None if no fields provided
    """
    partner_info_data = {}

    if standard == 'x12':
        x12_info = build_x12_partner_info(**kwargs)
        if x12_info:
            partner_info_data['x12_partner_info'] = x12_info

    elif standard == 'edifact':
        edifact_info = build_edifact_partner_info(**kwargs)
        if edifact_info:
            partner_info_data['edifact_partner_info'] = edifact_info

    elif standard == 'hl7':
        hl7_info = build_hl7_partner_info(**kwargs)
        if hl7_info:
            partner_info_data['hl7_partner_info'] = hl7_info

    elif standard == 'rosettanet':
        rosettanet_info = build_rosettanet_partner_info(**kwargs)
        if rosettanet_info:
            partner_info_data['rosetta_net_partner_info'] = rosettanet_info

    elif standard == 'tradacoms':
        tradacoms_info = build_tradacoms_partner_info(**kwargs)
        if tradacoms_info:
            partner_info_data['tradacoms_partner_info'] = tradacoms_info

    elif standard == 'odette':
        odette_info = build_odette_partner_info(**kwargs)
        if odette_info:
            partner_info_data['odette_partner_info'] = odette_info

    elif standard == 'custom':
        # Custom standard uses dict for partner info
        custom_info = kwargs.get('custom_partner_info', {})
        if custom_info:
            partner_info_data['custom_partner_info'] = custom_info

    # Return None if no standard-specific info provided
    if not partner_info_data:
        return None

    return PartnerInfo(**partner_info_data)


# ============================================================================
# Main Builder Function
# ============================================================================

def build_trading_partner_model(
    component_name: str,
    standard: str,
    classification: str = "tradingpartner",
    folder_name: str = "Home",
    description: str = "",
    **kwargs
) -> TradingPartnerComponent:
    """
    Build complete TradingPartnerComponent model from flat parameters.

    This is the main entry point for building trading partner JSON models.
    It maps all 70+ flat parameters to the nested JSON structure.

    Args:
        component_name: Trading partner name (required)
        standard: EDI standard (x12, edifact, hl7, rosettanet, custom, tradacoms, odette)
        classification: Partner type (tradingpartner or mycompany), defaults to tradingpartner
        folder_name: Folder location, defaults to "Home"
        description: Component description

        # Contact Information (10 fields)
        contact_name: Contact person name
        contact_email: Email address
        contact_phone: Phone number
        contact_fax: Fax number
        contact_address: Street address line 1
        contact_address2: Street address line 2
        contact_city: City
        contact_state: State/province
        contact_country: Country
        contact_postalcode: Postal/zip code

        # Communication Protocols
        communication_protocols: Comma-separated list or list of protocols

        # Protocol-specific fields (see individual builders for details)
        disk_*: Disk protocol fields
        ftp_*: FTP protocol fields
        sftp_*: SFTP protocol fields
        http_*: HTTP protocol fields (11 fields)
        as2_*: AS2 protocol fields (20 fields)
        oftp_*: OFTP protocol fields

        # Standard-specific fields (see individual builders for details)
        isa_id, isa_qualifier, gs_id: X12 fields
        unb_*: EDIFACT fields (5 fields)
        sending_*, receiving_*: HL7 fields (4 fields)
        duns_number, global_location_number: RosettaNet fields
        sender_code, recipient_code: TRADACOMS fields
        originator_code, destination_code: ODETTE fields
        custom_partner_info: dict for custom standard

    Returns:
        TradingPartnerComponent model ready for API submission

    Example:
        tp = build_trading_partner_model(
            component_name="Acme Corp",
            standard="x12",
            classification="tradingpartner",
            contact_email="orders@acme.com",
            isa_id="ACME",
            isa_qualifier="01",
            communication_protocols="http,as2",
            http_url="https://acme.com/edi",
            as2_url="https://acme.com/as2"
        )
    """
    # Build nested models
    contact_info = build_contact_info(**kwargs)
    partner_communication = build_partner_communication(**kwargs)
    partner_info = build_partner_info(standard, **kwargs)

    # Parse classification enum
    if isinstance(classification, str):
        if classification.lower() == "mycompany":
            classification = TradingPartnerComponentClassification.MYCOMPANY
        else:
            classification = TradingPartnerComponentClassification.TRADINGPARTNER

    # Parse standard enum
    if isinstance(standard, str):
        standard_map = {
            'x12': TradingPartnerComponentStandard.X12,
            'edifact': TradingPartnerComponentStandard.EDIFACT,
            'hl7': TradingPartnerComponentStandard.HL7,
            'custom': TradingPartnerComponentStandard.CUSTOM,
            'rosettanet': TradingPartnerComponentStandard.ROSETTANET,
            'tradacoms': TradingPartnerComponentStandard.TRADACOMS,
            'odette': TradingPartnerComponentStandard.ODETTE
        }
        standard = standard_map.get(standard.lower(), standard)

    # Get organization_id if provided
    organization_id = kwargs.get('organization_id')

    # Build top-level model
    tp_model = TradingPartnerComponent(
        component_name=component_name,
        standard=standard,
        classification=classification,
        folder_name=folder_name,
        description=description,
        partner_info=partner_info,
        contact_info=contact_info,
        partner_communication=partner_communication,
        organization_id=organization_id
    )

    return tp_model
