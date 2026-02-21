"""Tests for HTTP builder in trading_partner_builders.py."""

import sys
import os
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from boomi_mcp.models.trading_partner_builders import build_http_communication_options


class TestAuthTypeValidation:
    """Verify auth type validation and normalization."""

    def test_valid_auth_types(self):
        for auth_type in ['NONE', 'BASIC', 'PASSWORD_DIGEST', 'CUSTOM', 'OAUTH', 'OAUTH2']:
            result = build_http_communication_options(
                http_url='https://example.com',
                http_authentication_type=auth_type
            )
            assert result['HTTPSettings']['authenticationType'] == auth_type

    def test_invalid_auth_type_defaults_to_none(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='INVALID'
        )
        assert result['HTTPSettings']['authenticationType'] == 'NONE'

    def test_case_insensitive_auth_type(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='basic'
        )
        assert result['HTTPSettings']['authenticationType'] == 'BASIC'

    def test_password_digest_auth_sends_credentials(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='PASSWORD_DIGEST',
            http_username='user',
            http_password='pass'
        )
        assert result['HTTPSettings']['HTTPAuthSettings']['user'] == 'user'
        assert result['HTTPSettings']['HTTPAuthSettings']['password'] == 'pass'


class TestOAuth1Builder:
    """Verify OAuth 1.0 settings builder output."""

    def test_oauth1_basic_structure(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='OAUTH',
            http_oauth1_consumer_key='my-key',
            http_oauth1_consumer_secret='my-secret'
        )
        oauth1 = result['HTTPSettings']['HTTPOAuthSettings']
        assert oauth1['consumerKey'] == 'my-key'
        assert oauth1['consumerSecret'] == 'my-secret'

    def test_oauth1_full_settings(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='OAUTH',
            http_oauth1_consumer_key='key',
            http_oauth1_consumer_secret='secret',
            http_oauth1_access_token='token',
            http_oauth1_token_secret='tsecret',
            http_oauth1_realm='myrealm',
            http_oauth1_signature_method='SHA256',
            http_oauth1_request_token_url='https://req.example.com',
            http_oauth1_access_token_url='https://at.example.com',
            http_oauth1_authorization_url='https://auth.example.com',
            http_oauth1_suppress_blank_access_token='true'
        )
        oauth1 = result['HTTPSettings']['HTTPOAuthSettings']
        assert oauth1['consumerKey'] == 'key'
        assert oauth1['tokenSecret'] == 'tsecret'
        assert oauth1['realm'] == 'myrealm'
        assert oauth1['signatureMethod'] == 'SHA256'
        assert oauth1['requestTokenURL'] == 'https://req.example.com'
        assert oauth1['accessTokenURL'] == 'https://at.example.com'
        assert oauth1['authorizationURL'] == 'https://auth.example.com'
        assert oauth1['suppressBlankAccessToken'] is True

    def test_oauth1_not_set_for_basic_auth(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='BASIC',
            http_oauth1_consumer_key='key'
        )
        assert 'HTTPOAuthSettings' not in result['HTTPSettings']


class TestOAuth2Extended:
    """Verify extended OAuth 2.0 settings."""

    def test_oauth2_authorization_token_url(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='OAUTH2',
            http_oauth_token_url='https://token.example.com',
            http_oauth2_authorization_token_url='https://auth.example.com'
        )
        oauth2 = result['HTTPSettings']['HTTPOAuth2Settings']
        assert oauth2['accessTokenEndpoint']['url'] == 'https://token.example.com'
        assert oauth2['authorizationTokenEndpoint']['url'] == 'https://auth.example.com'

    def test_oauth2_credentials_extended(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='OAUTH2',
            http_oauth_client_id='cid',
            http_oauth2_access_token='mytoken',
            http_oauth2_use_refresh_token='true'
        )
        creds = result['HTTPSettings']['HTTPOAuth2Settings']['credentials']
        assert creds['clientId'] == 'cid'
        assert creds['accessToken'] == 'mytoken'
        assert creds['useRefreshToken'] is True

    def test_oauth2_access_token_params(self):
        params = json.dumps([{"name": "resource", "value": "api"}])
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='OAUTH2',
            http_oauth2_access_token_params=params
        )
        oauth2 = result['HTTPSettings']['HTTPOAuth2Settings']
        assert oauth2['accessTokenParameters'] == [{"name": "resource", "value": "api"}]

    def test_oauth2_malformed_json_ignored(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_authentication_type='OAUTH2',
            http_oauth2_access_token_params='not-valid-json'
        )
        oauth2 = result['HTTPSettings']['HTTPOAuth2Settings']
        assert 'accessTokenParameters' not in oauth2


class TestListenOptions:
    """Verify HTTP listen options builder."""

    def test_listen_options_basic(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_listen_object_name='myObject',
            http_listen_username='admin',
            http_listen_password='secret'
        )
        listen = result['HTTPListenOptions']
        assert listen['objectName'] == 'myObject'
        assert listen['username'] == 'admin'
        assert listen['password'] == 'secret'

    def test_listen_options_all_fields(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_listen_mime_passthrough='true',
            http_listen_object_name='obj',
            http_listen_operation_type='QUERY',
            http_listen_password='pass',
            http_listen_use_default='false',
            http_listen_username='user'
        )
        listen = result['HTTPListenOptions']
        assert listen['mimePassthrough'] is True
        assert listen['objectName'] == 'obj'
        assert listen['operationType'] == 'QUERY'
        assert listen['useDefaultListenOptions'] is False

    def test_no_listen_options_when_empty(self):
        result = build_http_communication_options(
            http_url='https://example.com'
        )
        assert 'HTTPListenOptions' not in result


class TestSeparateGetOptions:
    """Verify separate Get vs Send options behavior."""

    def test_explicit_get_options(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_get_method_type='GET',
            http_get_content_type='application/json'
        )
        send = result['HTTPSendOptions']
        get = result['HTTPGetOptions']
        assert send['methodType'] == 'POST'
        assert get['methodType'] == 'GET'
        assert get['dataContentType'] == 'application/json'

    def test_copy_from_send_removes_return_responses(self):
        """When no explicit get options, copy from send but remove returnResponses."""
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_return_responses='true',
            http_return_errors='true'
        )
        send = result['HTTPSendOptions']
        get = result['HTTPGetOptions']
        assert send.get('returnResponses') is True
        assert 'returnResponses' not in get
        # returnErrors should still be copied
        assert get.get('returnErrors') is True

    def test_explicit_get_does_not_copy_send(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_return_responses='true',
            http_get_method_type='GET'
        )
        get = result['HTTPGetOptions']
        assert get['methodType'] == 'GET'
        # Should not have send's returnResponses
        assert 'returnResponses' not in get


class TestHeadersAndPathElements:
    """Verify JSON-based headers and path elements parsing."""

    def test_request_headers_serialized(self):
        """requestHeaders should be present with proper nested structure."""
        headers = json.dumps([{"headerFieldName": "Auth", "targetPropertyName": "Authorization"}])
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_request_headers=headers
        )
        send = result['HTTPSendOptions']
        assert 'requestHeaders' in send
        assert send['requestHeaders']['@type'] == 'HttpRequestHeaders'
        assert send['requestHeaders']['header'] == [
            {"headerFieldName": "Auth", "targetPropertyName": "Authorization", "@type": ""}
        ]

    def test_valid_path_elements(self):
        elements = json.dumps([{"name": "id"}, {"name": "version"}])
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_path_elements=elements
        )
        send = result['HTTPSendOptions']
        assert send['pathElements']['@type'] == 'HttpPathElements'
        assert send['pathElements']['element'] == [
            {"@type": "Element", "name": "id"}, {"@type": "Element", "name": "version"}
        ]

    def test_malformed_json_headers_ignored(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_request_headers='not-json'
        )
        send = result['HTTPSendOptions']
        assert 'requestHeaders' not in send

    def test_response_headers_copied_to_explicit_get(self):
        headers = json.dumps([{"headerFieldName": "Content-Type", "targetPropertyName": "ct"}])
        result = build_http_communication_options(
            http_url='https://example.com',
            http_method_type='POST',
            http_get_method_type='GET',
            http_response_header_mapping=headers
        )
        get = result['HTTPGetOptions']
        assert 'responseHeaderMapping' in get


class TestSettingsFlags:
    """Verify HTTP settings boolean flags."""

    def test_use_custom_auth(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_use_custom_auth='true'
        )
        assert result['HTTPSettings']['useCustomAuth'] is True

    def test_use_basic_auth(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_use_basic_auth='false'
        )
        assert result['HTTPSettings']['useBasicAuth'] is False

    def test_use_default_settings(self):
        result = build_http_communication_options(
            http_url='https://example.com',
            http_use_default_settings='true'
        )
        assert result['HTTPSettings']['useDefaultSettings'] is True

    def test_flags_not_set_when_none(self):
        result = build_http_communication_options(
            http_url='https://example.com'
        )
        assert 'useCustomAuth' not in result['HTTPSettings']
        assert 'useBasicAuth' not in result['HTTPSettings']
        assert 'useDefaultSettings' not in result['HTTPSettings']


class TestNoUrl:
    """Verify builder returns None without URL."""

    def test_returns_none_without_url(self):
        result = build_http_communication_options(
            http_authentication_type='BASIC',
            http_username='user'
        )
        assert result is None
