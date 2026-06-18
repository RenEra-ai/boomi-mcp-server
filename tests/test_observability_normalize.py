"""Observability async poll normalization (SDK 3.0.0 async token endpoint).

``_action_get_observability_settings`` now polls via
``async_token_runtime_observability_settings(token)`` instead of a raw Serializer
GET. ``_normalize_observability_result`` turns the typed/dict/str/None results
into a settings list (or None to keep polling).
"""
from types import SimpleNamespace

import boomi_mcp.categories.runtimes as rt


def test_none_means_keep_polling():
    assert rt._normalize_observability_result(None) is None


def test_dict_envelope_with_result():
    out = rt._normalize_observability_result({"result": [{"runtimeId": "a"}]})
    assert out == [{"runtimeId": "a"}]


def test_dict_single_result_coerced_to_list():
    out = rt._normalize_observability_result({"result": {"runtimeId": "a"}})
    assert out == [{"runtimeId": "a"}]


def test_async_token_envelope_is_not_ready():
    # An async-operation-token echo is not the settings result.
    assert rt._normalize_observability_result({"@type": "AsyncOperationTokenResult"}) is None


def test_direct_settings_dict_counts_as_ready():
    out = rt._normalize_observability_result({"runtimeId": "a", "generalSettings": {}})
    assert out == [{"runtimeId": "a", "generalSettings": {}}]


def test_typed_response_mapped_to_dicts():
    row = SimpleNamespace(_map=lambda: {"runtimeId": "a"})
    typed = SimpleNamespace(result=[row])
    out = rt._normalize_observability_result(typed)
    assert out == [{"runtimeId": "a"}]


def test_typed_direct_settings_stranded_in_kwargs_is_ready():
    # SDK 3.0.0 hydrates a direct settings payload into the async-response model
    # with result unset and the fields in _kwargs; this must be returned, not
    # treated as "still processing".
    typed = SimpleNamespace(result=None, _kwargs={"runtimeId": "a", "generalSettings": {}})
    out = rt._normalize_observability_result(typed)
    assert out == [{"runtimeId": "a", "generalSettings": {}}]


def test_typed_empty_kwargs_keeps_polling():
    typed = SimpleNamespace(result=None, _kwargs={})
    assert rt._normalize_observability_result(typed) is None


def test_typed_async_token_echo_keeps_polling():
    # A still-processing token echo lands in _kwargs as asyncToken — not ready.
    typed = SimpleNamespace(result=None, _kwargs={"asyncToken": {"token": "t"}})
    assert rt._normalize_observability_result(typed) is None


def test_str_json_parsed():
    out = rt._normalize_observability_result('{"result": [{"runtimeId": "a"}]}')
    assert out == [{"runtimeId": "a"}]
