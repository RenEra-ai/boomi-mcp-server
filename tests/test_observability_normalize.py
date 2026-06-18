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


def test_str_json_parsed():
    out = rt._normalize_observability_result('{"result": [{"runtimeId": "a"}]}')
    assert out == [{"runtimeId": "a"}]
