"""Role list uses the SDK typed query with an empty RoleQueryConfig().

SDK 3.0.0's ``RoleQueryConfig()`` serializes to the "list all roles" body, so the
old raw-POST bypass was replaced with ``role.query_role(RoleQueryConfig())``. The
result normalizer handles both typed Role models and raw dict rows, and follows
``queryToken`` pagination.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.models import RoleQueryConfig
import boomi_mcp.categories.account as account


def test_list_all_roles_uses_role_query_config_and_paginates():
    page1 = SimpleNamespace(
        result=[SimpleNamespace(id_="r1", name="Administrator", description="", account_id="acct")],
        query_token="tok",
    )
    page2 = SimpleNamespace(
        result=[SimpleNamespace(id_="r2", name="Standard User", description="", account_id="acct")],
        query_token=None,
    )
    sdk = MagicMock()
    sdk.role.query_role.return_value = page1
    sdk.role.query_more_role.return_value = page2

    roles = account._list_all_roles(sdk)

    # Typed query used with an empty RoleQueryConfig (not a raw Serializer POST).
    cfg = sdk.role.query_role.call_args.kwargs["request_body"]
    assert isinstance(cfg, RoleQueryConfig)
    sdk.role.query_more_role.assert_called_once_with(request_body="tok")
    assert [r["id"] for r in roles] == ["r1", "r2"]
    assert roles[0]["name"] == "Administrator"


def test_list_all_roles_tolerates_raw_dict_rows():
    # On sparse hydration the SDK returns a raw dict instead of typed models.
    raw = {"result": [{"id": "r9", "name": "DictRole", "Description": "d", "accountId": "acct"}]}
    sdk = MagicMock()
    sdk.role.query_role.return_value = raw

    roles = account._list_all_roles(sdk)
    assert roles == [{"id": "r9", "name": "DictRole", "description": "d", "account_id": "acct"}]
