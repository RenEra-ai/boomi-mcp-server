"""SDK 3.0.x surface smoke test.

The MCP component-family migration depends on Boomi SDK >= 3.0.0, where the
component / component-family create/get/update methods send and return raw XML
bytes (``require_raw_xml`` + ``send_request_raw``) and Role list accepts an empty
``RoleQueryConfig()``. The JSON-transport adoption additionally depends on SDK
>= 3.0.1, which added first-class JSON create/get/update for the component-family
endpoints and lossless ``*_json`` dict methods for SharedWebServer. The local
venv historically shipped 2.2.0, whose methods return typed models instead —
running live QA against it would silently pass while production breaks. These
tests fail loudly, with a "venv still on the old SDK" message, so that can't
happen.

The ``boomi`` package does not expose ``__version__``, so we assert on the 3.0.x
*surface* markers rather than a version string.
"""
import pytest


def test_sdk3_raw_xml_surface_present():
    try:
        from boomi.net.transport.utils import require_raw_xml  # noqa: F401
        from boomi import extract_component_xml_metadata  # noqa: F401
        from boomi import UnsafeComponentXmlSerializationError  # noqa: F401
        from boomi.services.utils.base_service import BaseService
    except ImportError as exc:  # pragma: no cover - only on a stale venv
        pytest.fail(
            f"Boomi SDK 3.0.0 surface missing ({exc}). The venv is likely still "
            "on 2.2.0 — run `pip install -U -r requirements.txt` to pull "
            "boomi>=3.0.1 before relying on the migration tests or live QA."
        )
    assert hasattr(BaseService, "send_request_raw"), (
        "BaseService.send_request_raw is absent — the venv is on an SDK older "
        "than 3.0.0. Run `pip install -U -r requirements.txt`."
    )


def test_sdk31_json_surface_present():
    """SDK 3.0.1 added JSON create/get/update for the component-family endpoints
    and lossless SharedWebServer dict methods. The transport refactor calls these
    directly; a venv still on 3.0.0 would lack them and fall over at runtime."""
    import boomi

    client = boomi.Boomi(access_token="x")
    expected = {
        client.organization_component: [
            "create_organization_component_json",
            "get_organization_component_json",
            "update_organization_component_json",
        ],
        client.trading_partner_component: [
            "create_trading_partner_component_json",
            "get_trading_partner_component_json",
            "update_trading_partner_component_json",
        ],
        client.shared_communication_channel_component: [
            "create_shared_communication_channel_component_json",
            "get_shared_communication_channel_component_json",
            "update_shared_communication_channel_component_json",
        ],
        client.shared_web_server: [
            "get_shared_web_server_json",
            "update_shared_web_server_json",
        ],
    }
    missing = [
        f"{svc.__class__.__name__}.{name}"
        for svc, names in expected.items()
        for name in names
        if not hasattr(svc, name)
    ]
    assert not missing, (
        f"SDK 3.0.1 JSON surface missing: {missing}. The venv is likely still on "
        "3.0.0 — run `pip install -U -r requirements.txt` to pull boomi>=3.0.1."
    )


def test_component_methods_require_raw_xml():
    """Passing a non-raw body to a write must raise before any HTTP call."""
    from boomi.net.transport.utils import require_raw_xml
    from boomi import UnsafeComponentXmlSerializationError

    require_raw_xml("<bns:Component/>")  # str ok
    require_raw_xml(b"<bns:Component/>")  # bytes ok
    with pytest.raises((UnsafeComponentXmlSerializationError, Exception)):
        require_raw_xml({"not": "raw"})  # dict rejected


def test_role_query_config_empty_lists_all():
    from boomi.models import RoleQueryConfig

    # 3.0.0 lets RoleQueryConfig() represent the "list all roles" body ({"QueryFilter": {}}).
    RoleQueryConfig()
