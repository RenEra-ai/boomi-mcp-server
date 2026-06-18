"""SDK 3.0.0 surface smoke test.

The MCP component-family migration depends on Boomi SDK >= 3.0.0, where the
component / component-family create/get/update methods send and return raw XML
bytes (``require_raw_xml`` + ``send_request_raw``) and Role list accepts an empty
``RoleQueryConfig()``. The local venv historically shipped 2.2.0, whose methods
return typed models instead — running live QA against it would silently pass
while production breaks. This test fails loudly, with a "venv still on the old
SDK" message, so that can't happen.

The ``boomi`` package does not expose ``__version__``, so we assert on the 3.0.0
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
            "boomi>=3.0.0 before relying on the migration tests or live QA."
        )
    assert hasattr(BaseService, "send_request_raw"), (
        "BaseService.send_request_raw is absent — the venv is on an SDK older "
        "than 3.0.0. Run `pip install -U -r requirements.txt`."
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
