"""Regression test for BUG-39: cloud attachment rename must omit purge/restart fields.

Cloud attachments don't support purge_history_days, purge_immediate, or
force_restart_time.  Sending them causes an API error.  _action_update must
strip those fields for cloud attachments while preserving them for on-prem
runtimes.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from boomi.models import Atom

from boomi_mcp.categories.runtimes import _action_update


def _make_sdk(current_atom):
    """Return a mock SDK whose atom.get_atom returns *current_atom*."""
    sdk = MagicMock()
    sdk.atom.get_atom.return_value = current_atom
    sdk.atom.update_atom.return_value = current_atom
    return sdk


# -- Cloud attachment ---------------------------------------------------------

def test_cloud_attachment_update_omits_purge_and_restart():
    """Cloud attachment update sends only id_ and name."""
    current = SimpleNamespace(
        id_="cloud-001",
        name="OldName",
        is_cloud_attachment=True,
        purge_history_days=30,
        purge_immediate=False,
        force_restart_time=0,
        type_="CLOUD_ATTACHMENT",
        status="ONLINE",
    )
    sdk = _make_sdk(current)

    result = _action_update(sdk, profile="dev", resource_id="cloud-001", name="NewName")

    assert result["_success"] is True

    # Inspect the Atom that was passed to update_atom
    _, kwargs = sdk.atom.update_atom.call_args
    body: Atom = kwargs["request_body"]
    assert body.id_ == "cloud-001"
    assert body.name == "NewName"
    assert not hasattr(body, 'purge_history_days')
    assert not hasattr(body, 'purge_immediate')
    assert not hasattr(body, 'force_restart_time')


# -- On-prem runtime ---------------------------------------------------------

def test_onprem_runtime_update_preserves_purge_and_restart():
    """On-prem runtime update preserves purge/restart fields from GET."""
    current = SimpleNamespace(
        id_="atom-002",
        name="OldName",
        is_cloud_attachment=False,
        purge_history_days=14,
        purge_immediate=True,
        force_restart_time=120,
        type_="ATOM",
        status="ONLINE",
    )
    sdk = _make_sdk(current)

    result = _action_update(sdk, profile="dev", resource_id="atom-002", name="RenamedAtom")

    assert result["_success"] is True

    _, kwargs = sdk.atom.update_atom.call_args
    body: Atom = kwargs["request_body"]
    assert body.id_ == "atom-002"
    assert body.name == "RenamedAtom"
    assert body.purge_history_days == 14
    assert body.purge_immediate is True
    assert body.force_restart_time == 120


# -- Edge: missing is_cloud_attachment attribute ------------------------------

def test_update_defaults_to_onprem_when_flag_missing():
    """If the GET response lacks is_cloud_attachment, treat as on-prem."""
    current = SimpleNamespace(
        id_="atom-003",
        name="Legacy",
        purge_history_days=30,
        purge_immediate=False,
        force_restart_time=0,
        type_="ATOM",
        status="ONLINE",
        # no is_cloud_attachment attribute
    )
    sdk = _make_sdk(current)

    result = _action_update(sdk, profile="dev", resource_id="atom-003", name="Updated")

    assert result["_success"] is True

    _, kwargs = sdk.atom.update_atom.call_args
    body: Atom = kwargs["request_body"]
    # Should include purge/restart (on-prem default path)
    assert body.purge_history_days == 30
    assert body.force_restart_time == 0
