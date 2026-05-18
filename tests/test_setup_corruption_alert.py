"""Unit tests for scripts/setup_corruption_alert.py.

All tests mock subprocess so no gcloud calls leave the test process.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

# Load the script as a module despite the lack of __init__.py in scripts/.
_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "setup_corruption_alert.py"
_SPEC = importlib.util.spec_from_file_location("setup_corruption_alert", _SCRIPT)
mod = importlib.util.module_from_spec(_SPEC)
sys.modules["setup_corruption_alert"] = mod
_SPEC.loader.exec_module(mod)


# ---------- build_metric_filter ----------

def test_metric_filter_contains_required_fragments():
    f = mod.build_metric_filter("boomi-mcp-server")
    assert 'resource.type="cloud_run_revision"' in f
    assert 'resource.labels.service_name="boomi-mcp-server"' in f
    assert "severity=ERROR" in f
    assert mod.HEAL_TRIGGER_SUBSTRING in f
    # Substring is exactly what storage_healing_patch.py emits.
    assert "Corrupted oauth client document detected" in f


def test_metric_filter_scopes_to_passed_service_name():
    f = mod.build_metric_filter("staging-mcp")
    assert "staging-mcp" in f
    assert "boomi-mcp-server" not in f


# ---------- build_policy_json ----------

def test_policy_json_basic_shape():
    p = mod.build_policy_json(
        policy_name="my-policy",
        metric_name="my-metric",
        project="my-project",
        threshold=3,
        duration_seconds=300,
        notification_channels=[],
    )
    assert p["displayName"] == "my-policy"
    assert p["combiner"] == "OR"
    assert p["enabled"] is True
    assert p["notificationChannels"] == []
    assert len(p["conditions"]) == 1
    cond = p["conditions"][0]["conditionThreshold"]
    assert cond["thresholdValue"] == 3
    assert cond["duration"] == "0s"  # fire on first breaching point
    assert cond["comparison"] == "COMPARISON_GT"
    assert 'metric.type="logging.googleapis.com/user/my-metric"' in cond["filter"]


def test_policy_json_attaches_notification_channels():
    channels = [
        "projects/p/notificationChannels/1",
        "projects/p/notificationChannels/2",
    ]
    p = mod.build_policy_json(
        policy_name="x",
        metric_name="m",
        project="p",
        threshold=5,
        duration_seconds=60,
        notification_channels=channels,
    )
    assert p["notificationChannels"] == channels


def test_policy_json_alignment_period_matches_duration_seconds():
    p = mod.build_policy_json(
        policy_name="x", metric_name="m", project="p",
        threshold=10, duration_seconds=120, notification_channels=[],
    )
    agg = p["conditions"][0]["conditionThreshold"]["aggregations"][0]
    assert agg["alignmentPeriod"] == "120s"
    assert agg["perSeriesAligner"] == "ALIGN_DELTA"
    assert agg["crossSeriesReducer"] == "REDUCE_SUM"


def test_policy_json_documentation_mentions_kill_switch():
    """The policy doc must include the immediate-response kill switch
    so the on-call sees it without leaving the alert UI."""
    p = mod.build_policy_json(
        policy_name="x", metric_name="m", project="p",
        threshold=3, duration_seconds=300, notification_channels=[],
    )
    doc = p["documentation"]["content"]
    assert "BOOMI_AUTH_HEAL_CORRUPT_CLIENTS=false" in doc
    assert "gcloud run services update" in doc


def test_policy_json_is_serializable():
    p = mod.build_policy_json(
        policy_name="x", metric_name="m", project="p",
        threshold=3, duration_seconds=300, notification_channels=[],
    )
    # Must round-trip through JSON without losing structure.
    assert json.loads(json.dumps(p)) == p


# ---------- gcloud_run dry-run / wet-run ----------

def test_gcloud_run_dry_run_skips_subprocess(capsys, monkeypatch):
    called = {"n": 0}

    def fake_run(*a, **kw):
        called["n"] += 1
        raise AssertionError("subprocess.run must not be called in dry-run mode")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = mod.gcloud_run(["logging", "metrics", "list"], dry_run=True)
    assert result.returncode == 0
    out = capsys.readouterr().out
    assert out.startswith("[DRY-RUN]")
    assert "gcloud logging metrics list" in out
    assert called["n"] == 0


def test_gcloud_run_wet_run_invokes_subprocess(monkeypatch):
    captured = {}

    def fake_run(cmd, **kw):
        captured["cmd"] = cmd
        captured["kw"] = kw
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = mod.gcloud_run(["logging", "metrics", "list"], dry_run=False, capture=True)
    assert result.returncode == 0
    assert captured["cmd"] == ["gcloud", "logging", "metrics", "list"]
    assert captured["kw"]["capture_output"] is True
    assert captured["kw"]["text"] is True


# ---------- idempotency helpers ----------

def test_metric_exists_true_when_describe_succeeds(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="ok", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.metric_exists(project="p", metric_name="m") is True


def test_metric_exists_false_when_describe_fails(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="NOT_FOUND")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.metric_exists(project="p", metric_name="m") is False


def test_policy_exists_true_when_list_returns_id(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(
            args=cmd, returncode=0,
            stdout="projects/p/alertPolicies/12345\n", stderr="",
        )
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.policy_exists(project="p", policy_name="my-policy") is True


def test_policy_exists_false_when_list_empty(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.policy_exists(project="p", policy_name="my-policy") is False


def test_find_policy_id_returns_first_match(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(
            args=cmd, returncode=0,
            stdout="projects/p/alertPolicies/1\nprojects/p/alertPolicies/2\n", stderr="",
        )
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.find_policy_id(project="p", policy_name="x") == "projects/p/alertPolicies/1"


# ---------- gcloud_authed ----------

def test_gcloud_authed_returns_account_when_present(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="user@example.com\n", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.gcloud_authed() == "user@example.com"


def test_gcloud_authed_returns_none_when_no_account(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="\n", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.gcloud_authed() is None


def test_gcloud_authed_returns_none_on_failure(monkeypatch):
    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="not installed")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert mod.gcloud_authed() is None


# ---------- end-to-end --dry-run via main() ----------

def test_main_dry_run_exits_zero_and_prints_commands(monkeypatch, capsys):
    rc = mod.main(["--dry-run", "--notification-channel", "projects/p/notificationChannels/1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Project: boomimcp" in out
    assert "Service: boomi-mcp-server" in out
    assert "Metric:  boomi-mcp-oauth-client-corruption" in out
    assert "Policy:  boomi-mcp-oauth-client-corruption-rate" in out
    assert "[DRY-RUN]" in out
    assert "gcloud logging metrics create" in out
    assert "gcloud alpha monitoring policies create" in out
    assert "Policy JSON that would be created" in out
    assert "DRY-RUN complete. No GCP changes were made." in out


def test_main_wet_run_fails_fast_when_unauthenticated(monkeypatch, capsys):
    monkeypatch.setattr(mod, "gcloud_authed", lambda: None)
    rc = mod.main([])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no active gcloud account" in err
