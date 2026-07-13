"""Warmup environment-variable validation: finite positive durations, positive
integer waiter count, and the 65/60/4 fallbacks with operator warnings.

Runs `import server` in fresh subprocesses (no ML deps needed — cheap manifest
only, eager off, no tool call), matching the test_startup_no_deps conventions.
"""
import json
import os
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = str(Path(_HERE).parents[1])
_SRC = os.path.join(_ROOT, "src")
for _p in (_HERE, _SRC, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pytest

from _fixture_corpus import run_import_server

from boomi_mcp.kb.service import _env_float, _env_int


# --- the shared hardened parsers (also used for the KB query knobs) -----------

@pytest.mark.parametrize("value", ["0", "-1", "3.5", "abc"])
def test_env_int_rejects_non_positive_and_non_int(monkeypatch, value):
    monkeypatch.setenv("BOOMI_TEST_KNOB", value)
    assert _env_int("BOOMI_TEST_KNOB", 7) == 7


@pytest.mark.parametrize("value", ["0", "-0.1", "inf", "-inf", "nan", "xyz"])
def test_env_float_rejects_non_positive_and_non_finite(monkeypatch, value):
    monkeypatch.setenv("BOOMI_TEST_KNOB", value)
    assert _env_float("BOOMI_TEST_KNOB", 0.45) == 0.45


def test_env_parsers_accept_valid_values(monkeypatch):
    monkeypatch.setenv("BOOMI_TEST_KNOB", "5")
    assert _env_int("BOOMI_TEST_KNOB", 7) == 5
    monkeypatch.setenv("BOOMI_TEST_KNOB", "0.3")
    assert _env_float("BOOMI_TEST_KNOB", 0.45) == 0.3

PRINT_WARMUP_CONFIG_SCRIPT = """
import server
print("WARMUP_CONFIG", server._WARMUP_WAIT, server._WARMUP_EXPECTED,
      server._WARMUP_MAX_WAITERS)
"""


def _write_cheap_valid_manifest(dir_path):
    manifest = {
        "schema_version": "1",
        "collection_name": "boomi_docs",
        "embedding_model": "all-MiniLM-L6-v2",
        "chunk_count": 1,
        "build_timestamp": "2026-01-01T00:00:00Z",
    }
    with open(os.path.join(dir_path, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)


def _boot_with(tmp_path, extra_env):
    _write_cheap_valid_manifest(str(tmp_path))
    env = {
        "BOOMI_DOCS_ENABLED": "true",
        "BOOMI_DOCS_WARMUP_EAGER": "false",
        "BOOMI_DOCS_DB_PATH": str(tmp_path),
    }
    env.update(extra_env)
    result = run_import_server(PRINT_WARMUP_CONFIG_SCRIPT, env)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout + result.stderr


def test_defaults_are_65_60_4(tmp_path):
    out = _boot_with(tmp_path, {})
    assert "WARMUP_CONFIG 65.0 60.0 4" in out


def test_valid_overrides_are_applied(tmp_path):
    out = _boot_with(tmp_path, {
        "BOOMI_DOCS_WARMUP_WAIT_SECONDS": "10",
        "BOOMI_DOCS_WARMUP_EXPECTED_SECONDS": "8.5",
        "BOOMI_DOCS_WARMUP_MAX_WAITERS": "2",
    })
    assert "WARMUP_CONFIG 10.0 8.5 2" in out
    assert "[WARNING]" not in out


@pytest.mark.parametrize("bad_wait", ["inf", "-inf", "nan", "-1", "0", "abc"])
def test_invalid_wait_falls_back_with_warning(tmp_path, bad_wait):
    out = _boot_with(tmp_path, {"BOOMI_DOCS_WARMUP_WAIT_SECONDS": bad_wait})
    assert "WARMUP_CONFIG 65.0 60.0 4" in out
    assert "[WARNING] BOOMI_DOCS_WARMUP_WAIT_SECONDS" in out


@pytest.mark.parametrize("bad_expected", ["inf", "nan", "0", "-5", "soon"])
def test_invalid_expected_falls_back_with_warning(tmp_path, bad_expected):
    out = _boot_with(tmp_path, {"BOOMI_DOCS_WARMUP_EXPECTED_SECONDS": bad_expected})
    assert "WARMUP_CONFIG 65.0 60.0 4" in out
    assert "[WARNING] BOOMI_DOCS_WARMUP_EXPECTED_SECONDS" in out


@pytest.mark.parametrize("bad_waiters", ["0", "-2", "2.5", "many"])
def test_invalid_max_waiters_falls_back_with_warning(tmp_path, bad_waiters):
    out = _boot_with(tmp_path, {"BOOMI_DOCS_WARMUP_MAX_WAITERS": bad_waiters})
    assert "WARMUP_CONFIG 65.0 60.0 4" in out
    assert "[WARNING] BOOMI_DOCS_WARMUP_MAX_WAITERS" in out
