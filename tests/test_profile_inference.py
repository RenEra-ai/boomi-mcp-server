"""Issue #47: pure profile-inference layer tests.

Covers the four read-only inference modes (DB metadata, sample JSON, XSD,
sample XML) implemented in
``boomi_mcp.categories.components.builders.profile_inference``. The pure layer
parses caller-supplied artifacts and delegates to the issue-#43 helpers
(``profile_from_db_read_fields`` / ``profile_from_json_schema`` /
``profile_from_xml_schema``); inference metadata lives in a parallel ``fields``
list, never inside the builder nodes.
"""

from __future__ import annotations

import json as _json

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders import profile_inference as pi


# ---------------------------------------------------------------------------
# Task 1 — scaffold: codes, limit clamping, secret-name detection
# ---------------------------------------------------------------------------


def test_error_codes_present():
    for c in (
        "PROFILE_INFERENCE_INVALID_INPUT",
        "PROFILE_INFERENCE_INVALID_SAMPLE",
        "PROFILE_INFERENCE_UNSUPPORTED_SHAPE",
        "PROFILE_INFERENCE_AMBIGUOUS_SHAPE",
        "PROFILE_INFERENCE_INPUT_TOO_LARGE",
        "PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE",
        "PROFILE_INFERENCE_RECURSIVE_XML",
    ):
        assert getattr(pi, c) == c


def test_limits_clamp_and_lower():
    lim = pi._resolve_limits({"max_fields": 10, "max_nodes": 99999999})
    assert lim["max_fields"] == 10  # lowering allowed
    assert lim["max_nodes"] == pi._HARD_CAPS["max_nodes"]  # raise clamped to hard cap


def test_limits_default_when_none():
    lim = pi._resolve_limits(None)
    assert lim == pi._DEFAULT_LIMITS


def test_secret_named_detection_is_exact_not_substring():
    assert (
        pi._is_secret_named("API-Key")
        and pi._is_secret_named("password")
        and pi._is_secret_named("client_secret")
    )
    # exact whole-name match: must NOT false-positive on legit names containing a token
    for ok in (
        "customer_id",
        "authorization_date",
        "token_count",
        "bearer_name",
        "secret_santa_id",
    ):
        assert not pi._is_secret_named(ok)
