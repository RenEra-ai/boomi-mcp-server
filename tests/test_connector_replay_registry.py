"""The packaged registry: dark, fail-closed, and actually shipped."""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.models import RetrySafetyV1
from boomi_mcp.connector_replay.registry import (
    RegistryInvalid,
    _parse,
    load_registry,
)

_REPO = Path(__file__).resolve().parents[1]
_ASSET = _REPO / "src" / "boomi_mcp" / "connector_replay" / "registry_v1.json"
_ASSET_REL = "src/boomi_mcp/connector_replay/registry_v1.json"


def test_the_packaged_registry_ships_dark():
    """No evidence rows and no operation records until a capture is ingested.

    A registry shipping pre-filled rows would assert replay safety no execution
    observed. This is the pin that stops one being added by hand.
    """
    reg = load_registry()
    assert reg.evidence_records == ()
    payload = json.loads(_ASSET.read_text())
    assert payload["evidence_records"] == []
    assert payload["operation_records"] == []


def test_the_vocabulary_is_populated_from_executed_captures():
    reg = load_registry()
    assert reg.vocabulary, "the vocabulary must not be empty; nothing would map"
    assert reg.family_for("officialboomi-X3979C-rest-prod") == "rest"


def test_an_unmapped_connector_type_returns_none_rather_than_guessing():
    reg = load_registry()
    assert reg.family_for("officialboomi-X3979C-dbv2da-prod") is None
    assert reg.family_for("something-invented") is None


def test_every_absence_reads_as_unverified():
    """The fail-closed seam, asserted from several directions.

    Callers may retry only on an explicit affirmative verdict, so an unmapped
    connector, an unobserved action and an empty registry must all arrive as
    ``unverified`` — never as a permissive default.
    """
    reg = load_registry()
    for family, action in [("rest", "PATCH"), ("rest", "DELETE"), ("nope", "GET")]:
        assert reg.retry_safety(family, action) is RetrySafetyV1.UNVERIFIED


def test_a_registry_that_cannot_be_parsed_is_refused_not_skipped():
    with pytest.raises(RegistryInvalid):
        _parse({"schema_version": 999})
    with pytest.raises(RegistryInvalid):
        _parse(["not", "an", "object"])
    with pytest.raises(RegistryInvalid):
        _parse({"schema_version": 1, "vocabulary": [{"platform_connector_type": "x"}]})


def test_a_connector_type_mapped_twice_is_refused():
    dup = {
        "schema_version": 1,
        "vocabulary": [
            {"platform_connector_type": "t", "family": "rest",
             "action_source": "operation_component"},
            {"platform_connector_type": "t", "family": "database",
             "action_source": "operation_component"},
        ],
    }
    with pytest.raises(RegistryInvalid):
        _parse(dup)


def test_no_credential_material_in_the_packaged_asset():
    """The registry is published; a secret reaching it is published with it."""
    text = _ASSET.read_text().lower()
    for needle in ("password", "secret", "token", "credential://", "apikey", "api_key",
                   "authorization", "private_key", "@"):
        assert needle not in text, (
            "the packaged registry contains {0!r}; nothing credential-shaped may "
            "ship in it".format(needle)
        )


def test_the_asset_is_not_excluded_from_the_built_image():
    """A registry the image cannot read is a registry that fails closed at runtime.

    This is the docker-free half of the image-parity check: `importlib.resources`
    finds the file on disk in the test environment regardless, so a `.dockerignore`
    pattern that dropped it would pass every other test here and break only in a
    built image. Checked by simulating the ignore rules rather than by building,
    so it runs everywhere.
    """
    excluded = False
    matched: str | None = None
    for raw in (_REPO / ".dockerignore").read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        negated = line.startswith("!")
        pattern = line[1:] if negated else line
        hit = (
            fnmatch.fnmatch(_ASSET_REL, pattern)
            or fnmatch.fnmatch(_ASSET_REL, pattern.rstrip("/") + "/*")
            or any(fnmatch.fnmatch(part, pattern) for part in _ASSET_REL.split("/"))
        )
        if hit:
            excluded = not negated
            matched = raw
    assert not excluded, (
        "the packaged registry is excluded from the docker context by "
        "{0!r}; it would be absent from the image and every lookup would fail "
        "closed at runtime".format(matched)
    )


def test_the_dockerfile_copies_the_source_tree():
    """Pairs with the test above: exclusion is only half the question."""
    dockerfile = (_REPO / "Dockerfile").read_text()
    assert "COPY" in dockerfile
    copies_tree = any(
        line.strip().startswith("COPY") and line.strip().rstrip().endswith(". .")
        for line in dockerfile.splitlines()
    )
    assert copies_tree, (
        "the Dockerfile no longer copies the whole tree; the packaged registry "
        "needs an explicit COPY if the build became selective"
    )
