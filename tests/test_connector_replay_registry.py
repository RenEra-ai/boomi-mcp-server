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
        _parse({"schema_version": 1, "vocabulary": [{"platform_connector_type": "x"}],
                "evidence_records": [], "operation_records": []})


def test_a_connector_type_mapped_twice_is_refused():
    dup = {
        "schema_version": 1,
        "evidence_records": [],
        "operation_records": [],
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
    """The registry is published; a secret reaching it is published with it.

    A word-search is not the right test any more, and the reason is worth stating:
    the registry now NAMES credential-bearing fields in its projection exclusion
    list, which is the opposite of shipping their contents. A grep for 'password'
    cannot tell "this field is excluded" from "here is a password", so it would
    force the exclusion list to be nameless — and a nameless exclusion list cannot
    be reviewed.

    What must not ship is a credential VALUE. Every string in the document is
    checked against the field names the exclusions themselves declare, plus the
    shapes a secret takes.
    """
    payload = json.loads(_ASSET.read_text())
    excluded = set(payload["projection_allowlists"]["connection"]["excluded_fields"])
    assert {"password", "awsSecretKey", "privateCertificate"} <= excluded, (
        "the exclusion list no longer names the credential-bearing fields, so this "
        "test cannot tell exclusion from inclusion"
    )

    def strings(node, path="$"):
        if isinstance(node, dict):
            for k, v in node.items():
                yield from strings(v, path + "." + k)
        elif isinstance(node, list):
            for n, v in enumerate(node):
                yield from strings(v, path + "[%d]" % n)
        elif isinstance(node, str):
            yield path, node

    for path, value in strings(payload):
        # A field NAME is allowed only where the schema puts names.
        if value in excluded:
            assert "excluded_fields" in path, (
                "{0} carries the credential-bearing field name {1!r} outside the "
                "exclusion list".format(path, value))
        low = value.lower()
        for shape in ("credential://", "bearer ", "-----begin", "api_key=", "password="):
            assert shape not in low, "{0} contains {1!r}".format(path, shape)
        assert "@" not in value or path.endswith("family"), (
            "{0} contains an address-shaped value: {1!r}".format(path, value))


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


def test_operation_records_are_exposed_not_silently_dropped():
    """The loader read two keys and ignored the rest — including one it ships.

    A loader that silently drops a key its own data declares will one day drop the
    rows that decide whether a write may be retried, and it will do so quietly.
    """
    reg = load_registry()
    assert reg.operation_records == ()
    payload = json.loads(_ASSET.read_text())
    assert "operation_records" in payload, "the packaged file must declare the key"


def test_a_key_this_build_does_not_understand_is_refused():
    """Refusing beats ignoring: an unknown key is a disagreement, not a default."""
    with pytest.raises(RegistryInvalid) as err:
        _parse({"schema_version": 1, "vocabulary": [], "evidence_records": [],
                "operation_records": [], "surprise_rows": [{"family": "rest"}]})
    assert "surprise_rows" in str(err.value)


def test_underscore_prefixed_keys_are_commentary_and_allowed():
    """The packaged file carries prose under `_comment` and `_known_unmapped`."""
    reg = _parse({"schema_version": 1, "vocabulary": [], "evidence_records": [],
                  "operation_records": [],
                  "_comment": ["prose"], "_known_unmapped": [{"x": 1}]})
    assert reg.evidence_records == ()


def test_a_truncated_registry_is_not_mistaken_for_an_empty_one():
    """The dangerous shape: corruption that reads as a valid deny-all state.

    Defaulting an absent section to `[]` turned a truncated packaged file into a
    registry that looked intentionally empty. Empty IS the safe runtime state, so
    the corruption would have been invisible — it produces exactly the behaviour a
    healthy empty registry produces.
    """
    for missing in ("vocabulary", "evidence_records", "operation_records"):
        payload = {"schema_version": 1, "vocabulary": [], "evidence_records": [],
                   "operation_records": []}
        del payload[missing]
        with pytest.raises(RegistryInvalid) as err:
            _parse(payload)
        assert missing in str(err.value)


def test_a_section_of_the_wrong_type_is_refused():
    with pytest.raises(RegistryInvalid):
        _parse({"schema_version": 1, "vocabulary": {}, "evidence_records": [],
                "operation_records": []})
