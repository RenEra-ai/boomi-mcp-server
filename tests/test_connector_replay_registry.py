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


def test_every_packaged_row_is_reproducible_from_the_capture_archive():
    """No row may exist that re-ingesting the archive would not produce.

    THIS REPLACES THE DARKNESS PIN, which asserted the registry shipped with no
    rows at all. That pin protected one property — that a row asserting replay
    safety no execution observed cannot be added by hand — and it protected it
    only while the registry was empty, so ingesting real evidence would have
    retired the guarantee along with the assertion.

    The property survives in a stronger form: every packaged row must be
    REPRODUCED by re-running the ingester over the checksummed capture archive.
    A hand-written row fails because the ingester would not produce it; an
    altered capture fails because the archive manifest would not verify; and a
    row for a verb whose capture carries no counterparty attestation fails
    because the classifier refuses it. Emptiness was a proxy for that. This is
    the thing itself.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_ingest_script", _REPO / "scripts" / "ingest_connector_replay_evidence.py")
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)

    present = {name: verb for name, verb in script.ACTIONS.items()
               if (script.CAPTURES / name).is_dir()}
    reproduced = script.ingest(
        script.ARCHIVE, [script.CAPTURES / n for n in present],
        family="rest", actions=present,
    )
    payload = json.loads(_ASSET.read_text())
    packaged = payload["evidence_records"]
    assert packaged, (
        "the packaged registry carries no evidence rows, so this check would be "
        "vacuous; if the intent is to ship dark again, delete this test with the "
        "reason rather than leaving it green over nothing"
    )
    expected = [
        json.loads(r.model_dump_json(exclude_none=True))
        for r in sorted(reproduced, key=lambda r: (r.family, r.action))
    ]
    assert packaged == expected, (
        "the packaged rows are not what re-ingesting the archive produces; a row "
        "no capture supports has entered the registry"
    )


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
    # ABSENCES, chosen because they ARE absent: an unmapped connector family, a
    # verb the archive carries no attested capture for, and a family/action pair
    # that never appeared. The list previously named PATCH and DELETE, which was
    # correct only while the registry was empty — now that both are observed, a
    # test asserting they read unverified would be asserting the ingestion failed.
    for family, action in [("nope", "GET"), ("rest", "POST"), ("rest", "CONNECT")]:
        assert reg.retry_safety(family, action) is RetrySafetyV1.UNVERIFIED
    # ...and the positive side, so this is a fail-CLOSED seam rather than a
    # function that returns unverified for everything.
    assert reg.retry_safety("rest", "PATCH") is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT


def test_a_registry_that_cannot_be_parsed_is_refused_not_skipped():
    with pytest.raises(RegistryInvalid):
        _parse({"schema_version": 999})
    with pytest.raises(RegistryInvalid):
        _parse(["not", "an", "object"])
    with pytest.raises(RegistryInvalid):
        _parse({"schema_version": 1, "vocabulary": [{"platform_connector_type": "x"}],
                "evidence_records": [], "operation_records": [],
                "projection_allowlists": [], "semantics_definitions": []})


def test_a_connector_type_mapped_twice_is_refused():
    dup = {
        "schema_version": 1,
        "evidence_records": [],
        "operation_records": [],
        "projection_allowlists": [],
        "semantics_definitions": [],
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
    connection_spec = next(
        s for s in payload["projection_allowlists"] if s["component_kind"] == "connection")
    excluded = set(connection_spec["excluded_fields"])
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
                "operation_records": [], "projection_allowlists": [],
                "semantics_definitions": [], "surprise_rows": [{"family": "rest"}]})
    assert "surprise_rows" in str(err.value)


def test_narrative_keys_are_refused_not_tolerated():
    """The served contract carries NO prose, and this reverses an earlier test.

    An earlier version of the packaged file carried `_comment`, `_provenance` and
    `_known_unmapped`, and this test asserted they were allowed. The design forbids
    them: a served contract that also carries prose invites the prose to drift from
    the contract, and a reader cannot tell which one the code obeys. The
    explanations now live in a document beside the registry, where drifting is
    visible rather than authoritative.
    """
    with pytest.raises(RegistryInvalid) as err:
        _parse({"schema_version": 1, "vocabulary": [], "evidence_records": [],
                "operation_records": [], "projection_allowlists": [],
                "semantics_definitions": [], "_comment": ["prose"]})
    assert "_comment" in str(err.value)


def test_the_prose_that_was_removed_still_exists_somewhere_readable():
    """Removing narrative from a contract must not destroy the reasoning."""
    notes = _REPO / "docs" / "evidence" / "connector-replay-registry-notes.md"
    assert notes.is_file(), "the registry's rationale has no home"
    body = notes.read_text()
    # the three facts that were in the JSON
    assert "officialboomi-X3979C-rest-prod" in body
    assert "dbv2da" in body, "the deliberately-unmapped connector type is unexplained"
    assert "ships empty" in body


def test_a_truncated_registry_is_not_mistaken_for_an_empty_one():
    """The dangerous shape: corruption that reads as a valid deny-all state.

    Defaulting an absent section to `[]` turned a truncated packaged file into a
    registry that looked intentionally empty. Empty IS the safe runtime state, so
    the corruption would have been invisible — it produces exactly the behaviour a
    healthy empty registry produces.
    """
    for missing in ("vocabulary", "evidence_records", "operation_records",
                    "projection_allowlists", "semantics_definitions"):
        payload = {"schema_version": 1, "vocabulary": [], "evidence_records": [],
                   "operation_records": [], "projection_allowlists": [],
                   "semantics_definitions": []}
        del payload[missing]
        with pytest.raises(RegistryInvalid) as err:
            _parse(payload)
        assert missing in str(err.value)


def test_a_section_of_the_wrong_type_is_refused():
    with pytest.raises(RegistryInvalid):
        _parse({"schema_version": 1, "vocabulary": {}, "evidence_records": [],
                "operation_records": [], "projection_allowlists": [],
                "semantics_definitions": []})


def _vocab_only():
    return {"schema_version": 1, "projection_allowlists": [], "semantics_definitions": [],
            "operation_records": [], "evidence_records": [],
            "vocabulary": [{"platform_connector_type": "t", "family": "rest",
                            "action_source": "operation_component",
                            "recognised_actions": ["GET"], "safe_actions": ["GET"]}]}


def _a_row(**over):
    """A row built through the shared factory, so it satisfies the verdict binding."""
    from _connector_replay_factories import evidence_row

    return evidence_row(**over).model_dump(mode="json")


_A_ROW = _a_row()


def test_a_typed_row_that_resolves_to_nothing_is_refused():
    """Field types make a record well-formed; they do not make it authoritative.

    A record can name a family no vocabulary maps and still be perfectly typed. In a
    registry whose records decide whether a write may be retried, well-formed is not
    the bar.
    """
    with pytest.raises(RegistryInvalid) as err:
        _parse({**_vocab_only(), "evidence_records": [_a_row(family="database")]})
    assert "no vocabulary maps" in str(err.value)

    with pytest.raises(RegistryInvalid):
        _parse({**_vocab_only(), "evidence_records": [_a_row(action="BREW")]})


def test_a_resolvable_row_still_loads():
    """The control: cross-record validation must not be a blanket denial."""
    reg = _parse({**_vocab_only(), "evidence_records": [_A_ROW]})
    assert len(reg.evidence_records) == 1
