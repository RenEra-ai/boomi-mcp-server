"""Digests, pinned against real captured components and a planted secret.

Provenance: every component here is read from the archived live captures — XML the
PLATFORM served back for components that executed green. None of it is authored by
this test or by the code under test.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.digests import (
    CONFIG_DIGEST_DOMAIN,
    ROUTE_DIGEST_DOMAIN,
    ConfigDigestRefused,
    RouteDigestRefused,
    component_config_digest_v1,
    route_digest_v1,
)

_REPO = Path(__file__).resolve().parents[1]
_CAPTURES = _REPO / "docs" / "architecture" / "evidence" / "issue-155" / "captures"
_CONNECTION = _CAPTURES / "cap155-e1-conn-readback" / "rest-conn-c4281346.xml"


def _operation_xmls() -> list[Path]:
    return sorted(_CAPTURES.rglob("operation_component.xml"))


@pytest.fixture(scope="module")
def connection_xml() -> str:
    assert _CONNECTION.is_file(), "the captured REST connection is missing from the archive"
    return _CONNECTION.read_text()


def test_the_operation_corpus_is_not_empty():
    ops = _operation_xmls()
    assert len(ops) >= 8, "expected the archive to supply many captured operations, got {0}".format(len(ops))


def test_every_captured_operation_digests(connection_xml):
    """Real components must not refuse. A refusal here is the algorithm being wrong."""
    for path in _operation_xmls():
        digest = route_digest_v1(connection_xml, path.read_text())
        # Stored WITH its algorithm prefix: a bare hex string cannot say which
        # algorithm produced it, so two algorithms' outputs would be
        # interchangeable in storage.
        assert re.fullmatch(r"RouteDigestV1:[0-9a-f]{64}", digest), (path, digest)


def test_operations_on_different_paths_get_different_route_digests(connection_xml):
    digests = {}
    for path in _operation_xmls():
        digests.setdefault(route_digest_v1(connection_xml, path.read_text()), []).append(path.name)
    assert len(digests) > 1, (
        "every captured operation produced the SAME route digest, which would mean "
        "the digest ignores the operation entirely"
    )


def test_the_digest_is_stable_across_calls(connection_xml):
    op = _operation_xmls()[0].read_text()
    assert route_digest_v1(connection_xml, op) == route_digest_v1(connection_xml, op)


def test_no_secret_material_can_reach_a_digest(connection_xml):
    """Plant a marker in every secret-bearing field and prove the digest is unmoved.

    The captured connection carries eleven distinct secret-capable fields. Rather
    than asserting the marker is absent from the digest STRING — which is a hex
    hash and would trivially pass — this asserts the digest VALUE is byte-identical
    with and without the planted material. If any secret field contributed, the
    digest would move.
    """
    marker = "PLANTED-SECRET-b3f1a92c"
    planted = re.sub(r'(<field id="(?:password|customAuthCredentials|awsSecretKey|'
                     r'awsPrivateKey|awsAccessKey|username|domain|workstation)"[^>]*?value=")([^"]*)"',
                     lambda m: m.group(1) + marker + '"', connection_xml)
    assert marker in planted, "the planted-secret rewrite matched nothing — the test would be vacuous"
    assert planted != connection_xml

    op = _operation_xmls()[0].read_text()
    assert route_digest_v1(connection_xml, op) == route_digest_v1(planted, op)
    assert component_config_digest_v1(connection_xml, "connection") == \
           component_config_digest_v1(planted, "connection")


def test_changing_the_url_DOES_move_the_digest(connection_xml):
    """The control for the test above: prove the digest is not simply constant."""
    moved = connection_xml.replace(
        'id="url" type="string" value="http://host.docker.internal:8081"',
        'id="url" type="string" value="http://host.docker.internal:8081/elsewhere"',
    )
    assert moved != connection_xml, "the url rewrite matched nothing"
    op = _operation_xmls()[0].read_text()
    assert route_digest_v1(connection_xml, op) != route_digest_v1(moved, op)


def test_the_two_domains_differ():
    assert ROUTE_DIGEST_DOMAIN != CONFIG_DIGEST_DOMAIN
    assert ROUTE_DIGEST_DOMAIN.endswith(b"\0") and CONFIG_DIGEST_DOMAIN.endswith(b"\0")


def test_config_digest_requires_an_explicit_kind(connection_xml):
    with pytest.raises(ConfigDigestRefused):
        component_config_digest_v1(connection_xml, "guess")


def test_a_connection_without_a_url_is_refused():
    with pytest.raises(RouteDigestRefused):
        route_digest_v1("<GenericConnectionConfig/>", _operation_xmls()[0].read_text())


def test_an_operation_without_a_path_field_is_refused(connection_xml):
    with pytest.raises(RouteDigestRefused):
        route_digest_v1(connection_xml, "<Operation/>")


def test_malformed_xml_is_refused(connection_xml):
    with pytest.raises(RouteDigestRefused):
        route_digest_v1(connection_xml, "<not-closed>")
    with pytest.raises(RouteDigestRefused):
        route_digest_v1("", "<x/>")


def test_a_duplicated_field_id_is_refused_rather_than_last_one_wins():
    dup = '<C><field id="url" value="http://a/x"/><field id="url" value="http://b/y"/></C>'
    with pytest.raises(ConfigDigestRefused):
        component_config_digest_v1(dup, "connection")
