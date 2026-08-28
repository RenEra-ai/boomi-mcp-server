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


def _operation_xml() -> str:
    return _operation_xmls()[0].read_text()


def test_an_unknown_attribute_on_a_non_field_element_refuses():
    """Scope is every element the projection ADMITS, not the shapes named so far.

    Three consecutive reviews each found one more uncovered shape — a field, then a
    property, then a configuration element — because each fix named a place instead
    of the space. Measured before this rule: a stray attribute on
    `GenericOperationConfig` left the digest byte-identical.
    """
    op = _operation_xml()
    planted = op.replace("<GenericOperationConfig", '<GenericOperationConfig strayAttr="x"', 1)
    assert planted != op, "the plant matched nothing"
    with pytest.raises(ConfigDigestRefused):
        component_config_digest_v1(planted, "operation")


@pytest.mark.parametrize("attribute", ["operationType", "trackResponse",
                                       "returnApplicationErrors"])
def test_a_classified_configuration_attribute_reaches_the_digest(attribute):
    """A carried attribute must MOVE the digest, or classifying it changed nothing."""
    op = _operation_xml()
    found = re.search(rf'{attribute}="([^"]*)"', op)
    assert found, f"{attribute} is absent from the captured operation; this test is vacuous"
    planted = op.replace(found.group(0), f'{attribute}="MUTATED"', 1)
    assert component_config_digest_v1(planted, "operation") != \
           component_config_digest_v1(op, "operation")


def test_a_namespaced_attribute_is_not_the_unqualified_one(connection_xml):
    """`x:type` is a different attribute from `type` and must not pass as it.

    Measured before the QName comparison: the local-name allowlist admitted it while
    the projection read the unqualified name and found nothing, so a field carrying a
    namespaced type digested identically to a field carrying none.
    """
    planted = re.sub(r'(<field id="url"[^>]*?)\s*/>',
                     r'\1 xmlns:x="urn:x" x:type="password"/>', connection_xml, count=1)
    assert planted != connection_xml, "the plant matched nothing"
    with pytest.raises(ConfigDigestRefused):
        component_config_digest_v1(planted, "connection")


def test_the_projection_revision_is_inside_the_digested_payload(connection_xml):
    """Two projections are declared incomparable, so the revision must be digested.

    Recorded beside the digest it is a label; inside it, a component whose projected
    facts happen not to differ still cannot collide across revisions.
    """
    import xml.etree.ElementTree as ET

    from boomi_mcp.connector_replay.digests import (
        _parse, _project_tree, _projection_spec,
    )
    spec = _projection_spec("connection")
    root = _parse(connection_xml, ConfigDigestRefused, "connection")
    payloads = set()
    for revision in (1, 2):
        variant = dict(spec, projection_version=revision)
        payloads.add(ET.canonicalize(
            ET.tostring(_project_tree(root, variant, "connection"), encoding="unicode")))
    assert len(payloads) == 2, "the projection revision does not reach the payload"


def test_an_unknown_attribute_on_a_projected_field_refuses(connection_xml):
    """The third of three: fields closed, elements closed, attributes did not.

    Measured before the fix: a stray attribute on a projected field left the digest
    byte-identical, so a component whose configuration had changed still matched
    captured evidence — the one property this digest exists to provide.
    """
    planted = re.sub(r'(<field id="url"[^>]*?)\s*/>', r'\1 strayAttr="surprise"/>',
                     connection_xml, count=1)
    assert planted != connection_xml, "the attribute plant matched nothing"
    with pytest.raises(ConfigDigestRefused) as caught:
        component_config_digest_v1(planted, "connection")
    assert "strayAttr" in str(caught.value)


def test_an_attribute_outside_the_projection_scope_does_not_refuse(connection_xml):
    """The CONTROL for the refusal above, and the reason it is scoped.

    A component wrapper carries a couple of dozen metadata attributes — author,
    dates, folder, version. Refusing those would refuse every real component while
    proving nothing, so the scope is what the projection digests, not the document.
    """
    # The root element, NOT the XML declaration — an earlier form of this plant hit
    # `<?xml` and produced a parse refusal that read like an over-broad guard.
    root = re.search(r"<(?!\?)[A-Za-z][\w.:-]*", connection_xml)
    assert root, "no root element found"
    planted = connection_xml[:root.end()] + ' strayMeta="x"' + connection_xml[root.end():]
    assert planted != connection_xml, "the metadata plant matched nothing"
    assert component_config_digest_v1(planted, "connection") == \
           component_config_digest_v1(connection_xml, "connection")


def test_changing_a_projected_fields_type_moves_the_digest(connection_xml):
    """`type` is carried, so a retyped routing field is a different configuration.

    It was previously dropped: the projection read `id` and `value` and ignored
    everything else on the element.
    """
    retyped = connection_xml.replace('id="url" type="string"', 'id="url" type="password"', 1)
    assert retyped != connection_xml, "the retype matched nothing"
    assert component_config_digest_v1(retyped, "connection") != \
           component_config_digest_v1(connection_xml, "connection")


def test_every_captured_component_still_digests_under_the_closed_attributes(connection_xml):
    """The whole corpus must pass. A refusal here is the allowlist being wrong."""
    component_config_digest_v1(connection_xml, "connection")
    for path in _operation_xmls():
        component_config_digest_v1(path.read_text(), "operation")


def test_a_property_attribute_is_not_accepted_on_a_field():
    """Classification is element-QUALIFIED, not a flat set of attribute names.

    Generalising the per-shape allowlists to one flat set restored the fail-open one
    level up: `key` is structural on a property and reaches nothing on a field, and a
    connection's excluded `url` reaches nothing on a field either. Both were accepted.
    """
    op = _operation_xml()
    planted = re.sub(r'(<field id="path"[^>]*?)\s*/>', r'\1 key="behavior"/>', op, count=1)
    assert planted != op, "the plant matched nothing"
    with pytest.raises(ConfigDigestRefused):
        component_config_digest_v1(planted, "operation")


def test_an_excluded_attribute_is_not_accepted_on_a_foreign_element(connection_xml):
    """An exclusion is a decision about one element, not a licence everywhere."""
    planted = re.sub(r'(<field id="url"[^>]*?)\s*/>', r'\1 url="http://evil"/>',
                     connection_xml, count=1)
    assert planted != connection_xml, "the plant matched nothing"
    with pytest.raises(ConfigDigestRefused):
        component_config_digest_v1(planted, "connection")


def test_a_carried_attribute_is_bound_to_the_element_that_owns_it():
    """Which element carries an attribute is part of the configuration.

    Keyed only by element NAME, two fields were indistinguishable: moving the sole
    `type` from one to the other left the payload byte-identical. The two documents
    here hold the same attributes and differ only in ownership.
    """
    op = _operation_xml()
    fields = re.findall(r'<field id="(?:path|followRedirects)"[^>]*?/>', op)
    assert len(fields) == 2 and all('type="string"' in f for f in fields), \
        "the captured operation no longer supplies two typed fields; this test is vacuous"
    first, second = fields
    only_first = op.replace(second, second.replace(' type="string"', ''), 1)
    only_second = op.replace(first, first.replace(' type="string"', ''), 1)
    assert only_first != only_second, "the two variants are identical; this test is vacuous"
    assert component_config_digest_v1(only_first, "operation") != \
           component_config_digest_v1(only_second, "operation")


def test_a_carried_attribute_is_bound_to_the_field_id_not_only_its_position():
    """Position among admitted elements is not an identity.

    Constructed collision, measured before the fix: a PROJECTED field and an EXCLUDED
    one both carrying `type`, with ownership swapped, produced the same ordered
    sequence of records — so the routing field's type changed from `string` to
    `boolean` and the digest did not move.
    """
    from boomi_mcp.connector_replay.registry import load_registry

    projection = load_registry().projection_for("connection", "rest")
    excluded = projection.excluded_fields[0]

    def document(url_type, other_type, url_first):
        url = f'<field id="url" type="{url_type}" value="http://host:8081"/>'
        other = f'<field id="{excluded}" type="{other_type}" value="x"/>'
        ordered = [url, other] if url_first else [other, url]
        return "<GenericConnectionConfig>" + "".join(ordered) + "</GenericConnectionConfig>"

    assert component_config_digest_v1(document("string", "boolean", True), "connection") != \
           component_config_digest_v1(document("boolean", "string", False), "connection")


#: The whole projection CONTRACT, pinned per revision: every published projection
#: specification plus the canonical payload each component kind produces. Pinning one
#: document's payload was not the contract — an edit to the operation allowlist, or an
#: excluded connection field, left that pin untouched and let incompatible semantics
#: share a revision. A revision bump adds a row; changing anything under an existing
#: revision fails here.
_CONTRACT_BY_REVISION = {
    4: "f86c4bc66b60fb2a",
}


def _exhaustive_component(projection) -> str:
    """A component exercising EVERY branch the projection can take, derived from it.

    Hand-written fixtures covered the branches I thought of: two minimal documents
    reached the value-field path and neither reached the property-key path, so a
    formatter change to properties alone left the contract digest untouched. The
    document is therefore built FROM the projection — one field per included value
    field, one per included property field with a keyed child, and every admitted
    element that carries a classified attribute — so a new branch cannot be omitted
    by forgetting it.
    """
    parts = []
    for index, field in enumerate(sorted(projection.included_value_fields)):
        parts.append(f'<field id="{field}" type="string" value="v{index}"/>')
    for field in sorted(projection.included_property_fields):
        parts.append(f'<field id="{field}" type="customproperties">'
                     f'<properties key="K-{field}" value="secret"/></field>')
    carried = {}
    for entry in sorted(projection.included_scope_attributes):
        element, _, attribute = entry.partition("/")
        if element != "field":
            carried.setdefault(element, []).append(attribute)
    body = "".join(parts)
    for element in sorted(carried, reverse=True):
        attrs = " ".join(f'{a}="a-{a}"' for a in sorted(carried[element]))
        body = f"<{element} {attrs}>{body}</{element}>"
    root = "GenericConnectionConfig" if projection.component_kind == "connection" else "Operation"
    return body if root in carried else f"<{root}>{body}</{root}>"


def _projection_contract() -> str:
    """A digest over every projection spec AND the payload each kind produces.

    Derived from the registry and the digest function, so it moves for a change to
    either the projection DATA or the payload FORMAT — the two things a revision is
    supposed to distinguish, and the two that were each forgotten once. The payload
    half is exercised over a DERIVED component per kind rather than a hand-written
    one, because a hand-written pair missed a whole branch.
    """
    import hashlib
    import json

    from boomi_mcp.connector_replay.registry import load_registry

    registry = load_registry()
    material = []
    for entry in sorted(registry.projection_allowlists,
                        key=lambda e: (e.family, e.component_kind)):
        material.append(json.loads(entry.model_dump_json()))
        material.append({
            f"{e_family}/{entry.component_kind}": component_config_digest_v1(
                _exhaustive_component(entry), entry.component_kind, entry.family)
            for e_family in (entry.family,)
        })
    payload = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def test_the_contract_fixture_reaches_every_projected_field():
    """The derived document must actually exercise what the projection publishes.

    A coverage claim nobody checks is the defect this whole guard exists to prevent,
    so it is asserted rather than assumed.
    """
    from boomi_mcp.connector_replay.registry import load_registry

    for entry in load_registry().projection_allowlists:
        document = _exhaustive_component(entry)
        for field in entry.included_value_fields:
            assert f'id="{field}"' in document, f"{entry.component_kind}: {field} unreached"
        for field in entry.included_property_fields:
            assert f'id="{field}"' in document and "<properties " in document, (
                f"{entry.component_kind}: the property branch is unreached")


def test_a_projection_change_requires_a_new_revision():
    """Mechanized, because the written rule was forgotten twice running.

    The first version of this guard pinned a single URL-only connection payload, which
    is not the contract: an operation allowlist edit passed it untouched. Measured —
    the planted edit was green under the same revision.
    """
    from boomi_mcp.connector_replay.registry import load_registry

    revisions = {e.projection_version for e in load_registry().projection_allowlists}
    assert len(revisions) == 1, (
        f"the published projections disagree on their revision ({sorted(revisions)}); "
        "a digest cannot say which projection produced it"
    )
    revision = revisions.pop()
    assert revision in _CONTRACT_BY_REVISION, (
        f"projection revision {revision} has no pinned contract; a revision bump adds "
        "a row here, so what it publishes is recorded rather than assumed"
    )
    assert _projection_contract() == _CONTRACT_BY_REVISION[revision], (
        f"the projection contract changed under revision {revision} (pinned "
        f"{_CONTRACT_BY_REVISION[revision]}, got {_projection_contract()}). Advance the "
        "revision with the contract, and add its row above"
    )
