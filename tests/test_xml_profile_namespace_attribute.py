"""Issue #47 follow-up: namespace + attribute support in the XML profile builder
(#26) and inference (#47). Golden references live in
analysis/issue47_fidelity/ground_truth/soap.profile.xml.xml (real Boomi import).

Contract additions under test:
  * element/attribute nodes may carry `namespace`: {"uri": str, "prefix": str|None}
  * a child node with kind="attribute" emits as <XMLAttribute>, placed before
    sibling element children, with no occurrence attributes.
"""
from __future__ import annotations

from xml.etree import ElementTree as ET

from boomi_mcp.categories.components.builders import profile_inference as pi
from boomi_mcp.categories.components.builders.xml_profile_builder import (
    XMLGeneratedProfileBuilder,
)

NS = {"bns": "http://api.platform.boomi.com/"}


def _build(root):
    return XMLGeneratedProfileBuilder().build(
        component_type="profile.xml",
        profile_type="xml.generated",
        component_name="T",
        root=root,
    )


def _root_element(xml):
    comp = ET.fromstring(xml)
    return comp.find("bns:object/XMLProfile/DataElements/XMLElement", NS)


def test_attribute_child_emits_xmlattribute_node_before_elements():
    root = {
        "name": "Timecard",
        "kind": "element",
        "required": True,
        "min_occurs": 1,
        "max_occurs": 1,
        "children": [
            {"name": "version", "kind": "attribute", "data_type": "character", "required": False},
            {"name": "Date", "kind": "element", "data_type": "character", "required": False, "min_occurs": 0, "max_occurs": 1},
        ],
    }
    xml = _build(root)
    el = _root_element(xml)
    assert el is not None and el.attrib["name"] == "Timecard"

    attr = el.find("XMLAttribute")
    assert attr is not None, "attribute child must emit an <XMLAttribute> node"
    assert attr.attrib["name"] == "version"
    assert attr.attrib["dataType"] == "character"
    assert attr.attrib["required"] == "false"
    assert attr.attrib["isMappable"] == "true" and attr.attrib["isNode"] == "true"
    # attributes carry no element-occurrence attributes
    assert "maxOccurs" not in attr.attrib and "minOccurs" not in attr.attrib
    assert attr.find("DataFormat/ProfileCharacterFormat") is not None

    # ordering: the <XMLAttribute> precedes the child <XMLElement> (Boomi layout)
    child_tags = [c.tag for c in list(el) if c.tag in ("XMLAttribute", "XMLElement")]
    assert child_tags == ["XMLAttribute", "XMLElement"], child_tags


def test_text_element_with_attribute_keeps_data_type():
    # <Hours unit="h">7.5</Hours> — a leaf element that also carries an attribute.
    root = {
        "name": "Hours",
        "kind": "element",
        "required": False,
        "min_occurs": 0,
        "max_occurs": 1,
        "data_type": "number",
        "children": [
            {"name": "unit", "kind": "attribute", "data_type": "character", "required": False},
        ],
    }
    xml = _build(root)
    el = _root_element(xml)
    assert el.attrib["dataType"] == "number", "text element must keep its leaf data type"
    assert el.find("DataFormat/ProfileNumberFormat") is not None
    attr = el.find("XMLAttribute[@name='unit']")
    assert attr is not None and attr.attrib["dataType"] == "character"


SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"


def test_namespaced_nodes_emit_namespace_table_and_useNamespace():
    root = {
        "name": "Envelope",
        "kind": "element",
        "required": True,
        "min_occurs": 1,
        "max_occurs": 1,
        "namespace": {"uri": SOAP_NS, "prefix": "soapenv"},
        "children": [
            {"name": "id", "kind": "attribute", "data_type": "character", "required": False},
            {
                "name": "Body",
                "kind": "element",
                "data_type": "character",
                "required": False,
                "min_occurs": 0,
                "max_occurs": 1,
                "namespace": {"uri": SOAP_NS, "prefix": "soapenv"},
            },
        ],
    }
    xml = _build(root)
    comp = ET.fromstring(xml)

    ns_nodes = comp.findall("bns:object/XMLProfile/Namespaces/XMLNamespace", NS)
    by_name = {n.attrib.get("name"): n for n in ns_nodes}
    assert "Empty Namespace" in by_name, "Empty Namespace (key -1) must be preserved"
    assert by_name["Empty Namespace"].attrib["key"] == "-1"
    assert SOAP_NS in by_name, "distinct namespace URI must get an <XMLNamespace> entry"
    soap_key = by_name[SOAP_NS].attrib["key"]
    assert soap_key not in ("-1", "", None)
    assert by_name[SOAP_NS].attrib.get("prefix"), "namespace must carry a prefix"

    env = _root_element(xml)
    assert env.attrib["useNamespace"] == soap_key
    body = env.find("XMLElement[@name='Body']")
    assert body is not None and body.attrib["useNamespace"] == soap_key
    attr = env.find("XMLAttribute[@name='id']")
    assert attr is not None and attr.attrib["useNamespace"] == "-1"


def _emit_from_inferred(inferred):
    return XMLGeneratedProfileBuilder().build(
        component_type="profile.xml",
        profile_type="xml.generated",
        component_name="P",
        root=inferred["profile_config"]["root"],
    )


def test_infer_sample_xml_namespaces_and_attributes_end_to_end():
    sample = '<a:Root xmlns:a="urn:x"><a:Item id="1"><a:Name>n</a:Name></a:Item></a:Root>'
    inferred = pi.infer_profile_from_sample_xml(sample)
    assert inferred["component_type"] == "profile.xml"
    comp = ET.fromstring(_emit_from_inferred(inferred))

    ns_names = [
        n.attrib["name"]
        for n in comp.findall("bns:object/XMLProfile/Namespaces/XMLNamespace", NS)
    ]
    assert "urn:x" in ns_names, ns_names

    root_el = comp.find("bns:object/XMLProfile/DataElements/XMLElement", NS)
    assert root_el.attrib["name"] == "Root" and root_el.attrib["useNamespace"] != "-1"
    item = root_el.find("XMLElement[@name='Item']")
    assert item is not None and item.attrib["useNamespace"] != "-1"
    attr = item.find("XMLAttribute[@name='id']")
    assert attr is not None and attr.attrib["useNamespace"] == "-1"
    assert item.find("XMLElement[@name='Name']") is not None


def test_infer_xsd_namespace_and_attribute_end_to_end():
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" '
        'targetNamespace="urn:po" elementFormDefault="qualified">'
        '<xs:element name="PurchaseOrder"><xs:complexType>'
        '<xs:sequence><xs:element name="Id" type="xs:string"/></xs:sequence>'
        '<xs:attribute name="version" type="xs:string" use="required"/>'
        '</xs:complexType></xs:element></xs:schema>'
    )
    inferred = pi.infer_profile_from_xsd(xsd)
    comp = ET.fromstring(_emit_from_inferred(inferred))

    ns_names = [
        n.attrib["name"]
        for n in comp.findall("bns:object/XMLProfile/Namespaces/XMLNamespace", NS)
    ]
    assert "urn:po" in ns_names

    root_el = comp.find("bns:object/XMLProfile/DataElements/XMLElement", NS)
    assert root_el.attrib["name"] == "PurchaseOrder"
    root_use = root_el.attrib["useNamespace"]
    assert root_use != "-1"

    attr = root_el.find("XMLAttribute[@name='version']")
    assert attr is not None and attr.attrib["required"] == "true"
    assert attr.attrib["useNamespace"] == "-1"  # local attribute, unqualified

    # elementFormDefault=qualified => the nested Id element is also namespaced
    idn = root_el.find("XMLElement[@name='Id']")
    assert idn is not None and idn.attrib["useNamespace"] == root_use


# Aderant-style SOAP payload (namespaces + attributes + xml:lang) — mirrors the
# real Boomi import captured in analysis/issue47_fidelity/ground_truth/soap.profile.xml.xml
SOAP_SAMPLE = (
    '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
    'xmlns:ader="http://aderant.com/expert/services">'
    '<soapenv:Header><ader:AuthToken id="tok-1">v</ader:AuthToken></soapenv:Header>'
    '<soapenv:Body><ader:SubmitTimecard version="2">'
    '<ader:Timecard employeeId="E-100" status="draft">'
    '<ader:Date>2026-01-15</ader:Date>'
    '<ader:Hours unit="h">7.5</ader:Hours>'
    '<ader:Narrative xml:lang="en">x</ader:Narrative>'
    '</ader:Timecard></ader:SubmitTimecard></soapenv:Body></soapenv:Envelope>'
)
SOAP_ENV = "http://schemas.xmlsoap.org/soap/envelope/"
ADER = "http://aderant.com/expert/services"
XML_NS = "http://www.w3.org/XML/1998/namespace"


def test_infer_soap_sample_end_to_end_matches_golden_structure():
    inferred = pi.infer_profile_from_sample_xml(SOAP_SAMPLE)
    comp = ET.fromstring(_emit_from_inferred(inferred))

    ns_by_name = {
        n.attrib["name"]: n.attrib["key"]
        for n in comp.findall("bns:object/XMLProfile/Namespaces/XMLNamespace", NS)
    }
    for uri in (SOAP_ENV, ADER, XML_NS):
        assert uri in ns_by_name, f"{uri} missing from {list(ns_by_name)}"

    env = comp.find("bns:object/XMLProfile/DataElements/XMLElement", NS)
    assert env.attrib["name"] == "Envelope"
    assert env.attrib["useNamespace"] == ns_by_name[SOAP_ENV]

    header = env.find("XMLElement[@name='Header']")
    body = env.find("XMLElement[@name='Body']")
    assert header.attrib["useNamespace"] == ns_by_name[SOAP_ENV]
    assert body.attrib["useNamespace"] == ns_by_name[SOAP_ENV]

    auth = header.find("XMLElement[@name='AuthToken']")
    assert auth.attrib["useNamespace"] == ns_by_name[ADER]
    assert auth.find("XMLAttribute[@name='id']") is not None

    submit = body.find("XMLElement[@name='SubmitTimecard']")
    assert submit.attrib["useNamespace"] == ns_by_name[ADER]
    assert submit.find("XMLAttribute[@name='version']") is not None

    tc = submit.find("XMLElement[@name='Timecard']")
    assert tc.attrib["useNamespace"] == ns_by_name[ADER]
    assert {a.attrib["name"] for a in tc.findall("XMLAttribute")} == {"employeeId", "status"}

    hours = tc.find("XMLElement[@name='Hours']")
    assert hours.find("XMLAttribute[@name='unit']") is not None

    narrative = tc.find("XMLElement[@name='Narrative']")
    lang = narrative.find("XMLAttribute[@name='lang']")
    assert lang is not None and lang.attrib["useNamespace"] == ns_by_name[XML_NS]


def test_preservation_policy_owns_namespaces_table():
    # Codex P2: dynamic <Namespaces> must be replaced on structured update,
    # else regenerated useNamespace keys point at a stale preserved table.
    paths = [p.path for p in XMLGeneratedProfileBuilder.PRESERVATION_POLICY.owned_paths]
    assert "bns:object/XMLProfile/DataElements" in paths
    assert "bns:object/XMLProfile/Namespaces" in paths


def test_xsd_target_namespace_prefixed_type_ref_resolves():
    # Codex P2: type="tns:OrderType" where tns == targetNamespace is a
    # same-document reference, not a foreign one.
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:tns="urn:po" '
        'targetNamespace="urn:po">'
        '<xs:element name="Order" type="tns:OrderType"/>'
        '<xs:complexType name="OrderType"><xs:sequence>'
        '<xs:element name="Id" type="xs:string"/></xs:sequence></xs:complexType>'
        '</xs:schema>'
    )
    idx = pi.infer_profile_from_xsd(xsd)["field_index_by_path"]
    assert idx["Order"]["namespace"]["uri"] == "urn:po"
    assert "Order/Id" in idx


def test_xsd_attribute_form_qualified_namespaces_attribute():
    # Codex P2: attributeFormDefault="qualified" puts local attributes in the
    # target namespace.
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:po" '
        'attributeFormDefault="qualified">'
        '<xs:element name="Order"><xs:complexType>'
        '<xs:sequence><xs:element name="Id" type="xs:string"/></xs:sequence>'
        '<xs:attribute name="rev" type="xs:string"/>'
        '</xs:complexType></xs:element></xs:schema>'
    )
    idx = pi.infer_profile_from_xsd(xsd)["field_index_by_path"]
    assert idx["Order/@rev"]["namespace"]["uri"] == "urn:po"


def test_xsd_attribute_unqualified_by_default():
    # Default attributeFormDefault=unqualified -> attribute stays unqualified.
    xsd = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:po">'
        '<xs:element name="Order"><xs:complexType>'
        '<xs:sequence><xs:element name="Id" type="xs:string"/></xs:sequence>'
        '<xs:attribute name="rev" type="xs:string"/>'
        '</xs:complexType></xs:element></xs:schema>'
    )
    idx = pi.infer_profile_from_xsd(xsd)["field_index_by_path"]
    assert "namespace" not in idx["Order/@rev"]


def test_xsd_rebound_xs_prefix_resolves_via_binding():
    # 'xs' is (unconventionally) bound to the targetNamespace while the XML
    # Schema namespace uses 'xsd'. type="xs:OrderType" is a same-document ref,
    # not a built-in — prefixes are arbitrary, so the URI binding decides.
    xsd = (
        '<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:xs="urn:po" targetNamespace="urn:po">'
        '<xsd:element name="Order" type="xs:OrderType"/>'
        '<xsd:complexType name="OrderType"><xsd:sequence>'
        '<xsd:element name="Id" type="xsd:string"/></xsd:sequence></xsd:complexType>'
        '</xsd:schema>'
    )
    idx = pi.infer_profile_from_xsd(xsd)["field_index_by_path"]
    assert idx["Order"]["namespace"]["uri"] == "urn:po"
    assert "Order/Id" in idx


def test_xsd_conventional_prefix_builtin_resolves_despite_nonxsd_binding():
    # Pathological-but-legal: 'xs' is bound to a non-XSD URI document-wide while
    # the schema uses 'xsd' for XML Schema. A recognized built-in local name
    # under the conventional xs/xsd prefix still resolves as built-in (no full
    # element-scope resolver needed), so the valid schema is NOT rejected.
    xsd = (
        '<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xs="urn:other">'
        '<xsd:element name="R"><xsd:complexType><xsd:sequence>'
        '<xsd:element name="A" type="xs:string"/>'
        '</xsd:sequence></xsd:complexType></xsd:element></xsd:schema>'
    )
    idx = pi.infer_profile_from_xsd(xsd)["field_index_by_path"]
    assert idx["R/A"]["data_type"] == "character"
