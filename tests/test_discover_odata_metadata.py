"""Issue #13 (M7): handler tests for ``discover_odata_metadata_action``.

URL-only surface. Covers OData v4 + v2 EDMX success (v2 association resolution,
v4 navigation bindings), exact-URL fetch (no /$metadata inference), and the auth
/ malformed / invalid-spec / unreachable / redirect / SSRF / size / DOCTYPE
branches via a mocked streaming client.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import boomi_mcp.categories.schema_discovery as sd
from boomi_mcp.categories.schema_discovery import discover_odata_metadata_action

_PUBLIC_IP = "93.184.216.34"

_EDMX_V4 = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices>
  <Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
   <EntityType Name="Person">
     <Key><PropertyRef Name="Id"/></Key>
     <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
     <Property Name="Name" Type="Edm.String" MaxLength="100"/>
     <NavigationProperty Name="Trips" Type="Collection(NS.Trip)" Partner="Owner"/>
   </EntityType>
   <EntityContainer Name="Container">
     <EntitySet Name="People" EntityType="NS.Person">
       <NavigationPropertyBinding Path="Trips" Target="Trips"/>
     </EntitySet>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""

_EDMX_V2 = b"""<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
 <edmx:DataServices>
  <Schema Namespace="NS" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
   <EntityType Name="Order">
     <Key><PropertyRef Name="Id"/></Key>
     <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
     <NavigationProperty Name="Customer" Relationship="NS.Order_Customer" FromRole="Order" ToRole="Customer"/>
   </EntityType>
   <Association Name="Order_Customer">
     <End Role="Order" Type="NS.Order"/>
     <End Role="Customer" Type="NS.Customer"/>
   </Association>
   <EntityContainer Name="Ctx">
     <EntitySet Name="Orders" EntityType="NS.Order"/>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""


# Real-world OData v2 shape (services.odata.org/V2/Northwind): the LEGACY
# 2007/06/edmx wrapper with Version="1.0", CSDL 3.0 schema ns (2009/11/edm, shared
# with v3), and the authoritative m:MaxDataServiceVersion="2.0" protocol signal.
_EDMX_V2_CSDL3 = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
 <edmx:DataServices m:DataServiceVersion="1.0" m:MaxDataServiceVersion="2.0"
                    xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
  <Schema Namespace="NorthwindModel" xmlns="http://schemas.microsoft.com/ado/2009/11/edm">
   <EntityType Name="Category">
     <Key><PropertyRef Name="CategoryID"/></Key>
     <Property Name="CategoryID" Type="Edm.Int32" Nullable="false"/>
     <Property Name="CategoryName" Type="Edm.String" Nullable="false" MaxLength="15"/>
   </EntityType>
   <EntityContainer Name="NorthwindEntities">
     <EntitySet Name="Categories" EntityType="NorthwindModel.Category"/>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""

# Same wrapper/CSDL, but MaxDataServiceVersion="3.0" -> OData v3, out of scope.
_EDMX_V3_CSDL3 = _EDMX_V2_CSDL3.replace(
    b'm:MaxDataServiceVersion="2.0"', b'm:MaxDataServiceVersion="3.0"'
)


def _stream_client(status=200, body=b"", raise_exc=None):
    resp = MagicMock()
    resp.status_code = status
    resp.iter_bytes.return_value = iter([body] if body else [])
    stream_cm = MagicMock()
    stream_cm.__enter__.return_value = resp
    stream_cm.__exit__.return_value = False
    client = MagicMock()
    if raise_exc is not None:
        client.stream.side_effect = raise_exc
    else:
        client.stream.return_value = stream_cm
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return MagicMock(return_value=client), client


def _public_gai(host, *a, **k):
    return [(2, 1, 6, "", (_PUBLIC_IP, 0))]


def _call(url, body=None, status=200, raise_exc=None, options=None):
    cls, client = _stream_client(status, body or b"", raise_exc)
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_odata_metadata_action(metadata_url=url, options=options)
    return r, cls, client


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------

def test_odata_v4_success():
    r, _, _ = _call("https://svc.example.com/TripPin/$metadata", _EDMX_V4)
    assert r["_success"] is True
    assert r["format"] == "odata_metadata" and r["version"] == "4.0"
    assert r["source_mode"] == "url"
    assert r["schemas"] == ["NS"]
    et = r["entity_types"][0]
    assert et["name"] == "Person" and et["keys"] == ["Id"]
    nav = et["navigation_properties"][0]
    assert nav["collection"] is True and nav["target_type"] == "NS.Trip" and nav["partner"] == "Owner"
    eset = r["entity_sets"][0]
    assert eset["name"] == "People"
    assert eset["navigation_bindings"][0] == {"path": "Trips", "target": "Trips"}


def test_odata_v2_success_association_resolved():
    r, _, _ = _call("https://svc.example.com/odata/$metadata", _EDMX_V2)
    assert r["_success"] is True and r["version"] == "2.0"
    nav = r["entity_types"][0]["navigation_properties"][0]
    assert nav["target_type"] == "NS.Customer"
    assert nav["relationship"] == "Order_Customer"


def test_odata_v2_real_world_csdl3_maxdataserviceversion():
    """Regression (Bug #154): a real v2 service uses CSDL 3.0 (2009/11/edm) shared
    with v3; the m:MaxDataServiceVersion='2.0' protocol signal must classify it
    as v2 rather than falling through to ODATA_INVALID_SPEC."""
    r, _, _ = _call("https://services.odata.org/V2/Northwind/Northwind.svc/$metadata", _EDMX_V2_CSDL3)
    assert r["_success"] is True and r["version"] == "2.0"
    assert r["entity_types"][0]["name"] == "Category"
    assert r["entity_sets"][0]["name"] == "Categories"


def test_odata_v3_maxdataserviceversion_rejected():
    """Same CSDL 3.0 shape but MaxDataServiceVersion='3.0' -> out of scope (v2+v4
    only) -> ODATA_INVALID_SPEC, not silently parsed as v2."""
    r, _, _ = _call("https://services.odata.org/V3/Northwind/Northwind.svc/$metadata", _EDMX_V3_CSDL3)
    assert r["error_code"] == "ODATA_INVALID_SPEC"


def test_exact_url_fetched_no_metadata_appended():
    url = "https://svc.example.com/service"  # note: NO /$metadata
    r, cls, client = _call(url, _EDMX_V4)
    assert r["_success"] is True
    method, fetched_url = client.stream.call_args[0][0], client.stream.call_args[0][1]
    assert method == "GET"
    assert fetched_url == url  # exact URL, unchanged


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def test_missing_url_invalid_input():
    assert discover_odata_metadata_action(metadata_url="")["error_code"] == "ODATA_INVALID_INPUT"


def test_odata_401_auth_failure():
    r, _, _ = _call("https://svc.example.com/$metadata", status=401)
    assert r["error_code"] == "ODATA_AUTH_FAILURE"


def test_odata_malformed_xml_parse_error():
    r, _, _ = _call("https://svc.example.com/$metadata", b"<edmx:Edmx>")
    assert r["error_code"] == "ODATA_PARSE_ERROR"


def test_odata_non_edmx_invalid_spec():
    r, _, _ = _call("https://svc.example.com/$metadata", b"<root/>")
    assert r["error_code"] == "ODATA_INVALID_SPEC"


def test_odata_unknown_version_invalid_spec():
    payload = b"""<edmx:Edmx Version="3.0" xmlns:edmx="http://schemas.microsoft.com/ado/2009/11/edmx">
     <edmx:DataServices><Schema Namespace="NS" xmlns="http://schemas.microsoft.com/ado/2009/11/edm"/></edmx:DataServices></edmx:Edmx>"""
    r, _, _ = _call("https://svc.example.com/$metadata", payload)
    assert r["error_code"] == "ODATA_INVALID_SPEC"


def test_odata_doctype_rejected():
    payload = b'<!DOCTYPE x><edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"/>'
    r, _, _ = _call("https://svc.example.com/$metadata", payload)
    assert r["error_code"] == "ODATA_INVALID_SPEC"


def test_odata_redirect_blocked():
    r, _, _ = _call("https://svc.example.com/$metadata", status=302)
    assert r["error_code"] == "ODATA_REDIRECT_BLOCKED"


def test_odata_timeout_unreachable():
    r, _, _ = _call("https://svc.example.com/$metadata", raise_exc=httpx.ConnectTimeout("t"))
    assert r["error_code"] == "ODATA_UNREACHABLE_ENDPOINT"


def test_odata_ssrf_blocked():
    assert discover_odata_metadata_action(metadata_url="http://169.254.169.254/$metadata")["error_code"] == "ODATA_SSRF_BLOCKED"


def test_odata_size_limit():
    r, _, _ = _call("https://svc.example.com/$metadata", b"<" * 100, options={"max_input_chars": 5})
    assert r["error_code"] == "ODATA_SIZE_LIMIT_EXCEEDED"


def test_odata_error_envelope_flags_and_no_leak():
    r = discover_odata_metadata_action(metadata_url="http://127.0.0.1/$metadata")
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert "127.0.0.1" not in json.dumps(r)


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------

def test_odata_field_truncation():
    r, _, _ = _call("https://svc.example.com/$metadata", _EDMX_V4, options={"max_fields": 1})
    assert r["_success"] is True and r["truncated"] is True


_EDMX_V2_MULT = b"""<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
 <edmx:DataServices m:MaxDataServiceVersion="2.0" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
  <Schema Namespace="NS" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
   <EntityType Name="Order"><Key><PropertyRef Name="Id"/></Key>
     <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
     <NavigationProperty Name="Items" Relationship="NS.Order_Items" FromRole="Order" ToRole="Items"/>
     <NavigationProperty Name="Customer" Relationship="NS.Order_Customer" FromRole="Order" ToRole="Customer"/>
   </EntityType>
   <Association Name="Order_Items">
     <End Role="Order" Type="NS.Order" Multiplicity="1"/>
     <End Role="Items" Type="NS.Item" Multiplicity="*"/>
   </Association>
   <Association Name="Order_Customer">
     <End Role="Order" Type="NS.Order" Multiplicity="*"/>
     <End Role="Customer" Type="NS.Customer" Multiplicity="1"/>
   </Association>
   <EntityContainer Name="Ctx"><EntitySet Name="Orders" EntityType="NS.Order"/></EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""


def test_odata_v2_navigation_collection_from_multiplicity():
    """v2 navigation `collection` must be a boolean derived from the target role's
    Multiplicity ('*' -> collection), per the response contract (§6 impl-review
    #5)."""
    r, _, _ = _call("https://svc.example.com/odata/$metadata", _EDMX_V2_MULT)
    assert r["_success"] is True and r["version"] == "2.0"
    navs = {n["name"]: n for n in r["entity_types"][0]["navigation_properties"]}
    assert navs["Items"]["collection"] is True and navs["Items"]["target_type"] == "NS.Item"
    assert navs["Customer"]["collection"] is False and navs["Customer"]["target_type"] == "NS.Customer"


_EDMX_V2_ALIAS = b"""<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
 <edmx:DataServices m:MaxDataServiceVersion="2.0" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
  <Schema Namespace="My.Long.Namespace" Alias="AA" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
   <EntityType Name="Order"><Key><PropertyRef Name="Id"/></Key>
     <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
     <NavigationProperty Name="Items" Relationship="AA.Rel" FromRole="Order" ToRole="Items"/>
   </EntityType>
   <Association Name="Rel">
     <End Role="Order" Type="AA.Order" Multiplicity="1"/>
     <End Role="Items" Type="AA.Item" Multiplicity="*"/>
   </Association>
   <EntityContainer Name="Ctx"><EntitySet Name="Orders" EntityType="AA.Order"/></EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""


def test_odata_v2_alias_qualified_association_resolved():
    """A navigation referencing an association via a schema ALIAS ('AA.Rel') must
    resolve to that association's multiplicity, not fall back to an ambiguous
    short name (repo-gate: index associations under aliases)."""
    r, _, _ = _call("https://svc.example.com/odata/$metadata", _EDMX_V2_ALIAS)
    assert r["_success"] is True and r["version"] == "2.0"
    nav = r["entity_types"][0]["navigation_properties"][0]
    assert nav["collection"] is True and nav["target_type"] == "AA.Item"


def test_odata_utf16_doctype_rejected():
    """A UTF-16 EDMX with a DOCTYPE must be rejected via the encoding-robust
    screen, not slip past a UTF-8 decode (Codex P1)."""
    payload = (
        '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE x>'
        '<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"/>'
    ).encode("utf-16")
    r, _, _ = _call("https://svc.example.com/$metadata", payload)
    assert r["error_code"] == "ODATA_INVALID_SPEC"
