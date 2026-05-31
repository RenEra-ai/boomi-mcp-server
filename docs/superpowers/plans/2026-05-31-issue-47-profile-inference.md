# Issue #47 — Read-Only Profile Inference Discovery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: this plan is executed **inline in the main session** via superpowers:executing-plans (NOT subagents — the repo CLAUDE.md QA + Codex dev loop must fire in-session). TDD, checkbox steps.

**Goal:** Add a read-only `infer_profile_fields` MCP tool that turns four discovered artifact kinds — DB metadata summaries, sample JSON, XSD, and sample XML — into issue-#43 builder-ready profile-field contracts, marking ambiguity for caller confirmation and never mutating Boomi.

**Architecture:** A pure inference layer parses each artifact and **delegates** to the existing issue-#43 helpers (`profile_from_db_read_fields` / `profile_from_json_schema` / `profile_from_xml_schema`) so the builder contract (`profile_config`, `field_index_by_path`, `mappable_paths`) stays byte-identical to what builders already consume. Inference metadata (confidence / ambiguities / confirmation_required) lives in a **parallel** `fields[]` array + top-level `issues[]` — **never injected into the builder nodes**. A thin action layer wraps the pure fns in the read-only envelope; a `@mcp.tool` wrapper registers it; meta_tools discoverability is updated.

**Tech Stack:** Python 3, stdlib only (`json`, `xml.etree.ElementTree`), pydantic already present. Reuses `BuilderValidationError`, `profile_generation.py`, `_FORBIDDEN_SECRET_FIELDS`. pytest.

---

## Relationship to the architect plan JSON (source of truth) + intentional deviations

Source of truth: `docs/plans/issue_47_profile_inference_discovery_plan.json`. This plan realizes that intent. **Two conscious deviations (flagged for the plan-reviewer / architect):**

1. **File layout.** The architect JSON puts the action at `src/boomi_mcp/categories/profile_inference.py` and says helpers may live "in `profile_generation.py` or a sibling helper". The **user's explicit instruction** (and the resume-state memory) places the pure inference fns in a new **`src/boomi_mcp/categories/components/builders/profile_inference.py`** (sibling of `profile_generation.py`) and the **action `infer_profile_fields_action` in `integration_authoring.py`**. User instruction outranks the JSON on module placement; the *intent* (pure layer + read-only wrapper, reuse #43) is unchanged. Chosen layout:
   - `…/components/builders/profile_inference.py` — `PROFILE_INFERENCE_*` codes + 4 pure `infer_profile_*` fns (parse → delegate).
   - `categories/integration_authoring.py` — `infer_profile_fields_action` (envelope + option normalization + error mapping).
   - `server.py` — `@mcp.tool infer_profile_fields`.
   - `categories/meta_tools.py` — capabilities + schema_template + (A) note rewrites.

2. **`implementation_changes.json_sample_inference` vs `test_plan.json_sample_tests` tension on "empty / heterogeneous arrays".** implementation_changes says "mark … empty arrays, heterogeneous arrays … as ambiguous"; test_plan says "Empty arrays, scalar arrays, … heterogeneous object/scalar arrays … return structured errors." Chosen reconciliation (defensible, satisfies the **tests**, which are the gate):
   - **Structural shapes we cannot represent at all → hard errors** (`PROFILE_INFERENCE_UNSUPPORTED_SHAPE`): scalar root; root/nested array-of-scalars; root/nested empty array; array mixing objects and scalars; heterogeneous-object arrays whose objects cannot be unioned.
   - **Leaf-level type uncertainty that still yields a representable field → ambiguous** (`confidence="ambiguous"`, `confirmation_required=true`, `ready_for_builder=false`, field kept with a safe `character` fallback): mixed scalar types for one key, null-only values, key present in only some array rows (optional-across-rows).
   This is called out explicitly so the architect can overrule toward "ambiguous, not error" for empty/heterogeneous arrays if they prefer.

3. **`reject_unsupported_generation_source` guard kept (NOT removed).** The architect JSON `implementation_changes.profile_generation_helpers` says "stop rejecting the four issue #47 modes" in that dispatcher. We deliberately **keep** the guard rejecting them with `UNSUPPORTED_PROFILE_GENERATION_SOURCE`: #47 inference is a NEW layer (`profile_inference.py`), not a rewrite of the #43 explicit-contract dispatcher, and the existing `test_reject_unsupported_generation_source_known_deferred_modes` (which asserts those 4 modes still raise) must stay green. The JSON's intent — "the four modes now work" — is satisfied by the new `infer_profile_fields` entrypoint, not by mutating the legacy dispatcher.

Everything else follows the JSON verbatim, including: leave the `(B)` `transform.map` `existing_profile_index_discovery` / `MAP_PROFILE_INDEX_UNAVAILABLE` blocks deferred; namespace cases **fail** (never silently strip); stdlib parsers only; no Boomi/credential calls from the wrapper.

---

## File Structure

| Path | Responsibility | Create/Modify |
|------|----------------|---------------|
| `src/boomi_mcp/categories/components/builders/profile_inference.py` | `PROFILE_INFERENCE_*` codes; 4 pure `infer_profile_*` fns; shared parse/limit/type/secret helpers; per-path inference-metadata + `fields[]`/`issues[]` assembly; delegates to #43 helpers. | **Create** |
| `src/boomi_mcp/categories/integration_authoring.py` | `infer_profile_fields_action(source_type, artifact, options=None)` — read-only envelope, option normalization, error→envelope mapping, `ready_for_builder` gating. | Modify |
| `server.py` | Import + `@mcp.tool infer_profile_fields` (readOnlyHint=true, openWorldHint=false); no `profile` param; options JSON-string parse keeping safety flags on parse error. | Modify |
| `src/boomi_mcp/categories/meta_tools.py` | `list_capabilities_action` entry; `get_schema_template` `profile_inference` resource; rewrite (A) `inferred_from_sample_json` / `inferred_from_xsd` notes. | Modify |
| `tests/test_profile_inference.py` | Pure-layer tests (all 4 modes, happy + negative + hygiene). | **Create** |
| `tests/test_infer_profile_fields_wrapper.py` | Action + MCP wrapper tests (envelope, read-only, options, registration, list_capabilities-when-registered, stability). | **Create** |
| `tests/test_schema_template_profile_inference.py` | `get_schema_template(resource_type='profile_inference')` coverage. | **Create** |
| `tests/test_schema_template_profile_json_generated.py` / `…_xml_generated.py` | Add assertions that the (A) notes now point at `infer_profile_fields` (keep existing `#47` assertions green). | Modify |
| `tests/test_meta_tools_list_capabilities.py` | Add `infer_profile_fields` membership/flags assertion. | Modify |

---

## Shared contract (the pure layer returns; the action adds the envelope)

Pure `infer_profile_*` return a dict that **extends** the #43 helper output:

```python
{
    # delegated #43 builder contract — kept byte-identical (no #47 keys injected):
    "generation_mode": <source_type>,        # OVERRIDDEN to the #47 source_type name
    "component_type": "profile.json|profile.xml|profile.db",
    "profile_type":  "json.generated|xml.generated|database.read",
    "component_name": <options.component_name or None>,
    "profile_config": {...},                  # exactly as the #43 helper produced
    "field_index_by_path": {...},             # exactly as the #43 helper produced
    "mappable_paths": [...],                  # exactly as the #43 helper produced
    # issue-#47 enrichment (PARALLEL — never inside field_index_by_path nodes):
    "fields": [
        {"path","name","kind","data_type","required","mappable",
         "confidence": "high|medium|low|ambiguous",
         "ambiguities": [<reason str>, ...],
         "confirmation_required": bool},
        ...   # one per field_index_by_path entry, pre-order (insertion order)
    ],
    "ready_for_builder": bool,                # not truncated AND no confirmation_required AND no blocking issue
    "issues": [ {"severity","code","field","message"}... ],   # advisory warnings/inferences
    "truncated": False,
    "truncation": None,
}
```

The **action** prepends `_success`, `read_only=True`, `boomi_mutation=False`, `raw_xml_exposed=False`.

**Confidence/confirmation policy:** `medium` = safe default applied (e.g. missing nullable → `required=False`) → `confirmation_required=False`, can still be builder-ready. `ambiguous` = uncertainty that needs a human → `confirmation_required=True` → `ready_for_builder=False`. Structural nodes are `high`/no-confirm unless explicitly sample-derived.

**`PROFILE_INFERENCE_*` codes** (module constants, mirrors architect `structured_errors`):
`PROFILE_INFERENCE_INVALID_INPUT`, `PROFILE_INFERENCE_INVALID_SAMPLE`, `PROFILE_INFERENCE_UNSUPPORTED_SHAPE`, `PROFILE_INFERENCE_AMBIGUOUS_SHAPE`, `PROFILE_INFERENCE_INPUT_TOO_LARGE`, `PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE`, `PROFILE_INFERENCE_RECURSIVE_XML`. (`AMBIGUOUS_SHAPE` is used only when a caller would otherwise get an *empty* contract — normal ambiguity is non-fatal via `confirmation_required`.) Propagated #43 codes (`DUPLICATE_PROFILE_FIELD_PATH`, `INVALID_PROFILE_FIELD_PATH`, `UNSUPPORTED_PROFILE_FIELD_TYPE`) surface **verbatim**.

**Limits:** defaults `max_input_chars=200_000`, `max_nodes=1_000`, `max_fields=500`; hard caps `2_000_000 / 10_000 / 5_000`. Effective = `max(1, min(requested, hard_cap))`. Oversize → `_success=False`, `code=PROFILE_INFERENCE_INPUT_TOO_LARGE`, `truncated=True`, `truncation={"kind","limit","observed"}`, `ready_for_builder=False`. **Never** echo artifact content or sample values; counts only.

**Secret hygiene:** reuse `_FORBIDDEN_SECRET_FIELDS` (from `map_builder`). `_is_secret_named(name)` normalizes (lowercase, `-`/space→`_`) then matches by **exact whole-name equality** against the forbidden set (the codebase-canonical `_scan_forbidden_secret_fields` semantics) — **NOT substring** — so legitimate columns like `authorization_date`, `token_count`, `bearer_name`, `secret_santa_id` are NOT withheld, while `password`, `api_key`, `client_secret`, `access_token` are. A leaf whose name is secret-named is **withheld** from the contract and recorded as a surfaced `issue` (`severity="warning"`, `code="PROFILE_INFERENCE_SECRET_FIELD_WITHHELD"`, the path in `field`) — never forwarded into a profile/map. The withheld field is a non-blocking warning: the contract for the remaining real fields is still valid, so `ready_for_builder` reflects the other fields' state (the withholding is surfaced, never silent). (Exact-vs-fuzzy is a judgment call flagged for architect review.)

---

## Task 0: Baseline guard

- [ ] **Step 1:** Confirm green baseline.
  Run: `PYTHONPATH=src python3 -m pytest tests/test_profile_generation.py tests/test_integration_authoring_wrapper.py tests/test_meta_tools_list_capabilities.py tests/test_schema_template_profile_json_generated.py tests/test_schema_template_profile_xml_generated.py -q`
  Expected: all pass (49 + wrappers + capabilities + schema templates).

---

## Task 1: Pure layer scaffold — module, codes, shared helpers

**Files:** Create `src/boomi_mcp/categories/components/builders/profile_inference.py`; Test `tests/test_profile_inference.py`.

- [ ] **Step 1 — failing test** (`tests/test_profile_inference.py`): codes exist + a `_resolve_limits` clamp + `_is_secret_shaped`.
```python
from boomi_mcp.categories.components.builders import profile_inference as pi

def test_error_codes_present():
    for c in ("PROFILE_INFERENCE_INVALID_INPUT","PROFILE_INFERENCE_INVALID_SAMPLE",
              "PROFILE_INFERENCE_UNSUPPORTED_SHAPE","PROFILE_INFERENCE_AMBIGUOUS_SHAPE",
              "PROFILE_INFERENCE_INPUT_TOO_LARGE","PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE",
              "PROFILE_INFERENCE_RECURSIVE_XML"):
        assert getattr(pi, c) == c

def test_limits_clamp_and_lower():
    lim = pi._resolve_limits({"max_fields": 10, "max_nodes": 99999999})
    assert lim["max_fields"] == 10                     # lowering allowed
    assert lim["max_nodes"] == pi._HARD_CAPS["max_nodes"]   # raise clamped to hard cap

def test_secret_named_detection_is_exact_not_substring():
    assert pi._is_secret_named("API-Key") and pi._is_secret_named("password") and pi._is_secret_named("client_secret")
    # exact whole-name match: must NOT false-positive on legit names containing a token
    for ok in ("customer_id","authorization_date","token_count","bearer_name","secret_santa_id"):
        assert not pi._is_secret_named(ok)
```
- [ ] **Step 2 — run, expect ImportError/fail.** `PYTHONPATH=src python3 -m pytest tests/test_profile_inference.py -q`
- [ ] **Step 3 — implement scaffold:** module docstring (safety contract: pure, no Boomi/credentials, no value echo, never inject #47 keys into builder nodes); the 7 `PROFILE_INFERENCE_*` constants + `PROFILE_INFERENCE_SECRET_FIELD_WITHHELD`; `_DEFAULT_LIMITS`, `_HARD_CAPS`, `_resolve_limits(options)->dict`; `_is_secret_shaped(name)` (import `_FORBIDDEN_SECRET_FIELDS` from `..builders.map_builder` — sibling builders import); `_inference_error(code,msg,*,field,hint,details)` returning `BuilderValidationError`; a `_FieldMeta` carrier and `_assemble(helper_result, source_type, meta_by_path, issues, component_name)` that overrides `generation_mode`, builds `fields[]` from `field_index_by_path` + `meta_by_path`, computes `ready_for_builder`.
- [ ] **Step 4 — run, expect PASS.**
- [ ] **Step 5 — commit:** `git add -A && git commit -m "issue #47: profile_inference scaffold (codes, limits, secret + assembly helpers)"`

---

## Task 2: DB metadata inference (`infer_profile_from_db_metadata`)

**Files:** Modify `profile_inference.py`; Test `tests/test_profile_inference.py`.

Type map (case-insensitive keyword scan on the column's `data_type|db_type|jdbc_type|type`): string-like→`character`; numeric→`number`; date/time/timestamp→`datetime`; **boolean/bit→ambiguous** (build as `character`, `confidence="ambiguous"`, `confirmation_required=True`); **binary/blob/image/varbinary/bytea→`PROFILE_INFERENCE_UNSUPPORTED_SHAPE`** (hard); unknown non-binary→ambiguous (`character` fallback). `required` from `nullable|required|mandatory|optional` (`nullable=False`/`required`/`mandatory`/`optional=False` ⇒ required); missing ⇒ `required=False` + `confidence="medium"`. Accept artifact as a bare list, or a dict with one of `columns|fields|result_columns`. Delegate to `profile_from_db_read_fields`; duplicate/reserved-char errors propagate verbatim.

- [ ] **Step 1 — failing tests** (representative; full matrix per architect `db_metadata_tests`):
```python
def test_db_metadata_happy_maps_core_types():
    r = pi.infer_profile_from_db_metadata({"columns": [
        {"name":"name","data_type":"varchar","nullable":False},
        {"name":"qty","data_type":"int"},
        {"name":"created","data_type":"timestamp"}]})
    assert r["generation_mode"] == "profile_from_db_metadata"
    assert r["component_type"] == "profile.db"
    by = {f["name"]: f for f in r["fields"]}
    assert by["name"]["data_type"]=="character" and by["name"]["required"] is True
    assert by["qty"]["data_type"]=="number"
    assert by["created"]["data_type"]=="datetime"
    assert r["ready_for_builder"] is True

def test_db_metadata_accepts_fields_and_result_columns_aliases():
    for key in ("fields","result_columns"):
        r = pi.infer_profile_from_db_metadata({key:[{"name":"a","data_type":"varchar"}]})
        assert r["mappable_paths"] == ["a"]

def test_db_metadata_missing_nullable_lowers_confidence_not_required():
    r = pi.infer_profile_from_db_metadata({"columns":[{"name":"a","data_type":"varchar"}]})
    f = r["fields"][0]
    assert f["required"] is False and f["confidence"]=="medium" and f["confirmation_required"] is False
    assert r["ready_for_builder"] is True

def test_db_metadata_boolean_is_ambiguous_candidate():
    r = pi.infer_profile_from_db_metadata({"columns":[{"name":"flag","data_type":"bit"}]})
    f = r["fields"][0]
    assert f["confidence"]=="ambiguous" and f["confirmation_required"] is True
    assert f["data_type"]=="character" and r["ready_for_builder"] is False

def test_db_metadata_binary_unsupported():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"columns":[{"name":"blobby","data_type":"varbinary"}]})
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE

def test_db_metadata_missing_type_is_invalid_input():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"columns":[{"name":"a"}]})
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_INPUT

def test_db_metadata_duplicate_name_propagates_43_error():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_db_metadata({"columns":[
            {"name":"a","data_type":"varchar"},{"name":"a","data_type":"int"}]})
    assert e.value.error_code == "DUPLICATE_PROFILE_FIELD_PATH"

def test_db_metadata_secret_named_field_withheld():
    r = pi.infer_profile_from_db_metadata({"columns":[
        {"name":"id","data_type":"int"},{"name":"password","data_type":"varchar"}]})
    assert [f["name"] for f in r["fields"]] == ["id"]
    assert any(i["code"]=="PROFILE_INFERENCE_SECRET_FIELD_WITHHELD" for i in r["issues"])
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** `infer_profile_from_db_metadata(artifact, *, options=None)`: validate container; extract columns; per column resolve name/type/required + meta; withhold secret-named; if zero usable fields after withholding → `PROFILE_INFERENCE_UNSUPPORTED_SHAPE`/`INVALID_INPUT`; call `profile_from_db_read_fields(resolved, component_name=...)`; `_assemble`.
- [ ] **Step 4 — run, expect PASS** (`-q tests/test_profile_inference.py -k db_metadata`).
- [ ] **Step 5 — commit:** `git commit -am "issue #47: DB-metadata inference mode"`

---

## Task 3: JSON sample inference (`infer_profile_from_sample_json`)

**Files:** Modify `profile_inference.py`; Test `tests/test_profile_inference.py`.

Accept `str` (→`json.loads`, errors→`PROFILE_INFERENCE_INVALID_SAMPLE`) or parsed `dict`/`list`. Object root → object node; array root of compatible objects → synthetic `Root` object with one `array` child (name from `options.array_item_name` or `"items"`) whose children are the **union** of object keys. Scalar/datetime detection (bool checked before int): `bool`→`boolean`; `int/float`→`number`; `str`→`datetime` if `options.datetime_detection` (default True) and ISO-like else `character`. Leaf ambiguity (→`character` fallback, `confidence="ambiguous"`, `confirmation_required=True`): mixed scalar types across rows, null-only, key present in only some rows (also `required=False`). Hard `PROFILE_INFERENCE_UNSUPPORTED_SHAPE`: scalar root, array-of-scalars (root or nested), empty array (root or nested), object/scalar-mixed array, un-unionable heterogeneous arrays, nested scalar arrays. Enforce `max_nodes`/`max_fields`. No value echo. Delegate to `profile_from_json_schema`.

- [ ] **Step 1 — failing tests** (representative; full matrix per `json_sample_tests`):
```python
def test_json_nested_object_paths():
    r = pi.infer_profile_from_sample_json('{"id":1,"name":"x","child":{"leaf":true}}')
    assert r["generation_mode"]=="profile_from_sample_json" and r["component_type"]=="profile.json"
    paths = set(r["field_index_by_path"])
    assert {"Root","Root/id","Root/name","Root/child","Root/child/leaf"} <= paths
    by={f["path"]:f for f in r["fields"]}
    assert by["Root/id"]["data_type"]=="number" and by["Root/child/leaf"]["data_type"]=="boolean"

def test_json_array_of_objects_uses_brackets_and_optional():
    r = pi.infer_profile_from_sample_json('[{"a":1,"b":2},{"a":3}]')
    assert "Root/items[]/a" in r["field_index_by_path"]
    by={f["path"]:f for f in r["fields"]}
    assert by["Root/items[]/b"]["required"] is False           # missing in row 2
    assert by["Root/items[]/b"]["confirmation_required"] is True
    assert r["ready_for_builder"] is False

def test_json_iso_datetime_detection():
    r = pi.infer_profile_from_sample_json('{"ts":"2026-01-01T00:00:00Z"}')
    assert {f["path"]:f for f in r["fields"]}["Root/ts"]["data_type"]=="datetime"

def test_json_mixed_scalar_is_ambiguous_not_error():
    r = pi.infer_profile_from_sample_json('[{"v":1},{"v":"x"}]')
    f={f["path"]:f for f in r["fields"]}["Root/items[]/v"]
    assert f["confidence"]=="ambiguous" and f["data_type"]=="character" and r["ready_for_builder"] is False

import pytest
@pytest.mark.parametrize("sample,code",[
    ('"just a string"', pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE),    # scalar root
    ('[1,2,3]',          pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE),    # array of scalars
    ('[]',               pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE),    # empty array
    ('[{"a":1}, 5]',     pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE),    # object/scalar mix
    ('{not json',        pi.PROFILE_INFERENCE_INVALID_SAMPLE),
])
def test_json_structural_errors(sample,code):
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_json(sample)
    assert e.value.error_code == code

def test_json_does_not_echo_values():
    import json as _j
    r = pi.infer_profile_from_sample_json('{"secretish_note":"SENSITIVE-VALUE-123"}')
    assert "SENSITIVE-VALUE-123" not in _j.dumps(r)
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** parse + `_json_tree_from_sample` (returns `{format:"json","root":...}` + `meta_by_path`) + node/field counting + delegate + `_assemble`.
- [ ] **Step 4 — run, expect PASS** (`-k json`).
- [ ] **Step 5 — commit:** `git commit -am "issue #47: JSON-sample inference mode"`

---

## Task 4: XSD inference (`infer_profile_from_xsd`)

**Files:** Modify `profile_inference.py`; Test `tests/test_profile_inference.py`.

Stdlib `xml.etree.ElementTree`. Safe parse helper `_safe_fromstring(text)`: reject `<!DOCTYPE`/`<!ENTITY` substrings (case-insensitive, pre-parse) → `PROFILE_INFERENCE_INVALID_SAMPLE`; parse errors → `INVALID_SAMPLE`. (This DOCTYPE/ENTITY pre-screen is the deliberate stdlib-only XXE / billion-laughs mitigation — stdlib ET does not expand external entities by default, and rejecting `<!ENTITY`/`<!DOCTYPE` outright also blocks internal-entity expansion bombs; comment it as such so a reviewer doesn't think defusedxml was forgotten.) Supported subset: top-level/inline `xs:element`, `xs:complexType`, `xs:sequence`, `xs:simpleType` restriction `base`, `minOccurs`/`maxOccurs`(+`unbounded`). Type map: string-like→`character`; `decimal|integer|int|long|short|byte|float|double|...`→`number`; `date|time|dateTime`→`datetime`; `boolean`→`boolean`. Reject (`PROFILE_INFERENCE_UNSUPPORTED_SHAPE`): `xs:choice|all|any|attribute`, mixed content, `import|include`, external `schemaLocation`, `complexContent|extension|restriction`(complex), `list|union`, substitution groups. `targetNamespace` or any prefixed (non-xsd) qualified element → `PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE`. Self-referential type on the resolution path → `PROFILE_INFERENCE_RECURSIVE_XML`. Build `{format:"xml","root":...}`, delegate to `profile_from_xml_schema`.

- [ ] **Step 1 — failing tests** (representative; full matrix per `xsd_tests`):
```python
_XSD_OK = '''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Order"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Qty" type="xs:int"/>
    <xs:element name="When" type="xs:dateTime"/>
    <xs:element name="Active" type="xs:boolean"/>
    <xs:element name="Line" maxOccurs="unbounded"><xs:complexType><xs:sequence>
        <xs:element name="Sku" type="xs:string"/>
    </xs:sequence></xs:complexType></xs:element>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>'''

def test_xsd_happy_subset():
    r = pi.infer_profile_from_xsd(_XSD_OK)
    assert r["generation_mode"]=="profile_from_xsd" and r["component_type"]=="profile.xml"
    idx=set(r["field_index_by_path"])
    assert "Order/Id" in idx and "Order[]/Line[]/Sku" in idx or "Order/Line[]/Sku" in idx
    by={f["path"]:f for f in r["fields"]}
    assert by["Order/Id"]["data_type"]=="character" and by["Order/Active"]["data_type"]=="boolean"

def test_xsd_invalid_xml():
    with pytest.raises(BuilderValidationError) as e: pi.infer_profile_from_xsd("<xs:schema")
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_SAMPLE

@pytest.mark.parametrize("frag",[
  '<xs:choice><xs:element name="a" type="xs:string"/></xs:choice>',
  '<xs:attribute name="a" type="xs:string"/>',
  '<xs:any/>',
])
def test_xsd_unsupported_constructs(frag):
    xsd=f'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="R"><xs:complexType><xs:sequence>{frag}</xs:sequence></xs:complexType></xs:element></xs:schema>'
    with pytest.raises(BuilderValidationError) as e: pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE

def test_xsd_target_namespace_rejected():
    xsd='<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:x"><xs:element name="R" type="xs:string"/></xs:schema>'
    with pytest.raises(BuilderValidationError) as e: pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE

def test_xsd_recursive_type():
    xsd='''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Node" type="NodeT"/>
      <xs:complexType name="NodeT"><xs:sequence>
        <xs:element name="Child" type="NodeT"/></xs:sequence></xs:complexType></xs:schema>'''
    with pytest.raises(BuilderValidationError) as e: pi.infer_profile_from_xsd(xsd)
    assert e.value.error_code == pi.PROFILE_INFERENCE_RECURSIVE_XML

def test_xsd_doctype_rejected():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_xsd('<!DOCTYPE x><xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"/>')
    assert e.value.error_code == pi.PROFILE_INFERENCE_INVALID_SAMPLE
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** `_safe_fromstring`, `_XSD = "{http://www.w3.org/2001/XMLSchema}"`, top-level type table, `_xsd_element_to_node(el, complex_types, type_path)` (recursion guard via `type_path` set), namespace guards, delegate.
- [ ] **Step 4 — run, expect PASS** (`-k xsd`).
- [ ] **Step 5 — commit:** `git commit -am "issue #47: XSD inference mode (conservative subset)"`

---

## Task 5: XML sample inference (`infer_profile_from_sample_xml`)

**Files:** Modify `profile_inference.py`; Test `tests/test_profile_inference.py`.

`_safe_fromstring` (shares Task 4 DOCTYPE/entity guard). Element-only. Group children by tag: tag count >1 ⇒ repeating (`max_occurs=-1`) and union the repeated instances' children (presence→`required`; absent in some ⇒ `required=False`, `min_occurs=0`). Leaf type from text (no echo): `true/false`→`boolean`; ISO-like (if `datetime_detection`)→`datetime`; all-digit w/o leading zero→`number`; else `character`; single-sample inference ⇒ `confidence="medium"`. Reject: any element with **attributes**, mixed content (non-whitespace text alongside child elements), namespaced tag (`{ns}` present)→`UNSUPPORTED_NAMESPACE`, invalid XML→`INVALID_SAMPLE`, same-tag ancestor (recursion)→`RECURSIVE_XML`. Delegate to `profile_from_xml_schema`.

- [ ] **Step 1 — failing tests** (representative; full matrix per `xml_sample_tests`):
```python
_XML_OK = '<Orders><Order><Id>A1</Id><Qty>5</Qty></Order><Order><Id>A2</Id><Qty>9</Qty><Note>hi</Note></Order></Orders>'

def test_xml_sample_repeating_and_optional():
    r = pi.infer_profile_from_sample_xml(_XML_OK)
    assert r["generation_mode"]=="profile_from_sample_xml" and r["component_type"]=="profile.xml"
    idx=set(r["field_index_by_path"])
    assert "Orders[]/Order[]/Id" in idx
    by={f["path"]:f for f in r["fields"]}
    assert by["Orders[]/Order[]/Note"]["required"] is False       # missing in first Order
    assert by["Orders[]/Order[]/Qty"]["data_type"]=="number"

def test_xml_sample_attributes_rejected():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_xml('<R><A x="1">v</A></R>')
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE

def test_xml_sample_namespaced_rejected():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_xml('<R xmlns:n="urn:x"><n:A>v</n:A></R>')
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_NAMESPACE

def test_xml_sample_mixed_content_rejected():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_xml('<R>text<A>v</A></R>')
    assert e.value.error_code == pi.PROFILE_INFERENCE_UNSUPPORTED_SHAPE

def test_xml_sample_recursive_rejected():
    with pytest.raises(BuilderValidationError) as e:
        pi.infer_profile_from_sample_xml('<Node><Node><leaf>v</leaf></Node></Node>')
    assert e.value.error_code == pi.PROFILE_INFERENCE_RECURSIVE_XML

def test_xml_sample_does_not_echo_text():
    import json as _j
    r = pi.infer_profile_from_sample_xml('<R><A>SENSITIVE-XML-TEXT</A></R>')
    assert "SENSITIVE-XML-TEXT" not in _j.dumps(r)
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** `_xml_element_to_node(el, ancestors)` (ancestor-tag set for recursion guard; attribute/mixed/namespace guards; sibling grouping + union), `_infer_text_type`, delegate.
- [ ] **Step 4 — run, expect PASS** (`-k xml`).
- [ ] **Step 5 — commit:** `git commit -am "issue #47: XML-sample inference mode"`

---

## Task 6: Action layer `infer_profile_fields_action`

**Files:** Modify `categories/integration_authoring.py`; Test `tests/test_infer_profile_fields_wrapper.py`.

`infer_profile_fields_action(source_type, artifact, options=None) -> dict`. Normalize `options` (dict | JSON string | None; invalid → `_success=False, code=PROFILE_INFERENCE_INVALID_INPUT`). Dispatch on `source_type` over `{profile_from_db_metadata, profile_from_sample_json, profile_from_xsd, profile_from_sample_xml}`; unknown → `PROFILE_INFERENCE_INVALID_INPUT` with `supported_source_types`. Apply `max_input_chars` to `str` artifacts **before** parsing → on oversize return `PROFILE_INFERENCE_INPUT_TOO_LARGE` envelope (`truncated=True`, `truncation={...}`, `ready_for_builder=False`). Wrap pure-fn result with `_success=True` + flags. Catch `BuilderValidationError` → structured error envelope (`code`,`field`,`hint`,`details`) **with flags**. Catch `Exception` → generic `PROFILE_INFERENCE_INVALID_INPUT`-class envelope, never leak artifact. **Every** return path carries `read_only=True, boomi_mutation=False, raw_xml_exposed=False`.

- [ ] **Step 1 — failing tests:**
```python
import os; os.environ["BOOMI_LOCAL"]="true"
from boomi_mcp.categories.integration_authoring import infer_profile_fields_action as act

def test_action_success_envelope_flags():
    r = act("profile_from_sample_json", '{"id":1}')
    assert r["_success"] and r["read_only"] and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert r["generation_mode"]=="profile_from_sample_json"

def test_action_options_json_string_ok():
    r = act("profile_from_sample_json", '{"id":1}', options='{"component_name":"Demo"}')
    assert r["component_name"]=="Demo"

def test_action_bad_options_json():
    r = act("profile_from_sample_json", '{"id":1}', options="{bad")
    assert r["_success"] is False and r["code"]=="PROFILE_INFERENCE_INVALID_INPUT" and r["read_only"] is True

def test_action_unknown_source_type():
    r = act("profile_from_unicorn", {})
    assert r["_success"] is False and r["code"]=="PROFILE_INFERENCE_INVALID_INPUT"
    assert "profile_from_sample_json" in r["details"]["supported_source_types"]

def test_action_oversize_input():
    r = act("profile_from_sample_json", '{"a":"'+("x"*10)+'"}', options={"max_input_chars":5})
    assert r["_success"] is False and r["code"]=="PROFILE_INFERENCE_INPUT_TOO_LARGE"
    assert r["truncated"] is True and r["ready_for_builder"] is False and "x"*10 not in str(r)

def test_action_error_envelope_keeps_flags():
    r = act("profile_from_sample_json", '"scalar root"')
    assert r["_success"] is False and r["code"]=="PROFILE_INFERENCE_UNSUPPORTED_SHAPE"
    assert r["read_only"] and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** action + private `_normalize_options` + `_inference_error_envelope`. Add codes import.
- [ ] **Step 4 — run, expect PASS.**
- [ ] **Step 5 — commit:** `git commit -am "issue #47: infer_profile_fields_action (read-only envelope)"`

---

## Task 7: MCP wrapper in `server.py`

**Files:** Modify `server.py`; Test `tests/test_infer_profile_fields_wrapper.py`.

Extend the integration-authoring import try-block to also import `infer_profile_fields_action` (guarded). Register inside the `if list_integration_archetypes_action:` block (so it shares the load guard) OR its own guard. `@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})`, signature `def infer_profile_fields(source_type: str, artifact, options: str = None)`. Docstring documents the 4 source types, options, read-only/no-Boomi contract, no `profile` param. If `options` is a JSON string, the action handles parsing; the wrapper passes it through. Never call `get_current_user`/`get_secret`/`Boomi`.

- [ ] **Step 1 — failing tests** (mirror `test_integration_authoring_wrapper.py`; use `_run_async`/`_resolve_tool`/`_call_tool`/`_payload` copied from that file):
```python
def test_infer_tool_registered_and_readonly():
    t=_resolve_tool("infer_profile_fields"); assert t is not None
    assert _annotation_value(t.annotations,"readOnlyHint") is True
    assert _annotation_value(t.annotations,"openWorldHint") is False

def test_infer_schema_has_no_profile_param():
    by={t.name:t for t in _listed_tools()}
    props=set(by["infer_profile_fields"].parameters["properties"])
    assert "profile" not in props and {"source_type","artifact"} <= props

def test_infer_call_tool_success_and_flags():
    p=_payload(_call_tool("infer_profile_fields",
        {"source_type":"profile_from_sample_json","artifact":'{"id":1}'}))
    assert p["_success"] and p["read_only"] and p["boomi_mutation"] is False

def test_infer_wrapper_no_boomi_or_credentials():
    with patch.object(server,"get_current_user") as u, patch.object(server,"get_secret") as s, patch.object(server,"Boomi") as b:
        r=server.infer_profile_fields("profile_from_sample_json", '{"id":1}')
    assert r["_success"]; u.assert_not_called(); s.assert_not_called(); b.assert_not_called()

def test_infer_stable_output():
    a={"source_type":"profile_from_sample_json","artifact":'{"id":1,"name":"x"}'}
    assert _payload(_call_tool("infer_profile_fields",a))==_payload(_call_tool("infer_profile_fields",a))
```
- [ ] **Step 2 — run, expect fail** (tool not registered).
- [ ] **Step 3 — implement** import + `@mcp.tool` wrapper + `[INFO] … registered` print.
- [ ] **Step 4 — run, expect PASS.**
- [ ] **Step 5 — commit:** `git commit -am "issue #47: register infer_profile_fields MCP tool"`

---

## Task 8: Discoverability — list_capabilities + get_schema_template + (A) note rewrites

**Files:** Modify `categories/meta_tools.py`; Tests: new `tests/test_schema_template_profile_inference.py`, modify `tests/test_meta_tools_list_capabilities.py`, `tests/test_schema_template_profile_json_generated.py`, `tests/test_schema_template_profile_xml_generated.py`.

8a. **list_capabilities:** add `infer_profile_fields` under `"Integration Authoring"` (`read_only=True`, `no_boomi_mutation=True`, parameters `source_type`/`artifact`/`options`, examples for all 4 modes). It is filtered by the live registry like the other authoring tools.

8b. **get_schema_template:** add `"profile_inference"` to `_VALID_RESOURCE_TYPES` and the `registry` dict in `get_schema_template_action`; new `_get_profile_inference_template(...)` returning a dict documenting the 4 modes, inputs, outputs (the success shape), safety flags (`read_only`/`boomi_mutation`/`raw_xml_exposed`), the `PROFILE_INFERENCE_*` error codes, and placeholder-only examples (no canned payloads — honor the forbidden-substring hygiene used by sibling schema-template tests).

8c. **(A) note rewrites:** at meta_tools `out_of_scope.inferred_from_sample_json` (~L2366) and `out_of_scope.inferred_from_xsd` (~L2547): rewrite to point at `infer_profile_fields` (`profile_from_sample_json`; `profile_from_xsd`/`profile_from_sample_xml`) **while retaining the `#47` tag** so the existing `test_template_out_of_scope_points_at_47` stays green. Leave `(B)` `existing_profile_index_discovery` / `MAP_PROFILE_INDEX_UNAVAILABLE` blocks unchanged.

- [ ] **Step 1 — failing tests:**
```python
# tests/test_schema_template_profile_inference.py
from boomi_mcp.categories.meta_tools import get_schema_template_action as gst
def test_profile_inference_template_lists_modes_and_flags():
    r=gst(resource_type="profile_inference")
    assert r["_success"] is True
    assert set(r["supported_source_types"])=={"profile_from_db_metadata","profile_from_sample_json","profile_from_xsd","profile_from_sample_xml"}
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
def test_profile_inference_unknown_resource_lists_it():
    r=gst(resource_type="does_not_exist")
    assert r["_success"] is False and "profile_inference" in r["valid_types"]

# add to test_meta_tools_list_capabilities.py
def test_infer_profile_fields_in_capabilities():
    t=list_capabilities_action()["tools"]
    assert t["infer_profile_fields"]["category"]=="Integration Authoring"
    assert t["infer_profile_fields"]["read_only"] is True
    assert t["infer_profile_fields"]["no_boomi_mutation"] is True

# add to test_schema_template_profile_json_generated.py
def test_inferred_from_sample_json_points_at_tool():
    r=_call(component_type="profile.json", protocol="json.generated")
    assert "infer_profile_fields" in r["out_of_scope"]["inferred_from_sample_json"]
```
  (Mirror the XML pointer test in `test_schema_template_profile_xml_generated.py`.)
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** 8a/8b/8c.
- [ ] **Step 4 — run, expect PASS** (`tests/test_schema_template_profile_inference.py tests/test_meta_tools_list_capabilities.py tests/test_schema_template_profile_json_generated.py tests/test_schema_template_profile_xml_generated.py`).
- [ ] **Step 5 — commit:** `git commit -am "issue #47: discoverability — capabilities, schema_template, (A) note rewrites"`

---

## Task 9: Full-suite regression + hygiene sweep

- [ ] **Step 1:** `PYTHONPATH=src python3 -m pytest tests/ -q` — expect all green (incl. unchanged #43, archetype, transform-review, deferred-mode `reject_unsupported_generation_source` tests).
- [ ] **Step 2:** Grep the new module for accidental value-echo paths; confirm no `Boomi(`/`get_secret`/`get_current_user`/`requests`/`http` imports in `profile_inference.py`.
- [ ] **Step 3:** Confirm `reject_unsupported_generation_source` still raises `UNSUPPORTED_PROFILE_GENERATION_SOURCE` (we did NOT remove the deferred-mode guard in profile_generation.py — #47 inference is a NEW layer, not a rewrite of that dispatcher).
- [ ] **Step 4 — commit** any fixups.

---

## Then (outside this plan doc — per CLAUDE.md, run in this session):
- **Stage 1:** boomi-qa-tester QA on `infer_profile_fields` (live `.fn()`), fix→re-run until zero issues.
- **Stage 1.5:** commit QA-clean baseline.
- **Stage 2:** Codex review (`receiving-code-review` discipline), fix→QA→re-review scoped to fix delta, until zero issues.
- **Architect review:** `codex-drive --resume` → review impl vs plan+diff → fix→re-run loop → until "no issues".
- **PR to `dev`** only when dev loop AND architect review are both clean.

---

## Self-Review (spec coverage vs architect JSON)

- `implementation_changes`: inference_module ✓(Tasks 6) · profile_generation_helpers — **deviation**: new sibling `profile_inference.py` instead of editing `profile_generation.py`; reuse of the three #43 helpers ✓ and the deferred-mode guard kept ✓(Task 9.3) · db_metadata_inference ✓(Task 2) · json_sample_inference ✓(Task 3, with empty/heterogeneous-array reconciliation flagged) · xsd_inference ✓(Task 4) · xml_sample_inference ✓(Task 5) · input_limits ✓(shared + Task 6) · mcp_registration_and_discoverability ✓(Tasks 7–8).
- `public_interfaces`: tool name/annotations/signature ✓; supported_source_types ✓; options keys ✓; success shape ✓ (`fields[]` superset incl. structural nodes — documented).
- `structured_errors`: all 7 codes defined + exercised ✓ (`AMBIGUOUS_SHAPE` scoped to empty-contract case — flagged).
- `test_plan`: db/json/xsd/xml/wrapper/schema_template/regression buckets each mapped to Tasks 2–9 ✓.
- `acceptance_criteria`: builder-ready-without-mutation ✓; confirmation_required+ready_for_builder=false ✓; XSD/XML namespace-subset-or-actionable-error ✓; DB caller-supplied-only/no-JDBC ✓; provably read-only wrapper ✓; negative coverage ✓; no invented mappings/templates ✓; only artifact-inference notes rewritten ✓.
- `assumptions`: tool name ✓; namespaces fail not strip ✓; stdlib only ✓; confidence enum ✓; builder-ready semantics ✓; no literal-UUID indexing ✓; (B) blocks deferred ✓.

**Open decisions for plan-reviewer / architect (plan-reviewer APPROVED all):** (1) module layout deviation; (2) empty/heterogeneous-array = error (not ambiguous); (3) `fields[]` includes structural nodes; (4) ambiguous DB boolean mapped to `character` fallback (vs dropped); (5) `reject_unsupported_generation_source` guard kept, not removed; (6) secret-named-field withholding uses exact whole-name match (not substring), and is unrequested-by-JSON scope justified by the resume-memory "never forward secret-named fields" rule. All defensible and test-backed; flag if the architect wants otherwise.
