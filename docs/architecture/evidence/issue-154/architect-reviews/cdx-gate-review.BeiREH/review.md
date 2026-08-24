I’ll treat this as a plan-conformance audit, not another general code review. I’ll first read both prior architect reviews and the reconciliation ledger, then independently trace plan items 1–10, with extra scrutiny on subprocess state compartments, served contracts, and any ledger claim the code does not support. The repository-required `receiving-code-review` skill is not available in this session, so I’ll apply its stated verify-before-accepting discipline directly and report only actionable, caller-relevant gaps.The prior findings are fully identified, and I’m now checking their recorded closures rather than accepting the ledger at face value. One audit-record inconsistency is already visible: evaluation 2’s missing public-boundary/effect-golden coverage is said to be “enumerated for disposition at CP-11,” but CP-11 discusses only the subprocess exit-rule recurrence. I’ll verify whether the current tree actually supplied that coverage before treating it as a finding.The item-by-item pass has now exposed a concrete item-5 regression that the previous closure did not cover: the invariant trusts authored source-path order without reconciling it to CFG execution edges. A correlated mutant can therefore put the entry role on the second executed connector, pass both CFG and emission-plan invariants, and select the wrong emitter shape. I’m reproducing that exact graph mutation locally before deciding the final finding set.The core grammar widenings themselves check out. The remaining faults are concentrated at the architecture boundaries: the independent CFG guard, subprocess composition/state transfer, and machine-served contract claims. I’m reconciling those against the ledger now so the final report contains only actionable divergences.Issues found. The grammar widening is largely faithful, but two P1 effect-authority defects and five concrete plan/contract gaps remain.

1. **[P1, items 7–8] Normal-only execution writes are lost before the next Branch leg.**  
   Branch analysis correctly derives each leg’s normal-completion guarantee, but [lineage.py:694](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:694) advances the next leg using `leg_end.execution`, which includes abnormal Decision arms.

   Reproducer: leg 0 ends in a Decision whose normal arm writes DPP `P` and stops while the other throws; leg 1 reads `P`. Every execution reaching leg 1 has `P`, yet validation reports `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE`, and the derived summary both requires and guarantees `P`. A truthful public declaration with no required read is rejected as `content-mismatch`.

   The correction must seed the next leg from normal states that actually reach it. The “derived compartment model” closure at [ledger line 155](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:155) overstates what shipped.

2. **[P1, items 7–8] Required recursive, capability-compositional subprocess inspection is absent.**  
   [process_ir_effects.py:664](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:664) tests child nodes against `DEFAULT_VALIDATION_CAPABILITIES`, ignoring the trusted capabilities supplied to `derive_subprocess_effect`. The resolver then constructs child capabilities containing only external writers at [process_ir_effects.py:1041](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1041).

   Consequently:

   - A child map remains `uninspectable_step` even with a verified map contract.
   - A valid child map declaration binds, but its parent subprocess declaration becomes silently INERT.
   - Every further `process_call` is unconditionally inert; there is no recursive summary or cycle-aware traversal.

   This behavior is explicitly pinned at [test_process_ir_effect_declarations.py:1811](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_effect_declarations.py:1811), but it contradicts the plan’s recursive-authority requirement and is not an accepted ledger deviation. The “all four limbs fixed” claim at [ledger line 132](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:132) is therefore false.

3. **[P2, item 5] The connector-entry invariant still accepts a graph that emits the source/target roles backwards.**  
   [invariants.py:458](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:458) derives the first root call solely by sorting `source_path`; it never reconciles that order with CFG ordering edges.

   With edges still `n1 → n2 → n3 → n4`, changing `n2` to path `/body/steps/2`, role `downstream`, and `n3` to `/body/steps/1`, role `entry` passes both CFG and emission-plan invariants. Emission then makes execution-first `n2` a connector target and execution-second `n3` the connector source.

   Derive order independently from edges, require agreement with source-path ordinals, then check the role. [ARCH-e2-05’s closure](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:136) covered only a path-only mutation.

4. **[P2, items 7–8] Canonical binding still selects authoritative content by raw declaration spelling.**  
   Alias occurrence matching uses component identity, but map content is fetched by `item.map_ref` at [process_ir_effects.py:902](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:902), and child roots likewise at [process_ir_effects.py:1028](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1028).

   In the reverse legal alias case—declaration uses the alias, occurrence/content uses the canonical ref, and both symbols share one `component_id`—map and subprocess declarations resolve `ok=True` but silently become INERT with no internal row. Content authority must be resolved through canonical identity before per-occurrence rebinding. This contradicts [ARCH-e2-03’s all-families closure](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:134).

5. **[P2, items 7–8 and 10] The planned public language alias is still not exported.**  
   `ProcessIRScriptLanguageV1` exists and is in the defining module’s `__all__` at [authoring_workflow.py:1169](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/authoring_workflow.py:1169), but it is absent from both public package lists at [models/__init__.py:183](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/__init__.py:183) and [models/__init__.py:384](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/__init__.py:384). `boomi_mcp.models.ProcessIRScriptLanguageV1` is therefore absent. The package export pin also omits it. [ARCH-e2-06](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:137) incorrectly records this as fixed.

6. **[P2, items 9–10] Machine-served effect text still contradicts runtime behavior.**

   - [process_ir_projection.py:839](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:839) says a write in one Branch path is not established. That is false for DPP/cache, which accumulate across sequential legs; it is true only for leg-local DDP copies.
   - [process_ir_projection.py:475](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:475) and the projected state authority at [lineage.py:121](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:121) say an external-writer declaration alone prevents the missing-writer error. Runtime correctly requires both the authored flag and verified capability at [lineage.py:535](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:535).

7. **[P2, items 5–8 and 10] Required acceptance coverage remains absent and was never dispositioned.**  
   [ARCH-e2-09](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:140) promises disposition at CP-11, but [CP-11](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_154_AUDIT_LEDGER.md:172) discusses only subprocess state aggregation.

   Still absent:

   - Public `AuthoringRequestV1` plan → compile → emit cases for map, registered script, subprocess, and external writer. The claimed map integration instead directly invokes the resolver at [test_process_ir_effect_declarations.py:1942](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_effect_declarations.py:1942).
   - The three planned effect XML goldens.
   - The synthetic additional-body-context witness, model-constructed catch-terminal enforcement witness, and corrupted nested-entry-role mutant.

### Plan fidelity

| Item | Assessment |
|---|---|
| 1 | Realized. |
| 2 | Realized. |
| 3 | Realized. |
| 4 | Realized. |
| 5 | Grammar and ordinary lowering realized; independent invariant diverged as finding 3. |
| 6 | Production derivation realized; mandated non-vacuity coverage remains incomplete. |
| 7 | Public models and fail-closed boundary largely realized; canonical identity and package export diverged. |
| 8 | Map/script authority and root-locality partly realized; Branch transfer and recursive subprocess composition materially diverged. |
| 9 | Runtime cache spelling and two-factor exception realized; served wording diverged. |
| 10 | Generated `semantic_rule.effect.*` namespace and schemas realized; served text and acceptance artifacts remain incomplete. |

No unexplained current over-delivery was found. The #179 absorption was owner-authorized, and the parallel `effect_authority.*` namespace is gone. The unified state-reference model, static language pin, script-map opacity, and metadata shape are honestly documented narrowings rather than hidden additions.

VERDICT: ISSUES FOUND
