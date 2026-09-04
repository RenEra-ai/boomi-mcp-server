# Connector replay evidence

*Generated from the packaged registry. Do not edit by hand — regenerate it.*

This is the complete record of what has been OBSERVED about re-executing a
connector action. Nothing here is derived from documentation or from the shape
of a component: a row exists because an execution produced it.

An action with no row is `unverified`, and `unverified` refuses a retry. That is
the safe direction: a registry that failed to load, or loaded empty, denies.

## Connector vocabulary

| platform connector type | family | action read from |
| --- | --- | --- |
| `officialboomi-X3979C-rest-prod` | rest | operation_component |

## Observed actions

| family | action | side effect | retry safety | executions | capture |
| --- | --- | --- | --- | --- | --- |
| rest | DELETE | write | conditionally_idempotent | 2 | `34614e7c081f` |
| rest | HEAD | read | unverified | 1 | `e403860b635d` |
| rest | OPTIONS | read | unverified | 1 | `67ed7efbf569` |
| rest | PATCH | write | conditionally_idempotent | 2 | `dec268ab22bd` |
| rest | POST | write | conditionally_idempotent | 2 | `80e530d635db` |
| rest | PUT | write | conditionally_idempotent | 2 | `a065632cfbf9` |
| rest | TRACE | read | unverified | 1 | `0c62c0fb6ca3` |

## Operation contract records

| contract reference | family | action | semantics | revision |
| --- | --- | --- | --- | --- |
| `$ref:icv1:rest:patch:resource_identity_upsert_static_route_same_effect:1` | rest | PATCH | resource_identity_upsert_static_route_same_effect | 1 |

## Contract key semantics

| semantics | revision | mechanism | key scope | duplicate guarantee |
| --- | --- | --- | --- | --- |
| `resource_identity_upsert_static_route_same_effect` | 1 | resource_identity_upsert | static_route | same_effect |

## Component projection allowlists

| family | component kind | projection | category | members | fingerprint |
| --- | --- | --- | --- | --- | --- |
| rest | `connection` | v4 | included_attributes | 0 | `4f53cda18c2baa0c` |
| rest | `connection` | v4 | included_value_fields | 1 | `172f6d9f96f85f97` |
| rest | `connection` | v4 | included_property_fields | 0 | `4f53cda18c2baa0c` |
| rest | `connection` | v4 | excluded_fields | 31 | `dce8043fd0642565` |
| rest | `connection` | v4 | included_elements | 17 | `6850ea386c57849c` |
| rest | `connection` | v4 | included_scope_attributes | 1 | `f096a2b9a61ce689` |
| rest | `connection` | v4 | excluded_scope_attributes | 5 | `67cbcc0fb988a3d3` |
| rest | `connection` | v4 | qname_aware_tags | 0 | `4f53cda18c2baa0c` |
| rest | `connection` | v4 | qname_aware_attrs | 0 | `4f53cda18c2baa0c` |
| rest | `operation` | v4 | included_attributes | 1 | `91506d2af5b1bef8` |
| rest | `operation` | v4 | included_value_fields | 2 | `85ae1e649ccb3d94` |
| rest | `operation` | v4 | included_property_fields | 2 | `dc6e82f1f7ce13e0` |
| rest | `operation` | v4 | excluded_fields | 0 | `4f53cda18c2baa0c` |
| rest | `operation` | v4 | included_elements | 16 | `f1def34c25acd857` |
| rest | `operation` | v4 | included_scope_attributes | 9 | `3b78734115dc3009` |
| rest | `operation` | v4 | excluded_scope_attributes | 1 | `50aa3f0ab73c86f5` |
| rest | `operation` | v4 | qname_aware_tags | 0 | `4f53cda18c2baa0c` |
| rest | `operation` | v4 | qname_aware_attrs | 0 | `4f53cda18c2baa0c` |
