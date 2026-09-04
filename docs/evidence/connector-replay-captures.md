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
| rest | `connection` | v4 | included_attributes | 0 | `e3b0c44298fc1c14` |
| rest | `connection` | v4 | included_value_fields | 1 | `28e5ebabd9d8f6e2` |
| rest | `connection` | v4 | included_property_fields | 0 | `e3b0c44298fc1c14` |
| rest | `connection` | v4 | excluded_fields | 31 | `d3885e6fb7404eb4` |
| rest | `connection` | v4 | included_elements | 17 | `22d68e9094d97c69` |
| rest | `connection` | v4 | included_scope_attributes | 1 | `905fe83d9dcb6dea` |
| rest | `connection` | v4 | excluded_scope_attributes | 5 | `3c399868931eb5e1` |
| rest | `connection` | v4 | qname_aware_tags | 0 | `e3b0c44298fc1c14` |
| rest | `connection` | v4 | qname_aware_attrs | 0 | `e3b0c44298fc1c14` |
| rest | `operation` | v4 | included_attributes | 1 | `eb3bbdd320c82c65` |
| rest | `operation` | v4 | included_value_fields | 2 | `7ae5bfdf9d4e5d6d` |
| rest | `operation` | v4 | included_property_fields | 2 | `a51413d4d4bae1be` |
| rest | `operation` | v4 | excluded_fields | 0 | `e3b0c44298fc1c14` |
| rest | `operation` | v4 | included_elements | 16 | `a4dbfa8985903e35` |
| rest | `operation` | v4 | included_scope_attributes | 9 | `8e5d174f7ebbbb5b` |
| rest | `operation` | v4 | excluded_scope_attributes | 1 | `707393cc3642e0ab` |
| rest | `operation` | v4 | qname_aware_tags | 0 | `e3b0c44298fc1c14` |
| rest | `operation` | v4 | qname_aware_attrs | 0 | `e3b0c44298fc1c14` |
