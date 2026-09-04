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
