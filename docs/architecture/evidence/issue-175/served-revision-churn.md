# Served revision churn — measured, and bounded to the ProcessIR revisions

The slice plan makes unexplained recipe-digest movement BLOCKING. Regenerating
`tests/fixtures/m12_12/legacy_reachability_inventory.json` moved four served
schema-template digests, two of which carry "recipe" in the name. This records what
actually differs, measured by rendering each template at the step-0 baseline and at the
slice tip and diffing the two — not inferred from the digest names.

Method: a detached `git worktree` at `3fd5027e16d4f9fe5377884a0140909a3b4d1e67`, each
template rendered through the same public `get_schema_template` entry point in both
trees, output diffed. (A worktree, not a stash: the tree under test must stay untouched.)

## What moved

**`schema_name=recipe_registry`** — exactly one line differs, and it is a ProcessIR
revision the registry embeds:

```
107c107
<    "process_ir": "6b43b7b42a45d4c7eee3dcf08f8c7ee8f78bc908300708f77f723f8570e3e384",
---
>    "process_ir": "df2feb519f744e6ad1859bdee2cfa7329906bc5266642363549b1ebe0a202844",
```

**`schema_name=validation_report`** — exactly the three ProcessIR revisions this slice is
supposed to move, and nothing else:

```
48,49c48,49
<   "capability_revision": "sha256:eb46f9fed47bc08c5f6f48f4ed19c3b41a1f28adb0c6bda31cbb3b78a9decf03",
<   "compiler_revision": "sha256:abe89cd6dd5a080c38f9fc51681f772993362b3a6bcbb922c22aa709ca543b80",
---
>   "capability_revision": "sha256:6fe6637bb93cbd3ff2c6cd487cf217701b04e7fa46eb90989c5540fd4a5a8517",
>   "compiler_revision": "sha256:5448e2e94986a3d3526e81652bd4e2b3a1d0490a75f3a9e77213750fbf8c269d",
51c51
<   "schema_revision": "sha256:dc3c173568c2c90940fa11ae8b87dc677e7cc9ca2a248f554429f9812f3e8160"
```

**`schema_name=recipe_contributions`** — its digest moved because the template genuinely
embeds the changed facts: the served text contains `process_call` 15 times and the new
`process_call_return_path_binding` capability once (counted on the tip).

## What did NOT move — the discriminator

Every individual recipe definition digest is byte-identical across the regeneration:
`recipe:boomi.adapter.api_to_api_sync@1.0.0`,
`recipe:boomi.adapter.api_to_database_sync@1.0.0`,
`recipe:boomi.archetype.api_to_database_sync@0.1.0`,
`recipe:boomi.compose.db_rest_fanout@1.0.0`,
`recipe:boomi.constraint.inbound_validate@1.0.0`, and the rest of the `recipe:boomi.*`
family. So no recipe SOURCE, registry content, or implementation changed; what moved is
the ProcessIR revision that several served envelopes quote.

That is the intended blast radius of a grammar/capability change, and it is bounded:
had a recipe's own digest moved, this slice would have been touching the recipes package,
which it does not.
