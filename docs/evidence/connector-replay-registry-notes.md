# Why the packaged registry looks the way it does

The registry's own JSON carries **no narrative fields**. The design forbids them —
narrative, header, body, URL, raw-path, credential and arbitrary-description fields are
all excluded — because a served contract that also carries prose invites the prose to
drift from the contract, and a reader cannot tell which one the code obeys.

The explanations that were briefly inside that file live here instead.

## The vocabulary has one entry, and that is deliberate

`officialboomi-X3979C-rest-prod` maps to the `rest` family. It was observed in 60
execution-connector rows across the archived captures for issue #155, every one from an
execution that completed.

The middle segment is Boomi's official-connector publisher namespace, **not** the customer
account: the same segment appears on a different family (`officialboomi-X3979C-dbv2da-prod`)
in this repository's goldens, and it does not match the capturing account's id. That is what
makes the mapping portable across accounts, which is the whole reason evidence is keyed on a
family rather than on a raw connector type.

## One connector type is deliberately NOT mapped

`officialboomi-X3979C-dbv2da-prod` appears in this repository's goldens but in **no executed
capture**. The allowlist admits only types observed in a green execution, so it is recorded
here rather than mapped. Adding it needs a capture, not an edit.

## The registry ships empty

`evidence_records` and `operation_records` are both empty. This slice lands the mechanism;
the rows are ingested from executed captures in a later one. A registry shipping pre-filled
rows would assert replay safety no execution observed.

Empty is also the safe state: every consumer treats an absent row as `unverified`, which
refuses a write retry. A registry that failed to load, or loaded empty, therefore denies.
