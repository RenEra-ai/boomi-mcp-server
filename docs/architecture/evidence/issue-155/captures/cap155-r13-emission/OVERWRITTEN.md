# This capture was overwritten on 2026-08-26 and its original bytes are gone

The r15 emission-control script was derived from the r13 one by `sed`, and the
substitution missed the hardcoded `OUTDIR` — so the r15 run (process
`69fa23be-eca8-40c0-82f5-a7e5e082ce13`, 2026-08-26T15:43Z) wrote into this
directory instead of `cap155-r15-emission/`. The clobber was detected because
this directory's own `SHA256SUMS` stopped verifying, which is the behaviour that
manifest exists for.

The overwriting bytes have been moved to `../cap155-r15-emission/` with a fresh
manifest. The r13 bytes themselves are NOT recoverable.

**What is not lost**: r13's substantive emission claim was the GUID-masked
`<shapes>` digest `d443808eae16c7ff`, and that digest has been independently
re-measured at every round since, including r15 at `40cf594`. The r13 report's
emission row therefore remains verifiable against every later capture; only this
round's raw files are gone.

Corrective action for the harness: the emission-control script now takes its
output directory from `P155_EVD`/an argument rather than a hardcoded literal, so
a per-round copy cannot silently inherit the previous round's path.
