#!/bin/bash
# usage: launch.sh <arm-name> <tree>
ARM=$1; TREE=$2
cd "$(dirname "$0")"
export BOOMI_LOCAL=true BOOMI_DOCS_ENABLED=false BOOMI_GOTCHAS_ENABLED=false PYTHONDONTWRITEBYTECODE=1
unset PYTHONPATH
PY=/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/.venv/bin/python
mkdir -p out
while read -r corpus start end; do
  [ -z "$corpus" ] && continue
  "$PY" run_arm.py "$TREE" "req_${corpus}.jsonl" "$start" "$end" "out/${ARM}_${corpus}_${start}.jsonl" > "out/${ARM}_${corpus}_${start}.log" 2>&1 &
done <<'LIST'
adversarial 0 3008
removed 0 4500
guarantee 0 6200
stress 0 8640
sweep 0 12000
sweep 12000 24000
sweep 24000 36000
sweep 36000 47096
other 0 10000
other 10000 20000
LIST
wait
echo "ALL DONE $ARM"
