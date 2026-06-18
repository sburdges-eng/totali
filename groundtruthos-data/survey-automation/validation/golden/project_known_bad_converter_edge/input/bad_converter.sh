#!/usr/bin/env bash
set -euo pipefail
out="$2"
cat > "$out" <<'CSV'
This,is,not,a,supported,header
1,2,3,4,5,6
CSV
