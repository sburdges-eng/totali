#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 <input.crd> <output.csv>" >&2
  exit 2
fi

input_path="$1"
output_path="$2"

if [[ ! -f "$input_path" ]]; then
  echo "input not found: $input_path" >&2
  exit 3
fi

cat > "$output_path" <<'CSV'
Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number
9001,1234.5,2345.6,345.7,CP GENERATED,CP GENERATED,PNTS,No,,Converted,
9002,1235.5,2346.6,346.7,FREB GENERATED,FREB GENERATED,PNTS,No,,Converted,
CSV
