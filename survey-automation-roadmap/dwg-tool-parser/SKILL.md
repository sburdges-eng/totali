---
name: dwg-tool-parser
description: Parse AutoCAD DWG and DXF files into structured JSON summaries with layers, blocks, entity counts, normalized geometry, topology graphs, and civil-survey domain coverage. Use when requests involve boundary/retracement surveys, GPS/GIS surveying, subdivisions/lot line adjustments/easements, site topography/control surveys, construction support surveys (as-built, route, roadway/bridge, hydro), remote/specialized survey tasks, CAD content audits, or DWG-to-DXF analysis pipelines.
---

# DWG Tool Parser

## Overview

Use this skill to produce reliable, machine-readable DWG/DXF summaries for land survey and civil engineering workflows first.
Prefer the bundled parser script for repeatable output and stable JSON structure across boundary, topo, utility, and roadway drawings.

## Workflow

1. Run the parser:
   `python3 scripts/parse_dwg.py <input.dwg|input.dxf> --output <report.json>`
2. If input is `.dwg`, provide a converter command:
   `python3 scripts/parse_dwg.py drawing.dwg --converter-cmd 'odafc "{input}" "{output}"' --output report.json`
3. Tune CAD normalization/topology if needed:
   `--precision 6 --tolerance 1e-6 --entity-limit 2000 --topology-entity-limit 5000`
4. Add project naming context to tune domain scoring:
   `--project-tag retracement --project-tag roadway --project-tag hydro`
5. If you always use the same converter, set `DWG_TO_DXF_CMD` and omit `--converter-cmd`.
6. Read the output keys:
   - `summary.entity_total`
   - `summary.geometry_entities_total`
   - `summary.entity_types`
   - `summary.layers`
   - `summary.blocks`
   - `summary.dxf_version`
   - `entities`
   - `topology.nodes`
   - `topology.edges`
   - `topology.loops`
   - `civil_survey.feature_counts`
   - `civil_survey.terrain`
   - `civil_survey.parcels`
   - `civil_survey.centerlines`
   - `civil_survey.utilities`
   - `civil_survey.spot_elevations`
   - `civil_survey.survey_domains`
   - `civil_survey.project_context`
   - `civil_survey.qa_flags`

## Civil/Survey First-Pass Uses

- Parcel and right-of-way extraction from closed boundary polylines.
- Contour/elevation analysis for topo and grading files.
- Centerline and linear-asset length screening.
- Utility/drainage layer signal extraction for network QA.
- Spot elevation and benchmark label harvesting for quick checks.
- Early QA to detect missing units, disconnected topology, or incomplete civil layers.

## Domain Coverage Output

The parser maps evidence into these survey domains and reports confidence:

- Boundary and retracement surveys
- GPS and GIS surveying
- Property subdivisions, lot line adjustments, easements
- Site topography and control surveys
- Construction support surveys (as-built, route, roadway/bridge, hydro projects)
- Remote and specialized surveying tasks

## Converter Command Rules

- Use a command template that includes both `{input}` and `{output}`.
- Quote placeholders in the template so paths with spaces work.
- Optional placeholders are available:
  - `{output_dir}`
  - `{output_stem}`
- Example starter template:
  `--converter-cmd 'ODAFileConverter "{input}" "{output_dir}" ACAD2018 DXF 0 1 "{output_stem}.dxf"'`
  Adapt it to your converter's exact CLI syntax.

## Output Contract

The parser emits JSON using this schema shape:

```json
{
  "input_file": "...",
  "input_type": "dwg|dxf",
  "parser_backend": "ezdxf|ascii-fallback",
  "conversion": {
    "used": true,
    "command": "...",
    "output_dxf": "..."
  },
  "summary": {
    "dxf_version": "AC1032",
    "insunits": 4,
    "entity_total": 120,
    "geometry_entities_total": 116,
    "entity_types": {
      "LINE": 60
    },
    "layers_total": 8,
    "layers": [
      "0",
      "Walls"
    ],
    "blocks_total": 3,
    "blocks": [
      "DoorTag"
    ]
  },
  "sample_entities": [],
  "entities_total": 120,
  "entities_returned": 120,
  "entities_truncated": false,
  "entities": [
    {
      "id": "10",
      "type": "LINE",
      "layer": "Walls",
      "geometry": {
        "kind": "line",
        "start": [
          0.0,
          0.0,
          0.0
        ],
        "end": [
          10.0,
          0.0,
          0.0
        ]
      }
    }
  ],
  "topology": {
    "tolerance": 1e-06,
    "node_count": 2,
    "edge_count": 1,
    "loop_count": 0,
    "connected_components": 1,
    "nodes": [],
    "edges": [],
    "loops": []
  },
  "civil_survey": {
    "project_context": {
      "project_tags": [
        "retracement",
        "roadway"
      ]
    },
    "survey_domains": {
      "boundary_retracement_surveys": {
        "confidence": "high",
        "score": 5,
        "evidence": [
          "3 closed parcel boundary candidates detected"
        ]
      }
    },
    "feature_counts": {
      "parcel_boundaries": 3,
      "centerlines": 2,
      "contours": 40,
      "spot_elevation_points": 12,
      "spot_elevation_labels": 8,
      "utility_entities": 18,
      "control_points": 4
    },
    "terrain": {
      "contour_count": 40,
      "contour_interval_estimate": 1.0
    },
    "qa_flags": []
  }
}
```

## Troubleshooting

- If `.dwg` parsing fails, validate your converter command independently.
- If `ezdxf` is unavailable or cannot parse a DXF, the script falls back to an ASCII parser for core geometry and counts.
- If topology is too large, reduce `--topology-entity-limit` or increase `--tolerance`.
- Read `references/backends.md` for backend selection and template guidance.
- Read `references/cad-logic.md` for mapping point-based rules into CAD-aware checks.
- Read `references/land-survey-civil.md` for civil/survey-first output interpretation and rule templates.
