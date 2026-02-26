# Land Survey and Civil Engineering Playbook

## Table of Contents

1. High-value deliverables
2. Recommended parser settings
3. Civil-specific report fields
4. Rule templates

## 1. High-Value Deliverables

Prioritize these outputs for civil/survey projects:

- Parcel candidates with area/perimeter checks.
- Contour counts, interval estimates, and elevation range.
- Centerline totals for quick linear-quantity checks.
- Utility/drainage entity and topology edge counts.
- Spot elevation and benchmark value extraction.
- QA flags for missing units and disconnected networks.
- Domain coverage confidence for:
  - Boundary and retracement surveys
  - GPS and GIS surveying
  - Property subdivisions, lot line adjustments, easements
  - Site topography and control surveys
  - Construction support surveys (as-built, route, roadway/bridge, hydro projects)
  - Remote and specialized surveying tasks

## 2. Recommended Parser Settings

Start here for most civil drawings:

`python3 scripts/parse_dwg.py drawing.dwg --converter-cmd 'odafc "{input}" "{output}"' --precision 3 --tolerance 0.01 --output report.json`

Tune scoring with your directory/project naming conventions:

`python3 scripts/parse_dwg.py drawing.dwg --project-tag retracement --project-tag lot_adjustment --project-tag roadway --output report.json`

Tighten/relax by workflow:

- Boundary/legal survey QA: `--precision 4 --tolerance 0.001`
- Utility concept plans: `--precision 3 --tolerance 0.01`
- Regional topo/base mapping: `--precision 2 --tolerance 0.1`

## 3. Civil-Specific Report Fields

- `civil_survey.units`: INSUNITS code + interpreted name.
- `civil_survey.bounds`: drawing extents in normalized coordinates.
- `civil_survey.layer_groups`: categorized layer groups (`parcel_boundary`, `contour`, `centerline`, etc.).
- `civil_survey.project_context`: naming tokens used for domain matching and tuning.
- `civil_survey.feature_counts`: detected civil features by class.
- `civil_survey.terrain`: contour/elevation summary.
- `civil_survey.parcels`: parcel samples with area/perimeter.
- `civil_survey.centerlines`: sampled longest centerline features.
- `civil_survey.utilities`: utility entity and topology-edge counts.
- `civil_survey.spot_elevations`: extracted point/label elevation values.
- `civil_survey.survey_domains`: confidence and evidence for major survey service areas.
- `civil_survey.qa_flags`: issues to review before downstream computations.

## 4. Rule Templates

1. Boundary completeness
   - Require `civil_survey.feature_counts.parcel_boundaries > 0`.
   - For each parcel sample, require area and perimeter present.

2. Terrain sanity
   - Require `civil_survey.terrain.contour_count > 0` for topo drawings.
   - Require non-null contour interval estimate when multiple contour elevations exist.

3. Utility connectivity
   - Require `civil_survey.utilities.entity_count > 0` on utility plans.
   - Investigate when `topology.connected_components > 1`.

4. Spot elevation availability
   - Require points or labels in `civil_survey.spot_elevations`.
   - Compare range against expected grade window for project phase.
