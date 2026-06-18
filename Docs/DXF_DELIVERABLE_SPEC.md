# TOTaLi DXF Deliverable Specification

**Status:** DRAFT FOR PARTNER SIGN-OFF
**Date:** 2026-06-18
**Pipeline version:** 0.1.0
**Authority config:** `config/pipeline.yaml` (primary), `config/pipeline_in_person.yaml` (confirmed identical layer map)

---

## 1. Purpose & Status

This document specifies the DXF file that the TOTaLi Assisted Drafting Pipeline delivers to the licensed PLS for review and certification. It is the partner-facing contract for layer naming, entity content, geometry conventions, units, and the human certification gate that must be satisfied before any geometry can leave DRAFT status.

This spec is a DRAFT presented for partner sign-off at the in-person meeting. The open questions in section 8 require partner decisions before layer names and conventions are treated as final.

---

## 2. Naming Convention

All pipeline-emitted layers follow the pattern:

```
TOTaLi-<DISCIPLINE>-<FEATURE>-<STATE>
```

### Discipline codes

| Code | Meaning |
|------|---------|
| `SURV` | Survey / terrain geometry (ground surface, breaklines, contours) |
| `PLAN` | Planimetric features (buildings, curbs, hardscape, utilities) |
| `QA` | Quality assurance meta-layers (occlusion zones, uncertainty flags) |

Source: `AGENTIC_COMPLETION_PLAN.md` §4 rule 9; `AGENTIC_COMPLETION_PLAN.md` §1 invariant 3.

### State tokens

| Token | Applied to | Meaning |
|-------|-----------|---------|
| `DRAFT` | All `SURV` and `PLAN` layers | AI/algorithmic output, not yet certified. Suffix is mandatory and enforced at pipeline startup via regex `^TOTaLi-[A-Z0-9]+(?:-[A-Z0-9_]+)+-DRAFT$`. Any non-conforming name raises `NonConformingLayerName` and aborts the pipeline before any DXF is written. |
| *(none)* | All `QA` layers | QA layers are exempt from the `-DRAFT` suffix; they carry advisory metadata, not certifiable geometry. Enforced by the same regex: `^TOTaLi-QA-[A-Z0-9_-]+$`. |

Source: `totali/cad_shielding/shield.py` lines 27-32 (`_LAYER_NAME_RE`, `_validate_layer_mapping`).

On PLS promotion, `-DRAFT` is stripped from the layer name in place: `item.layer = item.layer[:-6]` (source: `totali/linting/surveyor_lint.py` line 328). The certified layer suffix config key `certified_layer_suffix: ""` confirms this — the suffix is removed, not replaced with another token (source: `config/pipeline.yaml:cad_shielding.certified_layer_suffix`).

---

## 3. Full Layer Table

Every entry from `config/pipeline.yaml:cad_shielding.layer_mapping`. These names are verbatim from config; they are also identical in `config/pipeline_in_person.yaml`.

| Pipeline feature key | Layer name (DRAFT) | DXF entity type(s) | Discipline | Notes |
|---|---|---|---|---|
| `ground_surface` | `TOTaLi-SURV-DTM-DRAFT` | `3DFACE` | SURV | Delaunay TIN faces from classified ground points. Each face is one triangular 3DFACE entity. |
| `breaklines` | `TOTaLi-SURV-BRKLN-DRAFT` | `POLYLINE` (3D polyline) | SURV | Slope-discontinuity breaklines. Written as `add_polyline3d`. Also written as `LINE` segments in the ezdxf-absent fallback. |
| `contours_minor` | `TOTaLi-SURV-CONT-MINOR-DRAFT` | `LWPOLYLINE` | SURV | 1 ft interval (from `config:extraction.contours.interval_ft`). Written as `add_lwpolyline`. |
| `contours_index` | `TOTaLi-SURV-CONT-INDEX-DRAFT` | `LWPOLYLINE` | SURV | 5 ft interval (from `config:extraction.contours.index_interval_ft`). Written as `add_lwpolyline`. |
| `buildings` | `TOTaLi-PLAN-BLDG-DRAFT` | `LWPOLYLINE` (closed) | PLAN | Building footprint polygons. Polygon is closed (`close=True` + first point appended). |
| `curbs` | `TOTaLi-PLAN-CURB-DRAFT` | `POLYLINE` (3D polyline) | PLAN | Curb/edge-of-pavement linework. |
| `hardscape` | `TOTaLi-PLAN-HDSC-DRAFT` | `LWPOLYLINE` (closed) | PLAN | Hardscape polygons (parking, plazas). |
| `wire` | `TOTaLi-PLAN-WIRE-DRAFT` | `POLYLINE` (3D polyline) | PLAN | Wire/conductor linework (overhead utilities). |
| `occlusion_zones` | `TOTaLi-QA-OCCLUSION` | `LWPOLYLINE` (closed) | QA | See section 7. No `-DRAFT` suffix; QA-exempt. |
| `uncertainty_flags` | `TOTaLi-QA-FLAGS` | *(see section 7)* | QA | See section 7. No `-DRAFT` suffix; QA-exempt. |

Source: `config/pipeline.yaml:cad_shielding.layer_mapping`; entity types from `totali/cad_shielding/shield.py` `_write_dxf_ezdxf`.

Additionally, coded survey shots from a field-coded survey (`.asc`/`.crd` input) are emitted as `POINT` entities on a `pt.draft_layer` value that is carried from the ingested survey data. Those layer names must also conform to the `TOTaLi-*-DRAFT` invariant; they are validated at insertion time (source: `shield.py` lines 277-278).

**DXF version:** `R2018` (ezdxf `new("R2018")`). Source: `totali/cad_shielding/shield.py` line 265.

---

## 4. DRAFT vs Certified Promotion Rule

### The invariants (non-negotiable, hardcoded)

| Setting | Value | Source |
|---|---|---|
| `linting.auto_promote` | `false` — `NEVER set to true` (comment in config) | `config/pipeline.yaml:linting.auto_promote` |
| `linting.require_pls_signature` | `true` | `config/pipeline.yaml:linting.require_pls_signature` |
| `SurveyorLinter.auto_promote` | Hardcoded `False` in constructor; raises `AutoPromoteForbidden` if config passes any truthy value | `totali/linting/surveyor_lint.py` lines 29-38 |

### Lifecycle states

```
(pipeline output) → DRAFT
                      │
          ┌───────────┼───────────┐
      accept()    reject()    defer()
          │            │           │
       ACCEPTED    REJECTED    DEFERRED
                                   │
                           (resurfaces next session;
                            blocks promotion)
```

Source: `totali/linting/surveyor_lint.py` (`accept_item`, `reject_item`, `defer_item`); `totali/pipeline/models.py` (`GeometryStatus`).

### Promotion gate (`promote_to_certified`)

Promotion to CERTIFIED requires that **every** lint item in the set is either `ACCEPTED` or `REJECTED`. Both `DRAFT` (unreviewed) and `DEFERRED` (explicitly punted by the surveyor) block promotion:

```
draft_remaining   → blocks promotion (logged as "promote_blocked")
deferred_remaining → blocks promotion (DEFERRED is not a decision)
```

When the gate passes, `ACCEPTED` items are promoted to `CERTIFIED` and their layer names have `-DRAFT` stripped. `REJECTED` items are not promoted (not written to the deliverable). The audit log records the PLS name, license number, certified count, and timestamp under event `"certify"`.

Source: `totali/linting/surveyor_lint.py` lines 301-338.

### Certification record (Colorado jurisdiction)

A full `CertificationRecord` requires the surveyor to supply values for all fields in `REQUIRED_BOARD_ALTA_FIELDS`, which enforces:

- 12 statutory plat elements under C.R.S. § 38-51-106(1)(a)-(l) — including signature and seal, basis of bearings, monuments found/set, and linear units statement.
- 3 ALTA/NSPS Section 7 certification components (2021 standard).
- 5 ALTA/NSPS Table A items in scope for a LiDAR/drone pipeline: items 4 (gross land area), 5 (vertical relief + contour datum), 7 (building dimensions), 8 (substantial features), 11 (underground utility evidence).

Source: `totali/linting/certification.py` (`CERT_SCHEMA`, `REQUIRED_BOARD_ALTA_FIELDS`).

The certification record is SHA-256 hashed over its canonical content (tamper-evident). Source: `totali/linting/certification.py` line 181.

---

## 5. Blocks / Symbols and Linework Conventions

### What the pipeline currently emits

| Feature | Emitted as | Notes |
|---|---|---|
| DTM / TIN | Individual `3DFACE` entities (one per triangle) | No surface object; partner's CAD platform must reconstruct a surface if needed |
| Breaklines | `POLYLINE` (3D) | Raw slope discontinuities; smoothing not yet applied (ROADMAP §3.3) |
| Contours (minor) | `LWPOLYLINE` at z=elevation | 1 ft interval |
| Contours (index) | `LWPOLYLINE` at z=elevation | 5 ft index interval (every 5th minor) |
| Building footprints | Closed `LWPOLYLINE` | Convex hull today; alpha-shape is a ROADMAP item (§3.3) |
| Curbs | `POLYLINE` (3D) | PCA-sorted linear feature |
| Hardscape | Closed `LWPOLYLINE` | Convex hull |
| Wire / conductors | `POLYLINE` (3D) | |
| Coded survey shots | `POINT` | From field-coded `.asc`/`.crd` input; authoritative (confidence=1.0), `"authoritative": True` in manifest |

### What is NOT yet specified — OPEN items

- **Block library:** No INSERT entities are emitted. There is no defined block library for survey monuments, control points, benchmark symbols, north arrows, or title blocks. This is unspecified in the pipeline and must be defined with the partner.
- **Text styles / annotations:** No text, MTEXT, or ATTDEF entities are emitted by the pipeline. Annotation conventions (font, size, layer for labels) are not yet defined.
- **Color assignments:** No color or lineweight is assigned in `cad_shielding.layer_mapping`; layers are created with ezdxf defaults. No color standard is enforced by the pipeline.
- **Lineweight standard:** Not specified in config or code.
- **Symbol definitions for coded survey points:** Coded survey shots land as raw `POINT` entities using the `draft_layer` carried from the field code. No block symbols are substituted.

---

## 6. Units and Geometry Healing

### Elevation unit

All pipeline coordinates are in **US survey feet** (not international feet).

Source: `config/pipeline.yaml:geodetic.elevation_unit: "US_survey_foot"` (confirmed identical in `config/pipeline_in_person.yaml`).

CRS allowlist (Colorado State Plane + UTM for demo):

| EPSG | Description |
|------|-------------|
| 2231 | NAD83 Colorado North (ftUS) |
| 2232 | NAD83 Colorado Central (ftUS) — primary for BV / Chaffee Co |
| 2233 | NAD83 Colorado South (ftUS) |
| 6428 | NAD83(2011) Colorado North (ftUS) |
| 6430 | NAD83(2011) Colorado Central (ftUS) |

Geoid model: **GEOID18**. Required epoch: **2010.0**.

Source: `config/pipeline.yaml:geodetic`.

### Geometry healing tolerances

| Tolerance | Value | Meaning |
|---|---|---|
| `close_tolerance` | 0.001 ft | Minimum gap that triggers polygon closure |
| `degenerate_face_threshold` | 0.0001 ft² | TIN faces with area below this are quarantined (excluded from the DXF) |
| `self_intersection_check` | `true` | Self-intersecting geometry is detected and repaired |

Healing behavior: geometry that can be repaired is repaired in place; geometry that cannot be healed is **excluded (quarantined)** and never written to the DXF. Quarantine counts are logged under audit event `"heal"`. Source: `config/pipeline.yaml:cad_shielding.geometry_healing`; `totali/cad_shielding/shield.py` `_heal_geometry` (C-2 invariant).

---

## 7. QA Layers

These two layers are written by the pipeline and carry advisory metadata, not certifiable geometry. They do not carry the `-DRAFT` suffix because they are not subject to the DRAFT/CERTIFIED lifecycle.

### `TOTaLi-QA-OCCLUSION`

- **Content:** Closed `LWPOLYLINE` polygons marking zones where the LiDAR point cloud was occluded (canopy, structures, shadow masks) and no reliable ground classification was possible.
- **Meaning:** Geometry overlapping this zone is uncertain. The review worksheet marks these items with `OcclusionType.UNKNOWN`. Certification requirements include a "field verification plan" for all occlusion zones.
- **Source:** `config/pipeline.yaml:cad_shielding.layer_mapping.occlusion_zones`; `shield.py` lines 382-392; `surveyor_lint.py` certification requirements.

### `TOTaLi-QA-FLAGS`

- **Content:** Uncertainty flags generated during extraction from low-confidence classification and sparse DTM point density.
- **Meaning:** Advisory alerts for the reviewing PLS. Flag severity and colors come from `config/pipeline.yaml:linting.flag_colors`:
  - High confidence: `#00FF00` (green)
  - Medium confidence: `#FFAA00` (amber)
  - Low confidence: `#FF0000` (red)
  - Occluded: `#FF00FF` (magenta)
- **Note:** The `TOTaLi-QA-FLAGS` layer is populated via the QA flags list on `ExtractionResult`; the geometric representation (point vs. polygon vs. leader) is not yet specified in the pipeline code.
- **Source:** `config/pipeline.yaml:linting.flag_colors`; `totali/linting/surveyor_lint.py` `_generate_lint_report`.

---

## 8. Open Questions for Partner

These questions must be answered at the meeting before the layer standard is treated as final.

1. **Are these layer names final?** Do `TOTaLi-SURV-DTM-DRAFT`, `TOTaLi-SURV-BRKLN-DRAFT`, etc. match the partner firm's layer naming standards, or must they be mapped/aliased?

2. **Certified layer name convention:** On PLS certification, `-DRAFT` is stripped to yield e.g. `TOTaLi-SURV-DTM`. Is this the correct certified layer name, or does the partner require a different suffix/prefix on certified geometry (e.g. `-CERT`, `-FINAL`)?

3. **Block library:** The pipeline emits raw point, polyline, and polygon entities with no symbols. What survey monument symbols, control point markers, benchmark callouts, and title block formats are required? Who owns the block library — TOTaLi or the partner firm?

4. **Color and lineweight standard:** No color or lineweight is currently enforced at the layer level. Is there an existing layer-color-lineweight standard (e.g. from the partner firm's template DWT, or a Civil 3D layer state) that these layers must conform to?

5. **CAD platform target:** Is the deliverable consumed in Civil 3D, plain AutoCAD, or another platform? Civil 3D surface objects (TIN surface from the 3DFACE entities) and layer states affect how the DXF is used. Does the partner need a Civil 3D-compatible surface definition, or is the flat 3DFACE mesh sufficient?

6. **DWG vs DXF:** The pipeline currently outputs DXF only (`format: "dxf"`); DWG output is noted as a stub (`_FORMAT_STATUS: "dwg": "stub"`). Does the final deliverable need to be a DWG file, and if so, by what deadline?

7. **Annotation / text requirements:** No text or labels are emitted. What annotation is required on the deliverable (contour labels, spot elevations, building area callouts, linear units statement block per C.R.S. § 38-51-106(1)(l))?

8. **Occlusion zone handling:** `TOTaLi-QA-OCCLUSION` polygons mark areas of potential terrain fabrication risk. Does the partner have a protocol for the field verification plan that certification requires, and how should those polygons be flagged on the final deliverable?

9. **Coded survey shot symbols:** Field-coded points from `.crd`/`.asc` input land as raw `POINT` entities on their coded layer. Does the partner require block substitution for specific field codes (e.g., iron pin found → a specific symbol block)?

10. **Multi-file / tile delivery:** The current pipeline writes a single `totali_draft_output.dxf` per run. If the project area requires tiling (e.g., multiple LAS files per job), what is the expected tiling and file-naming convention for the deliverable set?

---

## Notes for Maintainer

The following adjacent issues were observed during research for this document and are NOT fixed here:

- `config/pipeline.yaml:project.pls_authority` is set to `"CO PLS #XXXXX"` (placeholder). This will appear in certification records; it must be populated before a real deliverable is signed.
- `ROADMAP.md` §3.3 notes that building footprints use convex hull (coarse); alpha-shape is a TODO item. Partners reviewing building footprint accuracy should be aware of this limitation.
- `ROADMAP.md` §3.3 notes that breakline smoothing/filtering is not yet applied (raw slope discontinuities). Partners should expect unsmoothed breaklines in the current output.
- The `uncertainty_flags` layer (`TOTaLi-QA-FLAGS`) is configured in `layer_mapping` but its geometric representation (point, polygon, leader, or annotation) is not yet defined in the extraction or shielding code.
- DWG output is a stub (`_FORMAT_STATUS["dwg"] = "stub"`). Any partner requirement for DWG delivery will need a development milestone.
- The interactive CAD overlay / ghost layer review UI is described in design documents but not yet built (`ROADMAP.md` §3.5). Current review workflow is the `review_worksheet.txt` text file.
