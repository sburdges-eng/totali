# In-person meeting — capture sheet (2026-06-18)

Fillable form for the partner + PLS walkthrough. Pair with
`MEETING_EVIDENCE_PACK_2026-06-18.md` (what we show) and the source specs
`DXF_DELIVERABLE_SPEC.md` / `PLS_CERTIFICATION_FIELDS.md`. Capture answers inline
in the **Decision** columns; post-meeting these feed config tests + layer_mapping
updates + the cert schema lock.

---

## Part A — DXF deliverable (walk `DXF_DELIVERABLE_SPEC.md §8`)

Mark each of the spec's 10 open questions (Q1–Q10) as the partner answers. Plus
one new question surfaced by the demo run:

| # | Question | Partner decision |
|---|----------|------------------|
| Q1 | Layer names final as `TOTaLi-<DISC>-<FEAT>-DRAFT`? | |
| Q2 | Certified-layer suffix (drop `-DRAFT` vs explicit `-CERT`)? | |
| Q3 | Block/symbol library required? which blocks? | |
| Q4 | Color / lineweight standard (by layer)? | |
| Q5 | Target CAD platform (Civil 3D vs Carlson) + layer-state expectations? | |
| Q6 | DWG output required, or DXF sufficient for v1? | |
| Q7 | Annotation/text style requirements? | |
| Q8 | Occlusion-zone verification protocol? | |
| Q9 | Coded-shot symbol conventions? | |
| Q10 | Tiling / sheet convention? | |
| **Q11 (NEW)** | **Demo emitted 14 coded-survey layers but the spec maps 10. The 4 extra (`SURV-MON`, `SURV-TOPO`, `SURV-WATER`, `PLAN-UTILITY` — all `-DRAFT`) come from field-code→layer mapping, not `cad_shielding.layer_mapping`. Are all 14 correct/wanted? Confirm names + the field-code→layer source of truth.** | |

**Post-meeting encode →** `config` `cad_shielding.layer_mapping` + field-code map updates, with a `tests/test_layer_mapping_contract.py`-style test asserting the agreed layer set.

---

## Part B — PLS certification fields (walk `PLS_CERTIFICATION_FIELDS.md`)

For each field the PLS marks: **Required?** (Y/N for this deliverable type) and
**Source** — confirm/override the *suggested* column. Suggested source =
`pipeline` (pipeline can draft a candidate value from geodetic/extraction
output) or `PLS` (requires PLS judgment, identity, legal/title, or field
observation). All 20 are in `REQUIRED_BOARD_ALTA_FIELDS` today.

### Colorado statutory plat — C.R.S. § 38-51-106(1)(a)–(l)
| Field key | Element | Suggested source | PLS: Required? | PLS: Source |
|-----------|---------|------------------|----------------|-------------|
| co_plat_boundary_scale_drawing (a) | Boundary scale drawing | pipeline | | |
| co_plat_rights_of_way_easements (b) | ROW / easements (or election not to show) | PLS (title) | | |
| co_plat_field_measured_dimensions (c) | Field-measured dimensions | pipeline | | |
| co_plat_responsible_charge_statement (d) | Responsible-charge statement | PLS | | |
| co_plat_basis_of_bearings (e) | Basis of bearings | pipeline (from CRS) | | |
| co_plat_monuments_found_and_set (f) | Monuments found/set + control | pipeline (coded MON) | | |
| co_plat_scale_and_bar (g) | Scale + bar | pipeline | | |
| co_plat_north_arrow (h) | North arrow | pipeline | | |
| co_plat_property_description (i) | Written property description | PLS (legal) | | |
| co_plat_signature_and_seal (j) | Signature + seal | PLS (sovereign) | | |
| co_plat_conflicting_boundary_evidence (k) | Conflicting boundary evidence | PLS | | |
| co_plat_linear_units_statement (l) | Linear units statement | pipeline (elevation_unit) | | |

### 2021 ALTA/NSPS § 7
| Field key | Element | Suggested source | PLS: Required? | PLS: Source |
|-----------|---------|------------------|----------------|-------------|
| alta_s7_certified_to | Parties certified to | PLS (client) | | |
| alta_s7_fieldwork_completion_date | Fieldwork completion date | pipeline (capture) / PLS | | |
| alta_s7_standard_reference | 2021 standard + Table A items reference | PLS | | |

### 2021 ALTA/NSPS Table A (in-scope items)
| Field key | Item | Suggested source | PLS: Required? | PLS: Source |
|-----------|------|------------------|----------------|-------------|
| alta_table_a_4_gross_land_area | 4 — Gross land area | pipeline (computed) | | |
| alta_table_a_5_vertical_relief | 5 — Vertical relief/contours/datum | pipeline | | |
| alta_table_a_7_building_dimensions | 7 — Building dimensions | pipeline (extraction) | | |
| alta_table_a_8_substantial_features | 8 — Substantial features | PLS (field obs) | | |
| alta_table_a_11_underground_utility_evidence | 11 — Underground utility evidence | PLS (locate) | | |

**Post-meeting encode →** any field the PLS removes from "required" or reclassifies
updates `REQUIRED_BOARD_ALTA_FIELDS` / `CERT_SCHEMA`; pipeline-sourced fields get
a populate-candidate path; re-run `tests/test_certification.py`.

---

## Part C — show & confirm (evidence pack)
- [ ] Open the 3 pipeline DXFs (BV coded / USGS LAS / Sales flow) in their tool.
- [ ] `verify_log PASS` on all 3 audit chains.
- [ ] DRAFT-only layers; `auto_promote: false`, `require_pls_signature: true`.
- [ ] Pre-converted civil DXFs (LibreDWG reference only — not pipeline output).
- [ ] Manual baseline: confirm the real per-job manual hours (SC2) — demo used 14400s (4h) as a placeholder.

## Part D — partner inputs to collect (unblock M2)
- [ ] **Partner job LAS** (real, ≥5 classes if possible) → true U4 E2E + richer U1 spike.
- [ ] Partner's **operating jurisdictions** (CRS + bounds) → promote into `pipeline.yaml jurisdiction_zones`.
- [ ] Paid-job greenlight + job ID → M2 SC1–SC4 evidence.
