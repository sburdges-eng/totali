# PLS Certification Fields — Board / ALTA Sign-off Material

**Status: DRAFT FOR BOARD/ALTA SIGN-OFF**
**Date: 2026-06-18**
**Item: U3 (partner-decision track, in-person follow-up)**

This document translates the machine-enforced certification schema
(`totali/linting/certification.py`) into a human-readable reference for the
partner PLS and the Colorado State Board of Licensure / title-insurance parties.
Nothing here is invented: every field key, label, and citation is quoted verbatim
from the source of truth at lines cited below.

---

## 1. Purpose and Status

TOTaLi's certification gate (U3) assembles a `CertificationRecord` that binds the
full defensibility chain — raw-LAS hash → classifier output → geometry extractor
output → per-item lint decisions → PLS identity/seal — to a fixed set of
board-required and ALTA-required fields before any deliverable is promoted out of
`DRAFT` status. The record is SHA-256-sealed at assembly time and re-verified by
`totali.audit.verify.verify_certification` before export.

This document is the sign-off artifact for the field set. The pipeline schema has
been advisor-confirmed (ADVISOR-RESOLVED OQ2/KTD4) for the partner's Colorado
jurisdiction. The partner PLS must confirm that every field listed here satisfies
their board requirements and that the Table A items in scope (4, 5, 7, 8, 11)
match their typical engagement scope.

**Pipeline invariants (from `artifacts/IN_PERSON_FOLLOWUP_PLAN_2026-06-18.md`):**

- `auto_promote: false` — AI output goes to `*-DRAFT` only; no automated promotion.
- `require_pls_signature: true` — certification cannot complete without a verified
  `CertifierIdentity` (name, license number, jurisdiction, optional seal reference).
- AI → DRAFT only; certified geometry requires explicit PLS accept/reject of every
  lint item (or an explicit written `defer_reason`).
- `audit_logs/` is an append-only SHA-256 chain; the `certify` event is logged with
  PLS name, license, jurisdiction, full chain references, field-key list, and the
  sealed record hash.

---

## 2. Authority Basis

| Standard | Scope in this pipeline |
|----------|------------------------|
| **Colorado C.R.S. § 38-51-106(1)(a)–(l)** | Twelve mandatory elements for any Colorado land-survey plat. All twelve are enforced as required fields. |
| **2021 ALTA/NSPS Minimum Standard Detail Requirements, § 7** | Certification statement components: named parties, fieldwork date, and standard reference. Three fields enforced. |
| **2021 ALTA/NSPS Table A optional items** | Client-selected scope items. The LiDAR/drone pipeline enforces items 4, 5, 7, 8, and 11 (five fields). Items outside this set are out-of-pipeline scope. |

Sources: `totali/linting/certification.py` module docstring (lines 1–17) and
`tests/test_certification.py` lines 1–12.

---

## 3. Field Tables

### 3a. Colorado Statutory Plat Elements — C.R.S. § 38-51-106(1)(a)–(l)

`certification.py` lines 48–81 (`_CO_PLAT_FIELDS`). All twelve are in
`REQUIRED_BOARD_ALTA_FIELDS`.

| # | Field Key | Label (verbatim) | Statutory Citation | Pipeline Population / Responsible Party | Required for Cert? |
|---|-----------|------------------|--------------------|------------------------------------------|--------------------|
| 1 | `co_plat_boundary_scale_drawing` | Scale drawing of the boundaries of the land parcel | C.R.S. § 38-51-106(1)(a) | Supplied by PLS from CAD plat drawing; pipeline provides georeferenced geometry as source material | Yes |
| 2 | `co_plat_rights_of_way_easements` | Recorded and apparent rights-of-way and easements (with source), or the client's statement electing not to show them | C.R.S. § 38-51-106(1)(b) | PLS must provide either the recorded/apparent R-O-W references or a written client election statement | Yes |
| 3 | `co_plat_field_measured_dimensions` | Field-measured dimensions necessary to establish the boundaries on the ground | C.R.S. § 38-51-106(1)(c) | Populated from pipeline-extracted geometry (breaklines, surface points); final values confirmed by PLS | Yes |
| 4 | `co_plat_responsible_charge_statement` | Statement that the survey was performed by, or under the responsible charge of, the professional land surveyor | C.R.S. § 38-51-106(1)(d) | PLS-supplied text; tied to `CertifierIdentity.name` and `license_number` in the record | Yes |
| 5 | `co_plat_basis_of_bearings` | Statement explaining how bearings were determined | C.R.S. § 38-51-106(1)(e) | PLS-supplied; must reference control network / GNSS session / PLSS bearing basis used for the job | Yes |
| 6 | `co_plat_monuments_found_and_set` | Description of all monuments found and set and all control monuments used in the survey | C.R.S. § 38-51-106(1)(f) | PLS-supplied field notes; pipeline does not auto-populate monument records | Yes |
| 7 | `co_plat_scale_and_bar` | Statement of scale or representative fraction and a bar/graphical scale | C.R.S. § 38-51-106(1)(g) | Derived from CAD sheet scale and DXF deliverable; PLS confirms on final plat | Yes |
| 8 | `co_plat_north_arrow` | North arrow | C.R.S. § 38-51-106(1)(h) | CAD deliverable element; PLS confirms present and oriented to stated basis of bearings | Yes |
| 9 | `co_plat_property_description` | Written property description (county, state, section, township, range, principal meridian or established subdivision/block/lot) | C.R.S. § 38-51-106(1)(i) | PLS-supplied legal description; pipeline project metadata may seed county/section/township if available | Yes |
| 10 | `co_plat_signature_and_seal` | Signature and seal of the professional land surveyor | C.R.S. § 38-51-106(1)(j) | `CertifierIdentity.seal_ref` carries the seal reference; physical/digital seal placement is PLS responsibility outside the pipeline | Yes |
| 11 | `co_plat_conflicting_boundary_evidence` | Any conflicting boundary evidence | C.R.S. § 38-51-106(1)(k) | PLS-supplied; pipeline lint flags geometry anomalies that may indicate conflicting evidence, but final determination is PLS-sovereign | Yes |
| 12 | `co_plat_linear_units_statement` | Statement defining the linear units used (conversion derived from the meter as defined by NIST) | C.R.S. § 38-51-106(1)(l) | Populated from pipeline geodetic report (CRS/units metadata); PLS confirms NIST-derived conversion statement | Yes |

### 3b. ALTA/NSPS 2021 Section 7 — Certification Statement Components

`certification.py` lines 84–94 (`_ALTA_SECTION7_FIELDS`). All three are in
`REQUIRED_BOARD_ALTA_FIELDS`.

| # | Field Key | Label (verbatim) | Standard Citation | Pipeline Population / Responsible Party | Required for Cert? |
|---|-----------|------------------|--------------------|------------------------------------------|--------------------|
| 1 | `alta_s7_certified_to` | Parties to whom the survey is certified | 2021 ALTA/NSPS § 7 | PLS-supplied; lender, title company, and/or buyer as specified in the engagement letter | Yes |
| 2 | `alta_s7_fieldwork_completion_date` | Date of the fieldwork completion stated in the certification | 2021 ALTA/NSPS § 7 | PLS-supplied; must match the date range of the LiDAR/drone acquisition and any subsequent ground-truth field sessions | Yes |
| 3 | `alta_s7_standard_reference` | Certification references the 2021 Minimum Standard Detail Requirements for ALTA/NSPS Land Title Surveys and the Table A items selected | 2021 ALTA/NSPS § 7 | PLS-supplied text; must enumerate the Table A items selected for this engagement (at minimum items 4, 5, 7, 8, 11 as scoped below) | Yes |

### 3c. ALTA/NSPS 2021 Table A — Items in Pipeline Scope

`certification.py` lines 97–115 (`_ALTA_TABLE_A_FIELDS`). All five are in
`REQUIRED_BOARD_ALTA_FIELDS`. Items outside {4, 5, 7, 8, 11} are currently
out-of-pipeline scope.

| # | Field Key | Label (verbatim) | Standard Citation | Pipeline Population / Responsible Party | Required for Cert? |
|---|-----------|------------------|--------------------|------------------------------------------|--------------------|
| 4 | `alta_table_a_4_gross_land_area` | Gross land area (and other areas if specified by the client) | 2021 ALTA/NSPS Table A, Item 4 | Derived by pipeline from classified boundary geometry; PLS confirms against legal description | Yes |
| 5 | `alta_table_a_5_vertical_relief` | Vertical relief with source of information, contour interval, datum, and originating benchmark | 2021 ALTA/NSPS Table A, Item 5 | Populated from pipeline surface model (LiDAR/drone DEM); datum/benchmark statement requires PLS input to name the originating control | Yes |
| 7 | `alta_table_a_7_building_dimensions` | Exterior building dimensions at ground level, exterior-footprint square footage, and measured building height (7a-c) | 2021 ALTA/NSPS Table A, Item 7 | Extracted by pipeline from classified building-footprint geometry (7a perimeter, 7b area, 7c height from point cloud); PLS reviews and accepts each lint item | Yes |
| 8 | `alta_table_a_8_substantial_features` | Substantial features observed during fieldwork (parking, signs, pools, landscaped areas, refuse, etc.) | 2021 ALTA/NSPS Table A, Item 8 | Pipeline classifies observed features; PLS must accept/reject each DRAFT classification before the field populates | Yes |
| 11 | `alta_table_a_11_underground_utility_evidence` | Evidence of underground utilities (11a plans/reports; 11b private locate markings) with the mandatory client/insurer/lender note | 2021 ALTA/NSPS Table A, Item 11 | Pipeline captures surface utility markers visible in the point cloud (11b); 11a requires PLS to attach or reference utility plans/reports; mandatory note is PLS-supplied | Yes |

---

## 4. Certification Gate

### How `REQUIRED_BOARD_ALTA_FIELDS` Is Enforced

`certification.py` line 126 derives `REQUIRED_BOARD_ALTA_FIELDS` directly from
`CERT_SCHEMA` (the ordered tuple of all 20 `CertField` objects):

```python
REQUIRED_BOARD_ALTA_FIELDS: tuple[str, ...] = tuple(f.key for f in CERT_SCHEMA)
```

`CERT_SCHEMA` is the single source of truth (line 119–123): CO statutory fields
first, then ALTA § 7, then Table A.

### What Blocks Promotion

1. **Open lint items.** `certify()` (lines 211–266) raises `CertificationBlocked`
   if any lint item carries status `DRAFT` or `FLAGGED` and no `defer_reason` is
   provided. An explicit written `defer_reason` is the only escape hatch; it is
   recorded in the `CertificationRecord` and emitted to the audit log.

2. **Missing or empty board/ALTA fields.** `verify_certification()` (in
   `totali/audit/verify.py`, lines 20–61) iterates `REQUIRED_BOARD_ALTA_FIELDS`
   and fails the record for any key that is absent from `board_alta_fields` or
   whose value is in `_EMPTY_VALUES` = `(None, "", [], {}, ())`. Note: field
   completeness is validated by `verify_certification`, not by `certify()`, so a
   record can be assembled incrementally; the gate fires at export/verification
   time.

3. **Incomplete certifier identity.** `verify_certification()` checks
   `CertifierIdentity.is_complete()`: all three of `name`, `license_number`, and
   `jurisdiction` must be non-empty. A missing `license_number` is a hard failure.

4. **Hash tamper.** If an `expected_record_hash` is supplied, `verify_certification()`
   recomputes the SHA-256 over the canonical record payload and rejects any
   mismatch.

### PLS Signature Requirement

The `require_pls_signature: true` invariant is enforced structurally: `certify()`
requires a `CertifierIdentity` argument (no default), and `CertifierIdentity.name`,
`license_number`, and `jurisdiction` must all be non-empty for `is_complete()` to
return `True`. An optional `seal_ref` field (`CertifierIdentity.seal_ref`) is
intended to carry a URI or file reference to the digital or scanned seal; placement
of the physical/digital seal on the output plat remains the PLS's direct
responsibility outside the pipeline.

### Audit Emission

Every successful `certify()` call emits a `"certify"` event to the audit log
(lines 253–264) containing: `project_id`, `pls_name`, `pls_license`,
`jurisdiction`, `raw_hash`, full `chain_refs`, sorted `board_alta_field_keys`, the
sealed `record_hash`, and any `defer_reason`. The audit log is SHA-256 chained and
append-only.

---

## 5. Sign-off Checklist for Board / Partner PLS

Use this checklist in the in-person meeting. Each item should be checked against
the partner's Board-required plat elements and their standard ALTA engagement scope.

### Colorado Statutory Elements (C.R.S. § 38-51-106(1)(a)–(l))

- [ ] **(a)** `co_plat_boundary_scale_drawing` — Does the pipeline's CAD geometry output satisfy the scale-drawing requirement, or does the partner need a specific sheet-scale workflow?
- [ ] **(b)** `co_plat_rights_of_way_easements` — Is the R-O-W/easement field expected to carry a structured reference list, or a free-text statement? Does the partner have a standard form for the client election statement?
- [ ] **(c)** `co_plat_field_measured_dimensions` — Are pipeline-extracted dimensions (from the DEM/breakline extractor) acceptable as field-measured, or does the Board require a separate field-note attestation?
- [ ] **(d)** `co_plat_responsible_charge_statement` — Confirm the standard wording the partner uses; the field accepts a free string, but the Board may require specific statutory language.
- [ ] **(e)** `co_plat_basis_of_bearings` — Confirm the datum/epoch statement wording for LiDAR-acquired projects (GNSS-controlled vs. PLSS-referenced). See also open question 3.
- [ ] **(f)** `co_plat_monuments_found_and_set` — Pipeline does not capture monument records from the point cloud. Confirm the partner's field workflow for supplying this field.
- [ ] **(g)** `co_plat_scale_and_bar` — Confirm the pipeline's DXF deliverable carries a machine-readable scale annotation that satisfies this requirement.
- [ ] **(h)** `co_plat_north_arrow` — Confirm presence in the DXF layer standard (see companion `DXF_DELIVERABLE_SPEC.md`).
- [ ] **(i)** `co_plat_property_description` — Confirm whether the partner expects a structured (county/section/township/range) or free-text legal description. Does the Board accept the pipeline-seeded metadata, or must the PLS type the final description independently?
- [ ] **(j)** `co_plat_signature_and_seal` — Confirm the seal capture mechanism (wet seal, digital PDF seal, or URI reference in `seal_ref`). See open question 2.
- [ ] **(k)** `co_plat_conflicting_boundary_evidence` — Confirm the partner's expectation: is a "none observed" statement sufficient, or must the field enumerate specific lint-flagged anomalies from the pipeline?
- [ ] **(l)** `co_plat_linear_units_statement` — Confirm the NIST-derived US Survey Foot vs. international foot distinction for this jurisdiction. Pipeline geodetic report currently states the CRS unit; does this satisfy the board's specific NIST-derivation requirement?

### ALTA/NSPS § 7 Certification Statement

- [ ] `alta_s7_certified_to` — Does the partner use a standard set of certification parties (lender + title company + buyer) or is it engagement-specific?
- [ ] `alta_s7_fieldwork_completion_date` — Confirm that LiDAR acquisition date is the controlling date, or whether a subsequent field-verification date overrides it.
- [ ] `alta_s7_standard_reference` — Confirm the exact 2021 ALTA/NSPS boilerplate wording the partner's firm uses, so the pipeline can supply a validated template rather than a free-text field.

### ALTA/NSPS Table A Items in Pipeline Scope

- [ ] **Item 4** `alta_table_a_4_gross_land_area` — Confirm area calculation method (2D projected vs. surface area) and whether "other areas" (e.g., impervious, wetland) are in scope for typical engagements.
- [ ] **Item 5** `alta_table_a_5_vertical_relief` — Confirm contour interval, datum statement wording, and whether the pipeline's benchmark reference (from GNSS control) satisfies ALTA requirements. See open question 3.
- [ ] **Item 7** `alta_table_a_7_building_dimensions` — Confirm which sub-items (7a exterior dimensions, 7b footprint area, 7c height) are included in the partner's standard scope.
- [ ] **Item 8** `alta_table_a_8_substantial_features` — Confirm what feature categories the partner routinely includes. Are there categories the pipeline's classifier does not currently produce that the partner would expect?
- [ ] **Item 11** `alta_table_a_11_underground_utility_evidence` — Confirm whether 11a (plans/reports) is in scope (requires external attachment), and confirm the standard wording for the mandatory client/insurer/lender note.

### Out-of-Pipeline Table A Items

The following Table A items are **not** currently enforced by the pipeline. If the
partner's standard scope includes any of these, they must be added to
`_ALTA_TABLE_A_FIELDS` and `CERT_SCHEMA` before certification:

Items 1, 2, 3, 6, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 (and any
optional items added in subsequent ALTA/NSPS revisions).

- [ ] **Confirm** none of the above are required for the partner's typical engagement scope.

---

## 6. Open Questions for Partner

1. **Table A scope.** Which Table A items are included in the partner's typical
   commercial/residential ALTA engagement? Are items 4, 5, 7, 8, and 11 always
   selected, or do they vary by project? Are any out-of-pipeline items (e.g., item 6
   zoning, item 19 parking count) routinely required?

2. **Seal/signature capture mechanism.** What is the partner's preferred workflow
   for `co_plat_signature_and_seal` / `alta_s7_certified_to` in a digital
   deliverable? Options: (a) wet-signed PDF plat attached as `seal_ref` URI, (b)
   embedded digital signature in the DXF/PDF, (c) notarized PDF in the project
   record. The pipeline's `CertifierIdentity.seal_ref` is a free string — the
   partner must confirm what value it should carry.

3. **Epoch and datum statement wording.** For LiDAR/drone-controlled surveys, the
   `co_plat_basis_of_bearings` and `alta_table_a_5_vertical_relief` fields both
   require a datum and epoch statement. What wording does the partner's Board expect
   for a GNSS-referenced, NAD83(2011) / NAVD88 project? Does the Board require
   explicit epoch (e.g., "2010.0") or is a CRS name sufficient?

4. **Responsible-charge statement wording.** Does the Colorado State Board of
   Licensure prescribe specific statutory language for
   `co_plat_responsible_charge_statement`, or is the partner's standard firm
   language sufficient? Is a template available?

5. **Monument records outside the pipeline.** `co_plat_monuments_found_and_set` is
   not populated by the pipeline (no monument detection from the point cloud).
   What is the partner's workflow for capturing this field — separate field form,
   survey notes attachment, or free-text entry into the certification UI?

6. **Colorado Board elements beyond the 12 codified in C.R.S. § 38-51-106(1).**
   Does the partner's Board of Licensure impose any additional plat-content
   requirements beyond subsections (a)–(l) (e.g., adjacent-owner names, certifier
   firm name/address, date of plat preparation, revision history block)? If so,
   these must be added to `_CO_PLAT_FIELDS`.

7. **ALTA/NSPS revision cycle.** The pipeline is coded against the 2021 standards.
   If the partner operates under a more recent ALTA/NSPS edition, confirm which
   edition governs and whether the § 7 certification language or Table A item
   numbering has changed.

---

## Notes for Maintainer

- The `totali/audit/verify.py` `verify_certification()` function (lines 20–61) takes
  `required_fields` as a caller-supplied argument rather than importing
  `REQUIRED_BOARD_ALTA_FIELDS` directly. A future refactor could default to the
  production set to reduce the risk of callers passing a stale or partial list.
- `CertificationRecord.status` defaults to `GeometryStatus.CERTIFIED.value` at
  construction time (line 172 of `certification.py`) regardless of whether
  `verify_certification` has been run. The status string does not enforce that
  verification passed; callers must check the `(ok, errors)` return value
  explicitly.
- The test suite (`tests/test_certification.py`) uses generic placeholder field keys
  (`required_field_x`, `required_field_y`) for most gate/chain tests, not the
  production `REQUIRED_BOARD_ALTA_FIELDS`. The class `TestProductionSchemaCompleteness`
  (lines 101–136) does exercise the production schema. Consider expanding production-
  schema coverage to the chain and tamper tests as well.
- `CertifierIdentity.seal_ref` is `Optional[str]` and is not currently validated by
  `is_complete()` or `verify_certification()`. If the Board requires a seal
  reference to be present (not just a signature text), an explicit non-None check
  should be added.
