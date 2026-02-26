# TOTaLi Project Outline

> Structural reference for total project completion.
>
> Each section maps to a codebase directory, references the specific source
> files that implement it, and notes the work remaining.

---

## 1. Defensible Spatial Drafting Pipeline

**Directory:** `totali/`
**Entry point:** `totali/main.py`
**Config:** `config/pipeline.yaml`

```
totali/
├── main.py                  ← CLI (click), loads config, runs orchestrator
├── __init__.py              ← v0.1.0
├── geodetic/
│   └── gatekeeper.py       ← Phase 1: CRS validate, PROJ transform, hash
├── segmentation/
│   └── classifier.py       ← Phase 2: ONNX inference or rule fallback
├── extraction/
│   └── extractor.py        ← Phase 3: Delaunay TIN, breaklines, contours
├── cad_shielding/
│   └── shield.py           ← Phase 4: Geometry heal, DXF write, entity manifest
├── linting/
│   └── surveyor_lint.py    ← Phase 5: Lint items, accept/reject, certify
├── pipeline/
│   ├── orchestrator.py     ← Sequential phase runner
│   └── models.py           ← Shared dataclasses (GeometryStatus, LintItem, etc.)
└── audit/
    └── logger.py           ← SHA-256 chained JSONL audit log
```

### Data flow

```
LAS/LAZ file
  │
  ▼
Phase 1: Geodetic Gatekeeper
  │  Validates CRS, transforms to target datum
  │  Outputs: standardized LAS + metadata JSON
  ▼
Phase 2: ML Segmentation
  │  Classifies points (ground, building, wire, etc.)
  │  Outputs: ClassificationResult (labels, confidences, occlusion mask)
  ▼
Phase 3: Deterministic Extraction
  │  Builds DTM, breaklines, contours, footprints
  │  Outputs: ExtractionResult (TIN, polylines, polygons, QA flags)
  ▼
Phase 4: CAD Shielding
  │  Heals geometry, writes DXF on DRAFT layers
  │  Outputs: DXF file + entity manifest (UUID per entity)
  ▼
Phase 5: Surveyor Linting
  │  Creates lint items with confidence/occlusion flags
  │  Outputs: lint report + review worksheet
  │
  ▼
Human review: Accept / Reject each item → Promote to CERTIFIED
  │
  ▼
Audit log: Every action recorded with SHA-256 chain
```

### Completion work

- [ ] Unit tests for all five phase modules
- [ ] Integration test: full LAS → DXF → audit-verify
- [ ] Ship or document ONNX model acquisition
- [ ] Sample LAS test data
- [ ] Alpha-shape footprints (replace convex hull)
- [ ] DWG output support
- [ ] Interactive review UI (ghost layer in CAD viewer)
- [ ] Vertical datum / geoid height cross-check
- [ ] Log rotation for audit logger

---

## 2. Geodetic Adjustment & ALTA Compliance (Laser Suite)

**Directory:** `laser-suite/`
**Package:** `laser-suite/python/laser_suite/`
**Tests:** `laser-suite/python/tests/`
**Config:** `laser-suite/config/`
**Docs:** `laser-suite/docs/`

```
laser_suite/
├── adjustment.py      ← WLS solver (SVD), covariance matrix
├── rpp.py             ← Relative Positional Precision, ALTA/NSPS compliance
├── encroachment.py    ← Boundary intersection, depth metrics
├── cli.py             ← run | laser | encroachment | export-civil3d
├── config.py          ← YAML config loader + validation
├── contracts.py       ← JSON artifact contracts (deterministic ordering)
├── schemas.py         ← Station, Observation, WeightRule, AdjacencyPair
├── io_csv.py          ← CanonicalBundle: 8 required CSVs
├── io_json.py         ← Deterministic JSON writer
└── geoid_interface.py ← Geoid data abstraction
```

### Key formulas (ref: `docs/formulas.md`)

- **WLS correction:** `x̂ = (AᵀPA)⁻¹ Aᵀ P l`
- **RPP actual:** `2.448 × √(λ_max(Σ_ij))`
- **RPP allowable:** `0.02 + 50×10⁻⁶ × distance_m`

### Completion work

- [ ] Ship geoid data files (GEOID18)
- [ ] ALTA Item 20 table generation (integrate C# bridge)
- [ ] Production CI workflow for laser-suite
- [ ] Performance benchmarks for large adjustment networks

---

## 3. Survey Data Automation

**Directory:** `survey-automation-roadmap/`
**Mirror:** `groundtruthos-data/survey-automation/`
**Tests:** 17+ unit, 3 integration
**Docs:** `docs/operations.md`, `docs/roadmap-pt2.md`, `docs/release-notes-v2.0.0.md`

### Capabilities (v2.0.0 — released)

- CSV, DXF, CRD, text-point ingestion
- Config-driven parser validation
- QC profiles: strict / standard / legacy
- Quarantine & triage workflow
- Warning noise controls (3723 → 0)
- Golden verification suite (6 suites PASS)
- Release-candidate gate enforcement
- Concurrent execution streams 1-6

### Completion work

- [ ] Implement deterministic intent-geometry bridge
  - Source binding (points.csv anchor)
  - Rule-driven intent derivation
  - Deterministic geometry derivation
  - Manifest/export with artifact contracts
  - CI contract and replay validation
  - **Ref:** `docs/deterministic-intent-geometry-bridge.md`
- [ ] DWG tool parser production integration (see §5)
- [ ] Automate `groundtruthos-data` sync

---

## 4. CAD Corpus Curation

**Directory:** `AUTOMATICCAD/`
**Goals:** `AUTOMATICCAD/GOALS.md`
**Plan:** `AUTOMATICCAD/NEXT_STEPS.md`
**Inventory:** `AUTOMATICCAD/cad_parsing_automation_inventory_2026-02-18.md`
**Report:** `AUTOMATICCAD/manifests/AUTOMATICCAD_REPORT_2026-02-18.md`

### Current baseline

| Metric | Value |
|--------|-------|
| Files scanned | 3,064,920 |
| CAD files matched | 382 |
| Unique/canonical | 160 |
| Duplicates | 200 |
| Timeout/errors | 22 |
| Unique DWG/DXF candidates | 10 |

### Five goals

1. **Complete CAD discovery** — resolve 22 failed paths
2. **Canonical deduplicated corpus** — SHA-256 dedup verified
3. **Curate for project-relevant content** — include/exclude rules
4. **Prioritize DWG/DXF inputs** — tag production vs fixture
5. **Reproducible pipeline** — runbook + QA report

### Completion work

- [ ] Retry 22 failed scan paths → `error_retry` CSV
- [ ] Write and freeze curation rules → `CURATION_RULES.md`
- [ ] Generate curated manifest and files directory
- [ ] Create DWG/DXF priority CSV with production/sample/fixture tags
- [ ] Write `RUNBOOK.md` with exact rerun procedure
- [ ] Publish final curated report

---

## 5. DWG/DXF Tool Parser

**Directory:** `survey-automation-roadmap/dwg-tool-parser/`
**Spec:** `SKILL.md`
**References:** `references/backends.md`, `references/cad-logic.md`, `references/land-survey-civil.md`
**Prototype:** `scripts/parse_dwg.py`

### Specified output contract (from SKILL.md)

```json
{
  "summary": { "entity_count": ..., "layers": [...], "blocks": [...] },
  "entities": [ { "type": ..., "layer": ..., "geometry": ... } ],
  "topology": { "nodes": [...], "edges": [...], "loops": [...] },
  "civil_survey": { "parcels": [...], "contours": [...], "utilities": [...] },
  "domain_confidence": { ... }
}
```

### Completion work

- [ ] Implement full `parse_dwg.py` per SKILL.md contract
- [ ] Support DWG → DXF converter backends
- [ ] Extract topology graph (nodes, edges, loops, connectivity)
- [ ] Detect civil-survey features using domain rules
- [ ] Compute domain confidence scores
- [ ] Integration tests with AUTOMATICCAD corpus files
- [ ] Wire into survey-automation pipeline

---

## 6. Snippet & Pattern Analysis Tools

**Directories:** `tools/`, `skills/`

| File | Purpose | Status |
|------|---------|--------|
| `extract_snippets_evidence.py` | Extract code snippets with evidence | ✅ Done |
| `extract_snippets_strict.py` | Strict snippet extraction rules | ✅ Done |
| `build_snippet_dependencies.py` | Dependency graph (Python, JS, Rust, C++) | ✅ Done |
| `generate_pattern_catalog.py` | Pattern catalog generation | ⚠️ Referenced but missing |

### Completion work

- [ ] Implement `generate_pattern_catalog.py` (or remove references)
- [ ] Run dependency graph on current codebase and publish results

---

## 7. Configuration & Infrastructure

### 7.1 Pipeline configuration (`config/pipeline.yaml`)

Fully specified for all five phases. Colorado NAD83-specific. Key settings:

- `auto_promote: false` (hardcoded, non-overridable)
- `require_pls_signature: true`
- Layer convention: `TOTaLi-SURV-{class}-DRAFT`
- Audit: JSONL + SHA-256

### 7.2 Package distribution

- `setup.py` — core, ML, CAD, and full extras
- `requirements.txt` — flat dependency list

### 7.3 CI/CD

- `laser-suite/.github/` — GitHub Actions structure exists
- No CI for `totali/` package
- `survey-automation-roadmap/` has gate scripts

### Completion work

- [ ] GitHub Actions workflow for `totali/` (lint, test, build)
- [ ] Unified CI that runs tests across all components
- [ ] Dependency vulnerability scanning (Dependabot or equivalent)

---

## 8. Documentation

### Existing documentation

| Document | Location | Covers |
|----------|----------|--------|
| Project README | `README.md` | Architecture, install, usage |
| Laser-suite README | `laser-suite/README.md` | CLI, CSV format |
| Survey-auto README | `survey-automation-roadmap/README.md` | Commands, exit codes |
| Operations runbook | `survey-automation-roadmap/docs/operations.md` | Daily ops, triage |
| Math formulas | `laser-suite/docs/formulas.md` | WLS, RPP |
| Civil3D bridge | `laser-suite/docs/civil3d-bridge.md` | C# integration |
| Artifact contracts | `laser-suite/docs/contracts.md` | JSON schema rules |
| Release notes v2 | `survey-automation-roadmap/docs/release-notes-v2.0.0.md` | v2.0.0 features |
| Intent-geometry bridge | `survey-automation-roadmap/docs/deterministic-intent-geometry-bridge.md` | Design spec |
| DWG parser skill | `survey-automation-roadmap/dwg-tool-parser/SKILL.md` | Parser contract |
| CAD goals | `AUTOMATICCAD/GOALS.md` | 5 named goals |
| CAD next steps | `AUTOMATICCAD/NEXT_STEPS.md` | Weekly execution plan |
| **This roadmap** | `ROADMAP.md` | Full project roadmap |
| **This outline** | `PROJECT_OUTLINE.md` | Structural reference |

### Research artifacts (root)

- `Deep Research Report on Defensible Spatial Pipelines.pdf`
- `Where Hybrid Survey Automation Still Breaks.pdf`
- `TOTaLi_Drafting_Proposal.docx`
- `TOTaLi_Drafting_Whitepaper.docx`

### Completion work

- [ ] Architecture diagram (visual, not ASCII)
- [ ] API reference (auto-generated from docstrings)
- [ ] Deployment guide (beyond `pip install`)
- [ ] Root `CHANGELOG.md`
- [ ] `LICENSE` file
- [ ] `CONTRIBUTING.md`

---

## 9. Testing Strategy

### Current test coverage

| Component | Unit | Integration | Golden | CI |
|-----------|------|-------------|--------|----|
| `totali/` | ❌ | ❌ | ❌ | ❌ |
| `laser-suite/` | ✅ 5 files | ✅ 4 files | ❌ | ⚠️ |
| `survey-automation/` | ✅ 17+ files | ✅ 3 files | ✅ 6 suites | ✅ |

### Test plan for completion

- [ ] `totali/` unit tests:
  - `test_gatekeeper.py` — CRS validation, transform, reject bad datum
  - `test_classifier.py` — Rule fallback, confidence thresholds, occlusion
  - `test_extractor.py` — TIN generation, breaklines, contours, QA flags
  - `test_shield.py` — Healing, DXF output, entity manifest
  - `test_surveyor_lint.py` — Lint items, accept/reject, promote gate
  - `test_orchestrator.py` — Phase sequencing, error handling
  - `test_audit_logger.py` — Hash chain, verification, event filtering
- [ ] `totali/` integration test:
  - End-to-end LAS → DXF with audit verification
- [ ] Cross-component integration:
  - Laser-suite adjustment output → totali extraction input
  - Survey-auto normalized data → totali pipeline
- [ ] DWG parser tests:
  - Parse known DWG/DXF from AUTOMATICCAD corpus
  - Validate output against SKILL.md contract

---

## 10. Dependency Map

```
                    ┌──────────────────┐
                    │  Survey Data     │
                    │  (CSV/DXF/CRD)   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Survey          │
                    │  Automation      │◄── DWG Tool Parser
                    │  (v2.0.0)        │
                    └────────┬─────────┘
                             │ normalized artifacts
                    ┌────────▼─────────┐
                    │  Laser Suite     │
                    │  (WLS/RPP/ALTA)  │
                    └────────┬─────────┘
                             │ adjusted coords
    ┌────────────┐  ┌────────▼─────────┐
    │  LAS/LAZ   │──►  TOTaLi Core    │
    │  Point     │  │  Pipeline        │◄── CAD Corpus
    │  Clouds    │  │  (5 phases)      │    (AUTOMATICCAD)
    └────────────┘  └────────┬─────────┘
                             │ DXF + audit log
                    ┌────────▼─────────┐
                    │  Human Review    │
                    │  (PLS Certify)   │
                    └──────────────────┘
```

---

## 11. Priority Order for Completion

Based on dependencies and impact:

1. **Core pipeline tests** — prove what exists works correctly
2. **AUTOMATICCAD corpus** — unblock DWG parser development
3. **DWG parser implementation** — enable full CAD ingestion
4. **Intent-geometry bridge** — connect survey-auto to pipeline
5. **Cross-component integration** — wire everything together
6. **CI/CD unification** — automate quality gates
7. **Documentation & release** — ship v1.0.0
