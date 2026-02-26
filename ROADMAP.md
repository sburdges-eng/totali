# TOTaLi Project Roadmap

> Last updated: 2026-02-26
>
> Principle: **AI Classifies → Algorithms Measure → Humans Certify**

This roadmap references the full codebase and documentation to track every
component required for total project completion. Each section maps to a
repository directory or module and lists what is done, what is in progress,
and what remains.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Component Status Summary](#2-component-status-summary)
3. [Phase 1 — Core Pipeline (`totali/`)](#3-phase-1--core-pipeline-totali)
4. [Phase 2 — Laser Suite (`laser-suite/`)](#4-phase-2--laser-suite-laser-suite)
5. [Phase 3 — Survey Automation (`survey-automation-roadmap/`)](#5-phase-3--survey-automation-survey-automation-roadmap)
6. [Phase 4 — CAD Corpus (`AUTOMATICCAD/`)](#6-phase-4--cad-corpus-automaticcad)
7. [Phase 5 — DWG/DXF Tool Parser](#7-phase-5--dwgdxf-tool-parser)
8. [Phase 6 — Integration & End-to-End Testing](#8-phase-6--integration--end-to-end-testing)
9. [Phase 7 — Documentation & Release](#9-phase-7--documentation--release)
10. [Cross-Cutting Concerns](#10-cross-cutting-concerns)
11. [Milestones & Timeline](#11-milestones--timeline)
12. [Exit Criteria](#12-exit-criteria)

---

## 1. Project Overview

TOTaLi is a defensible spatial drafting system (the TOTaLi-Assisted Drafting
Pipeline) that combines:

- **LiDAR point-cloud processing** (classification, DTM, breaklines, contours)
- **Geodetic adjustment** (WLS solver, RPP compliance, ALTA/NSPS standards)
- **Survey-data automation** (CSV/DXF/CRD ingestion, QC, quarantine)
- **CAD corpus curation** (DWG/DXF discovery, dedup, parsing)
- **Human-in-the-loop certification** (ghost suggestions, accept/reject, audit)

The pipeline enforces that no AI-generated geometry is ever auto-promoted to
certified status. Every action is logged in a SHA-256-chained audit trail.

---

## 2. Component Status Summary

| # | Component | Directory | Status |
|---|-----------|-----------|--------|
| 1 | Core Pipeline (5 phases) | `totali/` | ✅ Implemented |
| 2 | Pipeline Config | `config/pipeline.yaml` | ✅ Complete |
| 3 | Laser Suite (WLS/RPP) | `laser-suite/` | ✅ Implemented |
| 4 | Survey Automation CLI | `survey-automation-roadmap/` | ✅ v2.0.0 released |
| 5 | Ground Truth Data | `groundtruthos-data/` | ✅ Synced copy |
| 6 | CAD Corpus Curation | `AUTOMATICCAD/` | ⚠️ In progress |
| 7 | DWG/DXF Tool Parser | `survey-automation-roadmap/dwg-tool-parser/` | ⚠️ Spec only |
| 8 | Snippet/Pattern Tools | `tools/`, `skills/` | ✅ Complete |
| 9 | Package Distribution | `setup.py`, `requirements.txt` | ✅ Complete |
| 10 | End-to-End Integration | — | ❌ Not started |
| 11 | CI/CD Pipeline | `.github/` (laser-suite) | ⚠️ Partial |
| 12 | Project Documentation | `README.md`, docs | ⚠️ Partial |

---

## 3. Phase 1 — Core Pipeline (`totali/`)

The five-phase drafting pipeline.

### 3.1 Geodetic Gatekeeper (`totali/geodetic/gatekeeper.py`)

| Item | Status | Notes |
|------|--------|-------|
| CRS extraction from LAS VLRs | ✅ Done | Reads WKT, validates EPSG |
| Allowed-CRS whitelist enforcement | ✅ Done | Colorado NAD83 codes in config |
| PROJ-based CRS transformation | ✅ Done | Transforms to target CRS |
| Epoch & geoid validation | ✅ Done | NAD83(2011) epoch 2010.0, GEOID18 |
| SHA-256 input hashing | ✅ Done | Chain of custody |
| Mixed-datum rejection | ✅ Done | Fails on ambiguous CRS |
| **Remaining** | | |
| Multi-file batch processing | ❌ Todo | Single file only today |
| Vertical datum validation | ❌ Todo | Geoid height cross-check |

### 3.2 ML Segmentation (`totali/segmentation/classifier.py`)

| Item | Status | Notes |
|------|--------|-------|
| ONNX model loading | ✅ Done | PointTransformer architecture |
| Batched inference | ✅ Done | Configurable batch size |
| Rule-based fallback | ✅ Done | Elevation percentile heuristics |
| Per-point confidence scores | ✅ Done | Softmax probabilities |
| Occlusion detection | ✅ Done | Canopy, structure, shadow masks |
| 21-class mapping | ✅ Done | Config-driven class labels |
| **Remaining** | | |
| Trained ONNX model artifact | ❌ Todo | No model file shipped |
| Model versioning / registry | ❌ Todo | Path hardcoded in config |
| GPU inference path | ⚠️ Partial | Config supports CUDA, untested |

### 3.3 Deterministic Extraction (`totali/extraction/extractor.py`)

| Item | Status | Notes |
|------|--------|-------|
| Delaunay TIN from ground points | ✅ Done | Max-edge filtering |
| Breakline extraction | ✅ Done | Slope discontinuity detection |
| Contour generation | ✅ Done | Configurable interval (1 ft default) |
| Building footprint extraction | ✅ Done | Clustering + convex hull |
| Linear features (curb, wire) | ✅ Done | PCA + sorting |
| Hardscape polygons | ✅ Done | Convex hull |
| Occlusion zone geometry | ✅ Done | From classifier mask |
| QA flag generation | ✅ Done | Low confidence, sparse DTM alerts |
| **Remaining** | | |
| Alpha-shape building footprints | ❌ Todo | Convex hull is coarse |
| Breakline smoothing / filtering | ❌ Todo | Raw slope discontinuities |

### 3.4 CAD Shielding (`totali/cad_shielding/shield.py`)

| Item | Status | Notes |
|------|--------|-------|
| Geometry healing (degenerate faces) | ✅ Done | Validation + quarantine |
| DXF output via ezdxf | ✅ Done | Preferred path |
| Manual DXF fallback | ✅ Done | Minimal writer (breaklines) |
| Layer naming convention | ✅ Done | `TOTaLi-SURV-*-DRAFT` |
| Entity manifest (UUID per entity) | ✅ Done | SHA-256 audit records |
| **Remaining** | | |
| DWG output support | ❌ Todo | DXF only today |
| Middleware timeout / retry | ⚠️ Partial | Config present, limited testing |
| Entity-level quarantine UI | ❌ Todo | Quarantine logged, not surfaced |

### 3.5 Surveyor Linting (`totali/linting/surveyor_lint.py`)

| Item | Status | Notes |
|------|--------|-------|
| Lint item generation from manifest | ✅ Done | Confidence + occlusion flags |
| Accept / reject workflow | ✅ Done | Static methods, logs reviewer |
| Promote-to-certified gate | ✅ Done | All items must be reviewed |
| Review worksheet generation | ✅ Done | Human-readable checklist |
| auto_promote = false enforcement | ✅ Done | Hardcoded |
| PLS signature requirement | ✅ Done | Enforced |
| **Remaining** | | |
| Spatial intersection for occlusion | ❌ Todo | Simplified proxy in place |
| Interactive review UI (CAD overlay) | ❌ Todo | Ghost layer described, not built |

### 3.6 Pipeline Orchestrator (`totali/pipeline/`)

| Item | Status | Notes |
|------|--------|-------|
| Sequential phase execution | ✅ Done | Geodetic → Segment → Extract → Shield → Lint |
| Phase-level logging | ✅ Done | Start, complete, exception events |
| Selective phase execution | ✅ Done | `--phase` CLI flag |
| Context passing between phases | ✅ Done | Forward outputs |
| Dry-run mode | ✅ Done | `--dry-run` CLI flag |
| **Remaining** | | |
| Parallel phase execution | ❌ Todo | Sequential only |
| Phase retry / resume from failure | ❌ Todo | Restart from scratch |

### 3.7 Audit Logger (`totali/audit/logger.py`)

| Item | Status | Notes |
|------|--------|-------|
| SHA-256 chained JSONL logging | ✅ Done | Immutable append-only |
| Chain verification | ✅ Done | `verify_chain()` method |
| Event filtering | ✅ Done | By event type |
| Summary statistics | ✅ Done | Event counts |
| **Remaining** | | |
| Log rotation / archival | ❌ Todo | Single growing file |
| External audit export (PDF) | ❌ Todo | JSONL only |

---

## 4. Phase 2 — Laser Suite (`laser-suite/`)

Geodetic adjustment and ALTA/NSPS compliance engine.

**Source:** `laser-suite/python/laser_suite/`
**Tests:** `laser-suite/python/tests/` (5 unit + 4 integration)
**Config:** `laser-suite/config/pipeline.example.yaml`, `pipeline.prod.yaml`

| Item | Status | Notes |
|------|--------|-------|
| WLS adjustment solver (SVD) | ✅ Done | `adjustment.py` |
| RPP computation & compliance | ✅ Done | `rpp.py`, ALTA/NSPS thresholds |
| Encroachment analysis | ✅ Done | `encroachment.py`, boundary intersections |
| Canonical CSV bundle I/O | ✅ Done | `io_csv.py`, 8 required CSVs |
| JSON artifact contracts | ✅ Done | `contracts.py`, deterministic ordering |
| CLI (run, laser, encroachment, export) | ✅ Done | `cli.py` |
| Configuration validation | ✅ Done | `config.py` |
| Geoid interface | ✅ Done | `geoid_interface.py` |
| Unit tests (5 files) | ✅ Done | Adjustment, RPP, contracts, encroachment, CLI |
| Integration tests (4 files) | ✅ Done | Pass/fail cases, reproducibility, Civil3D export |
| C# Civil3D bridge stub | ✅ Done | `dotnet/Civil3DBridgeStub/` |
| **Remaining** | | |
| Geoid data files | ❌ Todo | Interface defined, no data shipped |
| ALTA Item 20 table generation | ⚠️ Partial | C# stub, not integrated |
| Production CI workflow | ❌ Todo | `.github/` structure exists |

---

## 5. Phase 3 — Survey Automation (`survey-automation-roadmap/`)

Mixed-format survey data ingestion, QC, and normalization pipeline.

**Source:** `survey-automation-roadmap/` (+ mirror in `groundtruthos-data/survey-automation/`)
**Tests:** 17+ unit tests, 3 integration tests
**Docs:** operations.md, roadmap-pt2.md, release-notes-v2.0.0.md, etc.

| Item | Status | Notes |
|------|--------|-------|
| CSV / DXF / CRD ingestion | ✅ Done | v2.0.0 |
| Config-driven parser validation | ✅ Done | Milestone 1 |
| CRD converter contract | ✅ Done | `crd-converter.md` |
| QC profiles (strict/standard/legacy) | ✅ Done | Milestone 3 |
| Quarantine & triage | ✅ Done | Automatic + manual modes |
| Warning noise controls | ✅ Done | 3723→0 warning reduction |
| Golden verification suite | ✅ Done | 6 test suites PASS |
| Release-candidate gate | ✅ Done | `v2_release_candidate_gate.sh` |
| PT II milestones 0-3 | ✅ Done | All complete 2026-02-13 |
| Concurrent execution streams 1-6 | ✅ Done | Scope through rollout |
| Deterministic intent-geometry bridge | ❌ Spec only | Design doc exists, no code |
| DWG tool parser integration | ❌ Spec only | SKILL.md + reference docs |
| **Remaining** | | |
| Intent-geometry bridge implementation | ❌ Todo | `deterministic-intent-geometry-bridge.md` |
| DWG parser production deployment | ❌ Todo | `parse_dwg.py` is prototype |
| `groundtruthos-data` sync automation | ❌ Todo | Manual copy today |

---

## 6. Phase 4 — CAD Corpus (`AUTOMATICCAD/`)

CAD file discovery, deduplication, and curation for downstream automation.

**Ref:** `AUTOMATICCAD/GOALS.md`, `AUTOMATICCAD/NEXT_STEPS.md`

| Goal | Status | Evidence |
|------|--------|----------|
| 1. Complete CAD discovery | ⚠️ In progress | 22 timeout/error paths unresolved |
| 2. Canonical deduplicated corpus | ⚠️ In progress | 160 unique files, SHA-256 dedup |
| 3. Curate for project-relevant content | ❌ Not started | Include/exclude rules pending |
| 4. Prioritize DWG/DXF inputs | ⚠️ In progress | 10 unique candidates identified |
| 5. Reproducible pipeline | ❌ Not started | Runbook pending |

**Execution Plan (from `NEXT_STEPS.md`):**

| Day | Focus | Deliverable |
|-----|-------|-------------|
| 1 | Resolve 22 failed paths | `error_retry` CSV + updated summary |
| 2 | Freeze curation rules | `CURATION_RULES` document |
| 3 | Build curated corpus | Curated manifest + files directory |
| 4 | Validate DWG/DXF priority set | Priority CSV with tags |
| 5 | Runbook + QA report | `RUNBOOK.md` + final curated report |

---

## 7. Phase 5 — DWG/DXF Tool Parser

**Ref:** `survey-automation-roadmap/dwg-tool-parser/SKILL.md`

| Item | Status | Notes |
|------|--------|-------|
| SKILL definition | ✅ Done | Capabilities, I/O contract |
| Backend converter contract | ✅ Done | `references/backends.md` |
| CAD-logic tolerance mapping | ✅ Done | `references/cad-logic.md` |
| Land survey domain rules | ✅ Done | `references/land-survey-civil.md` |
| Agent config (OpenAI) | ✅ Done | `agents/openai.yaml` |
| Parse script prototype | ⚠️ Partial | `scripts/parse_dwg.py` |
| Topology degenerate-edge guard | ✅ Done | Zero-length edges are skipped during topology build (covered by `tests/test_dwg_parser.py`) |
| **Remaining** | | |
| Production parser implementation | ❌ Todo | Prototype only |
| Topology graph extraction | ❌ Todo | Spec in SKILL.md |
| Civil-survey feature detection | ❌ Todo | Rule templates defined |
| Domain confidence scoring | ❌ Todo | Spec in SKILL.md |
| Integration with survey-automation | ❌ Todo | Bridge not built |

---

## 8. Phase 6 — Integration & End-to-End Testing

| Item | Status | Notes |
|------|--------|-------|
| Core pipeline unit tests | ❌ Todo | No tests in `totali/` |
| Core pipeline integration tests | ❌ Todo | No end-to-end test |
| Laser-suite unit tests | ✅ Done | 5 test files |
| Laser-suite integration tests | ✅ Done | 4 test files |
| Survey-automation unit tests | ✅ Done | 17+ test files |
| Survey-automation integration tests | ✅ Done | 3 test files |
| Cross-component integration | ❌ Todo | Pipeline ↔ laser-suite ↔ survey-auto |
| Sample data for testing | ⚠️ Partial | Laser-suite has samples, totali does not |
| CI/CD for totali package | ❌ Todo | No GitHub Actions workflow |

---

## 9. Phase 7 — Documentation & Release

| Item | Status | Notes |
|------|--------|-------|
| Root README | ✅ Done | Architecture, install, usage |
| Laser-suite README | ✅ Done | CLI docs, CSV format |
| Survey-automation README | ✅ Done | Command matrix, exit codes |
| Laser-suite formulas.md | ✅ Done | WLS, RPP math |
| Survey-automation operations.md | ✅ Done | Full runbook |
| **Remaining** | | |
| API reference docs | ❌ Todo | No docstring extraction |
| Architecture diagram | ❌ Todo | Text description only |
| Deployment guide | ❌ Todo | Install only |
| CHANGELOG.md (root) | ❌ Todo | Sub-projects have changelogs |
| Contributing guide | ❌ Todo | No CONTRIBUTING.md |
| License file | ❌ Todo | No LICENSE |

---

## 10. Cross-Cutting Concerns

### 10.1 Audit & Chain of Custody

- ✅ SHA-256 chained JSONL logging (`totali/audit/`)
- ✅ JSON artifact contracts (`laser-suite/`)
- ✅ Dataset snapshot checksums (`survey-automation/`)
- ❌ Unified audit format across all components

### 10.2 Configuration Management

- ✅ `config/pipeline.yaml` for core pipeline
- ✅ `laser-suite/config/pipeline.prod.yaml` for laser suite
- ✅ Survey-automation YAML config
- ❌ Unified configuration schema

### 10.3 Security & Compliance

- ✅ No auto-promotion of AI outputs (enforced)
- ✅ PLS signature requirement (enforced)
- ✅ Deterministic serialization (contracts.md)
- ❌ Formal threat model document
- ❌ Dependency vulnerability scanning

### 10.4 Data Governance

- ✅ DRAFT → ACCEPTED → CERTIFIED lifecycle
- ✅ Quarantine workflow for suspect data
- ❌ Data retention policy
- ❌ PII handling policy (survey data may contain addresses)

---

## 11. Milestones & Timeline

### Milestone 1 — Core Pipeline Hardening (Current)

**Goal:** Ensure all five pipeline phases are production-ready with tests.

- [ ] Add unit tests for each `totali/` module
- [ ] Ship or document ONNX model acquisition
- [ ] Add sample LAS data for development testing
- [ ] Validate full pipeline run end-to-end

### Milestone 2 — CAD Corpus Completion

**Goal:** Complete AUTOMATICCAD goals 1-5.

- [ ] Resolve 22 failed scan paths
- [ ] Freeze curation rules
- [ ] Build curated corpus with manifest
- [ ] Validate DWG/DXF priority set
- [ ] Publish reproducible runbook

### Milestone 3 — DWG Parser Production

**Goal:** Move DWG tool parser from spec to implementation.

- [ ] Implement `parse_dwg.py` per SKILL.md spec
- [ ] Extract topology graphs from DWG/DXF
- [ ] Detect civil-survey features with domain rules
- [ ] Integrate with survey-automation pipeline

### Milestone 4 — Intent-Geometry Bridge

**Goal:** Implement deterministic bridge from survey artifacts to geometry.

- [ ] Implement source binding (points.csv anchor)
- [ ] Implement rule-driven intent derivation
- [ ] Implement deterministic geometry derivation
- [ ] Implement manifest/export with artifact contracts
- [ ] Add CI contract and replay validation

### Milestone 5 — Cross-Component Integration

**Goal:** Connect all components into a unified workflow.

- [ ] Laser-suite output → totali pipeline input bridge
- [ ] Survey-automation normalized data → extraction input
- [ ] CAD corpus → shielding validation fixtures
- [ ] End-to-end integration test suite
- [ ] Unified CI/CD pipeline

### Milestone 6 — Documentation & Release

**Goal:** Production release with full documentation.

- [ ] Architecture diagram (visual)
- [ ] API reference documentation
- [ ] Deployment and operations guide
- [ ] Root CHANGELOG.md
- [ ] LICENSE and CONTRIBUTING.md
- [ ] Version 1.0.0 release tag

---

## 12. Exit Criteria

The project is **complete** when all of the following are true:

1. **Pipeline functional:** Full LAS → DXF workflow executes without error on
   sample data with all five phases producing expected outputs.
2. **Geodetic adjustment proven:** Laser-suite WLS solver and RPP computation
   pass all unit and integration tests with known-good survey data.
3. **Survey automation v2 stable:** All golden verification tests pass, warning
   count is zero, release-candidate gate passes.
4. **CAD corpus curated:** All five AUTOMATICCAD goals are marked done with
   evidence.
5. **DWG parser operational:** `parse_dwg.py` produces structured JSON from
   real DWG/DXF files per SKILL.md contract.
6. **Intent-geometry bridge live:** Deterministic bridge converts normalized
   survey artifacts to geometry artifacts with CI replay validation.
7. **Audit trail verified:** Chain of custody verifiable from ingest through
   certification for any entity in the output DXF.
8. **Tests pass:** All unit, integration, and golden tests pass across all
   components.
9. **Documentation complete:** README, API docs, operations guide, and
   architecture diagram are current and accurate.
10. **Reproducible:** Any team member can clone, install, and run the full
    pipeline from documented instructions.
