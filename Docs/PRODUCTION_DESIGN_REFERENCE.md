# Production Design for an Agentic Architectural and Surveying CAD Platform

**Role in TOTaLi:** north-star architectural reference. This document describes the
full end-state platform (architecture + surveying, native core + ML, JEPA + LNP,
vector memory, geometry kernel). TOTaLi itself is the **defensible-pipeline slice**
of this design — the validated ingestion, classification, extraction, CAD-shielding,
and human-certification layer. Sibling projects cover the other slices (auracad for
the geometry kernel, L4L for generative CAD + sim). See
`TOTALI_MAPPING_TO_PRODUCTION_DESIGN.md` for the current coverage map.

---

## Executive summary

A production-grade system for architectural drafting plus land surveying should be built as a **deterministic native core** with an **externally managed ML layer**, not as an end-to-end model-driven editor. The native core should own geometry, topology, geodesy, document state, file interchange, snapping, constraints, and undo/redo. The ML layer should remain advisory: it can rank, retrieve, compress context, suggest actions, detect anomalies, and propose parsed commands, but it should never become the authority that mutates geometry or code without passing deterministic validators. That split is consistent with how CAD frameworks such as OCCT separate modeling data from presentation/selection, and it aligns with the stateless nature of modern LLM APIs, which require the application—not the model—to manage memory, sequence, and authority.

The internal scene model should be **canonical and standards-informed**, but not identical to any one interchange format. IFC is the right semantic reference for BIM-style building and infrastructure exchange; the Simple Features model from the OGC gives a sound geometry vocabulary including measured coordinates and TIN; PROJ is the reference for CRS transforms; DXF is the practical drafting interchange baseline; LandXML covers survey points, surfaces, alignments, and parcels; and GDAL/PDAL/LASzip cover geospatial vectors, raster products, point clouds, and LAZ compression. Those standards should enter through adapters, while the canonical store remains optimized for editing, validation, provenance, and replay.

The orchestration layer should treat every LLM call as **fresh and stateless**. "Context reset" is not a model feature; it is an application pattern: load authoritative state, reconstruct only the minimum relevant context for the current task, send a fresh request, validate the result, persist accepted diffs, and discard the rest. Anthropic's Messages API is explicitly stateless, and prompt caching only optimizes repeated prefixes; it does not replace external memory management. For long-lived multi-step operations, a durable workflow layer is appropriate.

Vector memory should be **secondary memory**, not the source of truth. Use a transactional store for authoritative state and metadata, then add vector indexes for retrieval. FAISS is strong as an embedded hot cache, pgvector is excellent when embeddings must live next to relational data and joins, Qdrant is attractive when you need named vectors plus metadata filters, and Milvus becomes compelling when vector search must scale independently and needs explicit control over IVF/HNSW/quantization trade-offs.

The JEPA layer should be designed as a **latent scene model**. The key lesson from I-JEPA, V-JEPA, and VL-JEPA is that the model predicts target representations in latent space rather than reconstructing pixels or autoregressively emitting tokens. For CAD and surveying, the right use is latent prediction over scene tiles, object tokens, point-cloud tiles, and command context. Good downstream uses include next-best-tool suggestion, anomaly detection, retrieval, scope ranking, and context compression. Poor uses include direct authoritative geometry mutation.

The geometry kernel generation pipeline must be **incremental, test-gated, and numerically defensive**. Start with exact predicates and robust numeric policy before higher-level operations. CGAL's exact-predicate kernels, Shewchuk's adaptive robust predicates, and GEOS's progressively more aggressive robust overlay strategies all point in the same direction: robustness is not an afterthought. Every generated component should pass compile checks, unit tests, numerical/property tests, static analysis, sanitizer runs, and fuzzing before merge.

## Constraints, assumptions, and open questions

| Category | Item | Impact on design |
|---|---|---|
| Constraint | All authoritative geometry must be reproducible from deterministic code paths. | ML outputs are proposals only; execution passes through validators and typed command executors. |
| Constraint | Every geometry object must carry unambiguous CRS and unit context. | Scene schema must carry CRS/units at document level and allow explicit overrides only by policy. |
| Constraint | Large point clouds and raster artifacts cannot live inline in JSON scene blobs. | Store external artifacts and keep references, tile manifests, and provenance in the canonical scene. |
| Constraint | File interchange spans drafting, BIM, surveying, and geospatial ecosystems. | Use adapters for DXF, IFC, LandXML, LAS/LAZ, and GDAL/OGR-backed formats. |
| Assumption | "LNP" means a natural-language parser that compiles user text into a typed CAD/surveying command AST. | Parser architecture below assumes intent classification, entity extraction, schema validation, and command compilation. |
| Assumption | The JEPA layer is externally managed and non-authoritative. | It is optimized for retrieval, ranking, anomaly scoring, and latent prediction, not direct editing. |
| Assumption | A pragmatic v1 should prioritize 2D/2.5D architectural and survey workflows before full 3D B-rep authoring. | The kernel roadmap phases exact predicates, planar geometry, TIN, and 2.5D surfaces before more ambitious solids. |
| Open question | Is macOS-first enough, or is cross-platform parity required at launch? | This decides whether the renderer can be native Metal-first or must ship with Vulkan parity from phase one. |
| Open question | Is DWG read/write a launch requirement, or is DXF/IFC/LandXML sufficient initially? | DWG often pushes the design toward a licensed SDK path rather than an all-open stack. |
| Open question | Is collaborative multi-user editing required at launch? | If yes, event ordering, locking, and merge semantics move from "nice-to-have" to core architecture. |
| Open question | What regulatory or contractual deliverables are mandatory? | Plat/stamp workflows, IFC validation targets, parcel exports, and survey report outputs can alter the schema early. |
| Open question | What accuracy envelope is required for surveying outputs? | This affects tolerance policy, CRS grid dependencies, validation thresholds, and test corpus design. |

## System architecture

The most defensible architecture is a **native editing core plus external ML services**. Native side: scene/document state, geometry kernel, geodesy/survey math, import/export adapters, constraint execution, snapping/selection, rendering integration. External side: orchestration, JEPA training/inference, LNP parsing, validation automation.

Use **typed RPC for the control plane** (gRPC + Protobuf for task packets, command ASTs, validation requests, orchestration events) and **copy-avoiding transport for bulk data** (shared memory same-host for geometry buffers; Arrow Flight for tabular/batch streams; ZeroMQ for event buses).

| Module | Authority | Main responsibilities | Preferred boundary |
|---|---|---|---|
| Scene/document core | Authoritative | Canonical scene graph, revisions, units, layers, sheets, object IDs, provenance, undo/redo | In-process native library |
| Geometry kernel | Authoritative | Predicates, intersections, projections, offsets, booleans, meshing, topology helpers | In-process native library |
| Survey/geodesy core | Authoritative | CRS transforms, geodetic conversions, alignments, parcels, TIN/DEM operations | In-process native library |
| File adapters | Non-authoritative | IFC, DXF, LandXML, LAS/LAZ, raster/vector import/export | Plug-in adapters |
| Rendering/interaction | Non-authoritative | Viewports, draw lists, picking, snapping overlays, measurement graphics | Backend abstraction |
| Command executor | Authoritative | Executes typed AST commands after validation | In-process native library |
| LNP parser service | Advisory | Intent parsing, entity extraction, AST proposal | External service |
| JEPA service | Advisory | Latent retrieval, ranking, anomaly scoring, next-best-action hints | External service |
| Validator service | Authoritative gate | Build/test/numerical/security checks | External service or CI |
| Orchestrator | Authoritative for workflow | Planning, context assembly, retries, rollback, state updates | Service layer |

## Canonical scene schema, state store, and task packets

Keep the canonical scene **small, typed, and replayable**. Put references to large point clouds, DEMs, orthos, and mesh tiles in the scene, not the raw data itself.

| Schema layer | What it stores | Why it exists |
|---|---|---|
| Document | Project metadata, revision, authoring policy, extents, active CRS, unit policy | Stable top-level envelope for replay and interchange |
| Presentation | Layers, linetypes, colors, styles, sheets, viewports, annotation settings | Keeps drafting/view state separate from geometry |
| Semantics | Walls, slabs, control points, alignments, parcels, surfaces, labels, constraints | Lets architecture and surveying coexist without overloading raw geometry |
| Geometry | Point, LineString, arc, spline, polygon, mesh, TIN, measured geometry | Canonical geometric vocabulary grounded in standards |
| Topology | Adjacency, edge usage, containment, overlay ancestry, constraints graph | Needed for robust editing and validation |
| Geodesy | CRS, transform chain, geoid/grid refs, local site transform, epoch if needed | Prevents silent coordinate corruption |
| Provenance | Source file, source entity IDs, import transforms, generated-by info, confidence | Enables audit, rollback, and deterministic re-import |

Reference scene envelope (YAML):

```yaml
document:
  id: "doc-7f2b"
  name: "Campus Survey and Building Plan"
  revision: 128
  units: {length: "meter", angle: "degree"}
  crs: {authority: "EPSG", code: 26913, wkt_ref: "crs/epsg-26913.wkt"}
  extents: {min: [498221.13, 4429011.42, 1542.10], max: [499104.88, 4429788.04, 1567.33]}

layers:
  - {id: "L-BLDG-WALL", category: "architecture", visible: true}
  - {id: "L-SURV-CNTRL", category: "survey", visible: true}

geometry:
  points:
    - {id: "P-1001", xyz: [498500.235, 4429301.182, 1549.401]}
  curves:
    - {id: "C-2001", kind: "arc", start: "P-1001", end: "P-1002", center: [498510.0, 4429310.0, 1549.401]}
  surfaces:
    - {id: "TIN-3001", kind: "tin", artifact_ref: "artifacts/site_surface.copc"}

objects:
  - {id: "OBJ-ctl-01", semantic_class: "survey.control_point", layer_id: "L-SURV-CNTRL", geometry_ref: "P-1001"}
  - {id: "OBJ-wall-11", semantic_class: "building.wall", layer_id: "L-BLDG-WALL", geometry_ref: "C-2001"}

provenance:
  imports:
    - {source_file: "building.ifc", source_entity: "IfcWall/2F4j3..."}
    - {source_file: "site.landxml", source_entity: "CgPoint/CP-01"}
```

**Task packet** contract between orchestrator, model worker, and validators:

```yaml
task_id: "KERNEL-SEGINT-001"
task_type: "kernel_component"
target_module: "geometry_kernel"
target_component: "segment_intersection_2d"
authoritative_state: {state_hash: "sha256:8c0d...", accepted_base_commit: "abc1234"}
inputs:
  required_interfaces: ["include/geometry/point2.h", "include/geometry/orient2d.h"]
  allowed_files: ["include/geometry/segment_intersection.h", "src/geometry/segment_intersection.cpp", "tests/geometry/test_segment_intersection.cpp"]
constraints: {language: "C++20", deterministic_only: true, no_interface_breaking_changes: true}
success_criteria:
  compile_profile: "clang-debug"
  required_tests: ["geometry.segment_intersection.basic", "geometry.segment_intersection.degenerate"]
  sanitizer_profiles: ["asan", "ubsan"]
output_contract: {require_full_file_replacements: true, require_diff_summary: true, require_risk_notes: true}
```

## Agentic orchestration, context reconstruction, and vector memory

**The model never remembers; the system always reconstructs.** The Messages API is stateless; the caller sends conversation state every time. Prompt caching accelerates repeated prefixes (system, tools, architecture capsule) but does not create application memory. For production reliability, persist workflow state, validator results, and recovery checkpoints outside the model. Temporal is justified once tasks span machines/long builds/crash-recovery windows.

```
Task backlog → Planner → Retriever → Context builder → Stateless worker → Validator
                                                                              ├─ pass → Apply in worktree → State store → Vector refresh
                                                                              └─ fail → Rollback → Failure capsule → State store → Vector refresh
```

Prompt capsule has four parts: **immutable prefix** (cacheable), **task packet**, **retrieved working set**, **output contract**. Keep prefix stable, keep working set file-scoped, carry only last accepted diffs affecting the target module. Use long context as pressure valve, not default.

Vector memory: tiered design — transactional SQL for authoritative state, shared vector service for cross-run retrieval, small local hot cache per agent. Qdrant named vectors map one object to multiple embeddings (geometry / semantics / commands / code context); pgvector when embeddings must join relational data with ACID; FAISS as embedded cache; Milvus for very large independent-lifecycle deployments.

| Vector option | Best fit | Caveat |
|---|---|---|
| FAISS | Local hot cache inside orchestrator or dev workstation | Not an authoritative multi-tenant platform |
| pgvector | Authoritative metadata + embeddings in one operational store | Less specialized than dedicated vector services |
| Qdrant | Shared retrieval service for code, geometry, task, semantic embeddings | Additional service to operate |
| Milvus | Very large shared retrieval with explicit recall/latency tuning | Operational weight higher than pgvector/Qdrant |

## JEPA and LNP design

JEPA as a **scene-representation model**, not a text generator. Predict latent embeddings of missing or future scene state from partial context; use those embeddings for retrieval, ranking, anomaly detection, suggestion. Inputs: rasterized plan/sheet tiles, vector-geometry tokens, point-cloud/terrain tiles, execution context tokens. Training data from IFC, DXF, LandXML, point clouds via PDAL/LASzip, geospatial derivatives via GDAL/PROJ.

**Embedding spaces** (multi-space, not monolithic): `scene_global`, `tile_visual`, `object_semantic`, `object_geometry`, `command_context`, `code_module`. Each has one job, one evaluation target.

| JEPA axis | Design |
|---|---|
| Primary role | Retrieval, anomaly scoring, next-best-action ranking, latent scene prediction, context compression |
| Architecture | Context encoder + EMA/stopped-gradient target encoder + latent predictor; optional action-conditioned head |
| Latency targets | scene_global refresh < 250 ms p95, candidate ranking < 150 ms p95, top-k rerank < 100 ms p95 |
| Authority | Advisory only; never authoritative mutation |
| Core evaluation | Recall@k, anomaly AUROC/PR, suggestion hit rate, latency p95/p99, embedding drift, task-level lift over heuristics |

Training/inference split: **PyTorch for training, ONNX Runtime for production inference**. JAX for accelerator-heavy research; TensorFlow for orgs already on TF-serving; ONNX as interchange boundary.

LNP parser: **deterministic at the boundary** even if internal stages are probabilistic. Chain:
`utterance → sequence classification (intent) → token classification (entities/slots) → optional dependency parse → AST compilation → schema validation (Pydantic / JSON Schema) → symbol resolution → geometric precondition checks → executable command plan`.

Never hand raw text to the command executor.

## Geometry kernel generation, validation, diff/commit, rollback

Start from **numerical truth**, not features. Ordering: numeric types and policy first, exact predicates second, primitive geometry third, higher-level editing only after those are stable.

| Kernel stage | Scope | Acceptance gate |
|---|---|---|
| Numeric policy | Units, tolerances, exact/filtered predicate policy, deterministic serialization | Golden numeric tests pass; policy frozen in docs |
| Robust predicates | orient2d/3d, incircle, on-segment, bbox relations | Differential tests vs reference predicates; edge-case corpus passes |
| Primitive geometry | Point, segment, ray, line, polyline, arc, circle, basic spline wrappers | Unit tests + property tests + fuzzers |
| Intersections/projections | Segment/segment, line/arc, arc/arc, closest-point, offsets | Degenerate-case suite and symmetry tests pass |
| Planar graph + polygons | Noding, ring validation, clipping, overlay, parcel shapes | Differential tests vs GEOS/CGAL where applicable |
| Survey surfaces | TIN, contour extraction, DEM handoff, triangulation utilities | Terrain regression corpus and contour checks pass |
| Spatial index | R-tree / search acceleration | Query correctness and benchmark thresholds pass |
| Constraints/annotations | Dimensions, snaps, hatches, constraint graph, parcel labeling | UI-integration tests and replay tests pass |
| Advanced solids | Deferred unless 3D-authoring scope truly requires it | Separate program increment |

Every generated component passes: CMake/CMake Presets build, CTest execution, GoogleTest assertions, clang-tidy static checks, sanitizers (ASAN/UBSAN/TSAN), libFuzzer on parsers and numerically brittle paths, CI on multiple OS images, CodeQL security scan.

**No direct write to main.** Model output becomes a proposed patch, validators are the gate, Git is provenance ledger. State store records task packet ↔ prompt capsule hash ↔ validation report ↔ accepted commit.

## Performance, safety, recommended stack, roadmap

Performance targets as **acceptance thresholds**:

| Surface | Suggested target |
|---|---|
| 2D pan/zoom/redraw, typical drawings | 60 FPS on production reference hardware |
| Common select/snap operation | < 16 ms p95 local |
| LNP routine parse | < 50 ms p95 |
| LNP parse with context + entity resolution | < 100 ms p95 |
| JEPA candidate ranking | < 150 ms p95 |
| Context assembly from local stores | < 30 ms p95 |
| Build/test validation for one kernel component | < 5 min on CI fast lane |
| Full safety lane (sanitizers/fuzz smoke) | < 20 min on CI standard lane |

Rendering strategy: Metal on macOS, Vulkan elsewhere, OpenGL/ANGLE only as compatibility layers/tooling paths.

**Recommended stack** (production):

| Concern | Choice |
|---|---|
| Native core | C++20 + CMake + CMake Presets |
| Desktop shell | Qt 6 |
| Geometry references | Custom kernel + CGAL/GEOS differential tests |
| Geodesy / IO | PROJ + GDAL + PDAL + LASzip |
| BIM / drafting | IFC + DXF + LandXML adapters |
| ML training | PyTorch |
| ML inference | ONNX Runtime |
| Parser stack | Transformers + spaCy + Pydantic + JSON Schema |
| Workflow | Custom controller first; Temporal for durable distributed runs |
| Vector memory | pgvector + Qdrant + local FAISS cache |
| CI / quality gates | CTest + GoogleTest + clang-tidy + sanitizers + libFuzzer + GitHub Actions + CodeQL |
| Observability | OpenTelemetry + Prometheus + Tracy + Perfetto + Instruments |
| Supply-chain trust | Sigstore |

**Safety matrix** (NIST AI RMF + OWASP LLM top-10):

| Failure mode | Mitigation |
|---|---|
| Prompt injection through imported drawings / notes | Tool-call policy checks; never let retrieved text modify execution authority; isolate instructions from evidence |
| Insecure output handling | No raw model output reaches executor or repo without compile/test/schema gates |
| Model DoS / cost blowout | Token/latency budgets, throttles, strict task scoping, capped retrieved working set |
| CRS/unit corruption | Schema validation, import checks, transform-chain audit; explicit CRS/units at document level |
| Numerical instability / invalid topology | Exact predicates early; robust noding/overlay for 2D; differential tests; TopologyException handling |
| Supply-chain tampering | Artifact signatures (Sigstore), transparency logs, SBOM-aware workflows |
| Vulnerable or unsafe code patches | Static analysis, sanitizers, CodeQL, fuzzing; block merge on security/memory-safety gates |

**Roadmap** (phased delivery, v1 discipline):

| Phase | Deliverables | Acceptance |
|---|---|---|
| Foundation | ADRs, canonical schema, state store, task-packet contract, CI skeleton | All contracts versioned; import/export architecture frozen; validator skeleton on CI |
| Core editing | Numeric policy, predicates, primitive geometry, layers, sheets, selection/snap | Deterministic replay; kernel unit suite green; latency baseline |
| Survey stack | CRS pipeline, LandXML ingest, TIN/contours, point-cloud references, parcels/alignments | Round-trip survey corpus; CRS transformations validated |
| ML assistance | LNP parser, vector memory, JEPA retrieval/anomaly, prompt-capsule system | Parser benchmark; JEPA beats heuristic baseline on ranking |
| Orchestration | Stateless controller, rollback, worktree isolation, validator-enforced merges | No unauthorized write to main; failed tasks always restorable |
| Hardening | Observability, security gates, signature pipeline, long-run stability | Sanitizer/security lanes green; artifacts signed; SLOs live |
| Release | Packaging, installer, migration policy, support playbooks | Upgrade/recovery tested; reference workflows complete |

## Scope discipline (the one non-negotiable)

If you treat v1 as a full-stack reinvention of a mature solid-modeling platform, the project becomes structurally fragile. If you treat it as a **survey-aware architectural drafting platform with deterministic core editing, standards-aware interchange, and an external agentic assistance layer**, the design is coherent, testable, and producible.

TOTaLi's role in this design is the **survey half of the v1 v1**: deterministic pipeline, CRS gatekeeper, non-authoritative classifier, deterministic extraction, CAD-shielding middleware, surveyor-review + audit chain. It is the survey-stack phase of the roadmap above, already partway delivered. Sibling projects own the rest.
