# Deterministic Intent-Geometry Bridge

## Document Control
- Type: design specification only
- Implementation status: no implementation content in this document
- Runtime AI status: prohibited
- Scope: bridge from normalized survey artifacts to deterministic intent and geometry artifacts

## Summary
This design replaces runtime-ML bridge behavior with an offline-first deterministic runtime. Any AI-assisted work is limited to development-time authoring of static rule packs. Runtime execution is rule-driven, reproducible, and fail-closed on ambiguity.

## Governance Requirements
1. No runtime dependency on cloud APIs.
2. No runtime model inference.
3. Strict separation between development AI outputs and runtime execution.
4. All persisted artifacts are reproducible from source inputs and versioned configuration.
5. No absolute filesystem paths in persisted artifacts.
6. All persisted schemas define:
- `schemaVersion`
- canonical ordering
- explicit invariant list
7. Ambiguous conditions fail closed.

## Boundaries and Lane Separation
### Development Lane (Allowed)
1. Build and maintain static mapping dictionaries, regex rules, and test fixtures.
2. Produce immutable versioned rule packs and deterministic fixtures.
3. No dynamic runtime calls to external services.

### Runtime Lane (Required)
1. Consume only local files and static configuration.
2. Execute deterministic parsing, classification, grouping, topology checks, and export.
3. Use deterministic tie-breakers for all non-unique operations.
4. Persist canonical artifacts with deterministic key order.

## Deterministic Processing Model
### Stage 1: Source Binding
1. Input anchor is a completed pipeline run root.
2. `normalized/points.csv` is treated as immutable source of truth.
3. Runtime binds to upstream run identity and snapshot hash before processing.

### Stage 2: Intent Derivation (Rule-Driven)
1. Input: normalized points and versioned rule pack.
2. Deterministic rule precedence:
- exact code map
- exact normalized phrase map
- ordered regex rules
- ordered token and prefix rules
- deterministic `unmapped` fallback
3. Grouping is stable and keyed by canonical feature identity.
4. Feature identifiers are deterministic from run identity plus stable group index.

### Stage 3: Geometry Derivation (Deterministic Math)
1. Input: intent artifact plus source coordinates from normalized points.
2. Point ordering uses explicit tie-breakers:
- primary: feature-local sequence policy
- secondary: point id
- tertiary: source file
- quaternary: source line
3. Topology validation rules are deterministic and code-based.
4. No heuristic randomness and no runtime learned behavior.

### Stage 4: Manifest and Export
1. Emit run-scoped intent, geometry, and bridge manifest artifacts.
2. Record invariant evaluations and deterministic hashes.
3. Preserve all intermediate artifacts for replay and audit.

## Canonical Artifact Contract
All persisted bridge artifacts use this exact top-level order:
1. `schemaVersion`
2. `artifactType`
3. `invariants`
4. `metadata`
5. `paths`
6. `data`

### Section Constraints
1. `schemaVersion`: non-empty string.
2. `artifactType`: enumerated string.
3. `invariants`: explicit finite list and evaluation results only.
4. `metadata`: scalar metadata fields only.
5. `paths`: relative POSIX paths only.
6. `data`: typed domain payload only.

### Serialization Constraints
1. Deterministic insertion order only.
2. No key sorting at write time.
3. Dynamic collections sorted by declared deterministic keys.
4. No optional free-form maps in contract sections.

## Artifact Definitions
### Intent Artifact (`artifactType=intent_ir`)
- Required invariants:
1. `paths_are_relative`
2. `deterministic_key_order`
3. `source_snapshot_hash_bound`
4. `rule_pack_version_bound`
5. `classification_rule_order_stable`
- Metadata includes: run id, source snapshot id/hash, rule-pack version/hash.
- Paths include: source points artifact and local outputs as run-root-relative paths.
- Data includes: deterministic feature records and deterministic unmapped records.

### Geometry Artifact (`artifactType=geometry_ir`)
- Required invariants:
1. `paths_are_relative`
2. `deterministic_key_order`
3. `source_snapshot_hash_bound`
4. `rule_pack_version_bound`
5. `coordinate_resolution_is_source_of_truth`
6. `topology_checks_are_deterministic`
- Metadata includes: run id, intent hash, source snapshot id/hash.
- Paths include: run-root-relative references to intent and source artifacts.
- Data includes: deterministic geometry records and deterministic topology findings.

### Bridge Manifest (`artifactType=bridge_manifest`)
- Required invariants:
1. `paths_are_relative`
2. `deterministic_key_order`
3. `artifact_hashes_match_payloads`
4. `replay_from_manifest_is_possible`
- Metadata includes: run id, contract version, generation timestamp, tool version.
- Paths include: run-root-relative artifact paths only.
- Data includes: hash catalog, invariant results, and replay prerequisites.

## Path Governance and Resolution
1. Persisted artifacts must never store absolute filesystem paths.
2. Persisted paths are canonical run-root-relative POSIX paths.
3. Absolute paths may be derived only at runtime in memory.
4. Runtime path resolution anchor order:
- explicit config anchor directory when provided
- otherwise run output root
5. Runtime must reject path traversal and reject ambiguous path anchors.

## Failure Semantics (Fail Closed)
1. Unknown classification: emit deterministic `unmapped` record.
2. Missing referenced point: quarantine feature and emit explicit error code.
3. Invalid topology: emit explicit code and deterministic finding payload.
4. Contract violation: stop processing and mark run blocked.
5. Path policy violation: block artifact emission until corrected.

## CI and Release Gate Requirements
1. Validate canonical top-level key order for each target artifact.
2. Validate required sections and required invariant keys.
3. Fail on any absolute path in artifact `paths` section.
4. Run deterministic replay check and compare hashes.
5. Run cwd-independence test from multiple working directories.
6. Fail if contract schema version drifts without explicit migration.

## Security and Offline-First Model
1. Runtime uses only local files and local executables.
2. No runtime cloud authentication, API calls, or model downloads.
3. No machine-specific path leakage in persisted artifacts.
4. Immutable versioned rule packs and deterministic hash manifests are required.

## Reproducibility and Traceability
Each bridge run must record:
1. upstream run id
2. source snapshot id/hash
3. rule-pack version/hash
4. intent artifact hash
5. geometry artifact hash
6. bridge manifest hash
7. invariant evaluation results

A complete replay with identical inputs and versions must produce identical output hashes.

## Change Impact Declaration
- Affected subsystem:
1. deterministic bridge between normalized survey outputs and downstream geometry outputs
2. canonical artifact contract governance for bridge artifacts
3. CI contract validation and freeze-gate policy

- Freeze-readiness impact:
1. positive; removes runtime AI and cloud coupling
2. positive; defines explicit fail-closed behavior
3. positive; supports deterministic freeze validation

- Determinism impact:
1. positive; replaces probabilistic runtime behavior with ordered rule evaluation
2. positive; enforces canonical serialization and relative path policy
3. positive; requires replayable hash-bound manifests

- Security impact:
1. positive; no runtime external API surface
2. positive; reduced sensitive-path disclosure risk
3. positive; immutable artifacts improve auditability

## Freeze Status
- Status: freeze-ready by design
- Condition: implementation must pass contract, path, replay, and cwd-independence gates
