# AUTOMATICCAD curation rules (frozen)

**Version:** 2026-06-17  
**Status:** frozen for reproducible reruns (Day 2 gate)

## Include

| Rule ID | Path pattern | Rationale |
|---------|--------------|-----------|
| I-1 | `files/Users/**` | User-owned paths only |
| I-2 | Extensions `.dwg`, `.dxf` (case-insensitive) | Survey/civil interchange targets for TOTaLi |
| I-3 | Files traceable to manifest `canonical_path` | Dedupe by sha256 before staging |

## Exclude

| Rule ID | Path pattern | Rationale |
|---------|--------------|-----------|
| E-1 | `files/Applications/**` | macOS app bundles (FreeCAD, Shapr3D, etc.) — not user corpus |
| E-2 | `files/Library/**` | System Library mirrors — not user corpus |
| E-3 | `**/.DS_Store` | OS metadata |
| E-4 | `**/*.{obj,stl,stp}` under `files/Users/**` | Mesh/audio/JUCE fixtures — out of survey scope |
| E-5 | Paths under unrelated dev trees (`jepa-audio-*`, `automotive-soundsystem`, `kelly-listening-contract`) | Non-CAD project noise |

## Post-prune corpus (git-tracked)

10 DWG/DXF files under `AUTOMATICCAD/files/Users/`:

- `seanburdges/Desktop/110311 WATER BASE.dwg`
- `seanburdges/Dev/survey-automation-roadmap/.local-datasets/TOTL/*` (4 files)
- `seanburdges/Dev/survey-automation-roadmap/samples/input/sample_ascii.dxf`
- `seanburdges/Dev/survey-automation-roadmap/validation/golden/.../binary.dxf`
- `seanburdges/Downloads/*.dwg` (3 files)

## Regeneration

1. Do not re-import `Applications/` or `Library/` mirrors.
2. New discovery runs write manifests under `AUTOMATICCAD/manifests/`; only I-1..I-3 matches copy into `files/Users/`.
3. `.gitignore` blocks accidental re-commit of E-1/E-2 paths.
