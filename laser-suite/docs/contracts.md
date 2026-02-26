# Contract Rules

All JSON artifacts follow deterministic top-level ordering:

1. `schemaVersion`
2. `artifactType`
3. `invariants`
4. `metadata`
5. `paths`
6. `data`

Rules:
- No absolute paths in `paths`.
- `schemaVersion` must be present and first.
- `invariants` must be explicit and non-empty.
