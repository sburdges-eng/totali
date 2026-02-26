# Point Logic to CAD Logic Mapping

## Table of Contents

1. Core translation model
2. Mapping primitives
3. Rule examples
4. Practical tolerance guidance

## 1. Core Translation Model

Use point logic as a seed, then evaluate rules over CAD entities and topology:

- `point` -> `topology.nodes[*].point`
- `line between points` -> `topology.edges[*]` with `kind == "segment"`
- `closed area` -> `topology.loops[*]`
- `point cluster` -> connected component in topology graph

## 2. Mapping Primitives

- Coordinate checks:
  compare against `entities[*].geometry` coordinates after normalization.
- Connectivity checks:
  use `topology.adjacency` and `topology.connected_components`.
- Closed boundary checks:
  use `topology.loop_count`, `topology.loops[*].kind`, and loop nodes.
- CAD semantics:
  use entity metadata (`type`, `layer`, `linetype`, `color`) for domain rules.

## 3. Rule Examples

1. "Every wall endpoint must connect"
   - Filter `entities` where `layer == "Walls"` and `geometry.kind == "line"`.
   - For each derived topology node, require degree >= 2 in `topology.adjacency` unless marked terminal.

2. "Room boundary must be closed"
   - Filter loops by layer-derived entities or boundary layer.
   - Require at least one loop with `kind == "polyline"` and >= 4 nodes.

3. "No isolated CAD geometry"
   - Require `topology.connected_components == 1` for single-network drawings.

## 4. Practical Tolerance Guidance

- Start with `--tolerance 1e-6` for high-precision files.
- Increase to `1e-4` or `1e-3` when near-miss endpoints should snap as connected.
- Keep `--precision` aligned with tolerance to avoid unstable merges.
