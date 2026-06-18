# Classifier direction (U1) — adapt-pretrained vs train-custom

**Status:** DRAFT FOR PARTNER DECISION · 2026-06-18
**Decision owner:** Partner + project lead (this memo provides analysis + options, not the final call)
**Grounded in:** spike `tools/classifier_spike.py` → `artifacts/classifier_spike_2026-06-18.md`

> Reminder of the invariant: the classifier is **advisory only**. Its output lands on `*-DRAFT` layers; a PLS accepts/rejects and signs. Classifier accuracy changes *drafting efficiency*, never *certification correctness*. (`auto_promote:false`, `require_pls_signature:true`.)

## 1. What the spike measured

Ran the in-repo **rule-based baseline** classifier (`totali/segmentation/classifier.py::_classify_rules`, elevation-percentile binning) over a real USGS LPC tile and scored predictions against the tile's own ASPRS `classification` field as reference.

| Fact | Value |
|------|-------|
| Input | `USGS_CO_SanLuis_8764.las` (USGS public tile, San Luis Valley CO) |
| Points | 26,635 |
| ONNX model present (`models/point_transformer_v2.onnx`) | **No** — `models/` absent, `onnxruntime` not installed |
| ML path behavior | **Falls back to the rule baseline** — there is currently no real ML-vs-rules delta to measure |
| Rule baseline overall accuracy vs ASPRS reference | **0.70%** |
| Distinct ASPRS classes in the tile | **2** (unclassified + ground, ~50/50) |

## 2. Honest interpretation

1. **The rule baseline is not viable as a classifier.** 0.7% overall accuracy is below majority-class chance. Elevation-percentile binning has essentially no signal on flat valley terrain — it is a placeholder, not a contender.
2. **This tile cannot adjudicate adapt-vs-train.** With only 2 semantic classes (and one of them "unclassified"), no model — pretrained or custom — can be meaningfully evaluated here. A fair comparison needs a tile with ≥5 occupied classes (ground, vegetation tiers, building, etc.).
3. **No ML path exists to compare against yet.** The ONNX point-transformer is a config reference only; the model file is not present, so "ML vs baseline" currently collapses to "baseline vs reference."

## 3. Production-code concern surfaced by the spike (recommendation, not fixed)

`_classify_rules()` (≈ lines 231–235) **passes the input LAS's existing `classification` codes straight through into predictions** for already-classified points (confidence 0.85). Consequences:
- Isolated accuracy measurement is impossible without neutralizing this (the spike used a no-classification wrapper to get a true baseline number).
- In integration tests that feed pre-classified LAS, this can silently report near-100% "accuracy" that reflects passthrough, not classification.

**Recommendation:** decide explicitly whether passthrough-of-existing-codes is intended product behavior (reasonable: "trust the surveyor's/vendor's existing classification") or an evaluation hazard — and if kept, make it an explicit, logged mode rather than an implicit branch. *Not changed here — flagged for review per scope discipline.*

## 4. Options for the partner decision

| Option | When it wins | Cost / risk |
|--------|--------------|-------------|
| **A. Adapt a pretrained point model** (e.g. fine-tune a published point-transformer / KPConv on a small labeled subset of partner LAS) | Partner LAS resembles common ALS/MLS distributions; limited labeled data | Need a few labeled tiles; domain gap risk; pretrained license check |
| **B. Train custom on partner LAS** | Partner geography/equipment is idiosyncratic; ample labeled data can be produced | Highest labeling + training cost; longest to first result |
| **C. Stay rule/heuristic + surveyor field codes for now** | Near-term; coded-survey inputs already carry authoritative field codes (deterministic path) | No ML lift on raw LAS; weak on uncoded LPC |

## 5. What's needed before this decision can be made well (asks for the partner)

1. **A representative partner-job LAS** with **richer classification** (≥5 classes) — the San Luis USGS tile is not it.
2. Even a **small hand-labeled subset** (a few hundred points across classes) to serve as honest groundtruth instead of borrowing ASPRS codes.
3. Confirmation of the **target class taxonomy** the partner actually drafts to (vs the 16-class config map).

## 6. Recommendation (provisional)

Lean **Option A (adapt pretrained)** as the first experiment *once a richer labeled tile exists*, because labeled survey LiDAR is scarce and adaptation needs the least data — but **do not commit** until item 5.1/5.2 land. Until then, the deterministic **coded-survey field-code path** remains the trustworthy production route; raw-LAS ML classification stays a spike. Re-run `tools/classifier_spike.py` on the richer tile to produce the real adapt-vs-train number.

---

## 7. Synthetic richer-tile spike (2026-06-18, SIMULATED stand-in)

> **BANNER — SYNTHETIC DATA.**
> No real partner tile has been received. This section uses a programmatically generated LAS fixture
> (`tests/fixtures/survey_corpus/synth_multiclass.las`, seed 1701, deterministic) to mimic a
> richer multi-class tile. Results here **do not substitute for partner data** and are marked
> provisional/synthetic throughout. This section will be superseded once a real labeled tile lands.

### Fixture summary

| Attribute | Value |
|-----------|-------|
| Generator | `tools/generate_synth_multiclass_las.py` (seed 1701, deterministic) |
| File | `tests/fixtures/survey_corpus/synth_multiclass.las` |
| Points | **800** |
| ASPRS classes | **5** (2 ground, 3 low-veg, 5 high-veg, 6 building, 9 water) |
| Elevation design | water < ground < low-veg < high-veg ≈ building (overlapping bands) |
| CRS | EPSG:2232 Colorado Central |

### Class distribution

| Class | Name | Count | % |
|-------|------|-------|---|
| 2 | ground | 200 | 25.0% |
| 3 | low_vegetation | 150 | 18.8% |
| 5 | high_vegetation | 150 | 18.8% |
| 6 | building | 200 | 25.0% |
| 9 | water | 100 | 12.5% |

### Rule-baseline accuracy vs. synthetic labels

Classifier path: `_classify_rules` (elevation-percentile binning), `trust_existing_classification=False`
enforced via `_LasNoClassification` wrapper (same guard as the original B6 spike).

| Metric | Value |
|--------|-------|
| **Overall accuracy** | **42.9% (0.4288)** |
| Ground (cls 2) F1 | 0.5542 |
| Low-veg (cls 3) F1 | 0.7467 |
| High-veg (cls 5) F1 | 0.4018 |
| Building (cls 6) F1 | 0.0000 |
| Water (cls 9) F1 | 0.0000 |

**Key confusions:**
- Building (cls 6) → predicted as high-vegetation 100% of the time (elevation-band overlap; the rule baseline has no planarity or return-intensity signal to distinguish them).
- Water (cls 9) → predicted as ground 100% of the time (the below-grade depression z-range falls into the ground percentile bucket).

**Interpretation (synthetic context):**
The rule-based elevation-percentile classifier reaches ~43% overall on a 5-class problem where
majority-class chance is 25%. It has meaningful signal for vegetation tiers (low-veg F1 = 0.75)
but fails completely on building and water — the two classes most dependent on non-elevation
features (planarity, return density, intensity). This is consistent with the prior result on the
USGS tile and confirms the rule baseline's structural limitation: it is a placeholder, not a contender,
for any class that requires spectral or geometric cues beyond elevation.

### LOCKED PROVISIONAL DIRECTION (U1 — 2026-06-18, SYNTHETIC)

**Direction: Option A — Adapt a pretrained point cloud model.**

Rationale:
1. The rule baseline fails on the classes (building, water) that matter most for survey deliverables.
   The accuracy gap between rule-based and a pretrained point-transformer (which embeds planarity,
   return-count, and local geometry) is expected to be substantial — consistent with the published
   literature on PointNet++/Point-Transformer on AHN/ISPRS benchmarks.
2. Labeled survey LiDAR from a partner is scarce. Adaptation (fine-tuning a pretrained model on a
   small labeled subset) is the pragmatic fit given the likely data volume.
3. Custom training (Option B) remains open if partner geometry/equipment proves highly idiosyncratic —
   assess after seeing the real labeled tile.

**Contingencies (all MUST be satisfied before the direction becomes firm):**

- [ ] **C1.** Receive a real labeled partner tile with ≥5 occupied ASPRS classes.
- [ ] **C2.** Re-run `tools/classifier_spike.py` on that tile; confirm the rule-baseline gap
      is ≥20pp OA below a pretrained baseline (otherwise Option C may suffice).
- [ ] **C3.** Verify a pretrained model with a compatible open license exists for the target
      point density and sensor type.
- [ ] **C4.** Resolve the `_classify_rules` passthrough behavior (Section 3 above) before any
      integration-test accuracy measurement.

Until C1–C4 land, the **coded-survey field-code path** remains the sole production-trusted route.
Raw-LAS ML classification stays in spike/experimental status.

Spike artifact: `.tmp/classifier_spike_synth/results.json`
