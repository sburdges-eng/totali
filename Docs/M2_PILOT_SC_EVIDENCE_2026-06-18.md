# M2 Pilot — SC1–SC4 Evidence (2026-06-18)

> ⚠️ **SIMULATED PILOT (2026-06-18)** — pilot job #1 is represented by the in-person demo run, NOT a real paid engagement. SC items below mark REAL-NOW vs PENDING-REAL-PAID-JOB. Not evidence of an actual paid pilot.

Simulated job ID: **PILOT-SIM-001** (in-person demo, 2026-06-18)

---

## SC1–SC4 Evidence Table

| Criterion | Status | Evidence / Gap |
|-----------|--------|----------------|
| **SC1 — DXF a partner PLS would stamp** | **PENDING-REAL-PAID-JOB** (REAL-NOW partial) | **REAL-NOW:** Three DRAFT DXFs produced by the demo pipeline (tracks: BV_BASE coded, USGS LAS, Sales Key Flow). All layers are `*-DRAFT` or `QA-*` — zero certified geometry emitted. `auto_promote: false` and `require_pls_signature: true` are set in `config/pipeline_in_person.yaml`. `Docs/PLS_CERTIFICATION_FIELDS.md` defines the cert schema; the DRAFT-until-certified gate (U3) is enforced. The DXF a PLS would review is the demo output at `…/totali_draft_output.dxf`. **PENDING:** An actual licensed PLS has not reviewed or stamped any output. A real paid job with a real partner PLS accepting/rejecting/certifying DXF output is required for SC1 to be met. |
| **SC2 — Time savings measured** | **PENDING-REAL-PAID-JOB** (REAL-NOW partial) | **REAL-NOW:** Pipeline wall-clock captured — Sales Key Flow: `total_duration_sec ≈ 0.70 s` (synth_topo.las, 7-step narrative, `audit_verify=PASS`). Placeholder manual baseline: `14400 s` (4 h) — this is an assumed, not measured, figure used for demo narrative only. **PENDING:** A real per-job manual hours measurement from a real paid engagement is required before any time-savings percentage can be computed or published. The public time-savings % MUST NOT be claimed until a real manual baseline is measured. This is guarded item **KTD5**. |
| **SC3 — Audit independently verifiable** | **REAL-NOW** | `verify_log` → PASS on all three demo audit chains: `INPERSON-BVBASE_*.jsonl`, `INPERSON-LAS_*.jsonl`, `INPERSON-SALES_*.jsonl`. Append-only SHA-256 chain (`totali/audit/verify.py::verify_log`) validates clean — tamper or truncation would fail it. A third party can re-run `verify_log` against the same `.jsonl` files and independently confirm the result. This criterion is met on the demo output; it will carry over to real paid jobs by the same mechanism. |
| **SC4 — Paid jobs** | **PENDING-REAL-PAID-JOB** | Simulated job ID `PILOT-SIM-001` (in-person demo) is not a real paid engagement. A real signed contract, real partner job ID, and real job LAS/deliverable are required for SC4 to be met. Zero paid jobs exist as of 2026-06-18. |

---

## What Unblocks Each SC

- **SC1:** Partner PLS receives a DRAFT DXF from a real job, reviews it, and provides a stamp or explicit accept/reject decision. Requires partner's actual job LAS (still pending — see `SESSION_HANDOFF_2026-06-17.md §9`) and completion of the U4 validation gate.
- **SC2:** Log actual manual hours on a real job where the same deliverable would be produced by hand. Replace the `14400 s` placeholder with a measured figure before publishing any speed claim. (KTD5 guard must be lifted explicitly.)
- **SC3:** Already met mechanically on demo output. Carry forward by running `verify_log` on every real job audit chain.
- **SC4:** Execute a real paid engagement — signed contract, real job LAS, real deliverable to a real client or partner firm.

---

## Honesty Statement

No time-savings percentage and no "paid pilot" claim may be published or shared externally based on this simulated evidence; all external claims require a real paid job with measured baselines.
