import { useState } from "react";

const COLORS = {
  primary: "#1B3A5C",
  accent: "#2E75B6",
  danger: "#C0392B",
  warning: "#D4A017",
  success: "#27AE60",
  bg: "#F8F9FA",
  card: "#FFFFFF",
  border: "#DEE2E6",
  text: "#2C3E50",
  muted: "#6C757D",
  lightAccent: "#EBF5FB",
  lightDanger: "#FDEDEC",
  lightSuccess: "#EAFAF1",
  lightWarning: "#FEF9E7",
};

const sections = [
  {
    id: "convergence",
    title: "Points of Convergence",
    icon: "🤝",
    content: {
      intro: "Both documents independently arrive at the same core conclusions. This convergence across separate analyses strengthens confidence in these architectural decisions.",
      points: [
        {
          title: "Sheet Automation Is the Economic Lifeline",
          doc1: "Step 4 identifies sheet formatting as consuming 60% of drafter time. Step 8's kill question explicitly warns: \"failed to integrate cleanly into the archaic sheet-formatting workflows.\"",
          doc2: "Section E dedicates the entire 90-day prototype to sheet automation. Labels it \"The 4+ Hour ROI\" and provides specific algorithmic approaches (MBR, PCA, R-Tree, force-directed).",
          verdict: "STRONG AGREEMENT — Both documents treat sheet automation as the existential priority. The 8-Step doc diagnoses the problem; the Execution Plan prescribes the solution.",
          status: "agree"
        },
        {
          title: "Deterministic Firewall Is Non-Negotiable",
          doc1: "Step 3 mandates an \"impenetrable, mathematically rigid firewall\" between probabilistic and deterministic layers. \"When they disagree: Determinism violently wins.\"",
          doc2: "Section D resolves the NLP tension with AOT Lexical Compilation — ML proposes offline, deterministic bridge executes at runtime with O(1) string lookups. \"The ML engine is completely spun down.\"",
          verdict: "STRONG AGREEMENT — Both insist on temporal separation. The 8-Step doc frames it architecturally; the Execution Plan implements it operationally.",
          status: "agree"
        },
        {
          title: "PLS Liability Drives Every Decision",
          doc1: "Step 5: \"A Colorado PLS will only sign a TOTaLi-assisted plat if the system provides perfect epistemological transparency.\" XData provenance, confidence stratification, immutable audit trails.",
          doc2: "Section C defines three concrete gates: Geodetic Invariant (hash-match coordinates), Provenance Trace (XData on every entity), Deterministic Sabotage Test (system must loudly refuse to guess).",
          verdict: "STRONG AGREEMENT — Both center the PLS as the ultimate gatekeeper. The Execution Plan's Gate 3 (sabotage test) is a particularly strong operationalization of Step 3's \"determinism violently wins.\"",
          status: "agree"
        },
        {
          title: "Boundary Law Stays Human",
          doc1: "Step 2: \"It is mathematically impossible to natively encode absolute, discrete legal hierarchy into a continuous, probabilistic latent space.\" Step 4: Boundary/legal phases are strictly human.",
          doc2: "Section A confines all ML to topographic features. The bridge handles only \"abstract ML intent\" for planimetric geometry, never boundary resolution.",
          verdict: "STRONG AGREEMENT — Neither document attempts to automate boundary law. This constraint is the single most important architectural decision in the project.",
          status: "agree"
        },
        {
          title: "Framing as Compiler, Not AI",
          doc1: "Step 7: \"Frame it exactly like a software compiler. The ML simply translates the messy 'source code' (field notes) into an abstract syntax tree.\" \"You are selling the ultimate spatial spell-checker.\"",
          doc2: "Section D frames the runtime as a \"deterministic compiler executing O(1) string lookups.\" The entire ML component is relegated to offline pre-processing.",
          verdict: "AGREEMENT — Both use compiler metaphors deliberately. The Execution Plan implements this framing in its actual architecture, not just marketing.",
          status: "agree"
        }
      ]
    }
  },
  {
    id: "divergence",
    title: "Critical Divergences",
    icon: "⚡",
    content: {
      intro: "Where the documents disagree reveals the most important strategic decisions. Each divergence requires a resolution because contradictory execution will burn resources.",
      points: [
        {
          title: "DWG Parser: ODA SDK License vs. ezdxf + ODA Converter",
          doc1: "The existing codebase uses ezdxf for DXF parsing with ODAFileConverter as a DWG→DXF conversion step. parse_dwg.py has an ASCII fallback. No native DWG SDK dependency.",
          doc2: "\"We accept the risk and cost to license the Open Design Alliance (ODA) SDK. This instantly unblocks read/write capabilities via a C++/.NET wrapper.\"",
          verdict: "DIVERGENCE — The Execution Plan proposes a fundamentally different I/O strategy. ODA SDK licensing adds cost (~$5K–$25K/year), binary dependency management, and platform lock-in. The current ezdxf + converter approach is slower but zero-cost and Python-native. Resolution: Use ezdxf for read operations (already working), evaluate ODA SDK only if DWG write is required and ezdxf proves insufficient.",
          status: "diverge"
        },
        {
          title: "Sequencing: Parser-First vs. Foundation-First",
          doc1: "The Challenge Assessment sequences Phase 0 (corpus curation + sample LAS) before Phase 1 (pipeline testing) before Phase 2 (parser + bridge). Rationale: you need test data before you can test anything.",
          doc2: "Puts the DWG Parser build as Day 1–30 (Phase 1), calling it \"the central circulatory system.\" Corpus curation waits until Days 31–60 because it depends on the parser.",
          verdict: "DIVERGENCE — The Execution Plan's logic is internally consistent (you need the parser to programmatically fix the corpus), but the Challenge Assessment's logic is also valid (you need test data before you can validate the parser). Resolution: These can run in parallel. Start parser development on Day 1 AND begin manual corpus triage simultaneously. The parser doesn't need to be complete to start curating — manual inspection of the 22 failures can proceed immediately.",
          status: "diverge"
        },
        {
          title: "NLP Resolution: Static Rule Packs vs. AOT Compilation",
          doc1: "The Challenge Assessment resolves O-6 by declaring runtime NLP prohibited and development-time NLP permitted. Rule packs are versioned, frozen, and deployed as immutable config. New codes quarantine as \"unmapped.\"",
          doc2: "Proposes Ahead-of-Time (AOT) Lexical Compilation: per-project NLP analysis generates a static JSON mapping, a Survey Tech reviews and approves it, then the runtime uses only that locked dictionary.",
          verdict: "SUBTLE DIVERGENCE — Both prohibit runtime ML, but they differ on when the ML runs. The Challenge Assessment makes rule packs a release artifact (updated periodically across all projects). The Execution Plan makes them a per-project artifact (generated fresh for each upload). The per-project approach is more adaptive but requires a human review step for every project. Resolution: Hybrid — maintain a firm-wide base rule pack (Challenge Assessment approach) with per-project override dictionaries (Execution Plan approach). Unmapped codes generate a review prompt; approved mappings merge back into the base pack on the next release cycle.",
          status: "diverge"
        },
        {
          title: "Corpus Triage: Pareto 80/20 vs. Complete Resolution",
          doc1: "Phase 0 aims to resolve all 22 failed paths: \"All 22 failed scan paths resolved or formally excluded with documented reasons.\"",
          doc2: "\"We script fixes for the 4 paths causing 80% of the crashes. The remaining 18 edge-case paths are routed to a permanent DROP_FILE quarantine.\"",
          verdict: "DIVERGENCE — The Execution Plan is more aggressive about cutting losses. The Challenge Assessment wants formal documentation of every exclusion. Resolution: The Execution Plan's Pareto approach is correct for velocity, but the Challenge Assessment's documentation requirement is correct for auditability. Fix the top 4 programmatically, formally document and quarantine the remaining 18 with specific exclusion reasons in the manifest.",
          status: "diverge"
        },
        {
          title: "Civil 3D .NET API vs. ezdxf Post-Processor",
          doc1: "Identifies COM automation as a risk (R-1, score 16). Proposes standalone DXF post-processor via ezdxf as fallback if COM proves unreliable across Civil 3D versions.",
          doc2: "Commits fully to the native Civil 3D .NET API for sheet automation. \"This is a pure computational geometry and spatial packing problem executed via the native Civil 3D .NET API.\"",
          verdict: "SIGNIFICANT DIVERGENCE — The .NET API gives access to native Civil 3D objects (Layouts, Viewports, DVIEW Twist), which ezdxf cannot create. But it creates a hard platform dependency. Resolution: Build the core algorithms (MBR, PCA, R-Tree label dodge) in platform-agnostic Python first, then wrap them in a Civil 3D .NET adapter. If Civil 3D COM breaks, the algorithms still work via ezdxf with reduced viewport capability.",
          status: "diverge"
        },
        {
          title: "Autodesk Hostility as Failure Mode",
          doc1: "Not identified as a risk. The Challenge Assessment's risk register focuses on COM fragility (R-1) but doesn't consider Autodesk actively breaking compatibility.",
          doc2: "Failure Mode 2: \"Autodesk pushed a background update that changed their serialization schema, instantly breaking our DWG Parser. The system corrupted user master files, and enterprise trust was permanently destroyed.\"",
          verdict: "GAP IN CHALLENGE ASSESSMENT — The Execution Plan correctly identifies Autodesk platform risk as existential. AECC proxy objects, proprietary encryption, and forced updates are real threats. Resolution: Add R-9 to the risk register: Autodesk compatibility break (L:3, I:5, Score:15). Mitigation: DXF-first I/O strategy, version-pinned Civil 3D installations for production, and a proxy object detection/quarantine module.",
          status: "diverge"
        }
      ]
    }
  },
  {
    id: "obstacles",
    title: "Obstacle Deep Dive",
    icon: "🧱",
    content: {
      intro: "Combining both documents surfaces 18 distinct obstacles. Here are the ones that require immediate strategic decisions because they sit on the critical path.",
      points: [
        {
          title: "The COM/API Platform Lock (Critical Path Blocker)",
          doc1: "Risk R-1 (Score 16): COM automation unreliable across Civil 3D versions. Fallback: standalone DXF post-processor.",
          doc2: "Commits to .NET API. Sheet automation Days 1–30 build viewport logic directly against Civil 3D APIs.",
          verdict: "DECISION REQUIRED — If you commit to .NET and it breaks, the 90-day prototype is dead. If you build in ezdxf, you lose native viewport creation. Recommended: Build label dodge and legend generator in Python (no Civil 3D dependency). Build viewport/scaling in .NET with a feature flag to fall back to ezdxf paper space setup. Test on Civil 3D 2024, 2025, and 2026 before committing.",
          status: "diverge"
        },
        {
          title: "Sample LAS Data (Testing Prerequisite)",
          doc1: "Obstacle O-9: No sample LAS test data exists. Pipeline cannot be tested without it.",
          doc2: "Phase 3 (Days 61–90) uses \"synthetic, mathematically perfect point clouds.\" Acknowledges the need but places it late.",
          verdict: "BOTH PLANS NEED THIS EARLIER — Synthetic point clouds should be generated in Week 1 as a parallel task. They don't need to be perfect — they need to exercise every phase of the pipeline. Use laspy to generate a minimal LAS with known ground/building/wire classifications, known coordinates, and known edge cases (duplicate points, Z anomalies).",
          status: "diverge"
        },
        {
          title: "AECC Proxy Objects (Autodesk-Specific Data Loss)",
          doc1: "Not specifically identified. O-3 mentions DWG parser is spec-only but doesn't call out proxy objects.",
          doc2: "Explicitly identifies: \"60% of Civil 3D data exists as encrypted 'AECC' Proxy Objects.\" Names it as Failure Mode 2 trigger.",
          verdict: "GAP — This is a real and documented problem in the Civil 3D ecosystem. AECC objects (Alignments, Corridors, Profiles) are proprietary binary blobs that only Civil 3D can read. ezdxf sees them as opaque proxies. Resolution: The DWG parser must detect and flag proxy objects in its qa_flags output. Sheet automation should operate on standard AutoCAD entities (lines, polylines, text, blocks) and explicitly skip AECC objects with a logged warning.",
          status: "diverge"
        },
        {
          title: "Coordinate Hash Integrity (Gate 1 Feasibility)",
          doc1: "Not explicitly stated as a gate. The geodetic gatekeeper validates CRS/epoch/units but doesn't mandate round-trip hash matching.",
          doc2: "Gate 1: \"A raw survey CSV fed through the pipeline and exported to DWG must cryptographic-hash-match the original point coordinates to exactly 4 decimal places.\"",
          verdict: "IMPORTANT ADDITION — Gate 1 is a strong, testable acceptance criterion. However, 4 decimal places in US Survey Feet is 0.0001 ft = 0.0012 inches, which is well within survey precision. The existing geodetic gatekeeper should be extended with a round-trip hash test: ingest CSV → process → export → compare coordinates. Any drift is a hard failure.",
          status: "agree"
        }
      ]
    }
  },
  {
    id: "completion",
    title: "Completion Strategy",
    icon: "🎯",
    content: {
      intro: "Synthesizing both documents into a unified completion plan that takes the best elements of each approach.",
      points: [
        {
          title: "Week 1–2: Parallel Foundation Sprint",
          doc1: "Phase 0: Resolve corpus, create sample LAS, freeze curation rules.",
          doc2: "Phase 1: Start DWG parser build. Start sheet automation prototype.",
          verdict: "MERGED PLAN — Run three parallel tracks: (1) Manual corpus triage of the 22 failures (no parser needed). (2) Generate 3 synthetic LAS files for pipeline testing. (3) Begin sheet automation algorithms in platform-agnostic Python (label dodge R-Tree, MBR/PCA scaling). All three are independent and can start Day 1.",
          status: "agree"
        },
        {
          title: "Week 3–4: Pipeline Proof + Parser Integration",
          doc1: "Phase 1: Integration test LAS→DXF. XData embedding. ONNX documentation.",
          doc2: "Phase 2: Corpus curation with parser. Begin bridge implementation.",
          verdict: "MERGED PLAN — (1) Run first E2E pipeline test with synthetic LAS. Fix schema mismatches between phases. (2) Integrate DWG parser against the top-4 curated production drawings. (3) Add XData provenance to DXF output via ezdxf. (4) Document ONNX model status (rule-based fallback is the production path).",
          status: "agree"
        },
        {
          title: "Week 5–6: Bridge + Sheet Automation Core",
          doc1: "Phase 2: Bridge Stage 2-3, replay test, ADR for NLP constraint.",
          doc2: "Phase 2 continued: Bridge build. Sheet automation: legend generator.",
          verdict: "MERGED PLAN — (1) Complete bridge intent derivation and geometry derivation. Run replay test. (2) Build dynamic legend generator (scan DWG, cross-reference symbols, array into paper space). (3) Sign ADR: \"Runtime NLP prohibited. Firm-wide base rule pack + per-project override dictionaries.\" (4) Begin Civil 3D .NET viewport prototype in parallel with Python fallback.",
          status: "agree"
        },
        {
          title: "Week 7–9: Sheet Automation Delivery + Acceptance Testing",
          doc1: "Phase 3: Label dodge, confidence layers, planimetric auto-connect. PLS + drafter acceptance test.",
          doc2: "Phase 3: Label dodging (R-Tree + force-directed). E2E pipeline test. Full system integration.",
          verdict: "MERGED PLAN — (1) Deliver label dodge algorithm. (2) Deliver confidence stratification layers (green verified / magenta review). (3) Run PLS acceptance: click entities, verify XData, confirm sabotage test (conflicting data → magenta layer + halt). (4) Run drafter acceptance: time the full sheet generation workflow, measure against 4-hour baseline. (5) If Civil 3D .NET viewport works → ship it. If not → ship ezdxf paper space fallback with documented limitations.",
          status: "agree"
        },
        {
          title: "Week 10–13: Integration, Docs, Release",
          doc1: "Phase 4-5: Unified CI, ghost layer, threat model, v1.0.0.",
          doc2: "Not explicitly covered beyond Day 90.",
          verdict: "FOLLOW CHALLENGE ASSESSMENT — The Execution Plan ends at Day 90. Weeks 10–13 follow the Challenge Assessment's Phase 4-5: unified CI, ghost layer prototype, threat model, documentation, and release. The Execution Plan's Day 90 deliverable becomes the Phase 3 acceptance gate; everything after is hardening and release prep.",
          status: "agree"
        }
      ]
    }
  }
];

function StatusBadge({ status }) {
  const config = {
    agree: { bg: COLORS.lightSuccess, color: COLORS.success, label: "CONVERGE" },
    diverge: { bg: COLORS.lightDanger, color: COLORS.danger, label: "DIVERGE" },
  };
  const c = config[status] || config.agree;
  return (
    <span style={{
      display: "inline-block",
      padding: "2px 10px",
      borderRadius: 12,
      fontSize: 11,
      fontWeight: 700,
      letterSpacing: 0.5,
      background: c.bg,
      color: c.color,
    }}>
      {c.label}
    </span>
  );
}

function Card({ point }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div style={{
      background: COLORS.card,
      border: `1px solid ${COLORS.border}`,
      borderRadius: 8,
      marginBottom: 12,
      overflow: "hidden",
      boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
    }}>
      <button
        onClick={() => setExpanded(!expanded)}
        style={{
          width: "100%",
          padding: "14px 18px",
          background: "none",
          border: "none",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          textAlign: "left",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1, minWidth: 0 }}>
          <StatusBadge status={point.status} />
          <span style={{ fontWeight: 600, fontSize: 14, color: COLORS.text }}>
            {point.title}
          </span>
        </div>
        <span style={{ fontSize: 16, color: COLORS.muted, flexShrink: 0 }}>
          {expanded ? "▲" : "▼"}
        </span>
      </button>
      {expanded && (
        <div style={{ padding: "0 18px 18px", fontSize: 13, lineHeight: 1.6 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 14 }}>
            <div style={{ background: COLORS.lightAccent, padding: 12, borderRadius: 6, borderLeft: `3px solid ${COLORS.accent}` }}>
              <div style={{ fontWeight: 700, fontSize: 11, color: COLORS.accent, marginBottom: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>
                8-Step Architecture / Challenge Assessment
              </div>
              <div style={{ color: COLORS.text }}>{point.doc1}</div>
            </div>
            <div style={{ background: COLORS.lightWarning, padding: 12, borderRadius: 6, borderLeft: `3px solid ${COLORS.warning}` }}>
              <div style={{ fontWeight: 700, fontSize: 11, color: COLORS.warning, marginBottom: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>
                Execution Plan Response
              </div>
              <div style={{ color: COLORS.text }}>{point.doc2}</div>
            </div>
          </div>
          <div style={{
            background: point.status === "agree" ? COLORS.lightSuccess : COLORS.lightDanger,
            padding: 12,
            borderRadius: 6,
            borderLeft: `3px solid ${point.status === "agree" ? COLORS.success : COLORS.danger}`,
          }}>
            <div style={{ fontWeight: 700, fontSize: 11, color: point.status === "agree" ? COLORS.success : COLORS.danger, marginBottom: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>
              Resolution
            </div>
            <div style={{ color: COLORS.text }}>{point.verdict}</div>
          </div>
        </div>
      )}
    </div>
  );
}

function ScoreCard({ label, value, color }) {
  return (
    <div style={{
      background: COLORS.card,
      border: `1px solid ${COLORS.border}`,
      borderRadius: 8,
      padding: "16px 20px",
      textAlign: "center",
      minWidth: 120,
    }}>
      <div style={{ fontSize: 28, fontWeight: 800, color }}>{value}</div>
      <div style={{ fontSize: 11, color: COLORS.muted, marginTop: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
    </div>
  );
}

export default function App() {
  const [activeSection, setActiveSection] = useState("convergence");

  const allPoints = sections.flatMap(s => s.content.points);
  const agrees = allPoints.filter(p => p.status === "agree").length;
  const diverges = allPoints.filter(p => p.status === "diverge").length;

  const current = sections.find(s => s.id === activeSection);

  return (
    <div style={{ fontFamily: "'Inter', -apple-system, sans-serif", background: COLORS.bg, minHeight: "100vh", color: COLORS.text }}>
      <div style={{ background: COLORS.primary, color: "white", padding: "24px 32px" }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700 }}>
          TOTaLi Document Cross-Analysis
        </h1>
        <p style={{ margin: "6px 0 0", fontSize: 13, opacity: 0.8 }}>
          8-Step Architecture Audit + Challenge Assessment ↔ Execution Plan Response
        </p>
      </div>

      <div style={{ padding: "20px 32px" }}>
        <div style={{ display: "flex", gap: 16, marginBottom: 24, flexWrap: "wrap" }}>
          <ScoreCard label="Convergence Points" value={agrees} color={COLORS.success} />
          <ScoreCard label="Divergence Points" value={diverges} color={COLORS.danger} />
          <ScoreCard label="Total Obstacles" value="18" color={COLORS.accent} />
          <ScoreCard label="Critical Path Weeks" value="13" color={COLORS.primary} />
          <ScoreCard label="90-Day ROI Target" value="4+ hrs" color={COLORS.warning} />
        </div>

        <div style={{ display: "flex", gap: 8, marginBottom: 20, flexWrap: "wrap" }}>
          {sections.map(s => (
            <button
              key={s.id}
              onClick={() => setActiveSection(s.id)}
              style={{
                padding: "8px 16px",
                borderRadius: 6,
                border: `1px solid ${activeSection === s.id ? COLORS.accent : COLORS.border}`,
                background: activeSection === s.id ? COLORS.accent : COLORS.card,
                color: activeSection === s.id ? "white" : COLORS.text,
                cursor: "pointer",
                fontWeight: 600,
                fontSize: 13,
              }}
            >
              {s.icon} {s.title}
            </button>
          ))}
        </div>

        {current && (
          <div>
            <p style={{ fontSize: 14, color: COLORS.muted, marginBottom: 16, lineHeight: 1.6 }}>
              {current.content.intro}
            </p>
            {current.content.points.map((point, i) => (
              <Card key={i} point={point} />
            ))}
          </div>
        )}

        <div style={{
          marginTop: 32,
          padding: 20,
          background: COLORS.card,
          border: `1px solid ${COLORS.border}`,
          borderRadius: 8,
          borderLeft: `4px solid ${COLORS.primary}`,
        }}>
          <h3 style={{ margin: "0 0 8px", fontSize: 15, color: COLORS.primary }}>
            Bottom Line
          </h3>
          <p style={{ margin: 0, fontSize: 13, lineHeight: 1.7, color: COLORS.text }}>
            The two documents are architecturally aligned on the five most important decisions: sheet automation priority, deterministic firewall, PLS sovereignty, boundary law exclusion, and compiler framing.
            They diverge on six implementation choices, all of which have clear resolutions. The most consequential divergence is the Civil 3D .NET API commitment vs. ezdxf fallback — this decision must be made in Week 1 because the entire 90-day sheet automation timeline depends on it.
            The merged completion strategy runs three parallel tracks from Day 1 (corpus triage, synthetic LAS, platform-agnostic algorithms), converges at Week 4 for integration testing, delivers the sheet automation prototype at Week 9, and ships v1.0.0 at Week 13.
            The single highest-risk item across both documents is Autodesk platform hostility — identified only by the Execution Plan and absent from the Challenge Assessment's risk register. This should be added immediately as R-9 (L:3, I:5, Score:15).
          </p>
        </div>
      </div>
    </div>
  );
}
