# C++ Agentic Rules, Cautions, Workflows, Debug Strategies, and Practices

Scope: applies whenever an agent (Claude / Gemini / Codex) writes, modifies, reviews,
or refactors C/C++ code that TOTaLi depends on or integrates with. This includes:

- `dwg-tool-parser/` (DWG/DXF native parsing)
- The auracad C++ bridge consumed by TOTaLi for CAD geometry
- Any FFI surface that crosses Python ↔ C++ (pybind11, ctypes, cffi)
- Any Civil 3D / ObjectARX / RealDWG native module linked from `laser-suite/` or `AUTOMATICCAD/`
- Vendored C++ dependencies (PROJ, GDAL, PDAL, CGAL, OpenCASCADE, LibreDWG)

C++ bugs in this stack are **defensibility bugs**, not just correctness bugs. A miscompiled
transform, a UB-laced parser, or a silently-truncated coordinate corrupts the chain-of-custody.
Treat everything below as a hard constraint unless a memo in `Docs/` overrides it.

---

## 1. Dangers (what actually goes wrong)

### 1.1 Silent-correctness class
- **Undefined behavior tolerates everything.** Use-after-move, dangling refs, iterator
  invalidation, signed overflow, strict-aliasing violations — all compile clean, pass unit
  tests, crash in production or (worse) return wrong coordinates.
- **ODR / linking traps.** Merging translation units, inline functions with differing
  definitions across TUs, vendored headers at different versions — no compiler error, the
  linker picks whichever symbol it saw first. Especially dangerous in `dwg-tool-parser/`
  when vendored DWG libs collide with system-installed versions.
- **Real-time / deterministic-latency surfaces.** Not an audio thread here, but the Civil 3D
  bridge and segmentation ONNX callbacks have soft-RT budgets. `std::string`, `std::function`,
  `shared_ptr` ctors, hidden `mutex` locks, and heap allocs inside hot geometry loops
  regress throughput without showing up in unit tests.
- **Template / header blast radius.** One header change silently recompiles and changes
  codegen in dozens of TUs. Agents see the edit as local; ABI drift is not.
- **Floating-point determinism.** `-ffast-math`, FMA, x87 80-bit intermediates, and
  different SIMD lanes produce different coordinates. TOTaLi's audit requires
  bit-reproducible geometry — never enable `-ffast-math`, never reorder associative
  ops "for speed," never use `long double` where `double` was specified.

### 1.2 FFI / boundary class
- **Ownership across C / Python / Rust.** Raw pointers across FFI without an explicit
  ownership contract leak or double-free. Returning `std::string` / `std::vector` across
  `extern "C"` is UB. Always return `(ptr, len)` pairs with a matching free function.
- **Exception boundaries.** A C++ exception crossing `extern "C"` is UB. Wrap every
  FFI entry point in `try { ... } catch (...) { return error_code; }`.
- **GIL discipline.** Any pybind11 / CPython callback that spends >1ms outside Python
  must release the GIL (`py::gil_scoped_release`) and reacquire before touching any
  Python object. Agent-generated pybind11 code frequently forgets this.
- **String encodings.** DWG files contain mixed-encoding strings (CP1252, UTF-8, UTF-16LE).
  Never assume `char*` is UTF-8. Never `std::string` round-trip without an encoding tag.

### 1.3 Agentic-loop class (these compound)
- **"Build green" is not a success signal.** The 2026-Q2 KMiDi audit found ~6000 LOC of
  dead/dup code while CI was green for months. An agent loop that terminates on
  `cmake --build` is terminating on "compiles," not "correct."
- **Sanitizer blindness.** Without ASAN/UBSAN/TSAN/MSAN in the loop, the agent never sees
  the bugs it just wrote. Plain compile + unit tests is not a safety net for C++.
- **Over-eager dead-code removal.** Aggressive cleanup kills symbols dispatched by name
  (pybind11 module registries, ObjectARX command factories, vtable entries referenced
  only via `dlsym`, Civil 3D managed-interop stubs).
- **Stale mental model in long contexts.** A 1M-token window holds the repo but not its
  *invariants*. Agents forget which threads own which objects after 200K tokens of edits.
  Thread- and allocator-ownership rules belong in headers / this doc, not in working memory.
- **Destructive shortcuts.** `git reset --hard`, deleting worktrees with uncommitted work,
  wiping `build/` that holds generated headers. Opus 4.7 is more willing to act; prefer
  explicit user confirmation for any non-reversible op.

### 1.4 Security class
- C-idiomatic `memcpy` / `strcpy` / manual buffer arithmetic looks correct and passes
  review. Classic OWASP-adjacent bugs survive.
- **Untrusted input parsers** — DWG, DXF, LAS/LAZ, STEP, IFC, GeoJSON — must treat every
  length field, offset, and count as hostile. Agents routinely write parsers that trust
  header-declared lengths.
- **Integer width / sign mismatches** at 32↔64-bit boundaries produce silent wraps on
  large point clouds and long coordinate vectors.

---

## 2. Hard Rules (non-negotiable)

1. **No C++ edit ships without sanitizer coverage.** At minimum one of: ASAN+UBSAN build
   run against the changed test, or explicit written justification in the PR body
   stating why sanitizers do not apply.
2. **No FFI symbol returns C++ standard-library types.** Only POD, `(ptr, len)` pairs,
   or opaque handles with a paired `*_free()` function.
3. **No `-ffast-math`, no `-Ofast`, no `--fast-math` equivalent** anywhere in the
   geometry / geodetic / extraction paths.
4. **No agent deletes a source file in the same turn it detects it is unused.** Removal
   requires a separate review step with a "removed symbols" diff and a grep across
   Python bindings, CMake `install(EXPORT)`, and `.def` / `.exp` symbol files.
5. **No `--no-verify` on commits touching C++.** Pre-commit hooks run clang-format,
   clang-tidy, and header-include-cleaner. If a hook fails, fix the code, do not bypass.
6. **No destructive git ops on a C++ branch without explicit user approval.** This
   includes `reset --hard`, force-push, and deletion of stash entries or worktrees.
7. **No untrusted-input parser merges without a fuzz target.** Any new parsing entry
   point gets a libFuzzer / honggfuzz harness before merge.
8. **GIL is released around any C++ call that can take >1ms** or that can block on I/O
   or a lock. No exceptions "for simplicity."
9. **No C++ exception crosses an `extern "C"` boundary.** Every FFI entry point has a
   catch-all wrapper.
10. **No in-place edit of `audit_logs/` writers.** Any change to audit emission in C++
    requires re-verification of the SHA-256 hash chain (see §5.4).

---

## 3. Workflows (how the agent loop must run)

### 3.1 Minimum per-edit loop
Every C++ edit turn executes, in order:

1. `clang-format -i` on changed files
2. `cmake --build build/debug` with `-Wall -Wextra -Werror -Wpedantic`
3. `cmake --build build/asan` (ASAN+UBSAN) — run changed tests
4. `ctest -L changed` or pytest suite for any module that binds the changed code
5. `clang-tidy` with `bugprone-*,cppcoreguidelines-*,performance-*,misc-*,readability-identifier-naming`
6. Regression-guard script (see §3.4) if it exists in the repo being edited

An agent may not claim the edit is done until steps 1–5 pass. "Tests pass" without
a sanitizer build is not "tests pass" for C++.

### 3.2 Refactor workflow (cross-TU changes)
Refactors that touch >3 TUs or any public header:

1. Stop. Do not begin editing.
2. Produce a written plan: list of affected TUs, ABI impact, downstream consumers
   (Python bindings, C# interop, CMake exports).
3. Checkpoint current state (fresh branch, fresh worktree).
4. Edit in a fresh context window if the working context exceeds 200K tokens.
5. Run full test suite + sanitizer builds + bindings tests, not just changed subset.
6. Diff public headers and document ABI changes before merge.

### 3.3 FFI-edit workflow
Any change to a `extern "C"` surface or pybind11 module:

1. Update the C++ signature and the Python / C# binding in the same commit.
2. Run the Python binding smoke tests under ASAN (`LD_PRELOAD=libasan.so python -c ...`
   on Linux; `DYLD_INSERT_LIBRARIES` + `MallocNanoZone=0` on macOS).
3. Verify GIL release/reacquire with `py-spy record` or a `PyGILState_Check()` assertion
   in debug builds.
4. Round-trip at least one non-ASCII string through the FFI.

### 3.4 Regression-guard
When a C++ project under or adjacent to TOTaLi has a regression-guard script
(e.g. `tools/regression_guard.py` in KMiDi post-audit), the agent runs it on every
turn, not just in CI. If TOTaLi's C++ surface grows, add one here at `tools/cxx_guard.py`
covering: banned headers, banned API calls, prohibited flags, ODR checks.

---

## 4. Debug Strategies

### 4.1 Sanitizer matrix (run in this order)

| Tool          | Finds                                      | Build flag                                          |
|---------------|--------------------------------------------|-----------------------------------------------------|
| ASAN          | heap/stack OOB, UAF, leaks                 | `-fsanitize=address -fno-omit-frame-pointer -g`     |
| UBSAN         | signed overflow, shift, null deref, misalign | `-fsanitize=undefined -fno-sanitize-recover=all`   |
| TSAN          | data races, lock-order inversion           | `-fsanitize=thread -g` (separate build dir)         |
| MSAN          | uninitialized reads                        | `-fsanitize=memory -fno-omit-frame-pointer` (Linux) |
| LSAN          | leaks (standalone or via ASAN)             | `-fsanitize=leak` or ASAN with `detect_leaks=1`     |
| CFI           | indirect-call hijack, vtable corruption    | `-fsanitize=cfi -flto -fvisibility=hidden`          |

Run ASAN+UBSAN together (compatible). TSAN and MSAN each need their own build directory.
On macOS, disable malloc scribble and enable ASAN symbolization:
`export ASAN_OPTIONS=detect_leaks=1:symbolize=1:abort_on_error=1`
`export ASAN_SYMBOLIZER_PATH=$(xcrun -f llvm-symbolizer)`

### 4.2 Reproducing a field crash
1. Obtain the core dump or crash report (`~/Library/Logs/DiagnosticReports/` on macOS).
2. Load into `lldb` with the exact binary (check build ID / UUID match with
   `dwarfdump --uuid`).
3. If no core dump: enable `ulimit -c unlimited` and a Crashpad handler before re-running.
4. Reproduce under ASAN first. If ASAN-clean, try TSAN. If both clean, reach for UBSAN
   and then MSAN. If all four clean and the bug persists, suspect FP nondeterminism,
   miscompile at `-O3`, or a UB that the sanitizers legitimately miss
   (e.g. strict-aliasing, some integer UB under UBSAN's recover mode).

### 4.3 Geometry / geodetic numerical bugs
- Reproduce at the exact CRS + epoch the audit log records, not the default.
- Diff PROJ transforms with `cs2cs` and the C++ path — any mismatch is a bug in the
  C++ path, PROJ is authoritative.
- Bit-compare `double` outputs with `printf("%.17g")`, not `%g` or `%.6f`.
- Suspect FMA contraction: build with `-ffp-contract=off` and re-run.

### 4.4 Debug-print hygiene
- Never `printf` / `std::cout` from inside an ONNX callback or a Civil 3D command
  handler — output buffering changes timing and can hide races.
- Prefer `fprintf(stderr, ...)` + `fflush(stderr)` for crash-adjacent diagnostics.
- Remove or gate all debug prints before merging; a `#ifdef TOTALI_DEBUG` block is
  fine, a bare `std::cout` is a review rejection.

### 4.5 When the agent is stuck
Symptoms: same test fails after 3+ attempted fixes, or fix lands then breaks an
adjacent test. Stop the loop. Do not "try one more thing." Instead:

1. `git stash` or branch off the in-progress state.
2. Write down (in the conversation or a scratch file) the invariant that keeps getting
   broken and what each failed attempt assumed.
3. Consider whether the test itself is wrong (rare but real — especially for
   floating-point tolerances).
4. Reach for a sanitizer build before another code edit; the bug may have been visible
   the whole time.

---

## 5. Practices (discipline that compounds)

### 5.1 Context hygiene
- Module-level invariants (thread ownership, allocator rules, ABI guarantees, locking
  order) live in the header file and in this doc, **not** in ephemeral context.
- For edits exceeding ~200K tokens of accumulated context, checkpoint, open a fresh
  session, and hand off a written state summary — do not let the agent self-refresh
  from a stale working model.
- Keep `CLAUDE.md`, `AGENTS.md`, and `GEMINI.md` aligned; all three consumers read
  this doc, so put the rules here and reference them from each.

### 5.2 Review discipline
- Diff public headers separately from implementation. ABI review is its own pass.
- Grep for removed symbols across all binding layers before accepting a deletion.
- For any edit touching a parser on untrusted input (DWG / DXF / LAS / STEP / IFC /
  GeoJSON / WKB), the review explicitly confirms the fuzz harness still builds
  and covers the changed code path.
- Annotate "why" for any non-obvious geometric tolerance, CRS assumption, or
  endianness choice. Agents regenerate code; the comment survives.

### 5.3 Static analysis
- `clang-tidy` config in repo root; no per-file disables without a justification comment.
- `cppcheck` as a secondary (catches things clang-tidy misses, e.g. some stl-misuse).
- `include-what-you-use` quarterly; header hygiene prevents ODR and build-time bloat.
- Compiler `-Weverything` (clang) on a scheduled build, not CI — triage, don't fix all.

### 5.4 Audit-integrity rules
TOTaLi's `audit_logs/` is a defensible legal record. C++ code that emits audit events
must:

- Use the canonical JSONL schema — no ad-hoc fields from C++.
- Emit through the same SHA-256 chaining function Python uses, or write raw and let
  Python re-hash at the boundary (preferred).
- Never buffer audit events across process boundaries without an `fsync`.
- Never log PII or raw coordinates that were explicitly redacted upstream.

### 5.5 Dependency posture
- Pin vendored C++ deps by commit hash in CMake `FetchContent` or as git submodules.
- Upgrade only with a CHANGELOG review and a fresh sanitizer run.
- Never link against two versions of the same lib (ODR violation guaranteed).
- System-installed PROJ / GDAL must match the version used in CI; mismatch produces
  silently different transforms.

### 5.6 Destructive-action policy
Agents request user approval for, at minimum:
- `git reset --hard`, `git push --force`, branch deletion, worktree deletion
- `rm -rf build/` when CMake cache contains configured paths the user may want
- Deletion of any file under `audit_logs/`, `Datasets/`, `artifacts/` (never, actually — just never)
- Any edit that drops or downgrades a vendored C++ dependency

---

## 6. Pre-merge Checklist

Copy this into the PR body for any C++-touching change:

```
- [ ] clang-format clean
- [ ] Debug build: -Wall -Wextra -Werror -Wpedantic passes
- [ ] ASAN+UBSAN build passes changed tests
- [ ] TSAN build passes if change touches threading / FFI callbacks
- [ ] clang-tidy clean (or per-line suppressions justified in comments)
- [ ] Public header ABI diff reviewed (empty if no public header touched)
- [ ] FFI: GIL released around any >1ms call; no std-lib types cross extern "C"
- [ ] FFI: exception wrapper on every extern "C" entry
- [ ] Fuzz harness builds + covers changed parser path (if parser changed)
- [ ] No -ffast-math / -Ofast introduced
- [ ] Deletion diff reviewed for symbols referenced by Python / C# bindings
- [ ] Audit-event emission still produces identical SHA-256 chain on canonical input
- [ ] Regression-guard script (if present) passes
```

---

## 7. When rules and reality conflict

These rules are rigid by design. If a rule is genuinely wrong for a specific situation:

1. Do not silently break it. Do not add a "temporary" exemption.
2. Propose the change in a PR that edits this document, with rationale and the
   specific incident that motivated the change.
3. Get explicit human approval before landing the code that violates the prior rule.

The rules exist because every one of them maps to a past incident in this stack or
an adjacent one (auracad, KMiDi, L4L). Weakening them without incident review
reintroduces the incident.
