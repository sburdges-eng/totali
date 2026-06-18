import test from "node:test";
import assert from "node:assert/strict";
import fs from "fs/promises";
import os from "os";
import path from "path";

import { checkpoint, saveBaton } from "../src/checkpoint";
import { chunkCorpus, hashContent } from "../src/chunking";
import { DeterministicEmbedder } from "../src/embed/deterministic-embedder";
import { MockLlmClient } from "../src/llm/mock-llm-client";
import { runPipeline } from "../src/run";
import { createBaton } from "../src/schema/baton";
import { MemoryVectorStore } from "../src/store/memory-vector-store";

test("chunking is deterministic", () => {
  const text = "abc ".repeat(1000);
  const a = chunkCorpus(text, 40);
  const b = chunkCorpus(text, 40);
  assert.equal(a.length, b.length);
  assert.deepEqual(
    a.map((x) => ({ id: x.id, hash: x.hash })),
    b.map((x) => ({ id: x.id, hash: x.hash }))
  );
});

test("checkpoint memory hash matches expected", () => {
  const baton = createBaton("run-1", hashContent("corpus"));
  baton.memory.assertions.push({
    id: "a1",
    content: "fact",
    sourceChunk: "chunk-0",
    credibility: 0.9,
    verified: false
  });
  const cp = checkpoint(baton);
  assert.equal(cp.memoryHash, hashContent(JSON.stringify(baton.memory)));
});

test("end-to-end pipeline compiles with verified assertions", async () => {
  const result = await runPipeline({
    corpusText: "one two three four five six",
    client: new MockLlmClient(),
    embedder: new DeterministicEmbedder(),
    store: new MemoryVectorStore(),
    chunkSize: 10
  });

  assert.equal(result.baton.ready, true);
  assert.ok(result.baton.memory.assertions.length > 0);
  assert.ok(
    result.baton.memory.assertions.every((a) => a.verified === true),
    "all assertions should be verified by mock verify pass"
  );
});

test("corpusHash change aborts run", async () => {
  const corpusText = "immutable corpus content";
  const corpusHash = hashContent(corpusText);
  const baton = createBaton("run-bad", corpusHash);
  baton.corpusHash = "tampered";

  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "totali-baton-"));
  const batonPath = path.join(tempDir, "baton.json");
  await saveBaton(batonPath, baton);

  await assert.rejects(() =>
    runPipeline({
      corpusText,
      client: new MockLlmClient(),
      embedder: new DeterministicEmbedder(),
      store: new MemoryVectorStore(),
      resumeFromBatonPath: batonPath
    })
  );
});
