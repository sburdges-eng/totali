import { randomUUID } from "crypto";

import { checkpoint, loadBaton, saveBaton } from "./checkpoint";
import { chunkCorpus, hashContent } from "./chunking";
import { Embedder } from "./embed/embedder";
import { invokeWithBaton } from "./engine/invoke";
import { LlmClient } from "./llm/client";
import { aggregateAssertions } from "./phases/aggregate";
import { compileOutput } from "./phases/compile";
import { summarizeChunks } from "./phases/summarize";
import { verifyAssertions } from "./phases/verify";
import { createBaton, TotaliBaton } from "./schema/baton";
import { assertCorpusHashInvariant } from "./schema/invariants";
import { VectorStore } from "./store/vector-store";

export interface RunPipelineOptions {
  corpusText: string;
  client: LlmClient;
  embedder: Embedder;
  store: VectorStore;
  runId?: string;
  chunkSize?: number;
  checkpointEvery?: number;
  batonPath?: string;
  resumeFromBatonPath?: string;
  skipVerify?: boolean;
}

export interface RunPipelineResult {
  baton: TotaliBaton;
  output: unknown;
  aggregateRoots: unknown[];
}

function computeCorpusHash(corpusText: string): string {
  return hashContent(corpusText);
}

async function ingestChunks(
  embedder: Embedder,
  store: VectorStore,
  chunks: ReturnType<typeof chunkCorpus>
): Promise<void> {
  for (const chunk of chunks) {
    const embedding = await embedder.embed(chunk.content);
    await store.upsert(chunk.id, embedding, { hash: chunk.hash });
  }
}

export async function runPipeline(
  options: RunPipelineOptions
): Promise<RunPipelineResult> {
  const chunkSize = options.chunkSize ?? 200_000;
  const checkpointEvery = options.checkpointEvery ?? 0;
  const corpusHash = computeCorpusHash(options.corpusText);

  const baton =
    options.resumeFromBatonPath !== undefined
      ? await loadBaton(options.resumeFromBatonPath)
      : createBaton(options.runId ?? randomUUID(), corpusHash);

  assertCorpusHashInvariant(baton, corpusHash);

  const chunks = chunkCorpus(options.corpusText, chunkSize);
  await ingestChunks(options.embedder, options.store, chunks);

  await summarizeChunks(
    options.client,
    baton,
    corpusHash,
    chunks,
    checkpointEvery
  );

  const aggregateRoots = await aggregateAssertions(
    options.client,
    baton,
    corpusHash
  );

  if (!options.skipVerify) {
    await verifyAssertions(options.client, baton, corpusHash);
  }

  checkpoint(baton);

  const output = await compileOutput(options.client, baton, corpusHash);

  if (options.batonPath) {
    await saveBaton(options.batonPath, baton);
  }

  // Lightweight retrieval touchpoint, useful for health checks and demos.
  if (chunks.length > 0) {
    const q = await options.embedder.embed(chunks[0].content.slice(0, 256));
    await options.store.query(q, 8);
  }

  await invokeWithBaton(options.client, baton, corpusHash, "RUN_COMPLETE", {
    runId: baton.runId
  });

  return { baton, output, aggregateRoots };
}
