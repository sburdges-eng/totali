#!/usr/bin/env node
import fs from "fs/promises";
import path from "path";

import { DeterministicEmbedder } from "./embed/deterministic-embedder";
import { MockLlmClient } from "./llm/mock-llm-client";
import { runPipeline } from "./run";
import { MemoryVectorStore } from "./store/memory-vector-store";

interface CliArgs {
  corpusPath: string;
  batonPath?: string;
  resumeFrom?: string;
  chunkSize?: number;
  checkpointEvery?: number;
  skipVerify?: boolean;
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { corpusPath: "" };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    switch (token) {
      case "--corpus":
        args.corpusPath = argv[++i] ?? "";
        break;
      case "--baton-out":
        args.batonPath = argv[++i];
        break;
      case "--resume-from":
        args.resumeFrom = argv[++i];
        break;
      case "--chunk-size":
        args.chunkSize = Number(argv[++i]);
        break;
      case "--checkpoint-every":
        args.checkpointEvery = Number(argv[++i]);
        break;
      case "--skip-verify":
        args.skipVerify = true;
        break;
      default:
        break;
    }
  }
  if (!args.corpusPath) {
    throw new Error(
      "Missing --corpus <path>. Example: node dist/src/cli.js --corpus ./corpus.txt"
    );
  }
  return args;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const corpusText = await fs.readFile(path.resolve(args.corpusPath), "utf8");

  const result = await runPipeline({
    corpusText,
    client: new MockLlmClient(),
    embedder: new DeterministicEmbedder(),
    store: new MemoryVectorStore(),
    batonPath: args.batonPath ? path.resolve(args.batonPath) : undefined,
    resumeFromBatonPath: args.resumeFrom
      ? path.resolve(args.resumeFrom)
      : undefined,
    chunkSize: args.chunkSize,
    checkpointEvery: args.checkpointEvery,
    skipVerify: args.skipVerify
  });

  process.stdout.write(
    JSON.stringify(
      {
        runId: result.baton.runId,
        cursor: result.baton.cursor,
        ready: result.baton.ready,
        assertions: result.baton.memory.assertions.length,
        contradictions: result.baton.memory.contradictions.length
      },
      null,
      2
    ) + "\n"
  );
}

main().catch((err: unknown) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`totali-baton failed: ${message}\n`);
  process.exit(1);
});
