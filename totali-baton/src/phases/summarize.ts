import { checkpoint } from "../checkpoint";
import { invokeWithBaton } from "../engine/invoke";
import { LlmClient } from "../llm/client";
import { Assertion, TotaliBaton } from "../schema/baton";
import { Chunk } from "../chunking";

interface SummarizeResponse {
  assertions?: Assertion[];
}

export async function summarizeChunks(
  client: LlmClient,
  baton: TotaliBaton,
  expectedCorpusHash: string,
  chunks: Chunk[],
  checkpointEvery = 0
): Promise<void> {
  for (let i = baton.cursor; i < chunks.length; i += 1) {
    const chunk = chunks[i];
    const result = (await invokeWithBaton(
      client,
      baton,
      expectedCorpusHash,
      "SUMMARIZE_CHUNK",
      {
        chunkId: chunk.id,
        content: chunk.content
      }
    )) as SummarizeResponse;

    if (result.assertions?.length) {
      baton.memory.assertions.push(...result.assertions);
    }

    baton.cursor += 1;

    if (checkpointEvery > 0 && baton.cursor % checkpointEvery === 0) {
      checkpoint(baton);
    }
  }
}
