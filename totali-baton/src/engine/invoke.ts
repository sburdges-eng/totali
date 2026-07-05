import { TotaliBaton } from "../schema/baton";
import { assertCorpusHashInvariant } from "../schema/invariants";
import { LlmClient } from "../llm/client";

export async function invokeWithBaton(
  client: LlmClient,
  baton: TotaliBaton,
  expectedCorpusHash: string,
  task: string,
  input: unknown
): Promise<unknown> {
  assertCorpusHashInvariant(baton, expectedCorpusHash);

  return client.callLLM({
    system: `TASK: ${task}. Operate strictly on structured state.`,
    input: {
      batonMemory: baton.memory,
      payload: input
    }
  });
}
