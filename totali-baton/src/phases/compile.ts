import { invokeWithBaton } from "../engine/invoke";
import { LlmClient } from "../llm/client";
import { TotaliBaton } from "../schema/baton";

export async function compileOutput(
  client: LlmClient,
  baton: TotaliBaton,
  expectedCorpusHash: string
): Promise<unknown> {
  const verified = baton.memory.assertions.filter((a) => a.verified);
  const final = await invokeWithBaton(
    client,
    baton,
    expectedCorpusHash,
    "COMPILE_OUTPUT",
    { verifiedAssertions: verified }
  );
  baton.ready = true;
  return final;
}
