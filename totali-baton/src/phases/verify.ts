import { invokeWithBaton } from "../engine/invoke";
import { LlmClient } from "../llm/client";
import { TotaliBaton } from "../schema/baton";

interface VerifyResponse {
  valid: boolean;
  reason?: string;
}

export async function verifyAssertions(
  client: LlmClient,
  baton: TotaliBaton,
  expectedCorpusHash: string
): Promise<void> {
  for (const fact of baton.memory.assertions) {
    const verification = (await invokeWithBaton(
      client,
      baton,
      expectedCorpusHash,
      "VERIFY_FACT",
      { fact }
    )) as VerifyResponse;

    if (!verification.valid) {
      fact.verified = false;
      baton.memory.contradictions.push({
        factId: fact.id,
        reason: verification.reason ?? "Unspecified verification failure",
        sourceChunk: fact.sourceChunk
      });
    } else {
      fact.verified = true;
    }
  }
}
