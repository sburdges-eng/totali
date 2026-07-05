import { invokeWithBaton } from "../engine/invoke";
import { LlmClient } from "../llm/client";
import { Assertion, TotaliBaton } from "../schema/baton";

interface AggregateResponse {
  assertions?: Assertion[];
}

async function aggregatePair(
  client: LlmClient,
  baton: TotaliBaton,
  expectedCorpusHash: string,
  assertions: Assertion[]
): Promise<Assertion[]> {
  const result = (await invokeWithBaton(
    client,
    baton,
    expectedCorpusHash,
    "AGGREGATE",
    { assertions }
  )) as AggregateResponse;
  return result.assertions ?? [];
}

export async function aggregateAssertions(
  client: LlmClient,
  baton: TotaliBaton,
  expectedCorpusHash: string
): Promise<Assertion[]> {
  let layer = [...baton.memory.assertions];
  if (layer.length <= 1) {
    return layer;
  }

  while (layer.length > 1) {
    const nextLayer: Assertion[] = [];
    for (let i = 0; i < layer.length; i += 2) {
      if (i + 1 >= layer.length) {
        nextLayer.push(layer[i]);
        continue;
      }
      const aggregated = await aggregatePair(client, baton, expectedCorpusHash, [
        layer[i],
        layer[i + 1]
      ]);
      if (aggregated.length === 0) {
        // Preserve data if aggregation fails to produce output.
        nextLayer.push(layer[i], layer[i + 1]);
      } else {
        nextLayer.push(...aggregated);
      }
    }
    layer = nextLayer;
  }

  return layer;
}
