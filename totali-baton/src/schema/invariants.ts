import { TotaliBaton } from "./baton";

export function assertCorpusHashInvariant(
  baton: TotaliBaton,
  expectedCorpusHash: string
): void {
  if (baton.corpusHash !== expectedCorpusHash) {
    throw new Error(
      `Corpus hash invariant violated: expected=${expectedCorpusHash}, actual=${baton.corpusHash}`
    );
  }
}
