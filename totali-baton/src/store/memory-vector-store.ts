import { SearchResult, VectorStore } from "./vector-store";

interface StoredVector {
  embedding: number[];
  metadata: Record<string, unknown>;
}

function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length === 0 || b.length === 0 || a.length !== b.length) {
    return 0;
  }
  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < a.length; i += 1) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  if (normA === 0 || normB === 0) {
    return 0;
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

export class MemoryVectorStore implements VectorStore {
  private readonly vectors = new Map<string, StoredVector>();

  async upsert(
    id: string,
    embedding: number[],
    metadata: Record<string, unknown>
  ): Promise<void> {
    this.vectors.set(id, { embedding, metadata });
  }

  async query(vector: number[], k: number): Promise<SearchResult[]> {
    const scored = Array.from(this.vectors.entries()).map(([id, value]) => ({
      id,
      score: cosineSimilarity(vector, value.embedding),
      metadata: value.metadata
    }));

    return scored.sort((a, b) => b.score - a.score).slice(0, Math.max(0, k));
  }
}
