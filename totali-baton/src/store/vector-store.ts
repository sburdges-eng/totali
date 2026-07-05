export interface SearchResult {
  id: string;
  score: number;
  metadata?: Record<string, unknown>;
}

export interface VectorStore {
  upsert(
    id: string,
    embedding: number[],
    metadata: Record<string, unknown>
  ): Promise<void>;
  query(vector: number[], k: number): Promise<SearchResult[]>;
}
