import crypto from "crypto";

export interface Chunk {
  id: string;
  content: string;
  hash: string;
}

export function hashContent(data: string): string {
  return crypto.createHash("sha256").update(data).digest("hex");
}

export function chunkCorpus(text: string, size = 200_000): Chunk[] {
  if (size <= 0) {
    throw new Error("Chunk size must be > 0");
  }

  const chunks: Chunk[] = [];
  for (let i = 0; i < text.length; i += size) {
    const content = text.slice(i, i + size);
    chunks.push({
      id: `chunk-${Math.floor(i / size)}`,
      content,
      hash: hashContent(content)
    });
  }
  return chunks;
}
