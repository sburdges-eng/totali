import { Embedder } from "./embedder";

// Lightweight deterministic embedder for tests/dev runs.
export class DeterministicEmbedder implements Embedder {
  constructor(private readonly dims = 64) {
    if (dims <= 0) {
      throw new Error("dims must be > 0");
    }
  }

  async embed(text: string): Promise<number[]> {
    const vector = new Array(this.dims).fill(0);
    for (let i = 0; i < text.length; i += 1) {
      const code = text.charCodeAt(i);
      vector[i % this.dims] += code / 65535;
    }
    return vector;
  }
}
