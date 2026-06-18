export interface LlmCall {
  system: string;
  input: {
    batonMemory: unknown;
    payload: unknown;
  };
}

export interface LlmClient {
  callLLM(request: LlmCall): Promise<unknown>;
}
