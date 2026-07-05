import { randomUUID } from "crypto";

import { Assertion } from "../schema/baton";
import { LlmCall, LlmClient } from "./client";

interface VerifyResult {
  valid: boolean;
  reason?: string;
}

export class MockLlmClient implements LlmClient {
  async callLLM(request: LlmCall): Promise<unknown> {
    const match = request.system.match(/TASK:\s*([A-Z_]+)/);
    const task = match?.[1] ?? "UNKNOWN";
    const payload = request.input.payload as Record<string, unknown>;

    switch (task) {
      case "SUMMARIZE_CHUNK": {
        const content = String(payload.content ?? "");
        const chunkId = String(payload.chunkId ?? "unknown");
        const assertions: Assertion[] = [
          {
            id: randomUUID(),
            content: content.slice(0, 200),
            sourceChunk: chunkId,
            credibility: 0.8,
            verified: false
          }
        ];
        return { assertions };
      }
      case "AGGREGATE": {
        const assertions = (payload.assertions as Assertion[] | undefined) ?? [];
        if (assertions.length === 0) {
          return { assertions: [] as Assertion[] };
        }
        return {
          assertions: [
            {
              id: randomUUID(),
              content: assertions.map((a) => a.content).join(" "),
              sourceChunk: "aggregate",
              credibility:
                assertions.reduce((sum, a) => sum + a.credibility, 0) /
                assertions.length,
              verified: false
            }
          ]
        };
      }
      case "VERIFY_FACT": {
        const fact = payload.fact as Assertion | undefined;
        const valid = Boolean(fact && fact.content.trim().length > 0);
        const result: VerifyResult = valid
          ? { valid: true }
          : { valid: false, reason: "Empty assertion content" };
        return result;
      }
      case "COMPILE_OUTPUT": {
        return {
          compiled: true,
          verifiedCount: Array.isArray(payload.verifiedAssertions)
            ? payload.verifiedAssertions.length
            : 0
        };
      }
      default:
        return {};
    }
  }
}
