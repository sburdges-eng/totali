export interface TotaliBaton {
  runId: string;
  cursor: number;
  corpusHash: string;
  memory: BatonMemory;
  checkpoints: Checkpoint[];
  ready: boolean;
}

export interface BatonMemory {
  assertions: Assertion[];
  entities: Record<string, Entity>;
  schemas: Record<string, unknown>;
  contradictions: Contradiction[];
}

export interface Assertion {
  id: string;
  content: string;
  sourceChunk: string;
  credibility: number;
  verified: boolean;
}

export interface Entity {
  id: string;
  type: string;
  attributes?: Record<string, unknown>;
  refs?: string[];
}

export interface Contradiction {
  factId: string;
  reason: string;
  sourceChunk?: string;
}

export interface Checkpoint {
  timestamp: number;
  cursor: number;
  memoryHash: string;
}

export function createBaton(runId: string, corpusHash: string): TotaliBaton {
  return {
    runId,
    cursor: 0,
    corpusHash,
    memory: {
      assertions: [],
      entities: {},
      schemas: {},
      contradictions: []
    },
    checkpoints: [],
    ready: false
  };
}
