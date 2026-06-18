import fs from "fs/promises";
import path from "path";

import { hashContent } from "./chunking";
import { Checkpoint, TotaliBaton } from "./schema/baton";

export function checkpoint(baton: TotaliBaton): Checkpoint {
  const entry: Checkpoint = {
    timestamp: Date.now(),
    cursor: baton.cursor,
    memoryHash: hashContent(JSON.stringify(baton.memory))
  };
  baton.checkpoints.push(entry);
  return entry;
}

export async function saveBaton(filePath: string, baton: TotaliBaton): Promise<void> {
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(baton, null, 2), "utf8");
}

export async function loadBaton(filePath: string): Promise<TotaliBaton> {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw) as TotaliBaton;
}
