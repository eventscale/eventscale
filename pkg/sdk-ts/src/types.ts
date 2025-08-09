// Ambient declarations to satisfy TypeScript without @types/node or DOM libs
// These are used conditionally at runtime.

export interface EventMetadata {
  network: string;
  chain_id: string; // big.Int encoded as string in JSON
  contract: string; // hex address
  name: string;
  signature: string;
  block_number: number;
  block_hash: string;
  tx_hash: string;
  tx_index: number;
  log_index: number;
  other_logs?: unknown[]; // raw other ethereum logs, passthrough
  timestamp: number;
}

export interface EventWire {
  meta: EventMetadata;
  data: string; // base64-encoded bytes
}

export class Event {
  readonly meta: EventMetadata;
  readonly data: any;
  readonly topic: string;

  constructor(meta: EventMetadata, data: any, topic: string) {
    this.meta = meta;
    this.data = data;
    this.topic = topic;
  }

  static fromJsonBytes(bytes: Uint8Array, subject: string): Event | null {
    try {
      const text = new TextDecoder().decode(bytes);
      const obj = JSON.parse(text) as EventWire;
      const data = Buffer.from(obj.data, "base64").toString();
      return new Event(obj.meta, JSON.parse(data), subject);
    } catch {
      return null;
    }
  }
}

export const ANY_TOKEN = "*";