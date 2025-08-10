import { Context } from "./nats";
import { networkAddEventExtractorSubject } from "./subjects";
import { NatsConnection } from "@nats-io/nats-core";

export interface TargetEvent {
  network: string; // not sent in payload, only used for subject
  name: string;
  signature: string;
  contracts: string[]; // hex addresses or aliases
  need_other_logs: boolean;
}

export async function addTargetEvent(
  ctx: Context,
  event: TargetEvent
): Promise<void> {
  const subject = networkAddEventExtractorSubject(event.network);
  const json = JSON.stringify(event);
  await ctx.publish(subject, json);
}

export function addTargetEventAsync(
  nc: NatsConnection,
  event: TargetEvent
): void {
  const subject = networkAddEventExtractorSubject(event.network);
  const json = JSON.stringify(event);
  nc.publish(subject, json);
}
