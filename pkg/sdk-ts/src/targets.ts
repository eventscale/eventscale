import { Context } from "./nats";
import { networkAddEventExtractorSubject } from "./subjects";

export interface TargetEvent {
  network: string; // not sent in payload, only used for subject
  name: string;
  signature: string;
  contracts: string[]; // hex addresses or aliases
  need_other_logs: boolean;
}

export async function addTargetEventSync(ctx: Context, event: TargetEvent): Promise<void> {
  const subject = networkAddEventExtractorSubject(event.network);
  const { network, ...payload } = event;
  const json = JSON.stringify(payload);
  await ctx.publish(subject, json);
}

export function addTargetEventAsync(ctx: Context, event: TargetEvent): Promise<void> {
  const subject = networkAddEventExtractorSubject(event.network);
  const { network, ...payload } = event;
  const json = JSON.stringify(payload);
  return ctx.publishAsync(subject, json);
}
