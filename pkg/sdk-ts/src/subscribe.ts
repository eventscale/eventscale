import {
  AckPolicy,
  Consumer,
  ConsumerConfig,
  DeliverPolicy,
  ReplayPolicy,
} from "nats";
import { Context } from "./nats";
import { ANY_TOKEN, Event, EventWire } from "./types";
import { STREAM_NAME, eventSubject } from "./subjects";

export type EventHandlerFunc = (event: Event) => Promise<void> | void;

export interface SubscribeOptions {
  network?: string;
  contract?: string;
  event?: string;
  durableName?: string;
}

export class EventSubscriber {
  private ctx: Context;
  private consumer: Consumer;
  private stopped = false;
  private handler: EventHandlerFunc;

  // target
  readonly network: string;
  readonly contract: string;
  readonly event: string;

  constructor(
    ctx: Context,
    consumer: Consumer,
    handler: EventHandlerFunc,
    target: { network: string; contract: string; event: string }
  ) {
    this.ctx = ctx;
    this.consumer = consumer;
    this.network = target.network;
    this.contract = target.contract;
    this.event = target.event;
    this.handler = handler;
  }

  targetSubject(): string {
    return eventSubject(this.network, this.contract, this.event);
  }

  async start(): Promise<void> {
    while (!this.stopped) {
      const messages = await this.consumer.consume();

      try {
        for await (const m of messages) {
          const event = Event.fromJsonBytes(m.data, this.targetSubject());
          if (event) {
            try {
              await this.handler(event);
              m.ack();
            } catch (err) {
              console.log(`handler failed: ${err}`);
              m.nak();
            }
          } else {
            m.nak();
          }
        }
      } catch (err) {
        console.log(`consume failed: ${err}`);
      }
    }
  }

  stop(): void {
    this.stopped = true;
    this.consumer.delete();
  }
}

export async function subscribe(
  ctx: Context,
  handler: EventHandlerFunc,
  opts: SubscribeOptions = {}
): Promise<EventSubscriber> {
  const network = opts.network ?? ANY_TOKEN;
  const contract = opts.contract ?? ANY_TOKEN;
  const event = opts.event ?? ANY_TOKEN;

  const filter = eventSubject(network, contract, event);

  const conf: ConsumerConfig = {
    ack_policy: AckPolicy.Explicit,
    ack_wait: 30_000_000_000, // 30s in ns expected by server
    filter_subject: filter,
    deliver_policy: DeliverPolicy.New,
    replay_policy: ReplayPolicy.Instant,
  };

  const consumerInfo = await ctx.jsm.consumers.add(STREAM_NAME, conf);
  const consumer = await ctx.js.consumers.get(STREAM_NAME, consumerInfo.name);

  return new EventSubscriber(ctx, consumer, handler, {
    network,
    contract,
    event,
  });
}
