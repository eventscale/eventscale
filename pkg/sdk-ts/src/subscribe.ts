import {
  AckPolicy,
  Consumer,
  ConsumerConfig,
  DeliverPolicy,
  ReplayPolicy,
} from "nats";
import { NatsConnection, wsconnect } from "@nats-io/nats-core";

import { Context } from "./nats";
import { ANY_TOKEN, Event, EventWire } from "./types";
import { STREAM_NAME, eventSubject } from "./subjects";

export type EventHandlerFunc = (event: Event) => Promise<void> | void;

export interface Logger {
  debug?(message: string, ...args: any[]): void;
  info?(message: string, ...args: any[]): void;
  warn?(message: string, ...args: any[]): void;
  error?(message: string, ...args: any[]): void;
}

// Default logger that does nothing (silent)
const silentLogger: Logger = {};

// Console logger for development
export const consoleLogger: Logger = {
  debug: console.debug?.bind(console) || console.log?.bind(console),
  info: console.info?.bind(console) || console.log?.bind(console),
  warn: console.warn?.bind(console) || console.log?.bind(console),
  error: console.error?.bind(console) || console.log?.bind(console),
};

// Debug-enabled console logger (checks localStorage for debug flag)
export const debugConsoleLogger: Logger = {
  debug: (msg, ...args) => {
    if (typeof localStorage !== 'undefined' && localStorage.getItem('eventscale:debug') === 'true') {
      (console.debug || console.log)(`[Eventscale Debug] ${msg}`, ...args);
    }
  },
  info: console.info?.bind(console) || console.log?.bind(console),
  warn: console.warn?.bind(console) || console.log?.bind(console),
  error: console.error?.bind(console) || console.log?.bind(console),
};

export interface SubscribeOptions {
  network?: string;
  contract?: string;
  event?: string;
  durableName?: string;
  logger?: Logger;
}

export class EventSubscriber {
  private ctx: Context;
  private consumer: Consumer;
  private stopped = false;
  private handler: EventHandlerFunc;
  private logger: Logger;

  // target
  readonly network: string;
  readonly contract: string;
  readonly event: string;

  constructor(
    ctx: Context,
    consumer: Consumer,
    handler: EventHandlerFunc,
    target: { network: string; contract: string; event: string },
    logger: Logger = silentLogger
  ) {
    this.ctx = ctx;
    this.consumer = consumer;
    this.network = target.network;
    this.contract = target.contract;
    this.event = target.event;
    this.handler = handler;
    this.logger = logger;
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
              this.logger.error?.(`handler failed: ${err}`);
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
  }, opts.logger || silentLogger);
}

export class WebsocketContext {
  readonly nc: NatsConnection;
  readonly target: string;
  readonly handler: EventHandlerFunc;
  private logger: Logger;
  stopped = false;

  constructor(nc: NatsConnection, handler: EventHandlerFunc, target: string, logger: Logger = silentLogger) {
    this.nc = nc;
    this.handler = handler;
    this.target = target;
    this.logger = logger;
  }

  async start(): Promise<void> {
    while (!this.stopped) {
      const sub = this.nc.subscribe(this.target);

      try {
        for await (const m of sub) {
          this.logger.debug?.("received message", m.subject);

          const eventWire = m.json<EventWire>();
          // Browser-compatible base64 decoding
          const data = typeof Buffer !== 'undefined' 
            ? Buffer.from(eventWire.data, "base64").toString()
            : atob(eventWire.data);
          const event = new Event(eventWire.meta, JSON.parse(data), this.target);

          if (event) {
            try {
              this.logger.debug?.("received event", event.topic);
              await this.handler(event);
            } catch (err) {
              this.logger.error?.(`handler failed: ${err}`);
            }
          } else {
            this.logger.debug?.("received non-event message");
          }
        }
      } catch (err) {
        this.logger.error?.(`consume failed: ${err}`);
      }
    }
  }

  stop(): void {
    this.stopped = true;
    this.nc.drain();
  }
}

export async function websocketConnect(
  url: string,
  handler: EventHandlerFunc,
  opts: SubscribeOptions = {}
): Promise<WebsocketContext> {
  const network = opts.network ?? ANY_TOKEN;
  const contract = opts.contract ?? ANY_TOKEN;
  const event = opts.event ?? ANY_TOKEN;

  const filter = eventSubject(network, contract, event);
  const nc = await wsconnect({ servers: url });

  opts.logger?.debug?.("websocket connected", filter);

  return new WebsocketContext(nc, handler, filter, opts.logger || silentLogger);
}
