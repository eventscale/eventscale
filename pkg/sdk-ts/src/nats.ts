import { connect, NatsConnection, JetStreamClient, JetStreamManager, StringCodec} from "nats";

export class Context {
  readonly nc: NatsConnection;
  readonly js: JetStreamClient;
  readonly jsm: JetStreamManager;

  private constructor(nc: NatsConnection, js: JetStreamClient, jsm: JetStreamManager) {
    this.nc = nc;
    this.js = js;
    this.jsm = jsm;
  }

  static async connect(url: string): Promise<Context> {
    const nc = await connect({ servers: url });
    const js = nc.jetstream();
    const jsm = await nc.jetstreamManager();
    return new Context(nc, js, jsm);
  }

  async publish(subject: string, data: Uint8Array | string): Promise<void> {
    const payload = typeof data === "string" ? StringCodec().encode(data) : data;
    await this.js.publish(subject, payload);
  }

  publishAsync(subject: string, data: Uint8Array | string): Promise<void> {
    const payload = typeof data === "string" ? StringCodec().encode(data) : data;
    // In nats.js publish already returns a promise; we just return it.
    return this.js.publish(subject, payload).then(() => {});
  }

  async close(): Promise<void> {
    await this.nc.drain();
  }
}
