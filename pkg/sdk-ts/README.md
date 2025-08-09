# Eventscale TypeScript SDK (@eventscale/sdk)

TypeScript SDK mirroring `pkg/sdk-go` using NATS JetStream (`nats` package).

## Install

Install from npm:

```bash
npm install @eventscale/sdk
```

## Usage

### Connect

```ts
import { Context } from "@eventscale/sdk";

const ctx = await Context.connect("nats://127.0.0.1:4222");
```

### Subscribe to events

```ts
import { subscribe } from "@eventscale/sdk";

type TransferEvent = {
  from: string;
  to: string;
  value: bigint;
};

const sub = await subscribe(
  ctx,
  async (ev) => {
    const transfer = ev.data as TransferEvent; // parsed JSON payload
    console.log("event meta:", ev.meta);
    console.log("Transfer:", transfer);
  },
  { network: "*", contract: "*", event: "*" }
);

await sub.start();
```

### Add target event

```ts
import { addTargetEventSync } from "@eventscale/sdk";

await addTargetEventSync(ctx, {
  network: "ethereum",
  name: "Transfer",
  signature: "Transfer(address,address,uint256)",
  contracts: ["0x..."]
  , need_other_logs: false,
});
```

## Examples

- `examples/subscribe.ts`
- `examples/add_target_event.ts`

Build and run locally from `pkg/sdk-ts/`:

```bash
npm install
npm run build
npm run examples:build

# Subscribe (env vars are optional, shown with defaults)
NATS_URL=nats://127.0.0.1:4222 \
ES_NETWORK=ethereum \
ES_CONTRACT=USDC \
ES_EVENT=Transfer \
node --enable-source-maps dist-examples/subscribe.js

# Add target event
NATS_URL=nats://127.0.0.1:4222 \
node --enable-source-maps dist-examples/add_target_event.js
```
