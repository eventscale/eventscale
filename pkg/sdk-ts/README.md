# Eventscale TypeScript SDK (sdk-ts)

TypeScript SDK mirroring `pkg/sdk-go` using NATS JetStream (`nats` package).

## Install

From `pkg/sdk-ts/`:

```
npm install
npm run build
npm run examples:build
```

## Usage

### Connect

```ts
import { Context } from "./dist/nats.js";

const ctx = await Context.connect("nats://127.0.0.1:4222");
```

### Subscribe to events

```ts
import { subscribe } from "./dist/subscribe.js";

const sub = await subscribe(ctx, async (ev) => {
  console.log(ev.meta);
  const payload = ev.decodeJson<any>();
  console.log(payload);
}, { network: "*", contract: "*", event: "*" });

await sub.start(async () => {});
```

### Add target event

```ts
import { addTargetEventSync } from "./dist/targets.js";

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

Run after building examples:

```
NATS_URL=nats://127.0.0.1:4222 \
node --enable-source-maps dist-examples/subscribe.js
```
