import { Context, addTargetEventSync, addTargetEventAsync, TargetEvent } from "@eventscale/sdk";

async function main() {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";

  const evt: TargetEvent = {
    network: process.env.ES_NETWORK ?? "ethereum", // required
    name: process.env.ES_EVENT_NAME ?? "Transfer",
    signature: process.env.ES_EVENT_SIG ?? "Transfer(address,address,uint256)",
    contracts: (process.env.ES_CONTRACTS ?? "").split(",").filter(Boolean),
    need_other_logs: (process.env.ES_NEED_OTHER_LOGS ?? "false").toLowerCase() === "true",
  };

  const ctx = await Context.connect(url);

  if ((process.env.ASYNC ?? "false").toLowerCase() === "true") {
    await addTargetEventAsync(ctx, evt);
    console.log("Add target event published async");
  } else {
    await addTargetEventSync(ctx, evt);
    console.log("Add target event published sync");
  }

  await ctx.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
