import { Context, subscribe } from "@eventscale/sdk";

export interface TransferEvent {
  from: string;
  to: string;
  value: bigint;
}

async function main() {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  const network = process.env.ES_NETWORK ?? "ethereum"; // e.g. "ethereum"
  const contract = process.env.ES_CONTRACT ?? "USDC"; // e.g. "0xabc..." or alias
  const event = process.env.ES_EVENT ?? "Transfer"; // e.g. "Transfer"

  const ctx = await Context.connect(url);

  const sub = await subscribe(
    ctx,
    async (ev) => {
      const transfer = ev.data as TransferEvent;
      console.log("Transfer", transfer);
    },
    { network: network, contract: contract, event: event }
  );

  await sub.start();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
