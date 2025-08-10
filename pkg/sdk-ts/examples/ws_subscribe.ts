import { websocketConnect } from "@eventscale/sdk";

export interface TransferEvent {
  from: string;
  to: string;
  value: bigint;
}

async function main() {
  const url = process.env.NATS_URL ?? "ws://127.0.0.1:8080";
  const network = process.env.ES_NETWORK ?? "ethereum"; // e.g. "ethereum"
  const contract = process.env.ES_CONTRACT ?? "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"; // e.g. "0xabc..." or alias
  const event = process.env.ES_EVENT ?? "Transfer"; // e.g. "Transfer"

  const sub = await websocketConnect(
    url,
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
