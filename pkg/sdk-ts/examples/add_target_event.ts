import {
  websocketConnect,
  addTargetEvent,
  addTargetEventAsync,
  TargetEvent,
} from "@eventscale/sdk";

async function main() {
  const url = process.env.NATS_URL ?? "ws://127.0.0.1:8080";

  const evt: TargetEvent = {
    network: process.env.NETWORK ?? "ethereum", // required
    name: process.env.EVENT_NAME ?? "Transfer",
    signature: process.env.EVENT_SIG ?? "event Transfer(address indexed from, address indexed to, uint256 value)",
    contracts: ["0x6B175474E89094C44Da98b954EedeAC495271d0F"],
    need_other_logs: false,
  };

  const ctx = await websocketConnect(url, async (ev) => {}, {
    network: evt.network,
    contract: evt.contracts[0],
    event: evt.name,
  });

  addTargetEventAsync(ctx.nc, evt);
  console.log("Target event added");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
