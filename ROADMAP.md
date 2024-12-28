# ROADMAP

## v0.1 - Proof-of-Concept

- [x] Define Eventscale architecture
- [x] Design Eventscale configuration
- [x] Design Blockchain Event structure
- [x] Embed `nats-server` into Eventscale
- [x] Listens for new blocks from EVM Blockchain using `go-ethereum`
- [x] Extracts events from blockchain using `go-ethereum`
- [x] Publishes events to NATS subjects, which are defined in configuration 

## v0.2 (We're Here!)

- [x] SDK for Go
- [x] Examples for Go
- [x] Docker support

## v0.3

- [ ] Authorization using `NATS Accounts Model`
- [ ] CRUD for accounts

## v0.4

- [ ] Dynamic configuration for `Block Listener` via `NATS KV`
- [ ] Dynamic configuration for `Event Extractor` via `NATS KV`
- [ ] Dynamic configuration for `Event Producer` via `NATS KV` 
- [ ] Improved `Block Listener` block processing, possible to process new blocks and old blocks at the same time

## v0.5

- [ ] SDK for JS/TS
- [ ] JS/TS Examples

## v0.6

- [ ] Filtering Options for event subscription, a.k.a Event Handler

## v0.7

- [ ] Project structure redesign for modularity and SDK
