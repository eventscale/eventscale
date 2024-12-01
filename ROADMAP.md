# ROADMAP

## v0.1 - Proof-of-Concept (We're Here!)

- [x] Define Eventscale architecture
- [x] Design Eventscale configuration
- [x] Design Blockchain Event structure
- [x] Embed `NATS-Server` into Eventscale
- [x] Listens for new blocks from EVM Blockchain using `go-ethereum`
- [x] Extracts events from blockchain using `go-ethereum`
- [x] Publishes events to NATS subjects, which are defined in configuration 

## v0.2

- [] Docker support
- [] Project structure redesign for modularity and SDK

## v0.3

- [] Add authorization using `NATS Accounts model`