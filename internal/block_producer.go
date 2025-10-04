package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"

	"github.com/avelex/abidec"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lmittmann/w3/module/eth"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

type BlockProducer struct {
	logger      server.Logger
	chainClient *BlockchainClient
	pub         jetstream.Publisher

	lock      *sync.RWMutex
	events    map[common.Hash]*TargetEvent
	contracts map[common.Address]struct{}
}

func NewBlockProducer(logger server.Logger, chainClient *BlockchainClient, pub jetstream.Publisher, target []*TargetEvent) *BlockProducer {
	uniqueContracts := make(map[common.Address]struct{})
	uniqueEvents := make(map[common.Hash]*TargetEvent)

	for _, e := range target {
		e.NeedOtherLogs = true
		uniqueEvents[e.Abi.ID] = e

		for _, c := range e.Contracts {
			uniqueContracts[c] = struct{}{}
		}
	}

	return &BlockProducer{
		logger:      logger,
		chainClient: chainClient,
		pub:         pub,
		lock:        &sync.RWMutex{},
		events:      uniqueEvents,
		contracts:   uniqueContracts,
	}
}

func (p *BlockProducer) HandleBlocks(ctx context.Context, msg jetstream.Msg) error {
	var br BlocksRange
	if err := json.Unmarshal(msg.Data(), &br); err != nil {
		p.logger.Errorf("[%s] [BlockProducer] Failed to unmarshal blocks range: %v", p.chainClient.Name(), err)

		return fmt.Errorf("unmarshal blocks range: %w", err)
	}

	for blockNumber := br.Start; blockNumber <= br.End; blockNumber++ {
		var (
			header *types.Header
			logs   []types.Log
		)

		if err := p.chainClient.CallCtx(ctx,
			eth.HeaderByNumber(big.NewInt(int64(blockNumber))).Returns(&header),
			eth.Logs(ethereum.FilterQuery{
				FromBlock: big.NewInt(int64(blockNumber)),
				ToBlock:   big.NewInt(int64(blockNumber)),
				Addresses: p.getTargetContracts(),
				Topics:    [][]common.Hash{p.getTopics()},
			}).Returns(&logs),
		); err != nil {
			p.logger.Errorf("[%s] [BlockProducer] Failed to call chain client: %v", p.chainClient.Name(), err)

			return fmt.Errorf("chain client call: %w", err)
		}

		events, err := p.processLogs(ctx, header, logs)
		if err != nil {
			p.logger.Errorf("[%s] [BlockProducer] Failed to process logs: %v", p.chainClient.Name(), err)

			return fmt.Errorf("process logs: %w", err)
		}

		blockEvent := NewBlockEvent(p.chainClient.Name(), p.chainClient.ChainID(), header, events)
		payload, err := json.Marshal(blockEvent)
		if err != nil {
			p.logger.Errorf("[%s] [BlockProducer] Failed to marshal block event: %v", p.chainClient.Name(), err)

			return fmt.Errorf("marshal block event: %w", err)
		}

		if _, err := p.pub.Publish(ctx, blockEvent.TargetSubject(), payload); err != nil {
			p.logger.Errorf("[%s] [BlockProducer] Failed to publish block event: %v", p.chainClient.Name(), err)

			return fmt.Errorf("publish block event: %w", err)
		}
	}

	p.logger.Debugf("[%s] [BlockProducer] Processed blocks range: %d - %d", p.chainClient.Name(), br.Start, br.End)

	return nil
}

func (p *BlockProducer) getTargetContracts() []common.Address {
	p.lock.RLock()
	defer p.lock.RUnlock()

	contracts := make([]common.Address, 0, len(p.contracts))
	for c := range p.contracts {
		contracts = append(contracts, c)
	}

	return contracts
}

func (p *BlockProducer) getTopics() []common.Hash {
	p.lock.RLock()
	defer p.lock.RUnlock()

	topics := make([]common.Hash, 0, len(p.events))
	for hash := range p.events {
		topics = append(topics, hash)
	}

	return topics
}

func (p *BlockProducer) processLogs(ctx context.Context, header *types.Header, logs []types.Log) ([]Event, error) {
	txs := make(map[common.Hash]*types.Receipt)
	events := make([]Event, 0, len(logs))

	for _, log := range logs {
		p.lock.RLock()
		view, ok := p.events[log.Topics[0]]
		p.lock.RUnlock()

		if !ok {
			continue
		}

		data := make(map[string]any)
		if err := abidec.ParseLogIntoMap(view.Abi, data, &log); err != nil {
			p.logger.Errorf("[%s] [BlockProducer] Failed to parse log: %v", p.chainClient.Name(), err)
			continue
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			p.logger.Errorf("[%s] [BlockProducer] Failed to marshal data: %v", p.chainClient.Name(), err)
			continue
		}

		ev := Event{
			MetaData: EventMetadata{
				Network:     p.chainClient.Name(),
				ChainID:     p.chainClient.ChainID(),
				Contract:    log.Address,
				Name:        view.Name,
				Signature:   view.Signature,
				BlockNumber: header.Number.Uint64(),
				BlockHash:   header.Hash(),
				TxHash:      log.TxHash,
				TxIndex:     log.TxIndex,
				LogIndex:    log.Index,
				Timestamp:   int64(header.Time),
			},
			Data: bytes,
		}

		if view.NeedOtherLogs {
			if receipt, ok := txs[log.TxHash]; ok {
				ev.MetaData.OtherLogs = receipt.Logs
			} else {
				var txReceipt *types.Receipt
				if err := p.chainClient.CallCtx(ctx, eth.TxReceipt(log.TxHash).Returns(&txReceipt)); err != nil {
					p.logger.Errorf("[%s] [BlockProducer] Failed to get transaction receipt: %v", p.chainClient.Name(), err)
					continue
				}

				receipt := txReceipt

				ev.MetaData.OtherLogs = receipt.Logs
				txs[log.TxHash] = receipt
			}
		} else {
			ev.MetaData.OtherLogs = make([]*types.Log, 0)
		}

		events = append(events, ev)
	}

	return events, nil
}
