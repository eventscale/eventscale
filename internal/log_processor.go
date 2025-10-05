// Copyright 2024 The Eventscale Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/avelex/abidec"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lmittmann/w3/module/eth"
	"github.com/nats-io/nats-server/v2/server"
)

// LogProcessor handles common log processing logic shared between EventExtractor and BlockExtractor
type LogProcessor struct {
	logger      server.Logger
	chainClient *BlockchainClient
	lock        *sync.RWMutex
	events      map[common.Hash]*TargetEvent
	contracts   map[common.Address]struct{}
}

// NewLogProcessor creates a new LogProcessor instance
func NewLogProcessor(logger server.Logger, chainClient *BlockchainClient, targets []*TargetEvent) *LogProcessor {
	uniqueContracts := make(map[common.Address]struct{})
	uniqueEvents := make(map[common.Hash]*TargetEvent)

	for _, e := range targets {
		uniqueEvents[e.Abi.ID] = e
		for _, c := range e.Contracts {
			uniqueContracts[c] = struct{}{}
		}
	}

	return &LogProcessor{
		logger:      logger,
		chainClient: chainClient,
		lock:        &sync.RWMutex{},
		events:      uniqueEvents,
		contracts:   uniqueContracts,
	}
}

// GetTargetContracts returns a copy of target contracts
func (lp *LogProcessor) GetTargetContracts() []common.Address {
	lp.lock.RLock()
	defer lp.lock.RUnlock()

	contracts := make([]common.Address, 0, len(lp.contracts))
	for c := range lp.contracts {
		contracts = append(contracts, c)
	}

	return contracts
}

// GetTopics returns a copy of event topics
func (lp *LogProcessor) GetTopics() []common.Hash {
	lp.lock.RLock()
	defer lp.lock.RUnlock()

	topics := make([]common.Hash, 0, len(lp.events))
	for hash := range lp.events {
		topics = append(topics, hash)
	}

	return topics
}

// AddEvent adds a new event to track
func (lp *LogProcessor) AddEvent(event *TargetEvent) {
	lp.lock.Lock()
	defer lp.lock.Unlock()

	for _, c := range event.Contracts {
		lp.contracts[c] = struct{}{}
	}
	lp.events[event.Abi.ID] = event
}

// ProcessLogsToEvents converts blockchain logs to Event structures
func (lp *LogProcessor) ProcessLogsToEvents(ctx context.Context, logs []types.Log, timestamp int64) ([]Event, error) {
	txs := make(map[common.Hash]*types.Receipt)
	events := make([]Event, 0, len(logs))

	for _, log := range logs {
		lp.lock.RLock()
		targetEvent, ok := lp.events[log.Topics[0]]
		lp.lock.RUnlock()

		if !ok {
			continue
		}

		data := make(map[string]any)
		if err := abidec.ParseLogIntoMap(targetEvent.Abi, data, &log); err != nil {
			lp.logger.Errorf("[%s] [LogProcessor] Failed to parse log: %v", lp.chainClient.Name(), err)
			continue
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			lp.logger.Errorf("[%s] [LogProcessor] Failed to marshal data: %v", lp.chainClient.Name(), err)
			continue
		}

		ev := Event{
			MetaData: EventMetadata{
				Network:     lp.chainClient.Name(),
				ChainID:     lp.chainClient.ChainID(),
				Contract:    log.Address,
				Name:        targetEvent.Name,
				Signature:   targetEvent.Signature,
				BlockNumber: log.BlockNumber,
				BlockHash:   log.BlockHash,
				TxHash:      log.TxHash,
				TxIndex:     log.TxIndex,
				LogIndex:    log.Index,
				Timestamp:   timestamp,
			},
			Data: bytes,
		}

		if targetEvent.NeedOtherLogs {
			receipt, err := lp.getOrFetchReceipt(ctx, log.TxHash, txs)
			if err != nil {
				lp.logger.Errorf("[%s] [LogProcessor] Failed to get transaction receipt: %v", lp.chainClient.Name(), err)
				continue
			}
			ev.MetaData.OtherLogs = receipt.Logs
		} else {
			ev.MetaData.OtherLogs = make([]*types.Log, 0)
		}

		events = append(events, ev)
	}

	return events, nil
}

// getOrFetchReceipt retrieves receipt from cache or fetches from blockchain
func (lp *LogProcessor) getOrFetchReceipt(ctx context.Context, txHash common.Hash, cache map[common.Hash]*types.Receipt) (*types.Receipt, error) {
	if receipt, ok := cache[txHash]; ok {
		return receipt, nil
	}

	var txReceipt *types.Receipt
	if err := lp.chainClient.CallCtx(ctx, eth.TxReceipt(txHash).Returns(&txReceipt)); err != nil {
		return nil, fmt.Errorf("failed to get transaction receipt: %w", err)
	}

	cache[txHash] = txReceipt
	return txReceipt, nil
}
