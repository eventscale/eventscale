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
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/eventscale/eventscale/internal/types"
	"github.com/lmittmann/w3/module/eth"
	"github.com/nats-io/nats-server/v2/server"
)

// LogProcessor handles common log processing logic shared between EventExtractor and BlockExtractor
type LogProcessor struct {
	logger server.Logger
	bc     *BlockchainClient

	lock      *sync.RWMutex
	events    map[common.Hash]*TargetEvent
	contracts map[common.Address]struct{}
}

// NewLogProcessor creates a new LogProcessor instance
func NewLogProcessor(logger server.Logger, bc *BlockchainClient, targets []*TargetEvent) *LogProcessor {
	uniqueContracts := make(map[common.Address]struct{})
	uniqueEvents := make(map[common.Hash]*TargetEvent)

	for _, e := range targets {
		uniqueEvents[e.Abi.ID] = e
		for _, c := range e.Contracts {
			uniqueContracts[c] = struct{}{}
		}
	}

	return &LogProcessor{
		logger:    logger,
		bc:        bc,
		lock:      &sync.RWMutex{},
		events:    uniqueEvents,
		contracts: uniqueContracts,
	}
}

// GetTargetContracts returns a copy of target contracts
func (l *LogProcessor) GetTargetContracts() []common.Address {
	l.lock.RLock()
	defer l.lock.RUnlock()

	contracts := make([]common.Address, 0, len(l.contracts))
	for c := range l.contracts {
		contracts = append(contracts, c)
	}

	return contracts
}

// GetTopics returns a copy of event topics
func (l *LogProcessor) GetTopics() []common.Hash {
	l.lock.RLock()
	defer l.lock.RUnlock()

	topics := make([]common.Hash, 0, len(l.events))

	for hash := range l.events {
		topics = append(topics, hash)
	}

	return topics
}

// AddEvent adds a new event to track
func (l *LogProcessor) AddEvent(event *TargetEvent) {
	l.lock.Lock()
	defer l.lock.Unlock()

	for _, c := range event.Contracts {
		l.contracts[c] = struct{}{}
	}

	l.events[event.Abi.ID] = event
}

// ProcessLogsToEvents converts blockchain logs to Event structures
func (l *LogProcessor) ProcessLogsToEvents(ctx context.Context, logs []eth_types.Log) ([]types.Event, error) {
	txs := make(map[common.Hash]*eth_types.Receipt)
	events := make([]types.Event, 0, len(logs))

	for _, log := range logs {
		l.lock.RLock()
		targetEvent, ok := l.events[log.Topics[0]]
		l.lock.RUnlock()

		if !ok {
			continue
		}

		data := make(map[string]any)
		if err := abidec.ParseLogIntoMap(targetEvent.Abi, data, &log); err != nil {
			l.logger.Errorf("[%s] [LogProcessor] Failed to parse log: %v", l.bc.Name(), err)
			continue
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			l.logger.Errorf("[%s] [LogProcessor] Failed to marshal data: %v", l.bc.Name(), err)
			continue
		}

		ev := types.Event{
			MetaData: types.EventMetadata{
				Network:     l.bc.Name(),
				ChainID:     l.bc.ChainID(),
				Contract:    log.Address,
				Name:        targetEvent.Name,
				Signature:   targetEvent.Signature,
				BlockNumber: log.BlockNumber,
				BlockHash:   log.BlockHash,
				TxHash:      log.TxHash,
				TxIndex:     log.TxIndex,
				LogIndex:    log.Index,
				Timestamp:   int64(log.BlockTimestamp),
				OtherLogs:   make([]*eth_types.Log, 0),
			},
			Data: bytes,
		}

		if targetEvent.NeedOtherLogs {
			receipt, err := l.getOrFetchReceipt(ctx, log.TxHash, txs)
			if err != nil {
				l.logger.Errorf("[%s] [LogProcessor] Failed to get transaction receipt: %v", l.bc.Name(), err)
				continue
			}

			ev.MetaData.OtherLogs = receipt.Logs
		}

		events = append(events, ev)
	}

	return events, nil
}

// getOrFetchReceipt retrieves receipt from cache or fetches from blockchain
func (l *LogProcessor) getOrFetchReceipt(ctx context.Context, txHash common.Hash, cache map[common.Hash]*eth_types.Receipt) (*eth_types.Receipt, error) {
	if receipt, ok := cache[txHash]; ok {
		return receipt, nil
	}

	var txReceipt *eth_types.Receipt
	if err := l.bc.CallCtx(ctx, eth.TxReceipt(txHash).Returns(&txReceipt)); err != nil {
		return nil, fmt.Errorf("failed to get transaction receipt: %w", err)
	}

	cache[txHash] = txReceipt

	return txReceipt, nil
}
