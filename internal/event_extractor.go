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
	"math/big"
	"sync"
	"time"

	"github.com/avelex/abidec"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lmittmann/w3/module/eth"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

type TargetEvent struct {
	Name          string           `json:"name"`
	Signature     string           `json:"signature"`
	Contracts     []common.Address `json:"contracts"`
	NeedOtherLogs bool             `json:"need_other_logs"`

	Abi abi.Event `json:"-"`
}

type EventExtractor struct {
	logger      server.Logger
	chainClient *BlockchainClient
	pub         jetstream.Publisher

	lock      *sync.RWMutex
	events    map[common.Hash]*TargetEvent
	contracts map[common.Address]struct{}
}

func NewEventExtractor(chainClient *BlockchainClient, log server.Logger, pub jetstream.Publisher, target []*TargetEvent) *EventExtractor {
	uniqueContracts := make(map[common.Address]struct{})
	for _, e := range target {
		for _, c := range e.Contracts {
			uniqueContracts[c] = struct{}{}
		}
	}

	uniqueEvents := make(map[common.Hash]*TargetEvent)
	for _, e := range target {
		e.NeedOtherLogs = true
		uniqueEvents[e.Abi.ID] = e
	}

	return &EventExtractor{
		logger:      log,
		events:      uniqueEvents,
		contracts:   uniqueContracts,
		chainClient: chainClient,
		pub:         pub,
		lock:        &sync.RWMutex{},
	}
}

func (e *EventExtractor) TargetSubject() string {
	return SYSTEM_EVENT_EXTRACTOR_SUBJECT + "." + e.chainClient.Name()
}

func (e *EventExtractor) HandleAddEventExtractor(ctx context.Context, msg jetstream.Msg) error {
	var event TargetEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	e.logger.Debugf("[%s] [EventExtractor] Adding event: %s", e.chainClient.Name(), event.Name)

	abiEv, err := abidec.ParseEventSignature(event.Signature)
	if err != nil {
		e.logger.Errorf("[%s] [EventExtractor] Failed to parse event signature: %v", e.chainClient.Name(), err)
		return fmt.Errorf("failed to parse event signature: %w", err)
	}

	event.Abi = abiEv

	e.lock.Lock()
	for _, c := range event.Contracts {
		e.contracts[c] = struct{}{}
	}

	e.events[event.Abi.ID] = &event
	e.lock.Unlock()

	return nil
}

func (e *EventExtractor) HandleBlocksRange(ctx context.Context, msg jetstream.Msg) error {
	var br BlocksRange
	if err := json.Unmarshal(msg.Data(), &br); err != nil {
		e.logger.Errorf("[%s] [EventExtractor] Failed to unmarshal blocks range: %v", e.chainClient.Name(), err)

		return fmt.Errorf("failed to unmarshal blocks range: %w", err)
	}

	var logs []types.Log

	if err := e.chainClient.CallCtx(ctx, eth.Logs(ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(br.Start)),
		ToBlock:   big.NewInt(int64(br.End)),
		Addresses: e.getTargetContracts(),
		Topics:    [][]common.Hash{e.getTopics()},
	}).Returns(&logs)); err != nil {
		e.logger.Errorf("[%s] [EventExtractor] Failed to get logs: %v", e.chainClient.Name(), err)

		return fmt.Errorf("failed to get logs: %w", err)
	}

	e.logger.Tracef("[%s] [EventExtractor] Found %d logs", e.chainClient.Name(), len(logs))

	if err := e.processLogs(ctx, logs); err != nil {
		e.logger.Errorf("[%s] [EventExtractor] Failed to process logs: %v", e.chainClient.Name(), err)

		return fmt.Errorf("failed to process logs: %w", err)
	}

	e.logger.Debugf("[%s] [EventExtractor] Processed blocks range: %d - %d", e.chainClient.Name(), br.Start, br.End)

	return nil
}

func (ext *EventExtractor) publishEvents(ctx context.Context, events []Event) error {
	bytes, err := json.Marshal(&events)
	if err != nil {
		return fmt.Errorf("failed to marshal events: %w", err)
	}

	if _, err := ext.pub.Publish(ctx, ext.TargetSubject(), bytes); err != nil {
		return fmt.Errorf("failed to publish events: %w", err)
	}

	return nil
}

func (ext *EventExtractor) getTargetContracts() []common.Address {
	ext.lock.RLock()
	defer ext.lock.RUnlock()

	contracts := make([]common.Address, 0, len(ext.contracts))
	for c := range ext.contracts {
		contracts = append(contracts, c)
	}

	return contracts
}

func (ext *EventExtractor) getTopics() []common.Hash {
	ext.lock.RLock()
	defer ext.lock.RUnlock()

	topics := make([]common.Hash, 0, len(ext.events))
	for hash := range ext.events {
		topics = append(topics, hash)
	}

	return topics
}

func (ext *EventExtractor) processLogs(ctx context.Context, logs []types.Log) error {
	txs := make(map[common.Hash]*types.Receipt)

	for _, log := range logs {
		ext.lock.RLock()
		view, ok := ext.events[log.Topics[0]]
		ext.lock.RUnlock()

		if !ok {
			continue
		}

		data := make(map[string]any)
		if err := abidec.ParseLogIntoMap(view.Abi, data, &log); err != nil {
			ext.logger.Errorf("[%s] [EventExtractor] Failed to parse log: %v", ext.chainClient.Name(), err)
			continue
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			ext.logger.Errorf("[%s] [EventExtractor] Failed to marshal data: %v", ext.chainClient.Name(), err)
			continue
		}

		ev := Event{
			MetaData: EventMetadata{
				Network:     ext.chainClient.Name(),
				ChainID:     ext.chainClient.ChainID(),
				Contract:    log.Address,
				Name:        view.Name,
				Signature:   view.Signature,
				BlockNumber: log.BlockNumber,
				BlockHash:   log.BlockHash,
				TxHash:      log.TxHash,
				TxIndex:     log.TxIndex,
				LogIndex:    log.Index,
				Timestamp:   time.Now().Unix(),
			},
			Data: bytes,
		}

		if view.NeedOtherLogs {
			if receipt, ok := txs[log.TxHash]; ok {
				ev.MetaData.OtherLogs = receipt.Logs
			} else {
				var txReceipt *types.Receipt
				if err := ext.chainClient.CallCtx(ctx, eth.TxReceipt(log.TxHash).Returns(&txReceipt)); err != nil {
					ext.logger.Errorf("[%s] [EventExtractor] Failed to get transaction receipt: %v", ext.chainClient.Name(), err)
					continue
				}

				receipt := txReceipt

				ev.MetaData.OtherLogs = receipt.Logs
				txs[log.TxHash] = receipt
			}
		} else {
			ev.MetaData.OtherLogs = make([]*types.Log, 0)
		}

		if err := ext.publishEvents(ctx, []Event{ev}); err != nil {
			ext.logger.Errorf("[%s] [EventExtractor] Failed to publish event: %v", ext.chainClient.Name(), err)
			continue
		}
	}

	return nil
}
