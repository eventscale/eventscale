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
	"time"

	"github.com/avelex/abidec"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/nats-io/nats.go/jetstream"
)

type TargetEvent struct {
	Name      string
	Signature string
	Contracts []common.Address
	Abi       abi.Event
}

type EventExtractor struct {
	events      map[common.Hash]*TargetEvent
	contracts   map[common.Address]struct{}
	chainClient *BlockchainClient
	pub         jetstream.Publisher
}

func NewEventExtractor(chainClient *BlockchainClient, pub jetstream.Publisher, target []*TargetEvent) *EventExtractor {
	uniqueContracts := make(map[common.Address]struct{})
	for _, e := range target {
		for _, c := range e.Contracts {
			uniqueContracts[c] = struct{}{}
		}
	}

	uniqueEvents := make(map[common.Hash]*TargetEvent)
	for _, e := range target {
		uniqueEvents[e.Abi.ID] = e
	}

	return &EventExtractor{
		events:      uniqueEvents,
		contracts:   uniqueContracts,
		chainClient: chainClient,
		pub:         pub,
	}
}

func (e *EventExtractor) TargetSubject() string {
	return SYSTEM_EVENT_EXTRACTOR_SUBJECT + "." + e.chainClient.Name()
}

func (e *EventExtractor) HandleMsg(ctx context.Context, msg jetstream.Msg) error {
	var br BlocksRange
	if err := json.Unmarshal(msg.Data(), &br); err != nil {
		return fmt.Errorf("failed to unmarshal blocks range: %w", err)
	}

	logs, err := e.chainClient.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(br.Start)),
		ToBlock:   big.NewInt(int64(br.End)),
		Addresses: e.getTargetContracts(),
		Topics:    [][]common.Hash{e.getTopics()},
	})
	if err != nil {
		return fmt.Errorf("failed to get logs: %w", err)
	}

	events, err := e.parseLogs(logs)
	if err != nil {
		return fmt.Errorf("failed to parse logs: %w", err)
	}

	if err := e.publishEvents(events); err != nil {
		return fmt.Errorf("failed to publish events: %w", err)
	}

	return nil
}

func (ext *EventExtractor) publishEvents(events []Event) error {
	bytes, err := json.Marshal(&events)
	if err != nil {
		return fmt.Errorf("failed to marshal events: %w", err)
	}
	if _, err := ext.pub.PublishAsync(ext.TargetSubject(), bytes); err != nil {
		return fmt.Errorf("failed to publish events: %w", err)
	}
	return nil
}

func (ext *EventExtractor) getTargetContracts() []common.Address {
	contracts := make([]common.Address, 0, len(ext.contracts))
	for c := range ext.contracts {
		contracts = append(contracts, c)
	}
	return contracts
}

func (ext *EventExtractor) getTopics() []common.Hash {
	topics := make([]common.Hash, 0, len(ext.events))
	for hash := range ext.events {
		topics = append(topics, hash)
	}
	return topics
}

func (ext *EventExtractor) parseLogs(logs []types.Log) ([]Event, error) {
	events := make([]Event, 0, len(logs))
	for _, log := range logs {
		view, ok := ext.events[log.Topics[0]]
		if !ok {
			continue
		}

		data := make(map[string]any)
		if err := abidec.ParseLogIntoMap(view.Abi, data, &log); err != nil {
			return nil, fmt.Errorf("failed to parse log: %w", err)
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data: %w", err)
		}

		events = append(events, Event{
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
		})
	}
	return events, nil
}
