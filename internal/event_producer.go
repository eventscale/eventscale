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
	"maps"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/eventscale/eventscale/internal/types"
	"github.com/nats-io/nats.go/jetstream"
)

type EventProducer struct {
	networks map[BlockchainName]map[common.Address]string
	pub      jetstream.Publisher
}

func NewEventProducer(configs []EventConfig, pub jetstream.Publisher) *EventProducer {
	allNetworks := make(map[BlockchainName]map[common.Address]string)
	for _, config := range configs {
		for net, contracts := range config.Networks {
			if _, ok := allNetworks[net]; !ok {
				allNetworks[net] = make(map[common.Address]string)
			}

			maps.Copy(allNetworks[net], contracts)
		}
	}

	return &EventProducer{
		networks: allNetworks,
		pub:      pub,
	}
}

func (p *EventProducer) HandleEvents(ctx context.Context, msg jetstream.Msg) error {
	events := make([]types.Event, 0)

	if err := json.Unmarshal(msg.Data(), &events); err != nil {
		return fmt.Errorf("failed to unmarshal events: %w", err)
	}

	blocksOrders := make(map[BlockchainName][]uint64)
	blocks := make(map[BlockchainName]map[uint64][]types.Event)

	// process events like sequence
	for _, event := range events {
		blockchain := BlockchainName(event.MetaData.Network)

		if _, ok := blocks[blockchain]; !ok {
			blocks[blockchain] = make(map[uint64][]types.Event)
		}
		if _, ok := blocks[blockchain][event.MetaData.BlockNumber]; !ok {
			blocksOrders[blockchain] = append(blocksOrders[blockchain], event.MetaData.BlockNumber)
		}

		blocks[blockchain][event.MetaData.BlockNumber] = append(blocks[blockchain][event.MetaData.BlockNumber], event)

		bytes, err := json.Marshal(&event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		contractAlias := make([]string, 0)
		if name, ok := p.networks[blockchain][event.MetaData.Contract]; ok {
			contractAlias = append(contractAlias, name)
		}

		subject := event.TargetSubject(contractAlias...)

		if _, err := p.pub.Publish(ctx, subject, bytes); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}
	}

	// process events by blocks
	for network, blocksOrder := range blocksOrders {
		slices.Sort(blocksOrder)

		for _, blockNumber := range blocksOrder {
			blockEvents := blocks[network][blockNumber]

			slices.SortFunc(blockEvents, func(a, b types.Event) int {
				if a.MetaData.LogIndex < b.MetaData.LogIndex {
					return -1
				}
				if a.MetaData.LogIndex > b.MetaData.LogIndex {
					return 1
				}
				return 0
			})

			meta := blockEvents[0].MetaData
			block := types.NewEventBlock(blockNumber, meta.Network, meta.ChainID, blockEvents)

			bytes, err := json.Marshal(block)
			if err != nil {
				return fmt.Errorf("failed to marshal event block: %w", err)
			}

			if _, err := p.pub.Publish(ctx, block.TargetSubject(), bytes); err != nil {
				return fmt.Errorf("failed to publish event block: %w", err)
			}
		}
	}

	return nil
}
