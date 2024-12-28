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

	"github.com/ethereum/go-ethereum/common"
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
			for contract, name := range contracts {
				allNetworks[net][contract] = name
			}
		}
	}

	return &EventProducer{
		networks: allNetworks,
		pub:      pub,
	}
}

func (p *EventProducer) HandleMsg(ctx context.Context, msg jetstream.Msg) error {
	events := make([]Event, 0)

	if err := json.Unmarshal(msg.Data(), &events); err != nil {
		return fmt.Errorf("failed to unmarshal events: %w", err)
	}

	for _, event := range events {
		bytes, err := json.Marshal(&event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		contractAlias := make([]string, 0)
		if name, ok := p.networks[BlockchainName(event.MetaData.Network)][event.MetaData.Contract]; ok {
			contractAlias = append(contractAlias, name)
		}

		subject := event.TargetSubject(contractAlias...)

		if _, err := p.pub.PublishAsync(subject, bytes); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}
	}

	return nil
}
