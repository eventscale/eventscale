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
	"fmt"
	"time"

	"github.com/avelex/abidec"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/nats-io/nats.go/jetstream"
)

type NetRunnerConfig struct {
	NetConf NetworkConfig
	Events  []EventConfig
}

func (c NetRunnerConfig) GetTargetEvents() ([]*TargetEvent, error) {
	targetEvents := make([]*TargetEvent, 0, len(c.Events))
	for _, e := range c.Events {
		contracts, ok := e.Networks[c.NetConf.Name]
		if !ok {
			continue
		}

		abiEv, err := abidec.ParseEventSignature(e.Signature)
		if err != nil {
			return nil, fmt.Errorf("failed to parse event signature: %w", err)
		}

		contractsSlice := make([]common.Address, 0, len(contracts))
		for c := range contracts {
			contractsSlice = append(contractsSlice, c)
		}

		targetEvents = append(targetEvents, &TargetEvent{
			Name:      e.Name,
			Signature: e.Signature,
			Contracts: contractsSlice,
			Abi:       abiEv,
		})
	}
	return targetEvents, nil
}

type NetRunner struct {
	chainClient    *BlockchainClient
	blockListener  *BlockListener
	eventExtractor *EventExtractor
}

func InitNetRunner(ctx Context, conf NetRunnerConfig) (*NetRunner, error) {
	ethClient, err := ethclient.Dial(conf.NetConf.RPC)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", conf.NetConf.Name, err)
	}

	targets, err := conf.GetTargetEvents()
	if err != nil {
		return nil, fmt.Errorf("failed to get target events: %w", err)
	}

	chainClient := NewBlockchainClient(conf.NetConf.Name, ethClient)
	blockListener := NewBlockListener(conf.NetConf.BlocksProcessing, chainClient, ctx.JetStream)
	eventExtractor := NewEventExtractor(chainClient, ctx.JetStream, targets)

	return &NetRunner{
		chainClient:    chainClient,
		blockListener:  blockListener,
		eventExtractor: eventExtractor,
	}, nil
}

func (n *NetRunner) Register(ctx Context) error {
	blockCons, err := ctx.JetStream.CreateOrUpdateConsumer(ctx, STREAM_NAME, jetstream.ConsumerConfig{
		Name:          "new-blocks-" + n.chainClient.Name(),
		AckWait:       10 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: NetwrokNewBlocksSubject(n.chainClient.Name()),
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer new-blocks: %w", err)
	}

	if _, err := blockCons.Consume(messageHandlerWrapper(n.eventExtractor)); err != nil {
		return fmt.Errorf("failed to consume new-blocks: %w", err)
	}

	return nil
}

func (n *NetRunner) Start(ctx context.Context) error {
	return n.blockListener.Listen(ctx)
}

func messageHandlerWrapper(handler JetStreamHandler) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		// TODO: use context with timeout < AckWait
		if err := handler.HandleMsg(context.Background(), msg); err != nil {
			msg.Nak()
			return
		}
		msg.Ack()
	}
}
