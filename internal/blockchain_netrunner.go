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
	blockProducer  *BlockProducer
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
	blockListener := NewBlockListener(conf.NetConf.BlocksProcessing, chainClient, ctx.Logger, ctx.JetStream)
	eventExtractor := NewEventExtractor(chainClient, ctx.Logger, ctx.JetStream, targets)
	blockProducer := NewBlockProducer(ctx.Logger, chainClient, ctx.JetStream, targets)

	return &NetRunner{
		chainClient:    chainClient,
		blockListener:  blockListener,
		eventExtractor: eventExtractor,
		blockProducer:  blockProducer,
	}, nil
}

func (n *NetRunner) Register(ctx Context) error {
	blockConsName := "new-blocks-" + n.chainClient.Name()
	blockCons, err := ctx.JetStream.CreateOrUpdateConsumer(ctx, STREAM_NAME, jetstream.ConsumerConfig{
		Name:          blockConsName,
		Durable:       blockConsName,
		AckWait:       30 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: NetworkNewBlocksSubject(n.chainClient.Name()),
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer new-blocks: %w", err)
	}

	if _, err := blockCons.Consume(
		messageHandlerWrapper(n.eventExtractor.HandleBlocksRange),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			ctx.Logger.Errorf("[%s] Failed to consume new-blocks: %v", n.chainClient.Name(), err)
		}),
	); err != nil {
		return fmt.Errorf("failed to register consumer for new-blocks: %w", err)
	}

	addEventConsName := "add-event-extractor-" + n.chainClient.Name()
	addEventCons, err := ctx.JetStream.CreateOrUpdateConsumer(ctx, STREAM_NAME, jetstream.ConsumerConfig{
		Name:          addEventConsName,
		Durable:       addEventConsName,
		AckWait:       10 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: NetworkAddEventExtractorSubject(n.chainClient.Name()),
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer for add-event-extractor: %w", err)
	}

	if _, err := addEventCons.Consume(
		messageHandlerWrapper(n.eventExtractor.HandleAddEventExtractor),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			ctx.Logger.Errorf("[%s] Failed to consume add-event-extractor: %v", n.chainClient.Name(), err)
		}),
	); err != nil {
		return fmt.Errorf("failed to register consumer for add-event-extractor: %w", err)
	}

	blockHeaderConsName := "blocks-" + n.chainClient.Name()
	blockHeaderCons, err := ctx.JetStream.CreateOrUpdateConsumer(ctx, STREAM_NAME, jetstream.ConsumerConfig{
		Name:          blockHeaderConsName,
		Durable:       blockHeaderConsName,
		AckWait:       30 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: NetworkBlocksSubject(n.chainClient.Name()),
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer for blocks: %w", err)
	}

	if _, err := blockHeaderCons.Consume(
		messageHandlerWrapper(n.blockProducer.HandleBlocks),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			ctx.Logger.Errorf("[%s] Failed to consume blocks: %v", n.chainClient.Name(), err)
		}),
	); err != nil {
		return fmt.Errorf("failed to register consumer for blocks: %w", err)
	}

	return nil
}

func (n *NetRunner) Start(ctx context.Context) error {
	return n.blockListener.Listen(ctx)
}

func messageHandlerWrapper(handler JetStreamHandlerFunc) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		if err := handler(ctx, msg); err != nil {
			msg.Ack()
			return
		}
		msg.Ack()
	}
}
