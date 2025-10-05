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
	"github.com/lmittmann/w3"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/time/rate"
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
	ethClient, err := w3.Dial(conf.NetConf.RPC,
		w3.WithRateLimiter(
			rate.NewLimiter(
				rate.Every(conf.NetConf.RateLimit.Duration),
				conf.NetConf.RateLimit.Limit, // burst
			),
			nil, // cost request
		),
	)
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
	newBlocksForEventCons, err := ctx.stream.CreateOrUpdatePushConsumer(ctx, jetstream.ConsumerConfig{
		Name:           "new-blocks-for-event-extractor-" + n.chainClient.Name(),
		Durable:        "new-blocks-for-event-extractor-" + n.chainClient.Name(),
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        5 * time.Minute,
		FilterSubjects: []string{NetworkNewBlocksSubject(n.chainClient.Name())},
	})
	if err != nil {
		return fmt.Errorf("create push consumer new-blocks-for-event-extractor: %w", err)
	}

	if _, err := newBlocksForEventCons.Consume(
		messageHandlerWrapper(n.eventExtractor.HandleBlocksRange),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			ctx.Logger.Errorf("[%s] Failed to consume new-blocks-for-event-extractor: %v", n.chainClient.Name(), err)
		}),
	); err != nil {
		return fmt.Errorf("register consumer for new-blocks-for-event-extractor: %w", err)
	}

	newBlocksForBlockCons, err := ctx.stream.CreateOrUpdatePushConsumer(ctx, jetstream.ConsumerConfig{
		Name:           "new-blocks-for-block-producer-" + n.chainClient.Name(),
		Durable:        "new-blocks-for-block-producer-" + n.chainClient.Name(),
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        5 * time.Minute,
		FilterSubjects: []string{NetworkNewBlocksSubject(n.chainClient.Name())},
	})
	if err != nil {
		return fmt.Errorf("create push consumer new-blocks-for-block-producer: %w", err)
	}

	if _, err := newBlocksForBlockCons.Consume(
		messageHandlerWrapper(n.blockProducer.HandleBlocks),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			ctx.Logger.Errorf("[%s] Failed to consume new-blocks-for-block-producer: %v", n.chainClient.Name(), err)
		}),
	); err != nil {
		return fmt.Errorf("register consumer for new-blocks-for-block-producer: %w", err)
	}

	addEventCons, err := ctx.stream.CreateOrUpdatePushConsumer(ctx, jetstream.ConsumerConfig{
		Name:           "add-event-extractor-" + n.chainClient.Name(),
		Durable:        "add-event-extractor-" + n.chainClient.Name(),
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        1 * time.Minute,
		FilterSubjects: []string{NetworkAddEventExtractorSubject(n.chainClient.Name())},
	})
	if err != nil {
		return fmt.Errorf("create push consumer add-event-extractor: %w", err)
	}

	if _, err := addEventCons.Consume(
		messageHandlerWrapper(n.eventExtractor.HandleAddEventExtractor),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			ctx.Logger.Errorf("[%s] Failed to consume add-event-extractor: %v", n.chainClient.Name(), err)
		}),
	); err != nil {
		return fmt.Errorf("register consumer for add-event-extractor: %w", err)
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
			msg.Nak()
			return
		}
		msg.Ack()
	}
}
