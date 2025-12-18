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

package eventscale

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eventscale/eventscale/internal/subjects"
	"github.com/eventscale/eventscale/internal/types"
	"github.com/nats-io/nats.go/jetstream"
)

type Block struct {
	types.Block
	Topic string
}

type BlockHandlerFunc func(ctx context.Context, block Block) error

type BlockSubscriber struct {
	network  string
	name     string
	consumer jetstream.Consumer
	ctx      jetstream.ConsumeContext

	ackWait time.Duration
}

type BlockOpt func(e *BlockSubscriber)

func WithBlockNetwork(network string) BlockOpt {
	return func(e *BlockSubscriber) {
		e.network = network
	}
}

func WithBlockAckWait(ackWait time.Duration) BlockOpt {
	return func(e *BlockSubscriber) {
		e.ackWait = ackWait
	}
}

func WithBlockConsumerName(name string) BlockOpt {
	return func(e *BlockSubscriber) {
		e.name = name
	}
}

func SubscribeBlock(ctx *Context, handler BlockHandlerFunc, opts ...BlockOpt) (*BlockSubscriber, error) {
	stream, err := ctx.JetStream.Stream(ctx, subjects.STREAM_NAME)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream: %w", err)
	}

	sub := &BlockSubscriber{
		network: ANY_TOKEN,
		ackWait: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(sub)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckWait:       sub.ackWait,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverLastPolicy,
		FilterSubject: sub.TartgetSubject(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	consCtx, err := cons.Consume(BlockHandlerWrapper(handler))
	if err != nil {
		return nil, fmt.Errorf("failed to consume: %w", err)
	}

	sub.consumer = cons
	sub.ctx = consCtx

	return sub, nil
}

func (e *BlockSubscriber) Start(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-e.ctx.Closed():
	}

	e.ctx.Stop()
}

func (e *BlockSubscriber) TartgetSubject() string {
	return subjects.Blocks(e.network, ANY_TOKEN)
}

func BlockHandlerWrapper(handler BlockHandlerFunc) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		var block types.Block
		if err := json.Unmarshal(msg.Data(), &block); err != nil {
			msg.Nak()
			return
		}

		if err := handler(context.TODO(), Block{block, msg.Subject()}); err != nil {
			msg.Nak()
			return
		}

		msg.Ack()
	}
}
