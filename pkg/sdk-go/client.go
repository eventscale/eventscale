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
	"time"

	"github.com/eventscale/eventscale/internal"
	"github.com/nats-io/nats.go/jetstream"
)

const ANY_TOKEN = "*"

type Event struct {
	internal.Event
	Topic string
}

func (e Event) Decode(v any) error {
	return json.Unmarshal(e.Data, v)
}

type EventHandlerFunc func(ctx context.Context, event Event) error

type EventSubscriber struct {
	network  string
	contract string
	event    string

	consumer jetstream.Consumer
	ctx      jetstream.ConsumeContext
}

type Opt func(e *EventSubscriber)

func WithNetwork(network string) Opt {
	return func(e *EventSubscriber) {
		e.network = network
	}
}

func WithContract(contract string) Opt {
	return func(e *EventSubscriber) {
		e.contract = contract
	}
}

func WithEvent(event string) Opt {
	return func(e *EventSubscriber) {
		e.event = event
	}
}

func Subscribe(ctx *Context, handler EventHandlerFunc, opts ...Opt) (*EventSubscriber, error) {
	stream, err := ctx.JetStream.Stream(ctx, internal.STREAM_NAME)
	if err != nil {
		return nil, err
	}

	sub := &EventSubscriber{
		network:  ANY_TOKEN,
		contract: ANY_TOKEN,
		event:    ANY_TOKEN,
	}

	for _, opt := range opts {
		opt(sub)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckWait:       30 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: sub.TartgetSubject(),
	})
	if err != nil {
		return nil, err
	}

	consCtx, err := cons.Consume(EventHandlerWrapper(handler))
	if err != nil {
		return nil, err
	}

	sub.consumer = cons
	sub.ctx = consCtx

	return sub, nil
}

func (e *EventSubscriber) Start(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-e.ctx.Closed():
	}

	e.ctx.Stop()
}

func (e *EventSubscriber) TartgetSubject() string {
	return internal.EventSubject(e.network, e.contract, e.event)
}
