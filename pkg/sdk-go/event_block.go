package eventscale

import (
	"context"
	"encoding/json"
	"time"

	"github.com/eventscale/eventscale/internal/subjects"
	"github.com/eventscale/eventscale/internal/types"
	"github.com/nats-io/nats.go/jetstream"
)

type EventBlock struct {
	types.EventBlock
	Topic string
}

type EventBlockHandlerFunc func(ctx context.Context, event EventBlock) error

type EventBlockSubscriber struct {
	network string
	name    string

	consumer jetstream.Consumer
	ctx      jetstream.ConsumeContext

	ackWait time.Duration
}

type EventBlockOpt func(e *EventBlockSubscriber)

func WithEventBlockNetwork(network string) EventBlockOpt {
	return func(e *EventBlockSubscriber) {
		e.network = network
	}
}

func WithEventBlockAckWait(ackWait time.Duration) EventBlockOpt {
	return func(e *EventBlockSubscriber) {
		e.ackWait = ackWait
	}
}

func WithEventBlockConsumerName(name string) EventBlockOpt {
	return func(e *EventBlockSubscriber) {
		e.name = name
	}
}

func SubscribeEventBlock(ctx *Context, handler EventBlockHandlerFunc, opts ...EventBlockOpt) (*EventBlockSubscriber, error) {
	stream, err := ctx.JetStream.Stream(ctx, subjects.STREAM_NAME)
	if err != nil {
		return nil, err
	}

	sub := &EventBlockSubscriber{
		network: ANY_TOKEN,
		name:    ANY_TOKEN,
		ackWait: 1 * time.Minute,
	}

	for _, opt := range opts {
		opt(sub)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          sub.name,
		AckWait:       sub.ackWait,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverLastPolicy,
		FilterSubject: sub.TartgetSubject(),
	})
	if err != nil {
		return nil, err
	}

	consCtx, err := cons.Consume(EventBlockHandlerWrapper(handler))
	if err != nil {
		return nil, err
	}

	sub.consumer = cons
	sub.ctx = consCtx

	return sub, nil
}

func (e *EventBlockSubscriber) Start(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-e.ctx.Closed():
	}

	e.ctx.Stop()
}

func (e *EventBlockSubscriber) TartgetSubject() string {
	return subjects.EventsBlocks(e.network, ANY_TOKEN)
}

func EventBlockHandlerWrapper(handler EventBlockHandlerFunc) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		var block types.EventBlock
		if err := json.Unmarshal(msg.Data(), &block); err != nil {
			msg.Nak()
			return
		}

		if err := handler(context.TODO(), EventBlock{block, msg.Subject()}); err != nil {
			msg.Nak()
			return
		}

		msg.Ack()
	}
}
