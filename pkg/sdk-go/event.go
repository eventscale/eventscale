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

	ackWait time.Duration
}

type EventOpt func(e *EventSubscriber)

func WithNetwork(network string) EventOpt {
	return func(e *EventSubscriber) {
		e.network = network
	}
}

func WithContract(contract string) EventOpt {
	return func(e *EventSubscriber) {
		e.contract = contract
	}
}

func WithEvent(event string) EventOpt {
	return func(e *EventSubscriber) {
		e.event = event
	}
}

func WithAckWait(ackWait time.Duration) EventOpt {
	return func(e *EventSubscriber) {
		e.ackWait = ackWait
	}
}

func SubscribeEvent(ctx *Context, handler EventHandlerFunc, opts ...EventOpt) (*EventSubscriber, error) {
	stream, err := ctx.JetStream.Stream(ctx, internal.STREAM_NAME)
	if err != nil {
		return nil, err
	}

	sub := &EventSubscriber{
		network:  ANY_TOKEN,
		contract: ANY_TOKEN,
		event:    ANY_TOKEN,
		ackWait:  30 * time.Second,
	}

	for _, opt := range opts {
		opt(sub)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckWait:       sub.ackWait,
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

func EventHandlerWrapper(handler EventHandlerFunc) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		var event internal.Event
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			msg.Nak()
			return
		}

		if err := handler(context.TODO(), Event{event, msg.Subject()}); err != nil {
			msg.Nak()
			return
		}

		msg.Ack()
	}
}
