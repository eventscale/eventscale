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

	"github.com/eventscale/eventscale/internal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Context struct {
	*internal.Context
}

func Connect(ctx context.Context, url string) (*Context, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	return &Context{
		Context: &internal.Context{
			Context:   ctx,
			JetStream: js,
		},
	}, nil
}

func EventHandlerWrapper(handler EventHandlerFunc) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		var event internal.Event
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			msg.Nak()
			return
		}

		if err := handler(context.TODO(), Event{event}); err != nil {
			msg.Nak()
			return
		}

		msg.Ack()
	}
}
