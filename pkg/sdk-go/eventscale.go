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

	"github.com/ethereum/go-ethereum/common"
	"github.com/eventscale/eventscale/internal"
	"github.com/eventscale/eventscale/internal/subjects"
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

type TargetEvent struct {
	Network       string           `json:"-"`
	Name          string           `json:"name"`
	Signature     string           `json:"signature"`
	Contracts     []common.Address `json:"contracts"`
	NeedOtherLogs bool             `json:"need_other_logs"`
}

func (c *Context) AddTargetEventSync(ctx context.Context, event TargetEvent) error {
	subj := subjects.NetworkAddEventExtractor(event.Network)

	bytes, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := c.Publish(ctx, subj, bytes); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

func (c *Context) AddTargetEventAsync(ctx context.Context, event TargetEvent) error {
	subj := subjects.NetworkAddEventExtractor(event.Network)

	bytes, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := c.PublishAsync(subj, bytes); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}
