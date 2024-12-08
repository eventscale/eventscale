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
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

type BlocksRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type BlockListener struct {
	logger      server.Logger
	cfg         BlocksProcessingConfig
	chainClient *BlockchainClient
	pub         jetstream.Publisher
}

func NewBlockListener(cfg BlocksProcessingConfig, chainClient *BlockchainClient, log server.Logger, pub jetstream.Publisher) *BlockListener {
	return &BlockListener{
		logger:      log,
		cfg:         cfg,
		chainClient: chainClient,
		pub:         pub,
	}
}

func (l *BlockListener) TargetSubject() string {
	return SYSTEM_NEW_BLOCKS_SUBJECT + "." + l.chainClient.Name()
}

func (l *BlockListener) Listen(ctx context.Context) error {
	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()

	startedBlock := l.cfg.StartFrom

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			currentBlock, err := l.chainClient.BlockNumber(ctx)
			if err != nil {
				continue
			}

			if startedBlock > currentBlock {
				startedBlock = currentBlock
			}

			blockRange := currentBlock - startedBlock
			if blockRange > l.cfg.BatchLimit {
				blockRange = l.cfg.BatchLimit
			}

			endBlock := startedBlock + blockRange - 1

			br := BlocksRange{
				Start: startedBlock,
				End:   endBlock,
			}

			if err = l.publishNewBlocks(br); err != nil {
				continue
			}

			l.logger.Debugf("[%s] [BlockListener] Published new blocks range: %d - %d", l.chainClient.Name(), br.Start, br.End)

			startedBlock += blockRange
		}
	}
}

func (l *BlockListener) publishNewBlocks(r BlocksRange) error {
	bytes, err := json.Marshal(&r)
	if err != nil {
		return fmt.Errorf("failed to marshal new blocks: %w", err)
	}
	_, err = l.pub.PublishAsync(l.TargetSubject(), bytes)
	if err != nil {
		return fmt.Errorf("failed to publish new blocks: %w", err)
	}
	return nil
}
