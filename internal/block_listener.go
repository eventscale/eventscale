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
	if l.cfg.StartFrom == 0 {
		currentBlock, err := l.chainClient.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current block number: %w", err)
		}

		startedBlock = currentBlock
	}

	l.logger.Debugf("[%s] [BlockListener] Started from block: %d", l.chainClient.Name(), startedBlock)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			newStartedBlock, err := l.processBlocks(ctx, startedBlock)
			if err != nil {
				l.logger.Errorf("[%s] [BlockListener] Failed to process blocks: %v", l.chainClient.Name(), err)
				continue
			}
			startedBlock = newStartedBlock
		}
	}
}

// processBlocks handles the complete block processing cycle and returns the new starting block
func (l *BlockListener) processBlocks(ctx context.Context, startedBlock uint64) (uint64, error) {
	currentBlock, err := l.getCurrentBlock(ctx)
	if err != nil {
		return startedBlock, err
	}

	// Skip processing if started block is higher than current block
	if startedBlock > currentBlock {
		l.logger.Debugf("[%s] [BlockListener] Started block %d is ahead of current block %d, skipping", l.chainClient.Name(), startedBlock, currentBlock)
		return startedBlock, nil
	}

	// Calculate block range to process
	blocksRange, hasNewBlocks := l.calculateBlockRange(startedBlock, currentBlock)
	if !hasNewBlocks {
		l.logger.Debugf("[%s] [BlockListener] No new blocks to process", l.chainClient.Name())
		return startedBlock, nil
	}

	// Publish the new blocks range
	if err := l.publishNewBlocks(blocksRange); err != nil {
		return startedBlock, fmt.Errorf("failed to publish blocks range %d-%d: %w", blocksRange.Start, blocksRange.End, err)
	}

	l.logger.Debugf("[%s] [BlockListener] Published new blocks range: %d - %d", l.chainClient.Name(), blocksRange.Start, blocksRange.End)

	// Return the next starting block
	return blocksRange.End + 1, nil
}

// getCurrentBlock fetches the current block number from the blockchain
func (l *BlockListener) getCurrentBlock(ctx context.Context) (uint64, error) {
	currentBlock, err := l.chainClient.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current block number: %w", err)
	}
	return currentBlock, nil
}

// calculateBlockRange determines the range of blocks to process
func (l *BlockListener) calculateBlockRange(startedBlock, currentBlock uint64) (BlocksRange, bool) {
	if startedBlock > currentBlock {
		return BlocksRange{}, false
	}

	// Calculate how many blocks we can process (including the current block)
	availableBlocks := currentBlock - startedBlock + 1
	blockRange := min(availableBlocks, l.cfg.BatchLimit)
	if blockRange == 0 {
		return BlocksRange{}, false
	}

	endBlock := startedBlock + blockRange - 1

	return BlocksRange{
		Start: startedBlock,
		End:   endBlock,
	}, true
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
