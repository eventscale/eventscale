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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

type BlocksRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

func (br BlocksRange) Len() uint64 {
	return br.End - br.Start + 1
}

type BlocksSubscriber interface {
	HandleBlocksRange(ctx context.Context, br BlocksRange) error
}

type BlockListener struct {
	logger      server.Logger
	cfg         BlocksProcessingConfig
	chainClient *BlockchainClient
	pub         jetstream.Publisher
	kv          jetstream.KeyValue
	subscribers []BlocksSubscriber
}

func NewBlockListener(cfg BlocksProcessingConfig, chainClient *BlockchainClient, log server.Logger, pub jetstream.Publisher, kv jetstream.KeyValue, subs []BlocksSubscriber) *BlockListener {
	return &BlockListener{
		logger:      log,
		cfg:         cfg,
		chainClient: chainClient,
		pub:         pub,
		kv:          kv,
		subscribers: subs,
	}
}

func (l *BlockListener) TargetSubject() string {
	return SYSTEM_NEW_BLOCKS_SUBJECT + "." + l.chainClient.Name()
}

func (l *BlockListener) Listen(ctx context.Context) error {
	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()

	startedBlock, err := l.getStartedBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to get started block: %w", err)
	}

	l.logger.Debugf("[%s] [BlockListener] Started from block: %d", l.chainClient.Name(), startedBlock)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			processedBlock, err := l.processBlocks(ctx, startedBlock)
			if err != nil {
				l.logger.Errorf("[%s] [BlockListener] Failed to process blocks: %v", l.chainClient.Name(), err)

				continue
			}

			if _, err := l.kv.Put(ctx, fmt.Sprintf("%s-last-processed-block", l.chainClient.Name()), []byte(fmt.Sprintf("%d", processedBlock))); err != nil {
				l.logger.Errorf("[%s] [BlockListener] Failed to save last processed block: %v", l.chainClient.Name(), err)

				continue
			}

			startedBlock = processedBlock + 1
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
		l.logger.Tracef("[%s] [BlockListener] Started block %d is ahead of current block %d, skipping", l.chainClient.Name(), startedBlock, currentBlock)

		return startedBlock, nil
	}

	// Calculate block range to process
	blocksRange, hasNewBlocks := l.calculateBlockRange(startedBlock, currentBlock)
	if !hasNewBlocks {
		l.logger.Debugf("[%s] [BlockListener] No new blocks to process", l.chainClient.Name())

		return startedBlock, nil
	}

	// Publish the new blocks range
	if err := l.publishNewBlocks(ctx, blocksRange); err != nil {
		l.logger.Errorf("[%s] [BlockListener] Failed to publish blocks range %d-%d: %v", l.chainClient.Name(), blocksRange.Start, blocksRange.End, err)

		return startedBlock, fmt.Errorf("failed to publish blocks range %d-%d: %w", blocksRange.Start, blocksRange.End, err)
	}

	l.logger.Debugf("[%s] [BlockListener] Published new blocks range: %d - %d", l.chainClient.Name(), blocksRange.Start, blocksRange.End)

	// Return the next starting block
	return blocksRange.End, nil
}

func (l *BlockListener) getStartedBlock(ctx context.Context) (uint64, error) {
	saved, err := l.kv.Get(ctx, fmt.Sprintf("%s-last-processed-block", l.chainClient.Name()))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if l.cfg.StartFrom == 0 {
				currentBlock, err := l.chainClient.BlockNumber(ctx)
				if err != nil {
					return 0, fmt.Errorf("failed to get current block number: %w", err)
				}

				return currentBlock.Uint64(), nil
			}

			return l.cfg.StartFrom, nil
		}

		return 0, fmt.Errorf("failed to get started block: %w", err)
	}

	var startedBlockStr string
	if err := json.Unmarshal(saved.Value(), &startedBlockStr); err != nil {
		return 0, fmt.Errorf("failed to unmarshal started block: %w", err)
	}

	savedBlock, err := strconv.ParseUint(startedBlockStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse started block: %w", err)
	}

	return savedBlock + 1, nil
}

// getCurrentBlock fetches the current block number from the blockchain
func (l *BlockListener) getCurrentBlock(ctx context.Context) (uint64, error) {
	currentBlock, err := l.chainClient.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current block number: %w", err)
	}
	return currentBlock.Uint64(), nil
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

func (l *BlockListener) publishNewBlocks(ctx context.Context, r BlocksRange) error {
	for _, s := range l.subscribers {
		if err := s.HandleBlocksRange(ctx, r); err != nil {
			return fmt.Errorf("failed to handle blocks range: %w", err)
		}
	}

	bytes, err := json.Marshal(&r)
	if err != nil {
		return fmt.Errorf("failed to marshal new blocks: %w", err)
	}

	_, err = l.pub.Publish(ctx, l.TargetSubject(), bytes)
	if err != nil {
		return fmt.Errorf("failed to publish new blocks: %w", err)
	}

	return nil
}
