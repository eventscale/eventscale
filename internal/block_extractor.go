package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lmittmann/w3/module/eth"
	"github.com/lmittmann/w3/w3types"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

type BlockExtractor struct {
	logger       server.Logger
	chainClient  *BlockchainClient
	pub          jetstream.Publisher
	logProcessor *LogProcessor

	blocksLock *sync.Mutex
	blocks     []BlocksRange

	ctx    context.Context
	cancel context.CancelFunc
}

func NewBlockExtractor(logger server.Logger, chainClient *BlockchainClient, pub jetstream.Publisher, logProcessor *LogProcessor) *BlockExtractor {
	ctx, cancel := context.WithCancel(context.Background())

	ext := &BlockExtractor{
		logger:       logger,
		chainClient:  chainClient,
		pub:          pub,
		logProcessor: logProcessor,
		blocksLock:   &sync.Mutex{},
		blocks:       make([]BlocksRange, 0),
		ctx:          ctx,
		cancel:       cancel,
	}

	go ext.processBlocks(ctx)

	return ext
}

func (b *BlockExtractor) HandleBlocksRange(ctx context.Context, br BlocksRange) error {
	b.blocksLock.Lock()
	b.blocks = append(b.blocks, br)
	b.blocksLock.Unlock()

	return nil
}

func (b *BlockExtractor) HandleBlocks(ctx context.Context, msg jetstream.Msg) error {
	var br BlocksRange
	if err := json.Unmarshal(msg.Data(), &br); err != nil {
		b.logger.Errorf("[%s] [BlockExtractor] Failed to unmarshal blocks range: %v", b.chainClient.Name(), err)

		return fmt.Errorf("unmarshal blocks range: %w", err)
	}

	return b.handleBlocks(ctx, br)
}

func (b *BlockExtractor) processBlocks(ctx context.Context) error {
	ticker := time.NewTicker(DefaultBlockProcessorTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			br, ok := b.readFirstBlock()
			if !ok {
				continue
			}

			if err := b.handleBlocks(ctx, br); err != nil {
				b.logger.Errorf("[%s] [BlockExtractor] Failed to handle blocks: %v", b.chainClient.Name(), err)

				return fmt.Errorf("handle blocks: %w", err)
			}

			b.dequeueBlock()
		}
	}
}

func (b *BlockExtractor) handleBlocks(ctx context.Context, br BlocksRange) error {
	headers := make([]*types.Header, br.Len())
	calls := make([]w3types.RPCCaller, br.Len())
	index := 0

	for blockNumber := br.Start; blockNumber <= br.End; blockNumber++ {
		calls[index] = eth.HeaderByNumber(big.NewInt(int64(blockNumber))).Returns(&headers[index])
		index++
	}

	if err := b.chainClient.CallCtx(ctx, calls...); err != nil {
		b.logger.Errorf("[%s] [BlockExtractor] Failed to call chain client: %v", b.chainClient.Name(), err)

		return fmt.Errorf("chain client call: %w", err)
	}

	for _, header := range headers {
		blockEvent := NewBlockEvent(b.chainClient.Name(), b.chainClient.ChainID(), header)
		payload, err := json.Marshal(blockEvent)
		if err != nil {
			b.logger.Errorf("[%s] [BlockExtractor] Failed to marshal block event: %v", b.chainClient.Name(), err)

			return fmt.Errorf("marshal block event: %w", err)
		}

		if _, err := b.pub.Publish(ctx, blockEvent.TargetSubject(), payload); err != nil {
			b.logger.Errorf("[%s] [BlockExtractor] Failed to publish block event: %v", b.chainClient.Name(), err)

			return fmt.Errorf("publish block event: %w", err)
		}
	}

	b.logger.Debugf("[%s] [BlockExtractor] Processed blocks range: %d - %d", b.chainClient.Name(), br.Start, br.End)

	return nil
}

func (b *BlockExtractor) processLogs(ctx context.Context, header *types.Header, logs []types.Log) ([]Event, error) {
	return b.logProcessor.ProcessLogsToEvents(ctx, logs, int64(header.Time))
}

func (b *BlockExtractor) readFirstBlock() (BlocksRange, bool) {
	b.blocksLock.Lock()
	defer b.blocksLock.Unlock()

	if len(b.blocks) == 0 {
		return BlocksRange{}, false
	}

	return b.blocks[0], true
}

func (b *BlockExtractor) dequeueBlock() {
	b.blocksLock.Lock()
	defer b.blocksLock.Unlock()

	if len(b.blocks) == 0 {
		return
	}

	b.blocks = b.blocks[1:]
}

// Stop gracefully stops the block extractor
func (b *BlockExtractor) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
}
