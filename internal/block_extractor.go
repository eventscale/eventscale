package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"

	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/eventscale/eventscale/internal/types"
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
}

func NewBlockExtractor(logger server.Logger, chainClient *BlockchainClient, pub jetstream.Publisher, logProcessor *LogProcessor) *BlockExtractor {
	return &BlockExtractor{
		logger:       logger,
		chainClient:  chainClient,
		pub:          pub,
		logProcessor: logProcessor,
		blocksLock:   &sync.Mutex{},
		blocks:       make([]BlocksRange, 0),
	}
}

func (b *BlockExtractor) HandleBlocks(ctx context.Context, msg jetstream.Msg) error {
	var br BlocksRange
	if err := json.Unmarshal(msg.Data(), &br); err != nil {
		b.logger.Errorf("[%s] [BlockExtractor] Failed to unmarshal blocks range: %v", b.chainClient.Name(), err)

		return fmt.Errorf("unmarshal blocks range: %w", err)
	}

	return b.handleBlocks(ctx, br)
}

func (b *BlockExtractor) handleBlocks(ctx context.Context, br BlocksRange) error {
	headers := make([]*eth_types.Header, br.Len())
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
		block := types.NewBlock(b.chainClient.Name(), b.chainClient.ChainID(), header)
		payload, err := json.Marshal(block)
		if err != nil {
			b.logger.Errorf("[%s] [BlockExtractor] Failed to marshal block event: %v", b.chainClient.Name(), err)

			return fmt.Errorf("marshal block event: %w", err)
		}

		if _, err := b.pub.Publish(ctx, block.TargetSubject(), payload); err != nil {
			b.logger.Errorf("[%s] [BlockExtractor] Failed to publish block event: %v", b.chainClient.Name(), err)

			return fmt.Errorf("publish block event: %w", err)
		}
	}

	b.logger.Debugf("[%s] [BlockExtractor] Processed blocks range: %d - %d", b.chainClient.Name(), br.Start, br.End)

	return nil
}
