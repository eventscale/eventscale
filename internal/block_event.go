package internal

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockEvent struct {
	Network string        `json:"network"`
	ChainID *big.Int      `json:"chain_id"`
	Header  *types.Header `json:"header"`
}

func NewBlockEvent(network string, chainID *big.Int, header *types.Header) *BlockEvent {
	return &BlockEvent{
		Network: network,
		ChainID: chainID,
		Header:  header,
	}
}

func (e *BlockEvent) TargetSubject() string {
	return BlocksSubject(e.Network, e.Header.Number.String())
}
