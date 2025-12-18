package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/eventscale/eventscale/internal/subjects"
)

type Block struct {
	Network string        `json:"network"`
	ChainID *big.Int      `json:"chain_id"`
	Header  *types.Header `json:"header"`
}

func NewBlock(network string, chainID *big.Int, header *types.Header) *Block {
	return &Block{
		Network: network,
		ChainID: chainID,
		Header:  header,
	}
}

func (e *Block) TargetSubject() string {
	return subjects.Blocks(e.Network, e.Header.Number.String())
}
