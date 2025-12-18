package types

import (
	"math/big"
	"strconv"

	"github.com/eventscale/eventscale/internal/subjects"
)

type EventBlock struct {
	BlockNumber uint64   `json:"block_number"`
	Network     string   `json:"network"`
	ChainID     *big.Int `json:"chain_id"`
	Events      []Event  `json:"events"`
}

func NewEventBlock(blockNumber uint64, network string, chainID *big.Int, events []Event) *EventBlock {
	return &EventBlock{
		BlockNumber: blockNumber,
		Network:     network,
		ChainID:     chainID,
		Events:      events,
	}
}

func (b *EventBlock) TargetSubject() string {
	return subjects.EventsBlocks(b.Network, strconv.FormatUint(b.BlockNumber, 10))
}
