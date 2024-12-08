package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"

	"github.com/ethereum/go-ethereum/common"
	eventscale "github.com/eventscale/eventscale/pkg/sdk-go"
)

// event Transfer(address indexed from, address indexed to, uint256 value)
type Transfer struct {
	From   common.Address `json:"from"`
	To     common.Address `json:"to"`
	Amount *big.Int       `json:"value"`
}

// process transfers event
func handleTransfers(ctx context.Context, e eventscale.Event) error {
	var transfer Transfer
	if err := e.Decode(&transfer); err != nil {
		return fmt.Errorf("failed to decode event: %w", err)
	}

	fmt.Printf("Transfer: %s -> %s: %s\n", transfer.From, transfer.To, transfer.Amount.String())

	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	ectx, err := eventscale.Connect(ctx, "nats://localhost:4222")
	if err != nil {
		panic(err)
	}

	fmt.Println("Connected to eventscale")

	sub, err := eventscale.Subscribe(ectx, handleTransfers,
		eventscale.WithNetwork("ethereum"),
		eventscale.WithContract("USDC"),
		eventscale.WithEvent("Transfer"),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Subscribed to Transfer Event")

	sub.Start(ctx)
}
