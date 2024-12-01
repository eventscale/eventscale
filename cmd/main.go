package main

import (
	"context"
	"flag"
	"os"
	"os/signal"

	"github.com/eventscale/eventscale/internal"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	configPath := flag.String("c", "config.yaml", "Path to config file")
	flag.Parse()

	if err := internal.StartServer(ctx, *configPath); err != nil {
		panic(err)
	}
}
