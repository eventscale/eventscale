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
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/eventscale/eventscale/internal/subjects"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func StartServer(ctx context.Context, confPath string) error {
	cfg, err := LoadConfig(confPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	natsServer, err := startNATS(cfg.NATS.Path)
	if err != nil {
		return fmt.Errorf("failed to start nats server: %w", err)
	}

	commonLogger := natsServer.Logger()

	natsConn, err := nats.Connect(natsServer.ClientURL())
	if err != nil {
		return fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := jetstream.New(natsConn)
	if err != nil {
		return fmt.Errorf("failed to create jetstream: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      subjects.STREAM_NAME,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    DefaultStreamMaxAge,
		Subjects: []string{
			subjects.STREAM_NAME + ".>",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	commonLogger.Noticef("Eventscale stream created")

	extCtx := Context{
		Context:   ctx,
		JetStream: js,
		stream:    stream,
		Logger:    commonLogger,
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "eventscale-system-bucket",
	})
	if err != nil {
		return fmt.Errorf("failed to create key-value store: %w", err)
	}

	runners := make([]*NetRunner, 0, len(cfg.Networks))
	for _, net := range cfg.Networks {
		conf := NetRunnerConfig{
			SysConf: cfg.System,
			NetConf: net,
			Events:  cfg.Events,
		}

		runner, err := InitNetRunner(extCtx, kv, conf)
		if err != nil {
			return fmt.Errorf("failed to init net runner: %w", err)
		}

		if err := runner.Register(extCtx); err != nil {
			return fmt.Errorf("failed to register net runner: %w", err)
		}

		commonLogger.Noticef("Blockchain netrunner [%s] initialized and registered", net.Name)

		runners = append(runners, runner)
	}

	eventProducer := NewEventProducer(cfg.Events, js)

	extCons, err := js.CreateOrUpdateConsumer(ctx, subjects.STREAM_NAME, jetstream.ConsumerConfig{
		Name:          "event-extractor",
		AckWait:       DefaultConsumerAckWait,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subjects.SYSTEM_EVENT_EXTRACTOR_SUBJECT + ".*",
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer event-extractor: %w", err)
	}

	if _, err := extCons.Consume(messageHandlerWrapper(eventProducer.HandleEvents)); err != nil {
		return fmt.Errorf("failed to consume event-extractor: %w", err)
	}

	commonLogger.Noticef("Event extractor consumer created")

	wg := &sync.WaitGroup{}
	wg.Add(len(runners))

	// Channel to collect errors from runners
	errCh := make(chan error, len(runners))

	for _, r := range runners {
		go func(netr *NetRunner) {
			defer wg.Done()
			commonLogger.Noticef("Starting netrunner [%s]", netr.chainClient.Name())
			if err := netr.Start(ctx); err != nil {
				commonLogger.Errorf("Failed to start netrunner [%s]: %v", netr.chainClient.Name(), err)
				errCh <- fmt.Errorf("netrunner %s failed: %w", netr.chainClient.Name(), err)
			}
		}(r)
	}

	commonLogger.Noticef("Eventscale is ready")

	// Wait for shutdown signal or error
	shutdownCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownCh)
	}()

	select {
	case <-ctx.Done():
		commonLogger.Noticef("Shutdown signal received, stopping gracefully...")
		// Stop all runners
		for _, r := range runners {
			r.Stop()
		}
		// Wait for all runners to finish with timeout
		shutdownTimeout := time.NewTimer(DefaultShutdownTimeout)
		select {
		case <-shutdownCh:
			commonLogger.Noticef("All runners stopped gracefully")
		case <-shutdownTimeout.C:
			commonLogger.Warnf("Shutdown timeout exceeded, forcing exit")
		}
		shutdownTimeout.Stop()
	case err := <-errCh:
		commonLogger.Errorf("Runner error: %v", err)
		// Stop all runners on error
		for _, r := range runners {
			r.Stop()
		}
		return err
	case <-shutdownCh:
		commonLogger.Noticef("All runners completed")
	}

	// Shutdown NATS server
	natsServer.Shutdown()
	natsServer.WaitForShutdown()

	return nil
}

func startNATS(configPath string) (*server.Server, error) {
	fs := flag.NewFlagSet("nats-server", flag.ExitOnError)

	opts, err := server.ConfigureOptions(fs, []string{"-c", configPath}, server.PrintServerAndExit, fs.Usage, server.PrintTLSHelpAndDie)
	if err != nil {
		return nil, fmt.Errorf("failed to configure nats server options: %w", err)
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats server: %w", err)
	}

	ns.ConfigureLogger()

	if err := server.Run(ns); err != nil {
		return nil, fmt.Errorf("failed to run nats server: %w", err)
	}

	return ns, nil
}
