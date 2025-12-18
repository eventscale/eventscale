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
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"gopkg.in/yaml.v3"
)

type BlockchainName string

type Config struct {
	Version  int             `yaml:"version"`
	NATS     NATSConfig      `yaml:"nats"`
	System   SystemConfig    `yaml:"system"`
	Events   []EventConfig   `yaml:"events"`
	Networks []NetworkConfig `yaml:"networks"`
}

type NATSConfig struct {
	Path string `yaml:"path"`
}

type SystemConfig struct {
	BlockExtractor BlockExtractorConfig `yaml:"block_extractor"`
}

type BlockExtractorConfig struct {
	Disabled bool `yaml:"disabled"`
}

type EventConfig struct {
	Name          string                                       `yaml:"name"`
	Signature     string                                       `yaml:"signature"`
	NeedOtherLogs bool                                         `yaml:"need_other_logs"`
	Networks      map[BlockchainName]map[common.Address]string `yaml:"networks"`
}

type NetworkConfig struct {
	Name      BlockchainName `yaml:"name"`
	RPC       string         `yaml:"rpc"`
	RateLimit struct {
		Per   time.Duration `yaml:"per"`
		Limit int           `yaml:"limit"`
	} `yaml:"rate_limit"`
	BlocksProcessing BlocksProcessingConfig `yaml:"blocks_proc"`
}

type BlocksProcessingConfig struct {
	Force      bool          `yaml:"force"`
	StartFrom  uint64        `yaml:"start_from"`
	BatchLimit uint64        `yaml:"batch_limit"`
	Interval   time.Duration `yaml:"interval"`
}

func LoadConfig(path string) (Config, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Version == 0 {
		return fmt.Errorf("config version is required")
	}

	if c.NATS.Path == "" {
		return fmt.Errorf("NATS config path is required")
	}

	if len(c.Networks) == 0 {
		return fmt.Errorf("at least one network is required")
	}

	for i, net := range c.Networks {
		if net.Name == "" {
			return fmt.Errorf("network[%d]: name is required", i)
		}
		if net.RPC == "" {
			return fmt.Errorf("network %s: RPC URL is required", net.Name)
		}
		if net.RateLimit.Limit <= 0 {
			return fmt.Errorf("network %s: rate_limit.limit must be > 0", net.Name)
		}
		if net.RateLimit.Per <= 0 {
			return fmt.Errorf("network %s: rate_limit.per must be > 0", net.Name)
		}
		if net.BlocksProcessing.BatchLimit == 0 {
			return fmt.Errorf("network %s: blocks_proc.batch_limit must be > 0", net.Name)
		}
		if net.BlocksProcessing.Interval <= 0 {
			return fmt.Errorf("network %s: blocks_proc.interval must be > 0", net.Name)
		}
	}

	if len(c.Events) == 0 {
		return fmt.Errorf("at least one event is required")
	}

	for i, event := range c.Events {
		if event.Name == "" {
			return fmt.Errorf("event[%d]: name is required", i)
		}
		if event.Signature == "" {
			return fmt.Errorf("event %s: signature is required", event.Name)
		}
		if len(event.Networks) == 0 {
			return fmt.Errorf("event %s: at least one network is required", event.Name)
		}
	}

	return nil
}
