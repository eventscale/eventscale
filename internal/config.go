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
	Version  int             `yaml:"verison"`
	NATS     NATSConfig      `yaml:"nats"`
	Events   []EventConfig   `yaml:"events"`
	Networks []NetworkConfig `yaml:"networks"`
}

type NATSConfig struct {
	Path string `yaml:"path"`
}

type EventConfig struct {
	Name      string                                       `yaml:"name"`
	Signature string                                       `yaml:"signature"`
	Networks  map[BlockchainName]map[common.Address]string `yaml:"networks"`
}

type NetworkConfig struct {
	Name      BlockchainName `yaml:"name"`
	RPC       string         `yaml:"rpc"`
	RateLimit struct {
		Duration time.Duration `yaml:"duration"`
		Limit    int           `yaml:"limit"`
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

	return cfg, nil
}
