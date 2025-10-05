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
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				Version: 1,
				NATS:    NATSConfig{Path: "/path/to/nats.conf"},
				Networks: []NetworkConfig{
					{
						Name: "ethereum",
						RPC:  "https://eth.llamarpc.com",
						RateLimit: struct {
							Per   time.Duration `yaml:"per"`
							Limit int           `yaml:"limit"`
						}{
							Per:   1 * time.Second,
							Limit: 10,
						},
						BlocksProcessing: BlocksProcessingConfig{
							BatchLimit: 10,
							Interval:   5 * time.Second,
						},
					},
				},
				Events: []EventConfig{
					{
						Name:      "Transfer",
						Signature: "event Transfer(address indexed from, address indexed to, uint256 value)",
						Networks:  map[BlockchainName]map[common.Address]string{"ethereum": {}},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing version",
			config: Config{
				NATS:     NATSConfig{Path: "/path/to/nats.conf"},
				Networks: []NetworkConfig{},
				Events:   []EventConfig{},
			},
			wantErr: true,
			errMsg:  "config version is required",
		},
		{
			name: "missing NATS path",
			config: Config{
				Version:  1,
				NATS:     NATSConfig{},
				Networks: []NetworkConfig{},
				Events:   []EventConfig{},
			},
			wantErr: true,
			errMsg:  "NATS config path is required",
		},
		{
			name: "no networks",
			config: Config{
				Version:  1,
				NATS:     NATSConfig{Path: "/path/to/nats.conf"},
				Networks: []NetworkConfig{},
				Events:   []EventConfig{},
			},
			wantErr: true,
			errMsg:  "at least one network is required",
		},
		{
			name: "network missing RPC",
			config: Config{
				Version: 1,
				NATS:    NATSConfig{Path: "/path/to/nats.conf"},
				Networks: []NetworkConfig{
					{
						Name: "ethereum",
						RPC:  "",
					},
				},
				Events: []EventConfig{},
			},
			wantErr: true,
			errMsg:  "RPC URL is required",
		},
		{
			name: "no events",
			config: Config{
				Version: 1,
				NATS:    NATSConfig{Path: "/path/to/nats.conf"},
				Networks: []NetworkConfig{
					{
						Name: "ethereum",
						RPC:  "https://eth.llamarpc.com",
						RateLimit: struct {
							Per   time.Duration `yaml:"per"`
							Limit int           `yaml:"limit"`
						}{
							Per:   1 * time.Second,
							Limit: 10,
						},
						BlocksProcessing: BlocksProcessingConfig{
							BatchLimit: 10,
							Interval:   5 * time.Second,
						},
					},
				},
				Events: []EventConfig{},
			},
			wantErr: true,
			errMsg:  "at least one event is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil && tt.errMsg != "" {
				if err.Error() != tt.errMsg {
					t.Errorf("Config.Validate() error message = %v, want %v", err.Error(), tt.errMsg)
				}
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	tmpFile, err := os.CreateTemp("", "test-config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configContent := `version: 1
nats:
  path: /path/to/nats.conf
networks:
  - name: ethereum
    rpc: https://eth.llamarpc.com
    rate_limit:
      duration: 1s
      limit: 10
    blocks_proc:
      batch_limit: 10
      interval: 5s
events:
  - name: Transfer
    signature: event Transfer(address indexed from, address indexed to, uint256 value)
    networks:
      ethereum:
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": USDC
`

	if _, err := tmpFile.Write([]byte(configContent)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if cfg.Version != 1 {
		t.Errorf("Config.Version = %v, want 1", cfg.Version)
	}

	if len(cfg.Networks) != 1 {
		t.Errorf("len(Config.Networks) = %v, want 1", len(cfg.Networks))
	}

	if len(cfg.Events) != 1 {
		t.Errorf("len(Config.Events) = %v, want 1", len(cfg.Events))
	}
}

func TestLoadConfigInvalidFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("LoadConfig() expected error for nonexistent file, got nil")
	}
}
