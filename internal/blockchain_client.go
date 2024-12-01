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
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/ethclient"
)

type BlockchainClient struct {
	*ethclient.Client
	name    BlockchainName
	once    *sync.Once
	chainID *big.Int
}

func NewBlockchainClient(name BlockchainName, client *ethclient.Client) *BlockchainClient {
	return &BlockchainClient{
		Client:  client,
		name:    name,
		once:    &sync.Once{},
		chainID: big.NewInt(0),
	}
}

func (c *BlockchainClient) Name() string {
	return string(c.name)
}

func (c *BlockchainClient) ChainID() *big.Int {
	c.once.Do(func() {
		if chainID, err := c.Client.ChainID(context.Background()); err == nil {
			c.chainID.Set(chainID)
		}
	})
	return c.chainID
}
