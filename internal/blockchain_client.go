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

	"github.com/lmittmann/w3"
	"github.com/lmittmann/w3/module/eth"
)

type BlockchainClient struct {
	*w3.Client

	name    BlockchainName
	once    *sync.Once
	chainID *big.Int
}

func NewBlockchainClient(name BlockchainName, client *w3.Client) *BlockchainClient {
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
		var id uint64
		if err := c.Client.Call(eth.ChainID().Returns(&id)); err == nil {
			c.chainID = big.NewInt(int64(id))
		}
	})
	return c.chainID
}

func (c *BlockchainClient) BlockNumber(ctx context.Context) (*big.Int, error) {
	var num *big.Int
	if err := c.Client.CallCtx(ctx, eth.BlockNumber().Returns(&num)); err != nil {
		return nil, err
	}

	return num, nil
}
