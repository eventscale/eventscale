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

package types

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/eventscale/eventscale/internal/subjects"
)

type Event struct {
	MetaData EventMetadata `json:"meta"`
	Data     []byte        `json:"data"`
}

type EventMetadata struct {
	Network string   `json:"network"`
	ChainID *big.Int `json:"chain_id"`

	Contract  common.Address `json:"contract"`
	Name      string         `json:"name"`
	Signature string         `json:"signature"`

	// Event Location
	BlockNumber uint64      `json:"block_number"`
	BlockHash   common.Hash `json:"block_hash"`
	TxHash      common.Hash `json:"tx_hash"`
	TxIndex     uint        `json:"tx_index"`
	LogIndex    uint        `json:"log_index"`

	// Mayber easier to get log location from ethereum.Log ?

	// Other logs from the same transaction
	OtherLogs []*types.Log `json:"other_logs"`

	Timestamp int64 `json:"timestamp"`
}

func (e *Event) TargetSubject(contractAlias ...string) string {
	contract := e.MetaData.Contract.String()
	if len(contractAlias) > 0 && contractAlias[0] != "" {
		contract = contractAlias[0]
	}
	return subjects.Event(e.MetaData.Network, contract, e.MetaData.Name)
}

func (e *Event) Decode(v any) error {
	return json.Unmarshal(e.Data, v)
}
