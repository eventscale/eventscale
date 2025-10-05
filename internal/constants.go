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

import "time"

const (
	// NATS Stream and Consumer timeouts
	DefaultConsumerAckWait     = 10 * time.Second
	DefaultConsumerTimeout     = 5 * time.Minute
	DefaultStreamMaxAge        = 5 * time.Minute
	AddEventExtractorAckWait   = 1 * time.Minute
	MessageHandlerTimeout      = 5 * time.Minute
	
	// Block processing
	DefaultBlockProcessorTick  = 1 * time.Second
	DefaultBlockQueueCapacity  = 128
	
	// Graceful shutdown
	DefaultShutdownTimeout     = 30 * time.Second
)
