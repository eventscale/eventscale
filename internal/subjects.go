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

const (
	STREAM_NAME = "eventscale"

	SYSTEM_NEW_BLOCKS_SUBJECT                = "eventscale.system.new_blocks"
	SYSTEM_EVENT_EXTRACTOR_SUBJECT           = "eventscale.system.event.extractor"
	SYSTEM_EVENT_ADD_EVENT_EXTRACTOR_SUBJECT = "eventscale.system.add_event.extractor"

	EVENTS_SUBJECT = "eventscale.events"
)

func NetworkEventExtractorSubject(net string) string {
	return SYSTEM_EVENT_EXTRACTOR_SUBJECT + "." + net
}

func NetworkNewBlocksSubject(net string) string {
	return SYSTEM_NEW_BLOCKS_SUBJECT + "." + net
}

func NetworkAddEventExtractorSubject(net string) string {
	return SYSTEM_EVENT_ADD_EVENT_EXTRACTOR_SUBJECT + "." + net
}

func EventSubject(net string, contract string, eventName string) string {
	return EVENTS_SUBJECT + "." + net + "." + contract + "." + eventName
}
