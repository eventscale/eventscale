// Subjects and helpers mirrored from Go internal/subjects.go
export const STREAM_NAME = "eventscale";

export const SYSTEM_EVENT_ADD_EVENT_EXTRACTOR_SUBJECT =
  "eventscale.system.add_event.extractor";

export const EVENTS_SUBJECT = "eventscale.events";

export function networkAddEventExtractorSubject(net: string): string {
  return `${SYSTEM_EVENT_ADD_EVENT_EXTRACTOR_SUBJECT}.${net}`;
}

export function eventSubject(
  net: string,
  contract: string,
  eventName: string
): string {
  return `${EVENTS_SUBJECT}.${net}.${contract}.${eventName}`;
}
