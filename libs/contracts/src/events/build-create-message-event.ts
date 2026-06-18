import { randomUUID } from 'node:crypto';
import {
  type CreateMessageEventV1,
  type CreateMessageEventV1Payload,
} from './v1/create-message.event';
import { CreateMessageEvent } from './event-registry';

/**
 * Builds a complete, contract-shaped CreateMessageEvent.v1 from a payload
 * and an inbound correlationId. Used by the producer (gateway) so eventId
 * generation, timestamping, and trace initialization happen in exactly one
 * place rather than being duplicated at every emit call site.
 */
export function buildCreateMessageEventV1(
  payload: CreateMessageEventV1Payload,
  correlationId: string,
): CreateMessageEventV1 {
  return {
    type: CreateMessageEvent.name,
    eventId: randomUUID(),
    correlationId,
    timestamp: new Date().toISOString(),
    source: 'gateway',
    trace: ['gateway'],
    payload,
  };
}
