/**
 * RabbitMQ Topology — re-export shim
 *
 * The canonical definitions now live in `@app/contracts` (see
 * `libs/contracts/src/topology/topology.ts`) so the gateway's producer
 * outbox relay can share EXACTLY the same exchange/queue/routing-key
 * constants, retry curve, and command-vs-domain-event routing decision as
 * this service's consumer-side outbox relay — instead of the gateway
 * re-implementing (and risking silently diverging from) its own copy.
 *
 * This file is kept, unchanged in its exported surface, purely so every
 * existing import of `from './topology'` / `from '../reliability/topology'`
 * elsewhere in this app continues to resolve without modification.
 */
export {
  EXCHANGES,
  QUEUES,
  ROUTING_KEYS,
  RETRY_CONFIG,
  retryDelayMs,
  COMMAND_EVENT_TYPES,
  resolveOutboxRoute,
  type OutboxRoute,
} from '@app/contracts';
