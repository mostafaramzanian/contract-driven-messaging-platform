import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  Index,
} from 'typeorm';

export type GatewayOutboxStatus = 'pending' | 'sent' | 'failed';

/**
 * GatewayOutboxEvent — producer-side transactional outbox ledger.
 *
 * ## Why the gateway needs its own outbox (Architectural Gap #1)
 *
 * Before this fix, `AppController` published directly to RabbitMQ with
 * `this.client.emit(...)` inside the HTTP request/response cycle. If
 * RabbitMQ was unavailable at that instant, `client.emit()`'s underlying
 * publish failed, the event was never durably recorded anywhere, and there
 * was no retry: the request either failed outright or — worse — appeared
 * to succeed to the caller while the event silently vanished. There was no
 * equivalent, on the producer side, of the durability guarantee the
 * messaging service already has for its OWN outgoing events via
 * `OutboxEvent`/`OutboxRelayService`.
 *
 * This entity is the producer-side mirror of that pattern: an inbound HTTP
 * request that wants to emit `CreateMessageEvent` writes a row HERE,
 * synchronously, as part of the HTTP request (see
 * `GatewayOutboxTransactionService.record()`), and returns to the caller
 * immediately. A separate background process — `GatewayOutboxRelayService`
 * — asynchronously claims and publishes pending rows to RabbitMQ, with the
 * exact same crash-safety, fencing, and horizontal-scaling guarantees as
 * the messaging service's consumer-side relay (see that class's doc
 * comment, and `OutboxRelayService` in the messaging app, for the design
 * this intentionally mirrors field-for-field).
 *
 * Deliberately the SAME column shape as `apps/messaging/src/entities/
 * outbox-event.entity.ts`'s `OutboxEvent` — same locking columns, same
 * `event_id`/`lock_version` fencing fields, same `trace_context` carrier —
 * so `GatewayOutboxRelayService` can be (and is) a near-verbatim adaptation
 * of `OutboxRelayService`, not a divergent reimplementation. The two
 * entities are NOT the same TypeORM entity/table only because they live in
 * different NestJS applications with separate `TypeOrmModule.forRootAsync`
 * registrations (gateway has no reason to know about `Message`,
 * `ProcessedEvent`, etc., and vice versa) — they share the same physical
 * Postgres database (`DB_NAME`) but occupy their own table,
 * `gateway_outbox_events`, with no foreign-key relationship to the
 * messaging service's tables. No new database, no new microservice, no
 * Kafka — just a second table in the database both services already share.
 *
 * ## Relay lifecycle (identical to the messaging-service outbox — see
 * `apps/messaging/src/entities/outbox-event.entity.ts` for the full
 * lifecycle write-up; the mechanics are not repeated here)
 */
@Entity('gateway_outbox_events')
export class GatewayOutboxEvent {
  @PrimaryGeneratedColumn()
  id: number;

  @Index()
  @Column({ name: 'event_type', type: 'varchar', length: 255 })
  eventType: string;

  @Column({ type: 'jsonb' })
  payload: unknown;

  @Index()
  @Column({
    name: 'correlation_id',
    type: 'varchar',
    length: 36,
    nullable: true,
  })
  correlationId?: string;

  @Index()
  @Column({ type: 'varchar', length: 16, default: 'pending' })
  status: GatewayOutboxStatus;

  @Column({ name: 'attempts', type: 'int', default: 0 })
  attempts: number;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError?: string;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @Column({ name: 'sent_at', type: 'timestamp', nullable: true })
  sentAt?: Date;

  // ── Relay-locking fields (mirrors messaging's migration 005) ────────────

  /** Timestamp when a relay instance claimed this row for publishing.
   *  NULL = not currently locked. */
  @Column({ name: 'locked_at', type: 'timestamp', nullable: true })
  lockedAt?: Date;

  /** Instance-id of the relay that holds the lock.
   *  Informational — correctness is via SKIP LOCKED, not this value. */
  @Column({ name: 'locked_by', type: 'varchar', length: 128, nullable: true })
  lockedBy?: string;

  /** Earliest time this row may be re-claimed (back-off schedule).
   *  Set to now() at INSERT; updated to now()+backoff on each failure. */
  @Column({
    name: 'next_retry_at',
    type: 'timestamp',
    default: () => 'CURRENT_TIMESTAMP',
  })
  nextRetryAt: Date;

  /** Per-row retry budget.  Rows with attempts >= maxAttempts are failed. */
  @Column({ name: 'max_attempts', type: 'int', default: 5 })
  maxAttempts: number;

  /**
   * Stable, application-level event identifier — generated once at row
   * creation by `GatewayOutboxTransactionService.record()` and reused
   * unchanged across every retry or operator-replay of this row. This is
   * the SAME `eventId` that ends up on the wire inside the event envelope
   * itself (see `buildCreateMessageEventV1`/`V2`) — not a second,
   * independent identifier — so a downstream consumer's idempotency check
   * recognizes a relay replay of this row as a redelivery of the same
   * logical event, not a new one.
   */
  @Column({ name: 'event_id', type: 'varchar', length: 36, nullable: true })
  eventId?: string;

  /**
   * Fencing token for the relay claim/publish/markSent cycle. See
   * `GatewayOutboxRelayService.claimBatch()`/`markSent()` for the race this
   * closes — identical mechanism to the messaging service's
   * `OutboxEvent.lockVersion`.
   */
  @Column({ name: 'lock_version', type: 'int', default: 0 })
  lockVersion: number;

  /**
   * W3C trace-context propagation carrier, captured from the active span
   * at the moment this row was written (inside the original HTTP
   * request's handler) — see
   * `GatewayOutboxTransactionService.record()`/`captureTraceContextCarrier()`.
   * Lets `GatewayOutboxRelayService.publishOne()` restore the ORIGINAL
   * caller's trace at publish time instead of this poll tick's ambient,
   * parentless context — keeping HTTP request → Gateway Outbox → Relay →
   * RabbitMQ → messaging-service consumer as one continuous distributed
   * trace.
   */
  @Column({ name: 'trace_context', type: 'jsonb', nullable: true })
  traceContext?: Record<string, string> | null;
}
