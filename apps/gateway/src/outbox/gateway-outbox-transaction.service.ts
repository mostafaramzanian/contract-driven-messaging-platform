import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import type { DataSource, EntityManager } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { captureTraceContextCarrier } from '@app/common';
import { GatewayOutboxEvent } from '../entities/gateway-outbox-event.entity';

export interface GatewayOutboxEventInput {
  eventType: string;
  payload: unknown;
  correlationId?: string;
  /**
   * Stable application-level event identifier to persist on the row.
   * Pass the SAME `eventId` that is already embedded in the event envelope
   * (e.g. from `buildCreateMessageEventV1`/`V2`) so a relay replay of this
   * row and the original envelope agree on identity — this is what lets
   * the messaging service's idempotency check recognize a replay as a
   * redelivery, not a new event. If omitted, a fresh UUID is generated
   * (kept for callers without a pre-existing eventId of their own).
   */
  eventId?: string;
  /** Override per-row retry budget. Defaults to entity default (5). */
  maxAttempts?: number;
}

/**
 * GatewayOutboxTransactionService
 *
 * Producer-side counterpart to the messaging service's
 * `OutboxTransactionService`. Provides `record()` — the single write path
 * by which `AppController` durably persists an outgoing event BEFORE any
 * attempt is made to reach RabbitMQ.
 *
 * ## Why this closes Architectural Gap #1
 *
 * The previous flow was: validate event → `client.emit()` directly against
 * RabbitMQ → respond to the HTTP caller based on whether THAT publish
 * succeeded. If RabbitMQ was down, slow, or mid-failover at that exact
 * moment, the event was gone — no persistence, no retry, no recovery,
 * full stop.
 *
 * The new flow is: validate event → `record()` (a single, already-atomic
 * Postgres INSERT, committed before this method returns) → respond
 * `202 Accepted` to the HTTP caller. The event's durability is now
 * guaranteed by Postgres, which this service depends on regardless of
 * RabbitMQ's availability — exactly the same trade a transactional outbox
 * makes on the consumer side. A separate process,
 * `GatewayOutboxRelayService`, takes the row from there.
 *
 * ## Why `runWithOutboxEvent`, not just `repo.save()`
 *
 * For a single INSERT, `repo.save()` would already be atomic on its own —
 * there is no "dual write" problem here today because the gateway has no
 * other business table to write alongside the outbox row (it is a pure
 * producer, not a system of record). `runWithOutboxEvent()` exists anyway,
 * structured as an explicit `QueryRunner` transaction around a caller-
 * supplied `work` callback, for two reasons:
 *
 *  1. **Symmetry with the messaging service's `OutboxTransactionService.
 *     runWithOutboxEvents()`** — same shape, same usage pattern, so an
 *     engineer who already understands one understands the other
 *     immediately. This is the literal "reuse existing outbox patterns"
 *     requirement, not just a similar-looking class name.
 *  2. **Forward compatibility** — if the gateway ever grows a reason to
 *     write its own business row alongside an outgoing event (e.g. a
 *     request-deduplication ledger, an audit log of accepted requests),
 *     that write slots into the SAME transaction as the outbox INSERT with
 *     no further refactor, exactly as `runWithOutboxEvents` already does
 *     for the messaging service's `Message` + `OutboxEvent` pair.
 *
 * `record()` is the convenience entry point most callers want today (no
 * additional business write); `runWithOutboxEvent()` is the lower-level
 * primitive it's built on, exported for that future case.
 */
@Injectable()
export class GatewayOutboxTransactionService {
  private readonly logger = new Logger(GatewayOutboxTransactionService.name);

  constructor(
    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Convenience wrapper around `runWithOutboxEvent()` for the common case
   * of "just persist this one outgoing event, no other business write".
   */
  async record(event: GatewayOutboxEventInput): Promise<GatewayOutboxEvent> {
    return this.runWithOutboxEvent(() => Promise.resolve(undefined), event);
  }

  /**
   * Execute `work` and atomically insert one outbox row in the same DB
   * transaction. Returns the persisted `GatewayOutboxEvent` row (not
   * `work`'s return value — unlike the messaging service's
   * `runWithOutboxEvents`, the row itself, including its generated `id`
   * and `eventId`, is what callers need: `AppController` returns it
   * directly in the HTTP response body).
   *
   * @param work   Callback for any additional business write the caller
   *               wants folded into the same transaction. Receives the
   *               transaction's `EntityManager` — MUST be used for any
   *               writes inside this callback (not an injected repository)
   *               so they share this QueryRunner's transaction.
   * @param event  The outbox-event descriptor to persist.
   */
  async runWithOutboxEvent(
    work: (em: EntityManager) => Promise<unknown>,
    event: GatewayOutboxEventInput,
  ): Promise<GatewayOutboxEvent> {
    const qr = this.dataSource.createQueryRunner();
    await qr.connect();
    await qr.startTransaction();

    try {
      // ── 1. Execute caller's business writes (no-op for record()) ───────
      await work(qr.manager);

      // ── 2. Insert the outbox row in the same transaction ───────────────
      const row = await this.insertOutboxEvent(qr.manager, event);

      // ── 3. Commit — durable BEFORE this method returns, BEFORE any
      //       attempt to reach RabbitMQ is made. ─────────────────────────
      await qr.commitTransaction();
      return row;
    } catch (err: unknown) {
      await qr.rollbackTransaction();
      this.logger.error(
        'Gateway outbox transaction rolled back',
        err instanceof Error ? err.stack : String(err),
        GatewayOutboxTransactionService.name,
      );
      throw err;
    } finally {
      await qr.release();
    }
  }

  private async insertOutboxEvent(
    em: EntityManager,
    event: GatewayOutboxEventInput,
  ): Promise<GatewayOutboxEvent> {
    const now = new Date();
    // Captured at the moment this row is written — i.e. still inside the
    // original HTTP request's span — so the relay can later propagate the
    // ORIGINAL caller's trace instead of its own ambient, parentless
    // context at publish time. See GatewayOutboxRelayService.publishOne.
    const traceContext = captureTraceContextCarrier();

    const row = em.create(GatewayOutboxEvent, {
      eventId: event.eventId ?? randomUUID(),
      eventType: event.eventType,
      payload: event.payload,
      correlationId: event.correlationId,
      status: 'pending',
      attempts: 0,
      nextRetryAt: now,
      maxAttempts: event.maxAttempts ?? 5,
      traceContext: Object.keys(traceContext).length > 0 ? traceContext : null,
    });

    return em.save(GatewayOutboxEvent, row);
  }
}
