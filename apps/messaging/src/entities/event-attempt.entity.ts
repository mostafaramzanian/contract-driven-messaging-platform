import { Entity, PrimaryColumn, Column, UpdateDateColumn } from 'typeorm';

/**
 * EventAttempt — durable, per-eventId delivery-attempt counter.
 *
 * See migration 006 for the full rationale. In short: `x-retry-count`
 * (an AMQP header) is not a reliable source of truth for "how many times
 * has this logical event been attempted" because it only survives
 * redelivery paths that explicitly carry it forward
 * (`RetryPublisherService.publishToRetry`). Any other redelivery path —
 * a manual requeue via the RabbitMQ management UI, or a future
 * outbox-relay republish of a previously-failed event — starts a fresh
 * message with no such header, silently resetting the counter.
 *
 * This entity is the durable counterpart: every delivery attempt for a
 * given `eventId`, however it arrived, increments the same row via an
 * atomic `INSERT ... ON CONFLICT (event_id) DO UPDATE` (see
 * `MessagingController`'s use of this entity), so `RETRY_CONFIG.MAX_ATTEMPTS`
 * is enforced as a true lifetime cap on the logical event, not a
 * per-incident one.
 */
@Entity('event_attempts')
export class EventAttempt {
  @PrimaryColumn({ name: 'event_id', type: 'varchar', length: 36 })
  eventId: string;

  @Column({ type: 'int', default: 0 })
  attempts: number;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
