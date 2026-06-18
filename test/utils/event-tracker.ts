import { EventLifecycleSubscriber } from '@app/contracts';

const DEFAULT_RABBITMQ_URL = 'amqp://guest:guest@localhost:5672';

/**
 * Thin integration-test wrapper around `EventLifecycleSubscriber`.
 *
 * This is the "event-driven test observer" replacing DB polling: it
 * connects directly to the same RabbitMQ broker the real gateway and
 * messaging containers use (via the host-mapped port from
 * docker-compose.test.yml, which is intentionally the same port number
 * the broker listens on inside the Docker network -- see the header
 * comment in docker-compose.test.yml), binds its own exclusive/
 * auto-delete queue to the lifecycle fanout exchange, and lets a test
 * `await` the exact moment a specific event reaches a specific stage --
 * instead of asking "has a row shown up yet?" on a fixed interval.
 *
 * Requires `EVENT_LIFECYCLE_TRACING=true` to be set for the gateway and
 * messaging containers (already the case in `.env.test`); if tracing is
 * disabled, no lifecycle records will ever arrive and every `waitFor*`
 * call will time out -- which is the correct, honest failure mode rather
 * than a silent false pass.
 */
export class EventTracker {
  private readonly subscriber: EventLifecycleSubscriber;

  constructor(
    amqpUrl: string = process.env.RABBITMQ_URL ?? DEFAULT_RABBITMQ_URL,
  ) {
    this.subscriber = new EventLifecycleSubscriber(amqpUrl);
  }

  async connect(): Promise<void> {
    await this.subscriber.connect();
  }

  async close(): Promise<void> {
    await this.subscriber.close();
  }

  /** All lifecycle records observed so far, across every event. */
  getRecords() {
    return this.subscriber.getRecords();
  }

  /**
   * Waits until `eventId` has been recorded as `emitted` by the gateway.
   */
  waitForEmitted(eventId: string, timeoutMs?: number) {
    return this.subscriber.waitFor(eventId, 'emitted', timeoutMs);
  }

  /**
   * Waits until `eventId` has been recorded as `received` by the
   * messaging service (i.e. it came off RabbitMQ and reached the
   * consumer's handler).
   */
  waitForReceived(eventId: string, timeoutMs?: number) {
    return this.subscriber.waitFor(eventId, 'received', timeoutMs);
  }

  /**
   * Waits until `eventId` has been recorded as `validated` by the
   * messaging service (i.e. it passed contract validation there, as
   * opposed to only at the gateway).
   */
  waitForValidated(eventId: string, timeoutMs?: number) {
    return this.subscriber.waitFor(eventId, 'validated', timeoutMs);
  }

  /**
   * Waits until `eventId` has been recorded as `persisted` -- the
   * messaging service successfully wrote the corresponding row.
   */
  waitForPersisted(eventId: string, timeoutMs?: number) {
    return this.subscriber.waitFor(eventId, 'persisted', timeoutMs);
  }

  /**
   * Waits until `eventId` has been recorded as `rejected` by whichever
   * service rejected it.
   */
  waitForRejected(eventId: string, timeoutMs?: number) {
    return this.subscriber.waitFor(eventId, 'rejected', timeoutMs);
  }

  /**
   * Convenience assertion for the full happy-path chain: emitted (by the
   * gateway) -> received -> validated -> persisted (all three by
   * messaging), in that order, each tied to the same `eventId`. Returns
   * the four records so the caller can assert further on their contents
   * (e.g. correlationId equality across hops).
   */
  async waitForFullChain(eventId: string, timeoutMs?: number) {
    const emitted = await this.waitForEmitted(eventId, timeoutMs);
    const received = await this.waitForReceived(eventId, timeoutMs);
    const validated = await this.waitForValidated(eventId, timeoutMs);
    const persisted = await this.waitForPersisted(eventId, timeoutMs);
    return { emitted, received, validated, persisted };
  }
}
