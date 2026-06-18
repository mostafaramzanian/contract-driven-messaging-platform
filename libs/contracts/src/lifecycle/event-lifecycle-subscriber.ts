import * as amqplib from 'amqplib';
import {
  EVENT_LIFECYCLE_EXCHANGE,
  type EventLifecycleRecord,
  type EventLifecycleStage,
} from './event-lifecycle-publisher';

/**
 * Subscribes to the event-lifecycle fanout exchange via a temporary,
 * exclusive, auto-delete queue bound with no routing key restriction (a
 * fanout exchange ignores routing keys, so every published record is
 * delivered to every bound queue, including this one). This is purely an
 * observer: it never competes with or affects the `messaging_queue`
 * consumer or the actual business event flow.
 *
 * Used by integration tests in place of DB polling: instead of asking
 * "did a row eventually appear?", a test can ask "did this specific event
 * pass through this specific lifecycle stage?" and get a real answer
 * driven by the event itself, with a bounded wait and a clear timeout
 * error if it never happens.
 */
export class EventLifecycleSubscriber {
  private connection: amqplib.ChannelModel | undefined;
  private channel: amqplib.Channel | undefined;
  private readonly records: EventLifecycleRecord[] = [];
  private readonly waiters: {
    predicate: (record: EventLifecycleRecord) => boolean;
    resolve: (record: EventLifecycleRecord) => void;
  }[] = [];

  constructor(private readonly amqpUrl: string) {}

  async connect(): Promise<void> {
    this.connection = await amqplib.connect(this.amqpUrl);
    this.channel = await this.connection.createChannel();
    await this.channel.assertExchange(EVENT_LIFECYCLE_EXCHANGE, 'fanout', {
      durable: false,
    });

    const { queue } = await this.channel.assertQueue('', {
      exclusive: true,
      autoDelete: true,
    });
    await this.channel.bindQueue(queue, EVENT_LIFECYCLE_EXCHANGE, '');

    await this.channel.consume(
      queue,
      (msg) => {
        if (!msg) {
          return;
        }
        try {
          const record = JSON.parse(
            msg.content.toString('utf8'),
          ) as EventLifecycleRecord;
          this.records.push(record);
          this.notifyWaiters(record);
        } catch {
          // Malformed lifecycle record: ignore it. This channel is
          // observability-only, so a parse failure here must never throw
          // or otherwise affect the test/process consuming it.
        }
        this.channel?.ack(msg);
      },
      { noAck: false },
    );
  }

  private notifyWaiters(record: EventLifecycleRecord): void {
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      if (this.waiters[i].predicate(record)) {
        const [waiter] = this.waiters.splice(i, 1);
        waiter.resolve(record);
      }
    }
  }

  /** All lifecycle records observed so far, in arrival order. */
  getRecords(): readonly EventLifecycleRecord[] {
    return this.records;
  }

  /**
   * Resolves with the first observed (already-seen or future) record for
   * `eventId` at `stage`, or rejects after `timeoutMs` if none arrives.
   * This is the deterministic, event-driven replacement for polling: the
   * wait ends the instant the matching event arrives, not on a fixed
   * interval, and fails fast with a clear message if it never does.
   */
  async waitFor(
    eventId: string,
    stage: EventLifecycleStage,
    timeoutMs = 10_000,
  ): Promise<EventLifecycleRecord> {
    const predicate = (record: EventLifecycleRecord): boolean =>
      record.eventId === eventId && record.stage === stage;

    const existing = this.records.find(predicate);
    if (existing) {
      return existing;
    }

    return new Promise<EventLifecycleRecord>((resolve, reject) => {
      const waiter = { predicate, resolve };
      this.waiters.push(waiter);

      const timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index !== -1) {
          this.waiters.splice(index, 1);
        }
        reject(
          new Error(
            `Timed out after ${timeoutMs}ms waiting for eventId=${eventId} to reach stage="${stage}". ` +
              `Observed stages for other events: ${JSON.stringify(
                this.records.map((r) => ({
                  eventId: r.eventId,
                  stage: r.stage,
                })),
              )}`,
          ),
        );
      }, timeoutMs);

      const originalResolve = waiter.resolve;
      waiter.resolve = (record) => {
        clearTimeout(timer);
        originalResolve(record);
      };
    });
  }

  async close(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();
  }
}
