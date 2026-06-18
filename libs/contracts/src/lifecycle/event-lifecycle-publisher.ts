import type { Channel, ChannelModel } from 'amqplib';

/**
 * Stages in an event's lifecycle, as it is observed by the producer and
 * consumer. This is intentionally small and specific to this codebase's
 * two-hop flow (gateway -> messaging) -- it is not a general distributed
 * tracing system.
 */
export type EventLifecycleStage =
  | 'emitted'
  | 'received'
  | 'validated'
  | 'rejected'
  | 'persisted';

export interface EventLifecycleRecord {
  stage: EventLifecycleStage;
  service: string;
  eventType: string;
  eventId: string;
  correlationId: string;
  timestamp: string;
  /** Present only for `rejected` records. */
  errors?: { path: string; message: string }[];
}

export const EVENT_LIFECYCLE_EXCHANGE = 'event-lifecycle.test';

/**
 * Whether lifecycle publishing is active for this process. Off by default;
 * enabled only via EVENT_LIFECYCLE_TRACING=true, which is set in
 * `.env.test` (see docker-compose.test.yml) and nowhere else. This keeps
 * the publisher's overhead and failure modes completely out of the
 * dev/production path -- it exists purely to give integration tests a
 * deterministic, event-driven signal to observe, replacing DB polling.
 */
export function isEventLifecycleTracingEnabled(): boolean {
  return process.env.EVENT_LIFECYCLE_TRACING === 'true';
}

/**
 * Publishes a lifecycle record to a dedicated fanout exchange, separate
 * from the `messaging_queue` used for actual business events. Publishing
 * here never touches or competes with the real consumer queue, so it
 * cannot change business behavior even if it fails.
 *
 * Failures are swallowed (logged via the provided logger, if any) rather
 * than thrown: a lifecycle-tracing problem must never be allowed to break
 * the actual event flow it is observing.
 */
export class EventLifecyclePublisher {
  private channel: Channel | undefined;
  private connection: ChannelModel | undefined;
  private connecting: Promise<void> | undefined;

  constructor(
    private readonly serviceName: string,
    private readonly amqpUrl: string,
    private readonly onError?: (error: unknown) => void,
  ) {}

  private async ensureChannel(): Promise<Channel | undefined> {
    if (!isEventLifecycleTracingEnabled()) {
      return undefined;
    }
    if (this.channel) {
      return this.channel;
    }
    if (!this.connecting) {
      this.connecting = this.connect();
    }
    await this.connecting;
    return this.channel;
  }

  private async connect(): Promise<void> {
    try {
      const amqplib = await import('amqplib');
      this.connection = await amqplib.connect(this.amqpUrl);
      this.channel = await this.connection.createChannel();
      await this.channel.assertExchange(EVENT_LIFECYCLE_EXCHANGE, 'fanout', {
        durable: false,
      });
    } catch (error) {
      this.onError?.(error);
      this.channel = undefined;
      this.connection = undefined;
    }
  }

  async publish(
    record: Omit<EventLifecycleRecord, 'service' | 'timestamp'>,
  ): Promise<void> {
    const channel = await this.ensureChannel();
    if (!channel) {
      return;
    }

    const fullRecord: EventLifecycleRecord = {
      ...record,
      service: this.serviceName,
      timestamp: new Date().toISOString(),
    };

    try {
      channel.publish(
        EVENT_LIFECYCLE_EXCHANGE,
        '',
        Buffer.from(JSON.stringify(fullRecord)),
      );
    } catch (error) {
      this.onError?.(error);
    }
  }

  async close(): Promise<void> {
    try {
      await this.channel?.close();
      await this.connection?.close();
    } catch {
      // Best-effort cleanup; nothing meaningful to do if this fails.
    }
  }
}
