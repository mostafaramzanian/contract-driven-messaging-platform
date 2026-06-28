import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqplib from 'amqplib';
import { EXCHANGES, ROUTING_KEYS, retryDelayMs } from './topology';
import { injectTraceContext } from '@app/common';

export interface RetryHeaders {
  'x-retry-count': number;
  'x-first-error': string;
  'x-error-class': string;
  'x-failed-at': string;
}

/**
 * RetryPublisherService
 *
 * Holds a dedicated amqplib connection for publishing retry messages.
 * We keep this separate from the NestJS RMQ transport connection so
 * that a channel error on publish does not affect the consumer.
 *
 * Usage: called from MessagingController when a TRANSIENT error occurs
 * and the message has not yet exceeded MAX_ATTEMPTS.
 */
@Injectable()
export class RetryPublisherService implements OnModuleDestroy {
  private readonly logger = new Logger(RetryPublisherService.name);
  private connection: amqplib.ChannelModel | undefined;
  private channel: amqplib.ConfirmChannel | undefined;
  private connecting: Promise<void> | undefined;

  constructor(private readonly configService: ConfigService) {}

  async onModuleDestroy(): Promise<void> {
    try {
      await this.channel?.close();
      await this.connection?.close();
    } catch {
      /* ignore */
    }
  }

  /**
   * Publish a message to the retry delay queue with per-message TTL
   * computed from the current attempt number (exponential back-off).
   *
   * The retry queue has `x-dead-letter-exchange = messaging.direct` and
   * `x-dead-letter-routing-key = messaging.work`, so after the TTL the
   * broker re-delivers the message to the main work queue.
   *
   * ## Production-readiness fixes (see review)
   *
   * Two changes from the original implementation, both required together
   * for `MessagingController.handleMessage`'s corrected ack-ordering to
   * actually be safe:
   *
   *  1. Uses a CONFIRM channel (`createConfirmChannel`, not
   *     `createChannel`) and awaits `waitForConfirms()` after publish.
   *     `channel.publish()`'s synchronous boolean return only reflects
   *     local write-buffer back-pressure — it says nothing about whether
   *     the broker actually received and durably queued the message.
   *     Without this, `markSent`/ack-style decisions made on that boolean
   *     alone can declare a message "sent" when the broker accepted the
   *     TCP write but crashed before persisting it.
   *  2. THROWS (instead of only logging a warning) when the broker
   *     rejects/nacks the publish, or when the synchronous `publish()`
   *     call itself signals back-pressure. This is what allows
   *     `MessagingController.handleMessage`'s retry branch to know its
   *     retry copy was NOT safely scheduled, and therefore correctly
   *     avoid acking (and thus permanently discarding) the original
   *     message — see that method's "Production-readiness fix:
   *     publish-before-ack" comment for the consumer-side half of this
   *     fix. Silently logging and continuing, as before, gave the caller
   *     no way to distinguish "scheduled" from "silently dropped".
   */
  async publishToRetry(
    payload: Buffer,
    headers: amqplib.MessagePropertyHeaders & Partial<RetryHeaders>,
    attemptNumber: number,
    correlationId?: string,
  ): Promise<void> {
    const ch = await this.getChannel();
    const ttl = retryDelayMs(attemptNumber);
    // Explicitly carry forward the W3C trace context across the retry hop.
    // amqplib's auto-instrumentation also injects traceparent on publish,
    // but we do it explicitly here so the ORIGINAL producer trace survives
    // even if the active context at publish time has changed (e.g. this
    // call runs outside the consumer span that received the message).
    const headersWithTrace = injectTraceContext(headers);

    const published = ch.publish(EXCHANGES.MAIN, ROUTING_KEYS.RETRY, payload, {
      persistent: true,
      headers: headersWithTrace,
      expiration: String(ttl), // per-message TTL (ms) as string per AMQP spec
      contentType: 'application/json',
    });

    if (!published) {
      // Local write-buffer back-pressure. The message was NOT queued for
      // sending — this is a real failure to schedule the retry, not a
      // warning to shrug off.
      throw new Error(
        `Retry publish back-pressured (local buffer full) for attempt=${attemptNumber}, correlationId=${correlationId ?? 'unknown'}`,
      );
    }

    // Wait for the broker to actually confirm (ack) this publish, not
    // just accept it into the local write buffer. Rejects if the broker
    // nacks the publish (e.g. an internal broker error, or — for a
    // quorum queue once configured per the review's RabbitMQ-readiness
    // recommendation — insufficient replica acknowledgment).
    await ch.waitForConfirms();

    this.logger.log(
      `Message scheduled for retry (attempt=${attemptNumber}, ttl=${ttl}ms)`,
      RetryPublisherService.name,
      correlationId,
    );
  }

  private async getChannel(): Promise<amqplib.ConfirmChannel> {
    if (this.channel) return this.channel;
    if (this.connecting) {
      await this.connecting;
      return this.channel!;
    }

    this.connecting = this.connect();
    await this.connecting;
    return this.channel!;
  }

  private async connect(): Promise<void> {
    const url =
      this.configService.get<string>('RABBITMQ_URL') ??
      'amqp://guest:guest@localhost:5672';

    this.connection = await amqplib.connect(url);
    // Confirm channel, not a plain channel — see publishToRetry's doc
    // comment for why this matters: it's what makes waitForConfirms()
    // available, which is the actual broker-durability guarantee this
    // service now relies on.
    this.channel = await this.connection.createConfirmChannel();

    this.connection.on('error', (err: Error) => {
      this.logger.error(
        `RetryPublisher connection error: ${err.message}`,
        err.stack,
        RetryPublisherService.name,
      );
      this.channel = undefined;
      this.connection = undefined;
      this.connecting = undefined;
    });

    this.connection.on('close', () => {
      this.logger.warn(
        'RetryPublisher connection closed',
        RetryPublisherService.name,
      );
      this.channel = undefined;
      this.connection = undefined;
      this.connecting = undefined;
    });
  }
}
