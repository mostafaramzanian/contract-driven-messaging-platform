import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqplib from 'amqplib';
import { QUEUES } from './topology';
import {
  MetricsService,
  TracingService,
  extractTraceContext,
} from '@app/common';
import { context as otelContext } from '@opentelemetry/api';

/**
 * DlqConsumerService
 *
 * Standalone amqplib consumer on the dead-letter queue (`messaging.dlq`).
 * NestJS's built-in RMQ transport can only bind one @MessagePattern handler
 * per queue, so the DLQ is consumed via a raw amqplib channel instead.
 *
 * Responsibilities:
 *  1. Log structured DLQ records (for alerting / dashboards).
 *  2. Ack every DLQ message — they have already been retried to exhaustion
 *     or classified as permanent/validation errors. Re-nacking here would
 *     create an infinite DLQ loop.
 *  3. (Future) Persist to a `failed_messages` audit table.
 *
 * Poison-message loop prevention:
 *  - Messages in the DLQ are always acked regardless of their content.
 *  - The topology never configures a DLX on messaging.dlq itself.
 */
@Injectable()
export class DlqConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DlqConsumerService.name);
  private connection: amqplib.ChannelModel | undefined;
  private channel: amqplib.Channel | undefined;

  constructor(
    private readonly configService: ConfigService,
    private readonly metrics: MetricsService,
    private readonly tracing: TracingService,
  ) {}

  async onModuleInit(): Promise<void> {
    const url =
      this.configService.get<string>('RABBITMQ_URL') ??
      'amqp://guest:guest@localhost:5672';

    try {
      this.connection = await amqplib.connect(url);
      this.channel = await this.connection.createChannel();

      // Prefetch 1 so we process DLQ messages sequentially.
      await this.channel.prefetch(1);

      await this.channel.consume(
        QUEUES.DLQ,
        (msg) => this.handleDlqMessage(msg),
        { noAck: false },
      );

      this.logger.log(
        `DLQ consumer started on queue: ${QUEUES.DLQ}`,
        DlqConsumerService.name,
      );
    } catch (err) {
      const stack = err instanceof Error ? err.stack : String(err);
      this.logger.error(
        'Failed to start DLQ consumer',
        stack,
        DlqConsumerService.name,
      );
      throw err;
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.channel?.close();
      await this.connection?.close();
    } catch {
      /* ignore */
    }
  }

  private handleDlqMessage(msg: amqplib.ConsumeMessage | null): void {
    if (!msg) {
      // Broker sent null — consumer was cancelled.
      this.logger.warn(
        'DLQ consumer cancelled by broker',
        DlqConsumerService.name,
      );
      return;
    }

    const headers: Record<string, unknown> = msg.properties.headers ?? {};
    const retryCount: number =
      typeof headers['x-retry-count'] === 'number'
        ? headers['x-retry-count']
        : 0;
    const errorClass: string =
      typeof headers['x-error-class'] === 'string'
        ? headers['x-error-class']
        : 'UNKNOWN';
    const firstError: string =
      typeof headers['x-first-error'] === 'string'
        ? headers['x-first-error']
        : 'unknown';
    const failedAt: string =
      typeof headers['x-failed-at'] === 'string'
        ? headers['x-failed-at']
        : 'unknown';
    const correlationId: string | undefined =
      typeof headers['x-correlation-id'] === 'string'
        ? headers['x-correlation-id']
        : undefined;

    // Parse the original death chain injected by RabbitMQ
    const xDeath = headers['x-death'] as
      | Array<Record<string, unknown>>
      | undefined;
    const originalQueue: string =
      typeof xDeath?.[0]?.['queue'] === 'string'
        ? xDeath[0]['queue']
        : 'unknown';
    const deathReason: string =
      typeof xDeath?.[0]?.['reason'] === 'string'
        ? xDeath[0]['reason']
        : 'unknown';

    let parsedPayload: unknown = null;
    try {
      parsedPayload = JSON.parse(msg.content.toString());
    } catch {
      parsedPayload = msg.content.toString();
    }

    // Structured log record — machine-parseable for alerting
    this.logger.error(
      JSON.stringify({
        dlq: true,
        correlationId,
        errorClass,
        retryCount,
        firstError,
        failedAt,
        originalQueue,
        deathReason,
        routingKey: msg.fields.routingKey,
        payload: parsedPayload,
      }),
      undefined,
      DlqConsumerService.name,
    );

    // Run the span/metric recording inside the extracted trace context so
    // this DLQ landing event is attached to the SAME trace as the original
    // producer request, rather than starting a disconnected root trace.
    const extractedCtx = extractTraceContext(headers);
    otelContext.with(extractedCtx, () => {
      this.tracing.addEvent('dlq_message_landed', {
        correlationId: correlationId ?? 'unknown',
        errorClass,
        retryCount,
        originalQueue,
        deathReason,
      });
    });

    this.metrics.dlqMessagesTotal.inc({
      service: 'messaging',
      error_class: errorClass,
      original_queue: originalQueue,
    });

    // Always ack — poison messages must not loop back.
    this.channel!.ack(msg);
  }
}
