import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqplib from 'amqplib';
import { EXCHANGES, QUEUES, ROUTING_KEYS } from '@app/contracts';

/**
 * TopologyService
 *
 * Runs once on module init and idempotently asserts the full RabbitMQ
 * exchange/queue/binding topology. NestJS's built-in RMQ transport creates
 * the queue it reads from, but it cannot declare the DLX, retry queue, or
 * DLQ bindings we need — so we do it here, before the transport connects.
 *
 * All declarations use `{ durable: true }` so topology survives broker
 * restarts without data loss.
 */
@Injectable()
export class TopologyService implements OnModuleInit {
  private readonly logger = new Logger(TopologyService.name);

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit(): Promise<void> {
    const url =
      this.configService.get<string>('RABBITMQ_URL') ??
      'amqp://guest:guest@localhost:5672';

    let connection: amqplib.ChannelModel | undefined;
    try {
      connection = await amqplib.connect(url);
      const channel = await connection.createChannel();

      await this.assertTopology(channel);

      await channel.close();
      this.logger.log(
        'RabbitMQ topology asserted successfully',
        TopologyService.name,
      );
    } catch (err) {
      const stack = err instanceof Error ? err.stack : String(err);
      this.logger.error(
        'Failed to assert RabbitMQ topology',
        stack,
        TopologyService.name,
      );
      // Re-throw: if topology cannot be asserted, the service must not start.
      throw err;
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch {
          /* ignore close errors */
        }
      }
    }
  }

  private async assertTopology(channel: amqplib.Channel): Promise<void> {
    // ── 1. Dead-letter exchange (fanout) ─────────────────────────────────
    // Attached as x-dead-letter-exchange on messaging.work.
    // Any nacked (requeue=false) or TTL-expired message from messaging.work
    // is forwarded here.
    await channel.assertExchange(EXCHANGES.DLX, 'fanout', { durable: true });
    this.logger.debug(`Exchange asserted: ${EXCHANGES.DLX} (fanout)`);

    // ── 2. DLQ exchange (direct) ──────────────────────────────────────────
    // Receives messages from retry queue after TTL expires OR directly from
    // DLX when retry is exhausted.
    await channel.assertExchange(EXCHANGES.DLQ, 'direct', { durable: true });
    this.logger.debug(`Exchange asserted: ${EXCHANGES.DLQ} (direct)`);

    // ── 3. Primary direct exchange ────────────────────────────────────────
    await channel.assertExchange(EXCHANGES.MAIN, 'direct', { durable: true });
    this.logger.debug(`Exchange asserted: ${EXCHANGES.MAIN} (direct)`);

    // ── 4. Dead-letter queue ──────────────────────────────────────────────
    await channel.assertQueue(QUEUES.DLQ, {
      durable: true,
      arguments: {
        // DLQ itself has no DLX — messages stop here.
        'x-queue-type': 'classic',
      },
    });
    await channel.bindQueue(QUEUES.DLQ, EXCHANGES.DLQ, ROUTING_KEYS.DEAD);
    this.logger.debug(`Queue asserted: ${QUEUES.DLQ}`);

    // ── 5. Retry delay queue ──────────────────────────────────────────────
    // No consumer on this queue. Messages expire here and go back to WORK
    // via x-dead-letter-exchange / x-dead-letter-routing-key.
    await channel.assertQueue(QUEUES.RETRY, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': EXCHANGES.MAIN,
        'x-dead-letter-routing-key': ROUTING_KEYS.WORK,
        // Per-message TTL is set dynamically (exponential back-off), so
        // we do NOT set x-message-ttl here at queue level.
      },
    });
    await channel.bindQueue(QUEUES.RETRY, EXCHANGES.MAIN, ROUTING_KEYS.RETRY);
    this.logger.debug(`Queue asserted: ${QUEUES.RETRY}`);

    // ── 6. Main work queue ────────────────────────────────────────────────
    // x-dead-letter-exchange: any nack(false) or TTL expiry lands in DLX.
    // We then route from DLX → RETRY (transient) or DLQ (permanent).
    // Actually: since DLX is fanout, ALL dead-letters go to DLX first.
    // The DlqConsumerService reads DLQ; retry routing is done in the
    // message handler before nacking by publishing to RETRY exchange instead.
    await channel.assertQueue(QUEUES.WORK, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': EXCHANGES.DLX,
        // No x-dead-letter-routing-key: DLX is fanout, no routing key needed.
      },
    });
    await channel.bindQueue(QUEUES.WORK, EXCHANGES.MAIN, ROUTING_KEYS.WORK);
    this.logger.debug(`Queue asserted: ${QUEUES.WORK}`);

    // ── 7. DLX → DLQ binding ─────────────────────────────────────────────
    // DLX is fanout → every dead-letter goes straight to DLQ.
    // (Retried messages are re-published explicitly to RETRY queue by the
    //  handler; they never touch DLX on their retry path.)
    await channel.bindQueue(QUEUES.DLQ, EXCHANGES.DLX, '');
    this.logger.debug(`Binding: ${EXCHANGES.DLX} (fanout) → ${QUEUES.DLQ}`);

    // ── 8. Domain-event bus (fanout) — Architectural Gap #2 fix ───────────
    // Carries facts about something that already happened (e.g.
    // `MessagePersisted`), produced by this service's own outbox relay via
    // `resolveOutboxRoute()` (see `@app/contracts/topology/topology.ts`).
    // Deliberately NOT bound to `messaging.work` and NOT consumed by
    // `MessagingController` — there is no path by which a domain event
    // published here can re-enter the CreateMessageEvent command queue and
    // trigger a self-generated retry/DLQ loop.
    //
    // `messaging.events.audit` has no `x-dead-letter-exchange` argument: a
    // poison domain event can fail here without ever generating DLQ
    // traffic on the command side, by construction.
    await channel.assertExchange(EXCHANGES.EVENTS, 'fanout', { durable: true });
    this.logger.debug(`Exchange asserted: ${EXCHANGES.EVENTS} (fanout)`);

    await channel.assertQueue(QUEUES.EVENTS_AUDIT, { durable: true });
    await channel.bindQueue(QUEUES.EVENTS_AUDIT, EXCHANGES.EVENTS, '');
    this.logger.debug(
      `Queue asserted: ${QUEUES.EVENTS_AUDIT} ← ${EXCHANGES.EVENTS} (fanout)`,
    );
  }
}
