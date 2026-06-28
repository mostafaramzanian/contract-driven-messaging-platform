/**
 * rabbitmq-client.ts
 *
 * Low-level RabbitMQ control utilities for reliability testing.
 * Provides direct AMQP operations that bypass the application layer:
 *  - Publishing raw messages to queues/exchanges
 *  - Consuming messages (with manual ack) to verify delivery
 *  - Inspecting queue depth / message counts
 *  - Purging queues between tests
 *  - Interacting with the RabbitMQ Management HTTP API
 *
 * All connections are plain amqplib — no NestJS, no framework abstractions.
 * This keeps tests honest: infrastructure is real, nothing is mocked.
 */

import * as amqplib from 'amqplib';
import {
  EXCHANGES,
  QUEUES,
  ROUTING_KEYS,
} from '../../apps/messaging/src/reliability/topology';

export const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';

const MANAGEMENT_BASE =
  process.env.RABBITMQ_MANAGEMENT_URL ?? 'http://127.0.0.1:15672';

export interface ConsumedMessage {
  content: unknown;
  headers: Record<string, unknown>;
  routingKey: string;
  deliveryTag: bigint;
  redelivered: boolean;
}

/**
 * RabbitMqTestClient
 *
 * Each reliability test creates its own instance. Connects once, reuses the
 * connection/channel for the test's lifetime, then closes in afterAll/afterEach.
 */
export class RabbitMqTestClient {
  private connection?: amqplib.ChannelModel;
  private channel?: amqplib.Channel;
  private confirmChannel?: amqplib.ConfirmChannel;

  async connect(): Promise<void> {
    this.connection = await amqplib.connect(RABBITMQ_URL);
    this.channel = await this.connection.createChannel();
    this.confirmChannel = await this.connection.createConfirmChannel();
  }

  async disconnect(): Promise<void> {
    try {
      await this.channel?.close();
      await this.confirmChannel?.close();
      await this.connection?.close();
    } catch {
      // Ignore errors on disconnect — connection may already be dead in outage tests
    }
  }

  getChannel(): amqplib.Channel {
    if (!this.channel) throw new Error('RabbitMqTestClient: not connected');
    return this.channel;
  }

  // ── Publishing ──────────────────────────────────────────────────────────

  /**
   * Publish a message to the main work exchange with publisher confirms.
   * Returns true when the broker acknowledges durability.
   */
  async publishToWork(
    payload: unknown,
    headers: Record<string, string | number | boolean> = {},
  ): Promise<void> {
    if (!this.confirmChannel) throw new Error('Not connected');
    const body = Buffer.from(JSON.stringify(payload));
    this.confirmChannel.publish(EXCHANGES.MAIN, ROUTING_KEYS.WORK, body, {
      persistent: true,
      contentType: 'application/json',
      headers,
    });
    await this.confirmChannel.waitForConfirms();
  }

  /**
   * Publish directly to the DLQ exchange — simulates a message that has
   * already been dead-lettered (for DLQ replay tests).
   */
  async publishToDlq(
    payload: unknown,
    headers: Record<string, string | number | boolean> = {},
  ): Promise<void> {
    if (!this.confirmChannel) throw new Error('Not connected');
    const body = Buffer.from(JSON.stringify(payload));
    this.confirmChannel.publish(EXCHANGES.DLQ, ROUTING_KEYS.DEAD, body, {
      persistent: true,
      contentType: 'application/json',
      headers,
    });
    await this.confirmChannel.waitForConfirms();
  }

  // ── Consuming ───────────────────────────────────────────────────────────

  /**
   * Drain up to `maxMessages` from a queue using basic.get (polling, not push).
   * Returns immediately with all available messages — does NOT block.
   * Caller must ack/nack each returned message.
   */
  async drainQueue(
    queue: string,
    maxMessages = 100,
  ): Promise<ConsumedMessage[]> {
    const ch = this.getChannel();
    const messages: ConsumedMessage[] = [];

    for (let i = 0; i < maxMessages; i++) {
      const msg = await ch.get(queue, { noAck: false });
      if (!msg) break;

      messages.push({
        content: JSON.parse(msg.content.toString()),
        headers: msg.properties.headers as Record<string, unknown>,
        routingKey: msg.fields.routingKey,
        deliveryTag: msg.fields.deliveryTag,
        redelivered: msg.fields.redelivered,
      });

      ch.ack(msg);
    }

    return messages;
  }

  /**
   * Wait until at least one message appears in `queue` and return it.
   * Uses a push consumer under the hood — resolves immediately on arrival.
   */
  async waitForMessage(
    queue: string,
    timeoutMs = 30_000,
  ): Promise<ConsumedMessage> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new Error(`waitForMessage(${queue}): timed out after ${timeoutMs}ms`),
        );
      }, timeoutMs);

      this.channel!.consume(
        queue,
        (msg) => {
          if (!msg) return;
          clearTimeout(timer);
          this.channel!.ack(msg);
          resolve({
            content: JSON.parse(msg.content.toString()),
            headers: msg.properties.headers as Record<string, unknown>,
            routingKey: msg.fields.routingKey,
            deliveryTag: msg.fields.deliveryTag,
            redelivered: msg.fields.redelivered,
          });
        },
        { noAck: false },
      ).catch(reject);
    });
  }

  /**
   * Wait for N messages from queue, returning them in arrival order.
   */
  async waitForNMessages(
    queue: string,
    n: number,
    timeoutMs = 45_000,
  ): Promise<ConsumedMessage[]> {
    const results: ConsumedMessage[] = [];

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new Error(
            `waitForNMessages(${queue}, ${n}): got ${results.length}/${n} in ${timeoutMs}ms`,
          ),
        );
      }, timeoutMs);

      this.channel!.consume(
        queue,
        (msg) => {
          if (!msg) return;
          this.channel!.ack(msg);
          results.push({
            content: JSON.parse(msg.content.toString()),
            headers: msg.properties.headers as Record<string, unknown>,
            routingKey: msg.fields.routingKey,
            deliveryTag: msg.fields.deliveryTag,
            redelivered: msg.fields.redelivered,
          });
          if (results.length >= n) {
            clearTimeout(timer);
            resolve(results);
          }
        },
        { noAck: false },
      ).catch(reject);
    });
  }

  // ── Queue inspection ────────────────────────────────────────────────────

  /** Returns the current message count in a queue via the management API */
  async getQueueDepth(queue: string): Promise<number> {
    try {
      const res = await fetch(
        `${MANAGEMENT_BASE}/api/queues/%2F/${encodeURIComponent(queue)}`,
        {
          headers: {
            Authorization:
              'Basic ' + Buffer.from('guest:guest').toString('base64'),
          },
        },
      );
      if (!res.ok) return 0;
      const data = (await res.json()) as { messages?: number };
      return data.messages ?? 0;
    } catch {
      return 0;
    }
  }

  /** Purge all messages from a queue */
  async purgeQueue(queue: string): Promise<void> {
    await this.channel?.purgeQueue(queue);
  }

  /** Purge work, retry, DLQ, and domain-event-audit queues between tests */
  async purgeAllQueues(): Promise<void> {
    await this.purgeQueue(QUEUES.WORK);
    await this.purgeQueue(QUEUES.RETRY);
    await this.purgeQueue(QUEUES.DLQ);
    await this.purgeQueue(QUEUES.EVENTS_AUDIT);
  }

  /** Check if the broker is reachable */
  async isReachable(): Promise<boolean> {
    try {
      const conn = await amqplib.connect(RABBITMQ_URL);
      await conn.close();
      return true;
    } catch {
      return false;
    }
  }
}

// ── Management API helpers ────────────────────────────────────────────────

/**
 * Restart a RabbitMQ application (stops and starts the Erlang application
 * within the running broker container) via the management HTTP API.
 * This simulates a broker restart without stopping the container.
 *
 * In tests that use `docker stop/start` instead, use the Docker helpers below.
 */
export async function rabbitmqAppStop(): Promise<void> {
  const res = await fetch(`${MANAGEMENT_BASE}/api/aliveness-test/%2F`, {
    headers: {
      Authorization: 'Basic ' + Buffer.from('guest:guest').toString('base64'),
    },
  });
  if (!res.ok)
    throw new Error(`RabbitMQ management not reachable: ${res.status}`);
}

/**
 * Poll until RabbitMQ management API responds.
 */
export async function waitForRabbitMqManagementReady(
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${MANAGEMENT_BASE}/api/overview`, {
        headers: {
          Authorization:
            'Basic ' + Buffer.from('guest:guest').toString('base64'),
        },
      });
      if (res.ok) return;
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 1_000));
  }
  throw new Error(`RabbitMQ management API not ready after ${timeoutMs}ms`);
}

export async function waitForRabbitMqAmqpReady(
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const conn = await amqplib.connect(RABBITMQ_URL);
      await conn.close();
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 1_000));
    }
  }
  throw new Error(`RabbitMQ AMQP not ready after ${timeoutMs}ms`);
}
