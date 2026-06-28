import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  HealthIndicator,
  HealthIndicatorResult,
  HealthCheckError,
} from '@nestjs/terminus';
import * as amqplib from 'amqplib';

/**
 * RabbitMQHealthIndicator
 *
 * Custom `@nestjs/terminus` health indicator that verifies the messaging
 * service can establish and close an AMQP connection.
 *
 * ## Why a connection per check?
 *
 * The existing reliability infrastructure (TopologyService,
 * DlqConsumerService, RetryPublisherService) each own their own
 * long-lived amqplib connections that are not exposed as injectable
 * tokens.  Injecting TopologyService here to borrow its connection would
 * create a tight coupling between unrelated concerns.
 *
 * For health checks the operational overhead of a brief connect → close
 * cycle is acceptable (health endpoints are sampled at 30 s by
 * Kubernetes, not called in the hot path) and gives a true liveness
 * signal: if the broker is reachable and accepting new connections, the
 * service can recover from a temporary connection loss.
 *
 * ## Timeout
 *
 * `amqplib.connect` uses a 10 s socket timeout by default.  We wrap the
 * call in a `Promise.race` against a shorter deadline (configurable via
 * `HEALTH_RABBIT_TIMEOUT_MS`, default 5 s) so a slow broker cannot cause
 * the health endpoint to time out at the Kubernetes probe level.
 */
@Injectable()
export class RabbitMQHealthIndicator extends HealthIndicator {
  /** Max ms to wait for the AMQP connect handshake to complete. */
  private readonly timeoutMs: number;

  constructor(private readonly configService: ConfigService) {
    super();
    this.timeoutMs = Number(
      this.configService.get<string>('HEALTH_RABBIT_TIMEOUT_MS', '5000'),
    );
  }

  /**
   * Attempt a connection to the configured AMQP broker and immediately
   * close it.  Returns `{ [key]: { status: 'up' } }` on success.
   *
   * @param key  The indicator key used in the health-check response JSON.
   */
  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const url =
      this.configService.get<string>('RABBITMQ_URL') ??
      'amqp://guest:guest@localhost:5672';

    let connection: amqplib.ChannelModel | undefined;

    try {
      connection = await this.connectWithTimeout(url, this.timeoutMs);
      await connection.close();

      return this.getStatus(key, true, {
        url: this.sanitiseUrl(url),
      });
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : 'RabbitMQ connection failed';

      throw new HealthCheckError(
        `${key} failed`,
        this.getStatus(key, false, {
          url: this.sanitiseUrl(url),
          error: message,
        }),
      );
    } finally {
      // Belt-and-suspenders: close if the error happened after connect()
      // but before our own close() call above.
      try {
        await connection?.close();
      } catch {
        /* already closed */
      }
    }
  }

  // ── Private helpers ──────────────────────────────────────────────────

  private connectWithTimeout(
    url: string,
    timeoutMs: number,
  ): Promise<amqplib.ChannelModel> {
    return Promise.race([
      amqplib.connect(url),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error(`Connection timeout after ${timeoutMs}ms`)),
          timeoutMs,
        ),
      ),
    ]);
  }

  /** Remove credentials from the URL before logging / returning. */
  private sanitiseUrl(url: string): string {
    try {
      const parsed = new URL(url);
      parsed.password = '***';
      parsed.username = '***';
      return parsed.toString();
    } catch {
      return 'amqp://***';
    }
  }
}
