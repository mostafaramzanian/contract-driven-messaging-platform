import { Controller, Get } from '@nestjs/common';
import {
  HealthCheck,
  HealthCheckService,
  TypeOrmHealthIndicator,
} from '@nestjs/terminus';
import { RabbitMQHealthIndicator } from './rabbitmq-health.indicator';

/**
 * MessagingHealthController
 *
 * Exposes two HTTP endpoints on the messaging service's internal HTTP
 * server (port 3006 by default, configured via `HEALTH_PORT` env var):
 *
 *  GET /internal/health/ready
 *    Readiness probe: reports UP only when all infrastructure dependencies
 *    (PostgreSQL, RabbitMQ) are reachable.  Kubernetes should stop sending
 *    traffic when this returns non-2xx.
 *
 *  GET /internal/health/live
 *    Liveness probe: reports UP as long as the Node process is alive and
 *    the event loop is not stuck.  Intentionally does NOT check external
 *    dependencies — a temporarily unreachable database should not cause a
 *    pod restart, only traffic removal (handled by the readiness probe).
 *
 * ## Response shape (from @nestjs/terminus)
 *
 * ```json
 * {
 *   "status": "ok",
 *   "info": {
 *     "database": { "status": "up" },
 *     "rabbitmq": { "status": "up" }
 *   },
 *   "error": {},
 *   "details": {
 *     "database": { "status": "up" },
 *     "rabbitmq": { "status": "up", "url": "amqp://***:***@localhost:5672/" }
 *   }
 * }
 * ```
 *
 * When a dependency is down `status` is `"error"` and the HTTP status
 * code is 503.
 */
@Controller('internal/health')
export class MessagingHealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly db: TypeOrmHealthIndicator,
    private readonly rabbitmq: RabbitMQHealthIndicator,
  ) {}

  /**
   * Readiness probe: checks PostgreSQL + RabbitMQ.
   * Returns 200 + `{ status: "ok" }` when all checks pass.
   * Returns 503 + `{ status: "error" }` when any check fails.
   */
  @Get('ready')
  @HealthCheck()
  readiness() {
    return this.health.check([
      // TypeORM's pingCheck executes `SELECT 1` against the configured DB
      () => this.db.pingCheck('database'),
      // Custom indicator that dials and immediately closes an AMQP connection
      () => this.rabbitmq.isHealthy('rabbitmq'),
    ]);
  }

  /**
   * Liveness probe: application is alive if the process is running and
   * the event loop is responsive.  No external checks to avoid restarting
   * pods due to transient infrastructure outages.
   */
  @Get('live')
  @HealthCheck()
  liveness() {
    // Empty check array → always returns { status: 'ok' }
    // This is intentional: the point of liveness is "is this process
    // alive and not deadlocked", not "can it reach its dependencies".
    return this.health.check([]);
  }
}
