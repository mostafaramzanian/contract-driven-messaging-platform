import { Controller, Get } from '@nestjs/common';
import {
  HealthCheck,
  HealthCheckService,
  HttpHealthIndicator,
} from '@nestjs/terminus';
import { ConfigService } from '@nestjs/config';

/**
 * GatewayHealthController
 *
 * Exposes liveness and readiness probes for the gateway service.
 *
 *  GET /health/ready
 *    Readiness: verifies the gateway's own HTTP server is up and the
 *    messaging service's health endpoint is reachable.  The gateway is
 *    considered "ready" when it can route to downstream services.
 *
 *  GET /health/live
 *    Liveness: returns 200 as long as the Node process is alive.
 *    No downstream checks — see the design note in the messaging
 *    health controller for the reasoning.
 *
 * ## Why check messaging from the gateway?
 *
 * The gateway routes all event publishing through RabbitMQ; it does not
 * hold a direct TCP connection to the messaging service at runtime.
 * Checking the messaging service's `/internal/health/ready` endpoint from
 * the gateway readiness probe provides an end-to-end signal that the
 * complete pipeline is ready to process events.
 *
 * ## Kubernetes probe configuration (example)
 *
 * ```yaml
 * livenessProbe:
 *   httpGet:
 *     path: /health/live
 *     port: 3005
 *   initialDelaySeconds: 10
 *   periodSeconds: 10
 *
 * readinessProbe:
 *   httpGet:
 *     path: /health/ready
 *     port: 3005
 *   initialDelaySeconds: 15
 *   periodSeconds: 30
 * ```
 */
@Controller('health')
export class GatewayHealthController {
  private readonly messagingHealthUrl: string;

  constructor(
    private readonly health: HealthCheckService,
    private readonly http: HttpHealthIndicator,
    configService: ConfigService,
  ) {
    // Internal messaging service health URL — defaults to the Docker Compose
    // service name and port used in docker-compose.yml / docker-compose.test.yml
    this.messagingHealthUrl = configService.get<string>(
      'MESSAGING_HEALTH_URL',
      'http://showcase-messaging:3006/internal/health/ready',
    );
  }

  /**
   * Readiness probe: gateway can process requests AND the messaging
   * service behind it is ready to consume events.
   */
  @Get('ready')
  @HealthCheck()
  readiness() {
    return this.health.check([
      // Check the messaging service's readiness endpoint.
      // On failure this returns 503, causing the load-balancer to stop
      // sending traffic to this gateway instance.
      () => this.http.pingCheck('messaging-service', this.messagingHealthUrl),
    ]);
  }

  /**
   * Liveness probe: the gateway process is alive.
   * No downstream checks — see class-level JSDoc.
   */
  @Get('live')
  @HealthCheck()
  liveness() {
    return this.health.check([]);
  }
}
