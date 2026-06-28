// MUST be the first import: initializes OpenTelemetry auto-instrumentation
// (pg, amqplib, http, express) before any of those modules are required
// elsewhere in the dependency graph.
import '@app/common/tracing/otel-bootstrap';

import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { MessagingModule } from './messaging.module';
import { PinoLoggerService } from '@app/common';
import { LoggingInterceptor } from './interceptors/logging.interceptor';

/**
 * bootstrap — Messaging Service (Evolution Stage)
 *
 * ## Architecture change: microservice → hybrid app
 *
 * In the previous iteration the messaging service was a pure NestJS
 * microservice (`NestFactory.createMicroservice`), which meant it had
 * no HTTP server and could not expose health-check endpoints.
 *
 * We now bootstrap a **hybrid application**:
 *  1. `NestFactory.create(MessagingModule)` creates an HTTP server.
 *  2. `app.connectMicroservice(...)` attaches the RMQ transport.
 *  3. `app.startAllMicroservices()` starts the RMQ consumer loop.
 *  4. `app.listen(HEALTH_PORT)` starts the HTTP server for health probes.
 *
 * The HTTP server is intentionally NOT behind Nginx (no public exposure).
 * It is reachable only inside the Docker / Kubernetes network by:
 *  - Kubernetes liveness/readiness probes
 *  - The gateway's `/health/ready` check (via `HttpHealthIndicator`)
 *  - Internal monitoring agents (Prometheus scraping /metrics, future)
 *
 * ## Ports
 *  - HEALTH_PORT (default 3006): HTTP server for /internal/health/*
 *  - No change to the RMQ transport — it does not bind a port.
 *
 * ## Logger
 *  - `PinoLoggerService` is retrieved from DI and set as the global NestJS
 *    logger.  All NestJS-internal log calls (bootstrap events, route
 *    registration, etc.) are emitted through Pino as NDJSON in production
 *    or pino-pretty in development.
 *
 * ## Global interceptor
 *  - `LoggingInterceptor` is registered globally so every RMQ handler
 *    emits structured before/after log records with correlationId, eventId,
 *    messageId, traceId, service, operation, and durationMs.
 *
 * ## Ordering guarantee (preserved from v2)
 *  - `ReliabilityModule` is imported first in `MessagingModule`.
 *  - `TopologyService.onModuleInit()` runs before `startAllMicroservices()`
 *    because NestJS resolves all module initialisation hooks before the
 *    microservice transport begins consuming.
 *  - This guarantees the DLX/DLQ/retry topology exists before the first
 *    message is delivered to our consumer.
 */
async function bootstrap() {
  const rabbitUrl =
    process.env.RABBITMQ_URL ?? 'amqp://guest:guest@showcase-rabbitmq:5672';
  const healthPort = Number.parseInt(process.env.HEALTH_PORT ?? '3006', 10);

  // ── 1. Create the HTTP / DI container ────────────────────────────────
  const app = await NestFactory.create(MessagingModule, {
    // Suppress NestJS's default console logger during startup; we replace
    // it with PinoLoggerService below.  We keep 'error' so fatal bootstrap
    // errors still surface before DI is ready.
    logger: ['error'],
    // Abort if any provider throws during initialisation
    abortOnError: true,
  });

  // ── 2. Wire Pino as the application logger ───────────────────────────
  const pinoLoggerService = app.get(PinoLoggerService);
  app.useLogger(pinoLoggerService);

  // ── Production-readiness fix: graceful shutdown ──────────────────────
  // Without this call, NestJS never invokes OnModuleDestroy on SIGTERM —
  // which previously meant OutboxRelayService.onModuleDestroy() (clears
  // its poll/reaper timers, closes its AMQP connection),
  // RetryPublisherService.onModuleDestroy(), DlqConsumerService's
  // shutdown logic, and OutboxService's cleanup all silently never ran on
  // a container stop / Kubernetes rolling deploy. The process would be
  // hard-killed by the orchestrator instead, with open AMQP connections
  // and any message mid-flight at kill time landing in the worst-case
  // crash window for the idempotency/retry-publish atomicity fixes
  // elsewhere in this service (see OutboxTransactionService and
  // MessagingController.handleMessage's "Production-readiness fix"
  // comments) — a clean shutdown reduces how often that window is
  // actually hit in practice, even though those other fixes are what
  // make hitting it survivable.
  app.enableShutdownHooks();

  // ── 3. Register the global logging interceptor ───────────────────────
  // Constructed manually so the PinoLoggerService instance is shared
  // with the one already wired above.
  app.useGlobalInterceptors(new LoggingInterceptor(pinoLoggerService));

  // ── 4. Attach the RMQ microservice transport ─────────────────────────
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.RMQ,
    options: {
      urls: [rabbitUrl],
      queue: 'messaging.work',
      noAck: false, // manual acknowledgements required
      prefetchCount: 10, // back-pressure: max unacked messages
      queueOptions: {
        durable: true,
        // x-dead-letter-exchange asserted by TopologyService.onModuleInit()
        // which runs before the consumer loop starts.
      },
    },
  });

  // ── 5. Health server prefix ──────────────────────────────────────────
  // All HTTP routes are under /internal to distinguish them from any
  // future external-facing routes and to prevent Nginx from exposing them.
  // MessagingHealthController registers /internal/health/ready and /live.
  // No global prefix is added here because the controller already includes
  // the /internal segment in its @Controller('internal/health') decorator.

  // Disable body-parser for the health-only HTTP server — all probes use
  // GET with no body.
  app.set?.('json spaces', 0);

  // ── 6. Start microservices and HTTP server ───────────────────────────
  await app.startAllMicroservices();
  await app.listen(healthPort, '0.0.0.0');

  pinoLoggerService.log(
    `Messaging service started (RMQ consumer active, health HTTP on :${healthPort})`,
    { service: 'messaging', operation: 'bootstrap' },
  );
}

void bootstrap();
