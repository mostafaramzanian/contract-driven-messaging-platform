// MUST be the first import: initializes OpenTelemetry auto-instrumentation
// (http, express, amqplib) before any of those modules are required
// elsewhere in the dependency graph.
import '@app/common/tracing/otel-bootstrap';

import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { PinoLoggerService } from '@app/common';

/**
 * bootstrap — Gateway Service (Evolution Stage)
 *
 * Changes from v2:
 *
 *  1. **Winston removed** — `WinstonModule.createLogger` / `nest-winston`
 *     replaced by `PinoLoggerService` retrieved from the DI container.
 *     All structured fields (correlationId, traceId, service, operation)
 *     are now emitted as Pino NDJSON records rather than Winston JSON.
 *
 *  2. `PinoLoggerService` is retrieved via `app.get()` after the app is
 *     created (DI is fully resolved at that point) and passed to
 *     `app.useLogger()` so NestJS-internal log calls also go through Pino.
 *
 * Everything else (CORS, global prefix, port, shutdown hooks) is unchanged.
 */
async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    // Suppress default NestJS logger during startup; replaced by Pino below.
    logger: ['error'],
    abortOnError: true,
  });

  // ── Wire Pino as the application-wide logger ─────────────────────────
  const pinoLoggerService = app.get(PinoLoggerService);
  app.useLogger(pinoLoggerService);

  // ── CORS ─────────────────────────────────────────────────────────────
  const isProduction = process.env.NODE_ENV === 'production';
  const allowedOrigins = process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(',')
    : ['http://localhost:8080'];

  app.enableCors({
    origin: isProduction ? allowedOrigins : '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
  });

  // ── Global prefix ─────────────────────────────────────────────────────
  app.setGlobalPrefix('api');

  // ── Graceful shutdown ─────────────────────────────────────────────────
  app.enableShutdownHooks();

  const port = process.env.PORT ?? 3005;
  await app.listen(port, '0.0.0.0');

  pinoLoggerService.log(`Gateway started and listening on port ${port}`, {
    service: 'gateway',
    operation: 'bootstrap',
  });
}

void bootstrap();
