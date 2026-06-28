import { Module, MiddlewareConsumer, NestModule } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import {
  LoggerModule,
  MetricsModule,
  TracingModule,
  CorrelationIdMiddleware,
  LoggingMiddleware,
} from '@app/common';
import { createEventLifecyclePublisherProvider } from '@app/contracts';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { GatewayHealthModule } from './health/health.module';
import { GatewayOutboxEvent } from './entities/gateway-outbox-event.entity';
import { GatewayOutboxModule } from './outbox/gateway-outbox.module';

/**
 * AppModule (gateway) — Producer Reliability Stage
 *
 * Changes (CRITICAL ISSUE #1 fix — Producer Reliability Gap):
 *
 *  1. `ClientsModule.register([{ name: 'MESSAGING_SERVICE', ... }])` has
 *     been REMOVED. The gateway no longer holds a live `ClientProxy`
 *     connection to RabbitMQ at all, and no longer publishes to it
 *     directly from an HTTP request handler. `AppController` now writes
 *     to `gateway_outbox_events` via `GatewayOutboxTransactionService`
 *     (a Postgres transaction, already durable before the HTTP response
 *     is sent) instead. `GatewayOutboxRelayService` — started by
 *     `GatewayOutboxModule`, completely decoupled from the HTTP request
 *     path — owns the only AMQP connection this service makes, and is
 *     the sole thing that actually talks to RabbitMQ. This is the
 *     architectural core of the fix: an HTTP request that wants to emit
 *     an event no longer needs RabbitMQ to be reachable AT ALL to
 *     durably accept that request.
 *
 *  2. `TypeOrmModule.forRootAsync` added — the gateway now has its own
 *     Postgres connection (same physical database as the messaging
 *     service, same `DB_*` env vars already provided via `.env`/
 *     `.env.test`/`.env.reliability`, just a different table:
 *     `gateway_outbox_events`). No new infrastructure container.
 *
 *  3. `GatewayOutboxModule` added — see that module's doc comment.
 *
 * Import order note: `GatewayOutboxModule` is listed after the database
 * registration (as it must be, since `TypeOrmModule.forFeature` inside it
 * depends on the connection already being configured) and before
 * `GatewayHealthModule`, matching the messaging app's existing convention
 * of infra-first, feature-modules-second, health-last.
 */
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    LoggerModule,
    TracingModule,
    MetricsModule,

    // ── Database ───────────────────────────────────────────────────────
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        type: 'postgres',
        host: configService.get<string>('DB_HOST'),
        port: Number.parseInt(configService.get<string>('DB_PORT') ?? '5432'),
        username: configService.get<string>('DB_USERNAME'),
        password: configService.get<string>('DB_PASSWORD'),
        database: configService.get<string>('DB_NAME'),
        // Table is created/evolved exclusively via migrations
        // (apps/gateway/src/migrations), never via synchronize.
        entities: [GatewayOutboxEvent],
        synchronize: false,
        logging: false,
        retryAttempts: 5,
        retryDelay: 3_000,
      }),
    }),
    TypeOrmModule.forFeature([GatewayOutboxEvent]),

    // ── Producer reliability (CRITICAL ISSUE #1) ─────────────────────────
    GatewayOutboxModule,

    GatewayHealthModule,
  ],
  controllers: [AppController],
  providers: [AppService, createEventLifecyclePublisherProvider('gateway')],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    consumer
      // Order matters: CorrelationId first so the ID is available to the
      // logger; LoggingMiddleware second so it reads the already-set header.
      .apply(CorrelationIdMiddleware, LoggingMiddleware)
      .forRoutes('*');
  }
}
