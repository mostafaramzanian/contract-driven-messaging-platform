import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { LoggerModule, MetricsModule, TracingModule } from '@app/common';
import { createEventLifecyclePublisherProvider } from '@app/contracts';
import { MessagingController } from './messaging.controller';
import { MessagingService } from './messaging.service';
import { Message } from './entities/message.entity';
import { ProcessedEvent } from './entities/processed-event.entity';
import { OutboxEvent } from './entities/outbox-event.entity';
import { EventAttempt } from './entities/event-attempt.entity';
import { MessagesModule } from './messages/messages.module';
import { ReliabilityModule } from './reliability/reliability.module';
import { RetryAttemptTrackerService } from './reliability/retry-attempt-tracker.service';
import { IdempotencyModule } from './idempotency/idempotency.module';
import { OutboxModule } from './outbox/outbox.module';
import { MessagingHealthModule } from './health/health.module';

/**
 * MessagingModule (updated — Evolution Stage)
 *
 * Changes from the previous iteration:
 *
 *  1. `ProcessedEvent` added to TypeORM entities array and
 *     `TypeOrmModule.forFeature([..., ProcessedEvent])` added so the
 *     idempotency repository is available within this module scope.
 *
 *  2. `IdempotencyModule` imported so `IdempotencyService` is injectable
 *     into `MessagingController`.
 *
 *  3. `MessagingHealthModule` imported to expose `/internal/health/ready`
 *     and `/internal/health/live` on the HTTP server that is started
 *     alongside the RMQ microservice in `main.ts` (hybrid-app pattern).
 *
 *  4. `LoggerModule` (now Pino-backed) is still imported first; it is
 *     @Global() so all nested modules receive `PinoLoggerService` without
 *     explicitly importing `LoggerModule`.
 *
 *  5. `ReliabilityModule` remains first in the import list so
 *     `TopologyService.onModuleInit()` runs before the RMQ transport
 *     starts consuming — see original comment below.
 *
 * Import order note (preserved from v2):
 *   NestJS instantiates providers in import order for modules with no
 *   circular dependencies.  `ReliabilityModule` must be listed first so
 *   `TopologyService.onModuleInit()` asserts the DLX/DLQ/retry topology
 *   before the RMQ consumer loop starts.
 */
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),

    // ── Infrastructure (must be first for topology-before-consumer ordering)
    LoggerModule,
    TracingModule,
    MetricsModule,
    ReliabilityModule,

    // ── Database ─────────────────────────────────────────────────────────
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
        // All four entities must be listed here for TypeORM to know about
        // them. Their tables are created/evolved exclusively via migrations.
        entities: [Message, ProcessedEvent, OutboxEvent, EventAttempt],
        synchronize: false,
        logging: false,
        retryAttempts: 5,
        retryDelay: 3_000,
      }),
    }),
    TypeOrmModule.forFeature([
      Message,
      ProcessedEvent,
      OutboxEvent,
      EventAttempt,
    ]),

    // ── Feature modules ───────────────────────────────────────────────
    MessagesModule,
    IdempotencyModule,
    OutboxModule,

    // ── Observability ─────────────────────────────────────────────────
    // Registers /internal/health/* on the hybrid-app HTTP server.
    MessagingHealthModule,
  ],
  controllers: [MessagingController],
  providers: [
    MessagingService,
    RetryAttemptTrackerService,
    createEventLifecyclePublisherProvider('messaging'),
  ],
  exports: [MessagingService],
})
export class MessagingModule {}
