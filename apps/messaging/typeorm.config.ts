import 'dotenv/config';
import { DataSource } from 'typeorm';
import { Message } from './src/entities/message.entity';
import { ProcessedEvent } from './src/entities/processed-event.entity';
import { OutboxEvent } from './src/entities/outbox-event.entity';
import { EventAttempt } from './src/entities/event-attempt.entity';

/**
 * AppDataSource — TypeORM CLI DataSource
 *
 * Used exclusively by the TypeORM CLI for:
 *   npm run migration:generate -- -n <MigrationName>
 *   npm run migration:run
 *   npm run migration:revert
 *
 * NOT used by the NestJS application at runtime (the app uses
 * TypeOrmModule.forRootAsync in MessagingModule, which creates its own
 * DataSource internally).
 *
 * Changes from v2:
 *  - `ProcessedEvent` added to `entities` so the CLI can diff it during
 *    `migration:generate`.
 *  - `EventAttempt` added (production-readiness review, migration 006) —
 *    durable, header-independent retry-count tracking.
 */
export const AppDataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST ?? 'localhost',
  port: Number.parseInt(process.env.DB_PORT ?? '5432'),
  username: process.env.DB_USERNAME ?? 'admin',
  password: process.env.DB_PASSWORD ?? '',
  database: process.env.DB_NAME ?? 'showcase_db',
  entities: [Message, ProcessedEvent, OutboxEvent, EventAttempt],
  migrations: ['apps/messaging/src/migrations/*.ts'],
  synchronize: false,
  logging: true,
});
