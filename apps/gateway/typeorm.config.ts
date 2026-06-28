import 'dotenv/config';
import { DataSource } from 'typeorm';
import { GatewayOutboxEvent } from './src/entities/gateway-outbox-event.entity';

/**
 * GatewayDataSource — TypeORM CLI DataSource for the gateway app.
 *
 * Used exclusively by the TypeORM CLI for:
 *   npm run migration:generate:gateway -- -n <MigrationName>
 *   npm run migration:run:gateway
 *   npm run migration:revert:gateway
 *
 * NOT used by the NestJS application at runtime (the gateway app uses
 * `TypeOrmModule.forRootAsync` in `AppModule`, which creates its own
 * DataSource internally — see `apps/gateway/src/app.module.ts`).
 *
 * Deliberately mirrors `apps/messaging/typeorm.config.ts` field-for-field
 * (same `DB_HOST`/`DB_PORT`/`DB_USERNAME`/`DB_PASSWORD`/`DB_NAME` env vars,
 * same physical database) — the gateway and messaging services share one
 * Postgres database, each owning its own non-overlapping set of tables.
 */
export const GatewayDataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST ?? 'localhost',
  port: Number.parseInt(process.env.DB_PORT ?? '5432'),
  username: process.env.DB_USERNAME ?? 'admin',
  password: process.env.DB_PASSWORD ?? '',
  database: process.env.DB_NAME ?? 'showcase_db',
  entities: [GatewayOutboxEvent],
  migrations: ['apps/gateway/src/migrations/*.ts'],
  synchronize: false,
  logging: true,
});
