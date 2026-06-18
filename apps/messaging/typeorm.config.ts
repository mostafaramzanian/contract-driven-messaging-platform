import 'dotenv/config';
import { DataSource } from 'typeorm';
import { Message } from './src/entities/message.entity';

export const AppDataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST ?? 'localhost',
  port: Number.parseInt(process.env.DB_PORT ?? '5432'),
  username: process.env.DB_USERNAME ?? 'admin',
  password: process.env.DB_PASSWORD ?? '',
  database: process.env.DB_NAME ?? 'showcase_db',
  entities: [Message],
  migrations: ['apps/messaging/src/migrations/*.ts'],
  synchronize: false,
  logging: true,
});
