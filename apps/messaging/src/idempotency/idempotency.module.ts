import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProcessedEvent } from '../entities/processed-event.entity';
import { IdempotencyService } from './idempotency.service';

/**
 * IdempotencyModule
 *
 * Provides `IdempotencyService` to any module that imports it.
 * Depends on TypeORM being configured at the application level
 * (via `TypeOrmModule.forRootAsync` in `MessagingModule`).
 *
 * Import order note: this module must be imported AFTER TypeOrmModule
 * in the root MessagingModule to ensure the DataSource is available
 * when TypeOrmModule.forFeature([ProcessedEvent]) bootstraps.
 */
@Module({
  imports: [
    // Register the ProcessedEvent repository in this module's scope.
    // The root TypeOrmModule.forRootAsync in MessagingModule supplies
    // the underlying DataSource connection.
    TypeOrmModule.forFeature([ProcessedEvent]),
  ],
  providers: [IdempotencyService],
  exports: [IdempotencyService],
})
export class IdempotencyModule {}
