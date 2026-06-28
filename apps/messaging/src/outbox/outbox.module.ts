import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OutboxEvent } from '../entities/outbox-event.entity';
import { OutboxRelayService } from './outbox-relay.service';
import { OutboxTransactionService } from './outbox-transaction.service';
import { OutboxAdminService } from './outbox-admin.service';
import { OutboxAdminController } from './outbox-admin.controller';

/**
 * OutboxModule
 *
 * Provides the full transactional-outbox stack:
 *
 *  OutboxTransactionService  — atomic business-write + event insert
 *  OutboxRelayService        — background poller / RabbitMQ publisher
 *  OutboxAdminService        — ops recovery (replay failed rows)
 *  OutboxAdminController     — POST /internal/outbox/* HTTP surface
 *
 * Only `OutboxTransactionService` and `OutboxAdminService` are exported
 * because:
 *  - `OutboxTransactionService` is needed by `MessagingService`.
 *  - `OutboxAdminService` may be needed by future admin modules.
 *  - `OutboxRelayService` is self-contained (starts its own polling loop
 *    via OnModuleInit) and should not be called from outside this module.
 */
@Module({
  imports: [TypeOrmModule.forFeature([OutboxEvent])],
  controllers: [OutboxAdminController],
  providers: [OutboxTransactionService, OutboxRelayService, OutboxAdminService],
  exports: [OutboxTransactionService, OutboxAdminService],
})
export class OutboxModule {}
