import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { GatewayOutboxEvent } from '../entities/gateway-outbox-event.entity';
import { GatewayOutboxRelayService } from './gateway-outbox-relay.service';
import { GatewayOutboxTransactionService } from './gateway-outbox-transaction.service';
import { GatewayOutboxAdminService } from './gateway-outbox-admin.service';
import { GatewayOutboxAdminController } from './gateway-outbox-admin.controller';

/**
 * GatewayOutboxModule
 *
 * Producer-side mirror of the messaging service's `OutboxModule`:
 *
 *  GatewayOutboxTransactionService — atomic, durable event-write path
 *                                    used by AppController (replaces the
 *                                    old direct `client.emit()` call)
 *  GatewayOutboxRelayService       — background poller / RabbitMQ
 *                                    publisher (self-contained, starts
 *                                    its own polling loop via
 *                                    OnModuleInit)
 *  GatewayOutboxAdminService       — ops recovery (replay failed rows)
 *  GatewayOutboxAdminController    — POST/GET /internal/outbox/* HTTP surface
 *
 * Only `GatewayOutboxTransactionService` is exported — `AppController` is
 * the only consumer outside this module. `GatewayOutboxRelayService` is
 * self-contained and should not be called from outside this module,
 * exactly like the messaging app's `OutboxRelayService`.
 */
@Module({
  imports: [TypeOrmModule.forFeature([GatewayOutboxEvent])],
  controllers: [GatewayOutboxAdminController],
  providers: [
    GatewayOutboxTransactionService,
    GatewayOutboxRelayService,
    GatewayOutboxAdminService,
  ],
  exports: [GatewayOutboxTransactionService],
})
export class GatewayOutboxModule {}
