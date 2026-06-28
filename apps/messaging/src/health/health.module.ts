import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { ConfigModule } from '@nestjs/config';
import { MessagingHealthController } from './health.controller';
import { RabbitMQHealthIndicator } from './rabbitmq-health.indicator';

/**
 * MessagingHealthModule
 *
 * Registers the `@nestjs/terminus` health-check infrastructure and the
 * messaging service's custom `RabbitMQHealthIndicator`.
 *
 * Import into `MessagingModule` so the health endpoints are reachable via
 * the service's internal HTTP server (port 3006).
 *
 * `TypeOrmHealthIndicator` is provided by `TerminusModule` and requires
 * an active `DataSource`, which `TypeOrmModule.forRootAsync` in
 * `MessagingModule` supplies.
 */
@Module({
  imports: [TerminusModule, ConfigModule],
  controllers: [MessagingHealthController],
  providers: [RabbitMQHealthIndicator],
})
export class MessagingHealthModule {}
