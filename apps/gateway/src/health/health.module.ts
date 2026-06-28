import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { GatewayHealthController } from './health.controller';

/**
 * GatewayHealthModule
 *
 * Imports:
 *  - `TerminusModule`  : provides `HealthCheckService` + built-in indicators
 *  - `HttpModule`      : required by `HttpHealthIndicator` to make outbound
 *                        HTTP requests to the messaging service's health endpoint
 *  - `ConfigModule`    : supplies `MESSAGING_HEALTH_URL` env-var injection
 *
 * Import into `AppModule` (gateway) to activate the health endpoints.
 */
@Module({
  imports: [TerminusModule, HttpModule, ConfigModule],
  controllers: [GatewayHealthController],
})
export class GatewayHealthModule {}
