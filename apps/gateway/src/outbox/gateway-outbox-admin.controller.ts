import {
  Controller,
  Post,
  Get,
  Param,
  Body,
  ParseIntPipe,
  HttpCode,
  HttpStatus,
  Logger,
  UseGuards,
} from '@nestjs/common';
import { InternalApiKeyGuard } from '@app/common';
import {
  GatewayOutboxAdminService,
  GatewayOutboxRowSummary,
} from './gateway-outbox-admin.service';

interface ReplayFailedBody {
  /** Optional filter: only replay events of this type. */
  eventType?: string;
}

/**
 * GatewayOutboxAdminController
 *
 * Operator surface for the gateway's producer-side outbox, mounted at
 * `/internal/outbox/*` — same convention, same `InternalApiKeyGuard`
 * protection, as the messaging service's `OutboxAdminController`. See
 * that controller's doc comment for the full rationale on why every
 * route here requires the `x-internal-api-key` header.
 *
 * `/internal/*` is mounted on the gateway's normal HTTP server (the
 * gateway, unlike the messaging service, has no separate internal-only
 * port — it never exposed one). It is therefore reachable through the
 * same Nginx proxy as `/api/*`. Operators deploying this behind a real
 * edge should additionally restrict `/internal/*` at the proxy layer
 * (e.g. an Nginx `location` block limited to an internal CIDR); this
 * guard is defense-in-depth, not a substitute for that network control.
 *
 * ## Endpoints
 *
 *  GET  /internal/outbox/failed             — list rows in `failed` status
 *  POST /internal/outbox/:id/replay         — replay one failed row
 *  POST /internal/outbox/replay-failed      — replay all (optionally filtered) failed rows
 */
@Controller('internal/outbox')
@UseGuards(InternalApiKeyGuard)
export class GatewayOutboxAdminController {
  private readonly logger = new Logger(GatewayOutboxAdminController.name);

  constructor(private readonly adminService: GatewayOutboxAdminService) {}

  @Get('failed')
  async listFailed(): Promise<GatewayOutboxRowSummary[]> {
    return this.adminService.listFailed();
  }

  @Post(':id/replay')
  @HttpCode(HttpStatus.OK)
  async replayOne(
    @Param('id', ParseIntPipe) id: number,
  ): Promise<{ replayed: boolean }> {
    this.logger.log(`Admin requested replay of gateway outbox event id=${id}`);
    return this.adminService.replayById(id);
  }

  @Post('replay-failed')
  @HttpCode(HttpStatus.OK)
  async replayFailed(
    @Body() body: ReplayFailedBody = {},
  ): Promise<{ replayedCount: number }> {
    const { eventType } = body;
    this.logger.log(
      `Admin requested bulk replay of failed gateway outbox events` +
        (eventType ? ` (eventType=${eventType})` : ''),
    );
    return this.adminService.replayAllFailed({ eventType });
  }
}
