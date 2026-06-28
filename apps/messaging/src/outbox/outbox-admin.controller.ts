import {
  Controller,
  Post,
  Param,
  Body,
  ParseIntPipe,
  HttpCode,
  HttpStatus,
  Logger,
  UseGuards,
} from '@nestjs/common';
import { OutboxAdminService, ReplayResult } from './outbox-admin.service';
import { InternalApiKeyGuard } from '@app/common';

interface ReplayFailedBody {
  /** Optional filter: only replay events of this type. */
  eventType?: string;
}

/**
 * OutboxAdminController
 *
 * Exposes operational endpoints for the outbox failure-recovery path.
 * Mounted at `/internal/outbox/*` on the messaging service's internal HTTP
 * server (port 3006, same host as `/internal/health/*`).
 *
 * ## Convention (matching MessagingHealthController)
 *
 *  - `@Controller('internal/outbox')` — mirrors `@Controller('internal/health')`.
 *  - The `/internal` prefix signals ops-only by convention.
 *
 * ## Endpoints
 *
 *  POST /internal/outbox/:id/replay
 *    Reset a single `failed` outbox row back to `pending`.
 *    Returns 404 if the row doesn't exist or is not `failed`.
 *
 *  POST /internal/outbox/replay-failed
 *    Reset ALL `failed` rows (or a subset by eventType) back to `pending`.
 *    Body: `{ "eventType": "MessagePersisted" }` (optional).
 *    Always returns 200; replayed=0 is not an error.
 *
 * ## Authorization (production-readiness fix)
 *
 *  Every route on this controller now requires `InternalApiKeyGuard`
 *  (`@app/common`'s `x-internal-api-key` header check). This corrects a
 *  real gap found in review: this controller previously had NO auth
 *  guard, on the stated assumption that "the internal HTTP server is not
 *  publicly routed" — an assumption that does not hold given this
 *  repository's actual `docker-compose.yml`, which maps the messaging
 *  service's internal HTTP port directly to the host (`"3006:3006"`),
 *  bypassing Nginx (which only proxies `/api`) entirely. Network
 *  isolation alone is not a substitute for authentication on an endpoint
 *  that can trigger arbitrary data-mutating replays; this guard is that
 *  authentication, intentionally lightweight (a single shared-secret
 *  header) for what is genuinely an operator-only internal surface — see
 *  `InternalApiKeyGuard`'s own doc comment for the full design rationale.
 *  `INTERNAL_API_KEY` must be set in the environment for ANY request to
 *  succeed (the guard fails closed if unset).
 */
@Controller('internal/outbox')
@UseGuards(InternalApiKeyGuard)
export class OutboxAdminController {
  private readonly logger = new Logger(OutboxAdminController.name);

  constructor(private readonly adminService: OutboxAdminService) {}

  /**
   * Reset a single failed outbox event to `pending`.
   *
   * POST /internal/outbox/:id/replay
   *
   * 200 — row reset successfully; body: `{ replayed: 1, ids: [id] }`
   * 404 — row not found or not in `failed` status
   */
  @Post(':id/replay')
  @HttpCode(HttpStatus.OK)
  async replayOne(
    @Param('id', ParseIntPipe) id: number,
  ): Promise<ReplayResult> {
    this.logger.log(`Admin requested replay of outbox event id=${id}`);
    return this.adminService.replayById(id);
  }

  /**
   * Reset all (optionally filtered) failed outbox events to `pending`.
   *
   * POST /internal/outbox/replay-failed
   * Body: `{}` or `{ "eventType": "MessagePersisted" }`
   *
   * 200 — always; body: `{ replayed: N, ids: [...] }`
   */
  @Post('replay-failed')
  @HttpCode(HttpStatus.OK)
  async replayFailed(
    @Body() body: ReplayFailedBody = {},
  ): Promise<ReplayResult> {
    const { eventType } = body;
    this.logger.log(
      `Admin requested bulk replay of failed outbox events` +
        (eventType ? ` (eventType=${eventType})` : ''),
    );
    return this.adminService.replayAllFailed({ eventType });
  }
}
