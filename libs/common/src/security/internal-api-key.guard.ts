import {
  CanActivate,
  ExecutionContext,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import type { Request } from 'express';
import { timingSafeEqual } from 'node:crypto';

export const INTERNAL_API_KEY_HEADER = 'x-internal-api-key';

/**
 * InternalApiKeyGuard
 *
 * Closes a real, previously-unaddressed security gap found in the
 * production-readiness review: `OutboxAdminController`'s replay
 * endpoints (`POST /internal/outbox/:id/replay`,
 * `POST /internal/outbox/replay-failed`) had no auth guard at all, and
 * their own doc comment's claim — "the internal HTTP server is not
 * publicly routed" — does not hold given this repository's actual
 * `docker-compose.yml`, which maps the messaging service's internal
 * HTTP port directly to the host (`"3006:3006"`), bypassing Nginx (which
 * only proxies `/api`) entirely. Anyone able to reach that host port
 * could previously trigger arbitrary outbox replays with zero
 * credentials.
 *
 * ## Design
 *
 * Deliberately simple — a single shared-secret header compared with a
 * constant-time comparison (`timingSafeEqual`), not a full auth/session
 * system. This matches the actual threat model for an *internal*,
 * operator-only admin surface (the goal is "not reachable by an
 * anonymous internet client who finds the port open", not "supports
 * per-user RBAC"). A full OAuth2/session-based scheme would be
 * over-engineering for this surface.
 *
 * ## Fails closed
 *
 * If `INTERNAL_API_KEY` is not configured at all, every request is
 * rejected — this is intentional. An operator who forgets to set the
 * env var gets a hard failure (and, ideally, a deploy-time health-check
 * failure on `MessagingHealthModule`'s readiness probe, not yet wired up
 * — see the deployment runbook), rather than the guard silently
 * defaulting to "allow everything", which would be the worse failure
 * mode for a security control.
 */
@Injectable()
export class InternalApiKeyGuard implements CanActivate {
  private readonly logger = new Logger(InternalApiKeyGuard.name);

  canActivate(context: ExecutionContext): boolean {
    const req = context.switchToHttp().getRequest<Request>();
    const expected = process.env.INTERNAL_API_KEY;

    if (!expected) {
      this.logger.error(
        'INTERNAL_API_KEY is not set — refusing all requests to this guarded route. ' +
          'Set INTERNAL_API_KEY in the environment to enable access.',
      );
      throw new UnauthorizedException('Internal API key not configured');
    }

    const provided = req.headers[INTERNAL_API_KEY_HEADER];
    if (typeof provided !== 'string' || !this.safeCompare(provided, expected)) {
      throw new UnauthorizedException('Invalid or missing internal API key');
    }

    return true;
  }

  /**
   * Constant-time string comparison, to avoid a timing side-channel that
   * could let an attacker recover the key one byte at a time by
   * measuring response latency across many requests. `timingSafeEqual`
   * requires equal-length buffers, so a length mismatch is checked first
   * (this leaks length, not content, which is an acceptable and standard
   * trade-off for this comparison).
   */
  private safeCompare(provided: string, expected: string): boolean {
    const providedBuf = Buffer.from(provided);
    const expectedBuf = Buffer.from(expected);
    if (providedBuf.length !== expectedBuf.length) {
      return false;
    }
    return timingSafeEqual(providedBuf, expectedBuf);
  }
}
