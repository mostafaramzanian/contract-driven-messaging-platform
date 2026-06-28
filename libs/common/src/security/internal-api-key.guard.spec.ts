import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { InternalApiKeyGuard, INTERNAL_API_KEY_HEADER } from './internal-api-key.guard';

function buildContext(headers: Record<string, string>): ExecutionContext {
  return {
    switchToHttp: () => ({
      getRequest: () => ({ headers }),
    }),
  } as unknown as ExecutionContext;
}

describe('InternalApiKeyGuard', () => {
  const originalEnv = process.env.INTERNAL_API_KEY;
  let guard: InternalApiKeyGuard;

  beforeEach(() => {
    guard = new InternalApiKeyGuard();
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    process.env.INTERNAL_API_KEY = originalEnv;
    jest.restoreAllMocks();
  });

  describe('fail-closed when unconfigured', () => {
    it('rejects every request when INTERNAL_API_KEY is not set, even with a header present', () => {
      delete process.env.INTERNAL_API_KEY;
      const context = buildContext({ [INTERNAL_API_KEY_HEADER]: 'anything' });

      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });

    it('rejects when INTERNAL_API_KEY is set to an empty string', () => {
      process.env.INTERNAL_API_KEY = '';
      const context = buildContext({ [INTERNAL_API_KEY_HEADER]: '' });

      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });
  });

  describe('with INTERNAL_API_KEY configured', () => {
    beforeEach(() => {
      process.env.INTERNAL_API_KEY = 'correct-horse-battery-staple';
    });

    it('allows a request with the correct key', () => {
      const context = buildContext({
        [INTERNAL_API_KEY_HEADER]: 'correct-horse-battery-staple',
      });
      expect(guard.canActivate(context)).toBe(true);
    });

    it('rejects a request with an incorrect key', () => {
      const context = buildContext({
        [INTERNAL_API_KEY_HEADER]: 'wrong-key',
      });
      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });

    it('rejects a request with no key header at all', () => {
      const context = buildContext({});
      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });

    it('rejects a key that differs only in length (no partial match)', () => {
      const context = buildContext({
        [INTERNAL_API_KEY_HEADER]: 'correct-horse-battery-staple-extra',
      });
      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });

    it('rejects a key that is a case-different variant of the correct one', () => {
      const context = buildContext({
        [INTERNAL_API_KEY_HEADER]: 'CORRECT-HORSE-BATTERY-STAPLE',
      });
      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });

    it('rejects a non-string header value (e.g. an array, from a repeated header)', () => {
      const context = buildContext({} as Record<string, string>);
      // Simulate Express's array-valued header for a repeated header name.
      (context.switchToHttp().getRequest() as { headers: Record<string, unknown> }).headers[
        INTERNAL_API_KEY_HEADER
      ] = ['correct-horse-battery-staple', 'correct-horse-battery-staple'];

      expect(() => guard.canActivate(context)).toThrow(UnauthorizedException);
    });
  });
});
