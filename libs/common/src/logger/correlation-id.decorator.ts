import { createParamDecorator, ExecutionContext } from '@nestjs/common';
import { CORRELATION_ID_HEADER } from './correlation-id.middleware';

interface RequestWithHeaders {
  headers: Record<string, string | string[] | undefined>;
}

export const CorrelationId = createParamDecorator(
  (data: unknown, ctx: ExecutionContext): string => {
    const request = ctx.switchToHttp().getRequest<RequestWithHeaders>();
    const headerValue = request.headers[CORRELATION_ID_HEADER];
    return typeof headerValue === 'string' ? headerValue : '';
  },
);
