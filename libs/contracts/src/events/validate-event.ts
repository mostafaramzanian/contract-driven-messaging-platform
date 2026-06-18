import { z } from 'zod';
import { EventRegistry, type EventType } from './event-registry';

export interface ValidationFailureDetail {
  path: string;
  message: string;
}

export type ValidateEventResult<T> =
  | { valid: true; event: T }
  | { valid: false; errors: ValidationFailureDetail[] };

/**
 * Validates `raw` against the schema registered for `eventType`.
 *
 * This never throws. Callers are expected to check `result.valid` and
 * decide what "fail fast" means in their context (reject an HTTP request,
 * nack/drop a RabbitMQ message, etc.) -- this helper only does the
 * validation and produces structured error detail suitable for logging,
 * per docs/observability.md's structured-log requirement for validation
 * failures.
 */
export function validateEvent<K extends EventType>(
  eventType: K,
  raw: unknown,
): ValidateEventResult<z.infer<(typeof EventRegistry)[K]>> {
  const schema = EventRegistry[eventType];
  const result = schema.safeParse(raw);

  if (result.success) {
    return { valid: true, event: result.data };
  }

  const errors: ValidationFailureDetail[] = result.error.issues.map(
    (issue) => ({
      path: issue.path.join('.') || '(root)',
      message: issue.message,
    }),
  );

  return { valid: false, errors };
}
