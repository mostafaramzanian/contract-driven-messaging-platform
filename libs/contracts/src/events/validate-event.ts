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
    // Cast is safe by construction, not a type-safety hole: `schema` was
    // looked up via this exact same `eventType: K`, so `result.data`'s
    // real runtime shape always matches `z.infer<(typeof EventRegistry)[K]>`
    // for whatever K the caller passed in. TypeScript cannot prove this
    // statically -- indexing a union-valued object type
    // (`EventRegistry[Key1] | EventRegistry[Key2] | ...`) by a generic key
    // only narrows to the union of *all* schemas' output types, not the
    // one specific schema that `K` actually selects at a given call site.
    // This became reachable the moment `EventRegistry` grew a second key
    // (`CreateMessageEvent.v2`); it was unreachable with only one key
    // because there was no union to conflate. The cast documents and
    // contains that one unprovable-but-true fact at a single point,
    // rather than loosening this function's exported signature -- callers
    // still get the fully narrowed `z.infer<(typeof EventRegistry)[K]>`
    // for their specific `K`, verified in
    // `event-registry.spec.ts`'s compile-time narrowing checks.
    return {
      valid: true,
      event: result.data as z.infer<(typeof EventRegistry)[K]>,
    };
  }

  const errors: ValidationFailureDetail[] = result.error.issues.map(
    (issue) => ({
      path: issue.path.join('.') || '(root)',
      message: issue.message,
    }),
  );

  return { valid: false, errors };
}
