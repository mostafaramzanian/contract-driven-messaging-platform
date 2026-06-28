/**
 * Error Classification Strategy
 *
 * Three tiers — decided before any nack/retry logic touches the message:
 *
 *  VALIDATION  → Permanent. Schema/contract violation detected before or
 *                during handler entry. No retry. Route to DLQ immediately.
 *                Examples: Zod parse failure, missing required fields.
 *
 *  TRANSIENT   → Retryable. Infrastructure or network blip. Nack with
 *                requeue=false so the retry exchange picks it up for
 *                exponential back-off (up to MAX_RETRY_ATTEMPTS).
 *                Examples: DB connection refused, ETIMEDOUT, ECONNRESET.
 *
 *  PERMANENT   → Non-retryable business / unknown error. Exhausted retries
 *                or errors that will not self-heal. Route to DLQ directly.
 *                Examples: unique constraint violations, unknown thrown values.
 */

export const enum ErrorClass {
  VALIDATION = 'VALIDATION',
  TRANSIENT = 'TRANSIENT',
  PERMANENT = 'PERMANENT',
}

/** Transient DB/network error codes surfaced by pg or typeorm */
const TRANSIENT_PG_CODES = new Set([
  '08000', // connection_exception
  '08003', // connection_does_not_exist
  '08006', // connection_failure
  '08001', // sqlclient_unable_to_establish_sqlconnection
  '08004', // sqlserver_rejected_establishment_of_sqlconnection
  '57P03', // cannot_connect_now (pg starting)
  '53300', // too_many_connections
  '40001', // serialization_failure (deadlock candidate)
  '40P01', // deadlock_detected
]);

const TRANSIENT_NODE_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'ENOTFOUND',
  'EPIPE',
  'ENETUNREACH',
  'EAI_AGAIN',
]);

export class ValidationError extends Error {
  readonly errorClass = ErrorClass.VALIDATION;
  constructor(
    message: string,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = 'ValidationError';
  }
}

export function classifyError(error: unknown): ErrorClass {
  // Explicit validation sentinel
  if (error instanceof ValidationError) {
    return ErrorClass.VALIDATION;
  }

  if (error instanceof Error) {
    // TypeORM wraps pg errors — check the driverError
    const pgCode: string | undefined = (
      error as unknown as Record<string, unknown>
    ).code as string | undefined;

    const driverCode: string | undefined = (
      (error as unknown as Record<string, unknown>).driverError as
        | Record<string, unknown>
        | undefined
    )?.code as string | undefined;

    if (pgCode && TRANSIENT_PG_CODES.has(pgCode)) return ErrorClass.TRANSIENT;
    if (driverCode && TRANSIENT_PG_CODES.has(driverCode))
      return ErrorClass.TRANSIENT;

    // Node.js system errors
    const nodeCode: string | undefined = (
      error as unknown as Record<string, unknown>
    ).code as string | undefined;
    if (nodeCode && TRANSIENT_NODE_CODES.has(nodeCode))
      return ErrorClass.TRANSIENT;

    // TypeORM QueryFailedError with UNIQUE violation (permanent)
    if (error.constructor?.name === 'QueryFailedError') {
      const uniquePgCode = (error as unknown as Record<string, unknown>)
        .code as string | undefined;
      if (uniquePgCode === '23505') return ErrorClass.PERMANENT; // unique_violation
      if (uniquePgCode === '23503') return ErrorClass.PERMANENT; // foreign_key_violation
      if (uniquePgCode === '23502') return ErrorClass.PERMANENT; // not_null_violation
    }
  }

  // Unknown / unclassified errors are treated as permanent to avoid
  // infinite retry loops on genuinely broken messages.
  return ErrorClass.PERMANENT;
}
