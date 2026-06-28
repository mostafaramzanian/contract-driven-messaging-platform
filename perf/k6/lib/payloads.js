/**
 * Event payload generators matching the CreateMessageEvent v1 and v2 schemas
 * from libs/contracts. Keep in sync with the Zod schemas.
 */

const SUBJECTS = [
  'user.account.created',
  'order.placed',
  'payment.processed',
  'inventory.reserved',
  'notification.scheduled',
];

const PRIORITIES = ['low', 'normal', 'high'];

/**
 * Generate a valid CreateMessageEvent v1 payload.
 * Used to test backward compatibility and upcaster paths.
 */
export function createMessageV1(overrides = {}) {
  return {
    eventType:     'CreateMessageEvent',
    schemaVersion: 'v1',
    correlationId: generateId(),
    timestamp:     new Date().toISOString(),
    payload: {
      subject:     randomElement(SUBJECTS),
      body:        `Load test message ${Date.now()}`,
      recipientId: generateId(),
      ...overrides.payload,
    },
    ...overrides,
  };
}

/**
 * Generate a valid CreateMessageEvent v2 payload.
 * Default for all throughput and latency tests.
 */
export function createMessageV2(overrides = {}) {
  return {
    eventType:     'CreateMessageEvent',
    schemaVersion: 'v2',
    correlationId: generateId(),
    timestamp:     new Date().toISOString(),
    payload: {
      subject:     randomElement(SUBJECTS),
      body:        `Load test message ${Date.now()}`,
      recipientId: generateId(),
      metadata: {
        tags:     ['load-test', `run-${__ENV.RUN_ID || 'local'}`],
        priority: randomElement(PRIORITIES),
      },
      ...overrides.payload,
    },
    ...overrides,
  };
}

/**
 * Generate a payload that will FAIL Zod validation.
 * Used in retry amplification tests to produce VALIDATION class errors.
 * The consumer will ack immediately — useful to verify ack-on-validation-failure.
 */
export function createInvalidPayload() {
  return {
    eventType:     'CreateMessageEvent',
    schemaVersion: 'v2',
    correlationId: generateId(),
    timestamp:     new Date().toISOString(),
    payload: {
      // Missing required `subject` field — will fail Zod validation
      body:        'intentionally invalid payload for load test',
      recipientId: 'not-a-uuid',  // wrong format
    },
  };
}

/**
 * Generate a payload that will SUCCEED validation but fail during
 * the atomic transaction (simulates a transient DB error for retry testing).
 * Requires a test hook in the consumer that artificially fails on specific markers.
 */
export function createTransientFailurePayload() {
  return createMessageV2({
    payload: {
      tags: ['force-transient-failure'],  // consumer test hook reads this
    },
  });
}

function generateId() {
  // RFC 4122 v4 UUID — matches the format expected by Zod
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

function randomElement(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}
