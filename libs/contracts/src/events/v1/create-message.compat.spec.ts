import {
  createMessageEventV1Schema,
  validateEvent,
  CreateMessageEvent,
  EventRegistry,
} from '../../index';

/**
 * A frozen, hand-written example of a valid CreateMessageEvent.v1, exactly
 * as a real v1 producer or a previously-recorded message would shape it.
 *
 * Deliberately NOT built via `buildCreateMessageEventV1` -- that helper is
 * part of the same codebase as the schema and could drift together with
 * it. This fixture is independent on purpose: if a future change to
 * `createMessageEventV1Schema` makes this exact, unchanged object fail
 * validation, that change is a breaking change to the v1 contract by
 * definition, regardless of whether the rest of the test suite still
 * passes.
 *
 * Do not "fix" this fixture to match a schema change. If a schema change
 * requires this fixture to be edited, that is the breaking-change signal
 * this test exists to catch -- the correct response is a new contract
 * version (`CreateMessageEvent.v2`), not an edit here.
 */
const FROZEN_V1_EVENT = {
  type: 'CreateMessageEvent.v1',
  eventId: '11111111-1111-4111-8111-111111111111',
  correlationId: '22222222-2222-4222-8222-222222222222',
  timestamp: '2026-01-01T00:00:00.000Z',
  source: 'gateway',
  trace: ['gateway'],
  payload: {
    subject: 'Frozen compatibility fixture',
    content: 'This object must keep validating across schema changes.',
  },
} as const;

describe('CreateMessageEvent.v1 backward compatibility', () => {
  it('still validates the frozen v1 fixture unchanged', () => {
    const result = validateEvent(CreateMessageEvent.name, FROZEN_V1_EVENT);
    expect(result.valid).toBe(true);
  });

  it('still validates the frozen v1 fixture with an optional recipient added', () => {
    const result = validateEvent(CreateMessageEvent.name, {
      ...FROZEN_V1_EVENT,
      payload: { ...FROZEN_V1_EVENT.payload, recipient: 'someone@example.com' },
    });
    expect(result.valid).toBe(true);
  });

  it('is still registered under the literal name "CreateMessageEvent.v1"', () => {
    // A rename here would silently break every existing
    // `@MessagePattern('CreateMessageEvent.v1')` consumer and every
    // already-published message still in flight.
    expect(CreateMessageEvent.name).toBe('CreateMessageEvent.v1');
    expect(Object.keys(EventRegistry)).toContain('CreateMessageEvent.v1');
  });

  it('still requires every envelope field the v1 contract has always required', () => {
    const requiredFields: (keyof typeof FROZEN_V1_EVENT)[] = [
      'type',
      'eventId',
      'correlationId',
      'timestamp',
      'source',
      'trace',
      'payload',
    ];

    for (const field of requiredFields) {
      const { [field]: _omit, ...withoutField } = FROZEN_V1_EVENT;
      const result = validateEvent(CreateMessageEvent.name, withoutField);
      expect(result.valid).toBe(false);
    }
  });

  it('still requires subject and content in the payload', () => {
    for (const field of ['subject', 'content'] as const) {
      const { [field]: _omit, ...payloadWithoutField } =
        FROZEN_V1_EVENT.payload;
      const result = validateEvent(CreateMessageEvent.name, {
        ...FROZEN_V1_EVENT,
        payload: payloadWithoutField,
      });
      expect(result.valid).toBe(false);
    }
  });

  it('still accepts "gateway" and "messaging" as valid service identifiers', () => {
    // The set of known services is part of the contract surface: removing
    // an entry here would reject events that previously validated.
    for (const service of ['gateway', 'messaging']) {
      const result = validateEvent(CreateMessageEvent.name, {
        ...FROZEN_V1_EVENT,
        source: service,
        trace: [service],
      });
      expect(result.valid).toBe(true);
    }
  });

  it('exposes the v1 schema as a stable export for consumers that import it directly', () => {
    expect(EventRegistry['CreateMessageEvent.v1']).toBe(
      createMessageEventV1Schema,
    );
  });
});
