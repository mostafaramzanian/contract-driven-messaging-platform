import {
  EventRegistry,
  type EventType,
  CreateMessageEvent,
  CreateMessageEventNameV2,
  createMessageEventV1Schema,
  createMessageEventV2Schema,
  validateEvent,
  buildCreateMessageEventV1,
} from '../index';

const VALID_UUID = '123e4567-e89b-12d3-a456-426614174000';

function validV2Event() {
  return {
    type: CreateMessageEventNameV2.name,
    schemaVersion: '2' as const,
    eventId: '33333333-3333-4333-8333-333333333333',
    correlationId: VALID_UUID,
    timestamp: '2026-01-01T00:00:00.000Z',
    source: 'gateway' as const,
    trace: ['gateway' as const],
    payload: { subject: 'Hello', content: 'World' },
  };
}

describe('EventRegistry — multi-version wiring', () => {
  it('registers both CreateMessageEvent.v1 and CreateMessageEvent.v2', () => {
    expect(Object.keys(EventRegistry)).toEqual(
      expect.arrayContaining([
        'CreateMessageEvent.v1',
        'CreateMessageEvent.v2',
      ]),
    );
    expect(Object.keys(EventRegistry)).toHaveLength(2);
  });

  it('maps CreateMessageEvent.v1 to the frozen v1 schema instance', () => {
    expect(EventRegistry[CreateMessageEvent.name]).toBe(
      createMessageEventV1Schema,
    );
  });

  it('maps CreateMessageEvent.v2 to the v2 schema instance', () => {
    expect(EventRegistry[CreateMessageEventNameV2.name]).toBe(
      createMessageEventV2Schema,
    );
  });

  it('keeps CreateMessageEvent.name immutable at "CreateMessageEvent.v1"', () => {
    // A regression here would silently break every existing
    // @MessagePattern(CreateMessageEvent.name) consumer.
    expect(CreateMessageEvent.name).toBe('CreateMessageEvent.v1');
  });

  it('gives CreateMessageEventNameV2 its own, distinct literal name', () => {
    expect(CreateMessageEventNameV2.name).toBe('CreateMessageEvent.v2');
    expect(CreateMessageEventNameV2.name).not.toBe(CreateMessageEvent.name);
  });
});

describe('validateEvent — dispatches to the correct schema per version', () => {
  it('still validates a v1 event against the v1 schema, unchanged by v2 existing', () => {
    const event = buildCreateMessageEventV1(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    const result = validateEvent(CreateMessageEvent.name, event);
    expect(result.valid).toBe(true);
  });

  it('rejects a v1-shaped event missing required v1 fields, same as before v2 existed', () => {
    const event = buildCreateMessageEventV1(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    const { payload: _omit, ...withoutPayload } = event;
    const result = validateEvent(CreateMessageEvent.name, withoutPayload);
    expect(result.valid).toBe(false);
  });

  it('validates a well-formed v2 event against the v2 schema', () => {
    const result = validateEvent(CreateMessageEventNameV2.name, validV2Event());
    expect(result.valid).toBe(true);
    if (result.valid) {
      expect(result.event.payload.priority).toBe('normal');
      expect(result.event.payload.metadata).toEqual({});
    }
  });

  it('rejects a v2-typed event validated without its required schemaVersion', () => {
    const { schemaVersion: _omit, ...withoutVersion } = validV2Event();
    const result = validateEvent(CreateMessageEventNameV2.name, withoutVersion);
    expect(result.valid).toBe(false);
  });

  it('rejects a v1 event validated against the v2 schema (cross-version mismatch)', () => {
    const v1Event = buildCreateMessageEventV1(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    // v1Event.type is 'CreateMessageEvent.v1', which v2's z.literal type
    // discriminator must reject regardless of payload shape.
    const result = validateEvent(CreateMessageEventNameV2.name, v1Event);
    expect(result.valid).toBe(false);
  });

  it('rejects a v2 event validated against the v1 schema (cross-version mismatch)', () => {
    const result = validateEvent(CreateMessageEvent.name, validV2Event());
    expect(result.valid).toBe(false);
  });

  describe('compile-time narrowing (type-level regression guard)', () => {
    // These assertions exist to fail `tsc`, not `jest`, if `validateEvent`'s
    // generic return type is ever accidentally widened back to a union of
    // both versions' payload shapes for a single, statically-known K. They
    // still run as a normal test (trivially passing at runtime) so they
    // are exercised in CI's `npm test`, not only in a separate `tsc` step
    // a contributor might forget to run locally.
    it('narrows result.event.payload to the v1 shape (no priority/metadata) for K="CreateMessageEvent.v1"', () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );
      const result = validateEvent(CreateMessageEvent.name, event);
      if (result.valid) {
        // @ts-expect-error -- v1 payload has no `priority` field; if this
        // ever stops erroring, validateEvent's per-key narrowing broke.
        const _shouldNotExist = result.event.payload.priority;
        expect(result.event.payload.subject).toBe('Hello');
      } else {
        throw new Error('expected v1 event to validate successfully');
      }
    });

    it('narrows result.event.payload to the v2 shape (includes priority/metadata) for K="CreateMessageEvent.v2"', () => {
      const result = validateEvent(
        CreateMessageEventNameV2.name,
        validV2Event(),
      );
      if (result.valid) {
        // Compiles only because TS knows this is specifically the v2
        // payload shape, not `V1Payload | V2Payload`.
        const priority: 'low' | 'normal' | 'high' =
          result.event.payload.priority;
        expect(priority).toBe('normal');
      } else {
        throw new Error('expected v2 event to validate successfully');
      }
    });
  });
});

describe('EventType', () => {
  it('includes both versioned literal strings', () => {
    const versions: EventType[] = [
      'CreateMessageEvent.v1',
      'CreateMessageEvent.v2',
    ];
    // Compiles only if EventType is exactly this two-member union (or a
    // superset); a TS error here means EventType silently lost a member.
    expect(versions).toHaveLength(2);
  });
});
