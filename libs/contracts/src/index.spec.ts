import {
  validateEvent,
  buildCreateMessageEventV1,
  CreateMessageEvent,
  createMessageEventV1Schema,
  EventRegistry,
} from './index';

const VALID_UUID = '123e4567-e89b-12d3-a456-426614174000';

describe('CreateMessageEvent.v1 contract', () => {
  it('is registered under its own name in the EventRegistry', () => {
    expect(EventRegistry[CreateMessageEvent.name]).toBe(
      createMessageEventV1Schema,
    );
  });

  describe('buildCreateMessageEventV1', () => {
    it('produces an event that satisfies its own schema', () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );

      const result = validateEvent(CreateMessageEvent.name, event);
      expect(result.valid).toBe(true);
    });

    it('sets trace to exactly [gateway] and source to gateway', () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );

      expect(event.source).toBe('gateway');
      expect(event.trace).toEqual(['gateway']);
    });

    it('generates a different eventId on each call', () => {
      const a = buildCreateMessageEventV1(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );
      const b = buildCreateMessageEventV1(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );

      expect(a.eventId).not.toBe(b.eventId);
    });
  });

  describe('validateEvent', () => {
    const baseEvent = buildCreateMessageEventV1(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );

    it('accepts a fully valid event', () => {
      const result = validateEvent(CreateMessageEvent.name, baseEvent);
      expect(result.valid).toBe(true);
    });

    it('rejects a non-UUID eventId', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        eventId: 'not-a-uuid',
      });
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ path: 'eventId' }),
          ]),
        );
      }
    });

    it('rejects a non-UUID correlationId', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        correlationId: 'also-not-a-uuid',
      });
      expect(result.valid).toBe(false);
    });

    it('rejects an empty subject', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        payload: { ...baseEvent.payload, subject: '' },
      });
      expect(result.valid).toBe(false);
    });

    it('rejects a missing trace array', () => {
      const { trace: _omit, ...withoutTrace } = baseEvent;
      const result = validateEvent(CreateMessageEvent.name, withoutTrace);
      expect(result.valid).toBe(false);
    });

    it('rejects an empty trace array', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        trace: [],
      });
      expect(result.valid).toBe(false);
    });

    it('rejects an unknown source/trace service id', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        source: 'unknown-service',
      });
      expect(result.valid).toBe(false);
    });

    it('rejects a wrong type discriminator', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        type: 'CreateMessageEvent.v2',
      });
      expect(result.valid).toBe(false);
    });

    it('rejects extra/unexpected top-level junk gracefully (still validates known fields)', () => {
      const result = validateEvent(CreateMessageEvent.name, {
        ...baseEvent,
        unexpectedField: 'should not break validation',
      });
      expect(result.valid).toBe(true);
    });
  });
});
