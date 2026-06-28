import {
  buildCreateMessageEventV2,
  validateEvent,
  CreateMessageEventNameV2,
  createMessageEventV2Schema,
} from '../index';

const VALID_UUID = '123e4567-e89b-12d3-a456-426614174000';

describe('buildCreateMessageEventV2', () => {
  it('produces an event that satisfies its own schema', () => {
    const event = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );

    const result = validateEvent(CreateMessageEventNameV2.name, event);
    expect(result.valid).toBe(true);
  });

  it('sets trace to exactly [gateway] and source to gateway', () => {
    const event = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );

    expect(event.source).toBe('gateway');
    expect(event.trace).toEqual(['gateway']);
  });

  it('generates a different eventId on each call', () => {
    const a = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    const b = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );

    expect(a.eventId).not.toBe(b.eventId);
  });

  it('sets type to "CreateMessageEvent.v2"', () => {
    const event = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    expect(event.type).toBe('CreateMessageEvent.v2');
  });

  it('sets schemaVersion to "2"', () => {
    const event = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    expect(event.schemaVersion).toBe('2');
  });

  it('sets correlationId to the value passed in', () => {
    const event = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    expect(event.correlationId).toBe(VALID_UUID);
  });

  describe('priority/metadata ergonomics (the reason for z.input<> typing)', () => {
    it('defaults priority to "normal" when the caller omits it entirely', () => {
      const event = buildCreateMessageEventV2(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );
      expect(event.payload.priority).toBe('normal');
    });

    it('defaults metadata to {} when the caller omits it entirely', () => {
      const event = buildCreateMessageEventV2(
        { subject: 'Hello', content: 'World' },
        VALID_UUID,
      );
      expect(event.payload.metadata).toEqual({});
    });

    it('preserves an explicitly supplied priority', () => {
      const event = buildCreateMessageEventV2(
        { subject: 'Hello', content: 'World', priority: 'high' },
        VALID_UUID,
      );
      expect(event.payload.priority).toBe('high');
    });

    it('preserves explicitly supplied metadata', () => {
      const event = buildCreateMessageEventV2(
        {
          subject: 'Hello',
          content: 'World',
          metadata: { campaignId: 'spring-2026' },
        },
        VALID_UUID,
      );
      expect(event.payload.metadata).toEqual({ campaignId: 'spring-2026' });
    });

    it('preserves an explicitly supplied recipient', () => {
      const event = buildCreateMessageEventV2(
        {
          subject: 'Hello',
          content: 'World',
          recipient: 'someone@example.com',
        },
        VALID_UUID,
      );
      expect(event.payload.recipient).toBe('someone@example.com');
    });
  });

  describe('input validation (delegated to createMessageEventV2PayloadSchema.parse)', () => {
    it('throws when subject is empty, the same constraint v1 enforces', () => {
      expect(() =>
        buildCreateMessageEventV2(
          { subject: '', content: 'World' },
          VALID_UUID,
        ),
      ).toThrow();
    });

    it('throws when an invalid priority value is supplied', () => {
      expect(() =>
        buildCreateMessageEventV2(
          // @ts-expect-error -- intentionally invalid priority for this test
          { subject: 'Hello', content: 'World', priority: 'urgent' },
          VALID_UUID,
        ),
      ).toThrow();
    });
  });

  it('round-trips through validateEvent against createMessageEventV2Schema directly', () => {
    const event = buildCreateMessageEventV2(
      { subject: 'Hello', content: 'World' },
      VALID_UUID,
    );
    const result = createMessageEventV2Schema.safeParse(event);
    expect(result.success).toBe(true);
  });
});
