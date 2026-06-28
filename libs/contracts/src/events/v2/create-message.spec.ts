import {
  createMessageEventV2Schema,
  createMessageEventV2PayloadSchema,
  messagePrioritySchema,
  messageMetadataSchema,
  MESSAGE_PRIORITIES,
  MAX_METADATA_ENTRIES,
  MAX_METADATA_STRING_LENGTH,
} from '../../index';

const VALID_EVENT_ID = '11111111-1111-4111-8111-111111111111';
const VALID_CORRELATION_ID = '22222222-2222-4222-8222-222222222222';

function baseV2Event(overrides: Record<string, unknown> = {}) {
  return {
    type: 'CreateMessageEvent.v2',
    schemaVersion: '2',
    eventId: VALID_EVENT_ID,
    correlationId: VALID_CORRELATION_ID,
    timestamp: '2026-01-01T00:00:00.000Z',
    source: 'gateway',
    trace: ['gateway'],
    payload: {
      subject: 'Hello v2',
      content: 'World',
    },
    ...overrides,
  };
}

describe('createMessageEventV2Schema', () => {
  describe('schemaVersion is required and pinned', () => {
    it('rejects a v2-typed event with no schemaVersion field at all', () => {
      const { schemaVersion: _omit, ...withoutVersion } = baseV2Event();
      const result = createMessageEventV2Schema.safeParse(withoutVersion);
      expect(result.success).toBe(false);
    });

    it('rejects schemaVersion: "1" on a v2-typed event', () => {
      const result = createMessageEventV2Schema.safeParse(
        baseV2Event({ schemaVersion: '1' }),
      );
      expect(result.success).toBe(false);
    });

    it('accepts schemaVersion: "2"', () => {
      const result = createMessageEventV2Schema.safeParse(baseV2Event());
      expect(result.success).toBe(true);
    });
  });

  describe('type discriminator', () => {
    it('rejects type: "CreateMessageEvent.v1" even with a valid v2 payload and schemaVersion', () => {
      const result = createMessageEventV2Schema.safeParse(
        baseV2Event({ type: 'CreateMessageEvent.v1' }),
      );
      expect(result.success).toBe(false);
    });
  });

  describe('payload defaults (upcast-readiness)', () => {
    it('defaults priority to "normal" when omitted', () => {
      const result = createMessageEventV2Schema.safeParse(baseV2Event());
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.payload.priority).toBe('normal');
      }
    });

    it('defaults metadata to {} when omitted', () => {
      const result = createMessageEventV2Schema.safeParse(baseV2Event());
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.payload.metadata).toEqual({});
      }
    });

    it('accepts an explicit priority and metadata', () => {
      const result = createMessageEventV2Schema.safeParse(
        baseV2Event({
          payload: {
            subject: 'Hello',
            content: 'World',
            recipient: 'someone@example.com',
            priority: 'high',
            metadata: { campaignId: 'spring-2026' },
          },
        }),
      );
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.payload.priority).toBe('high');
        expect(result.data.payload.metadata).toEqual({
          campaignId: 'spring-2026',
        });
        expect(result.data.payload.recipient).toBe('someone@example.com');
      }
    });
  });

  describe('payload required fields (carried over from v1)', () => {
    it('still requires subject and content', () => {
      for (const field of ['subject', 'content'] as const) {
        const payload: Record<string, unknown> = {
          subject: 'Hello',
          content: 'World',
        };
        delete payload[field];
        const result = createMessageEventV2Schema.safeParse(
          baseV2Event({ payload }),
        );
        expect(result.success).toBe(false);
      }
    });

    it('still treats recipient as optional', () => {
      const result = createMessageEventV2Schema.safeParse(baseV2Event());
      expect(result.success).toBe(true);
    });
  });

  describe('messagePrioritySchema', () => {
    it('accepts every declared priority level', () => {
      for (const priority of MESSAGE_PRIORITIES) {
        expect(messagePrioritySchema.safeParse(priority).success).toBe(true);
      }
    });

    it('rejects an unknown priority string', () => {
      expect(messagePrioritySchema.safeParse('urgent').success).toBe(false);
    });
  });

  describe('messageMetadataSchema', () => {
    it('accepts an empty object', () => {
      expect(messageMetadataSchema.safeParse({}).success).toBe(true);
    });

    it('accepts string-to-string entries up to the entry limit', () => {
      const entries = Object.fromEntries(
        Array.from({ length: MAX_METADATA_ENTRIES }, (_, i) => [
          `key${i}`,
          `value${i}`,
        ]),
      );
      expect(messageMetadataSchema.safeParse(entries).success).toBe(true);
    });

    it('rejects more entries than the configured limit', () => {
      const entries = Object.fromEntries(
        Array.from({ length: MAX_METADATA_ENTRIES + 1 }, (_, i) => [
          `key${i}`,
          `value${i}`,
        ]),
      );
      expect(messageMetadataSchema.safeParse(entries).success).toBe(false);
    });

    it('rejects a value longer than the configured max string length', () => {
      const tooLong = 'x'.repeat(MAX_METADATA_STRING_LENGTH + 1);
      expect(messageMetadataSchema.safeParse({ key: tooLong }).success).toBe(
        false,
      );
    });

    it('rejects a non-string value', () => {
      const result = messageMetadataSchema.safeParse({ key: 42 });
      expect(result.success).toBe(false);
    });
  });

  describe('payload schema exported standalone', () => {
    it('createMessageEventV2PayloadSchema validates a payload object on its own', () => {
      const result = createMessageEventV2PayloadSchema.safeParse({
        subject: 'Hello',
        content: 'World',
      });
      expect(result.success).toBe(true);
    });
  });

  describe('envelope fields carried over unchanged from v1', () => {
    it('still requires eventId, correlationId, timestamp, source, and trace', () => {
      const requiredFields = [
        'eventId',
        'correlationId',
        'timestamp',
        'source',
        'trace',
      ] as const;

      for (const field of requiredFields) {
        const event = baseV2Event() as Record<string, unknown>;
        delete event[field];
        const result = createMessageEventV2Schema.safeParse(event);
        expect(result.success).toBe(false);
      }
    });

    it('still accepts "gateway" and "messaging" as valid service identifiers', () => {
      for (const service of ['gateway', 'messaging']) {
        const result = createMessageEventV2Schema.safeParse(
          baseV2Event({ source: service, trace: [service] }),
        );
        expect(result.success).toBe(true);
      }
    });
  });
});
