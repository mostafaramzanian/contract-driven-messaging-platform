import {
  upcastCreateMessageEventV1ToV2,
  buildCreateMessageEventV1,
  createMessageEventV2Schema,
  type CreateMessageEventV1,
} from '../../index';

const VALID_CORRELATION_ID = '22222222-2222-4222-8222-222222222222';

function frozenV1Event(): CreateMessageEventV1 {
  // Deliberately hand-written, not built via buildCreateMessageEventV1,
  // for the same reason v1's own golden fixture
  // (v1/create-message.compat.spec.ts) is hand-written: independence from
  // the builder helper, so this test catches a real shape mismatch rather
  // than the builder and the upcaster silently agreeing with each other.
  return {
    type: 'CreateMessageEvent.v1',
    eventId: '11111111-1111-4111-8111-111111111111',
    correlationId: VALID_CORRELATION_ID,
    timestamp: '2026-01-01T00:00:00.000Z',
    source: 'gateway',
    trace: ['gateway'],
    payload: {
      subject: 'Frozen upcast fixture',
      content: 'This must upcast losslessly.',
    },
  };
}

describe('upcastCreateMessageEventV1ToV2', () => {
  describe('output validity', () => {
    it('produces an event that validates against createMessageEventV2Schema', () => {
      const v2 = upcastCreateMessageEventV1ToV2(frozenV1Event());
      const result = createMessageEventV2Schema.safeParse(v2);
      expect(result.success).toBe(true);
    });

    it('sets type to "CreateMessageEvent.v2"', () => {
      const v2 = upcastCreateMessageEventV1ToV2(frozenV1Event());
      expect(v2.type).toBe('CreateMessageEvent.v2');
    });

    it('sets schemaVersion to "2"', () => {
      const v2 = upcastCreateMessageEventV1ToV2(frozenV1Event());
      expect(v2.schemaVersion).toBe('2');
    });
  });

  describe('lossless carry-over of envelope fields', () => {
    it('preserves eventId exactly (identity must not change)', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.eventId).toBe(v1.eventId);
    });

    it('preserves correlationId exactly', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.correlationId).toBe(v1.correlationId);
    });

    it('preserves timestamp exactly (does not call new Date() / re-stamp)', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.timestamp).toBe(v1.timestamp);
      expect(v2.timestamp).toBe('2026-01-01T00:00:00.000Z');
    });

    it('preserves source exactly', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.source).toBe(v1.source);
    });

    it('preserves trace exactly, including length (does not append a hop)', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.trace).toEqual(v1.trace);
    });
  });

  describe('lossless carry-over of payload fields', () => {
    it('preserves subject exactly', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.payload.subject).toBe(v1.payload.subject);
    });

    it('preserves content exactly', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.payload.content).toBe(v1.payload.content);
    });

    it('preserves recipient when present on the v1 event', () => {
      const v1: CreateMessageEventV1 = {
        ...frozenV1Event(),
        payload: {
          ...frozenV1Event().payload,
          recipient: 'someone@example.com',
        },
      };
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.payload.recipient).toBe('someone@example.com');
    });

    it('leaves recipient undefined when absent on the v1 event (does not invent one)', () => {
      const v1 = frozenV1Event();
      expect(v1.payload.recipient).toBeUndefined();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.payload.recipient).toBeUndefined();
    });
  });

  describe('new v2-only fields get honest, non-invented defaults', () => {
    it('defaults priority to "normal" for a v1 event that never expressed a priority', () => {
      const v2 = upcastCreateMessageEventV1ToV2(frozenV1Event());
      expect(v2.payload.priority).toBe('normal');
    });

    it('defaults metadata to {} for a v1 event that never carried metadata', () => {
      const v2 = upcastCreateMessageEventV1ToV2(frozenV1Event());
      expect(v2.payload.metadata).toEqual({});
    });
  });

  describe('does not mutate its input', () => {
    it('leaves the original v1 event object reference-unchanged after upcasting', () => {
      const v1 = frozenV1Event();
      const v1Snapshot = JSON.parse(JSON.stringify(v1));
      upcastCreateMessageEventV1ToV2(v1);
      expect(v1).toEqual(v1Snapshot);
    });

    it('returns a payload object that is not the same reference as the v1 payload', () => {
      const v1 = frozenV1Event();
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      expect(v2.payload).not.toBe(v1.payload);
    });
  });

  describe('determinism / idempotency', () => {
    it('produces byte-for-byte identical output across repeated calls on the same input', () => {
      const v1 = frozenV1Event();
      const first = upcastCreateMessageEventV1ToV2(v1);
      const second = upcastCreateMessageEventV1ToV2(v1);
      expect(first).toEqual(second);
    });

    it('does not generate a new eventId on repeated calls (no randomUUID side effect)', () => {
      const v1 = frozenV1Event();
      const first = upcastCreateMessageEventV1ToV2(v1);
      const second = upcastCreateMessageEventV1ToV2(v1);
      expect(first.eventId).toBe(second.eventId);
      expect(first.eventId).toBe(v1.eventId);
    });

    it('does not generate a new timestamp on repeated calls (no new Date() side effect)', () => {
      const v1 = frozenV1Event();
      const first = upcastCreateMessageEventV1ToV2(v1);
      const second = upcastCreateMessageEventV1ToV2(v1);
      expect(first.timestamp).toBe(second.timestamp);
      expect(first.timestamp).toBe(v1.timestamp);
    });
  });

  describe('integration with buildCreateMessageEventV1', () => {
    it('upcasts a builder-produced v1 event into a schema-valid v2 event', () => {
      const v1 = buildCreateMessageEventV1(
        { subject: 'Hello', content: 'World' },
        VALID_CORRELATION_ID,
      );
      const v2 = upcastCreateMessageEventV1ToV2(v1);
      const result = createMessageEventV2Schema.safeParse(v2);
      expect(result.success).toBe(true);
      expect(v2.eventId).toBe(v1.eventId);
    });
  });
});
