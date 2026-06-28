import {
  resolveSchemaVersion,
  SCHEMA_VERSION_HEADER,
  DEFAULT_SCHEMA_VERSION,
} from '../index';

describe('resolveSchemaVersion', () => {
  describe('precedence', () => {
    it('prefers envelope.schemaVersion over everything else when present', () => {
      const result = resolveSchemaVersion({
        envelope: { schemaVersion: '2', type: 'CreateMessageEvent.v1' },
        header: '1',
        type: 'CreateMessageEvent.v1',
      });
      expect(result).toBe('2');
    });

    it('falls back to the header when envelope has no schemaVersion field', () => {
      const result = resolveSchemaVersion({
        envelope: { type: 'CreateMessageEvent.v1' },
        header: '2',
        type: 'CreateMessageEvent.v1',
      });
      expect(result).toBe('2');
    });

    it('falls back to the type suffix when neither envelope nor header carry a version', () => {
      const result = resolveSchemaVersion({
        envelope: { foo: 'bar' },
        type: 'CreateMessageEvent.v2',
      });
      expect(result).toBe('2');
    });

    it('reads the type suffix from envelope.type when sources.type is not separately provided', () => {
      const result = resolveSchemaVersion({
        envelope: { type: 'CreateMessageEvent.v2' },
      });
      expect(result).toBe('2');
    });

    it('defaults to v1 when no source yields a recognizable version', () => {
      const result = resolveSchemaVersion({});
      expect(result).toBe(DEFAULT_SCHEMA_VERSION);
      expect(result).toBe('1');
    });

    it('defaults to v1 for a real, already-shipped v1 envelope with no schemaVersion key at all', () => {
      // Mirrors the frozen v1 fixture shape exactly (no schemaVersion key).
      const result = resolveSchemaVersion({
        envelope: {
          type: 'CreateMessageEvent.v1',
          eventId: '11111111-1111-4111-8111-111111111111',
          correlationId: '22222222-2222-4222-8222-222222222222',
          timestamp: '2026-01-01T00:00:00.000Z',
          source: 'gateway',
          trace: ['gateway'],
          payload: { subject: 'x', content: 'y' },
        },
      });
      expect(result).toBe('1');
    });
  });

  describe('malformed / unexpected input safety', () => {
    it('never throws for primitive, null, or undefined envelope values', () => {
      const inputs: unknown[] = [null, undefined, 'a string', 42, true, []];
      for (const envelope of inputs) {
        expect(() => resolveSchemaVersion({ envelope })).not.toThrow();
      }
    });

    it('ignores an envelope.schemaVersion value outside the known set', () => {
      const result = resolveSchemaVersion({
        envelope: { schemaVersion: '99' },
        type: 'CreateMessageEvent.v2',
      });
      expect(result).toBe('2');
    });

    it('ignores a header value outside the known set and falls through to type', () => {
      const result = resolveSchemaVersion({
        header: 'not-a-version',
        type: 'CreateMessageEvent.v2',
      });
      expect(result).toBe('2');
    });

    it('coerces a Buffer-wrapped header value (defensive AMQP header decoding)', () => {
      const result = resolveSchemaVersion({
        header: Buffer.from('2', 'utf8'),
      });
      expect(result).toBe('2');
    });

    it('coerces a numeric header value', () => {
      const result = resolveSchemaVersion({ header: 2 });
      expect(result).toBe('2');
    });

    it('trims whitespace on a string header value', () => {
      const result = resolveSchemaVersion({ header: '  1  ' });
      expect(result).toBe('1');
    });

    it('ignores a type string with no .vN suffix', () => {
      const result = resolveSchemaVersion({ type: 'CreateMessageEvent' });
      expect(result).toBe(DEFAULT_SCHEMA_VERSION);
    });

    it('ignores a type string whose .vN suffix is not a known version', () => {
      const result = resolveSchemaVersion({ type: 'CreateMessageEvent.v7' });
      expect(result).toBe(DEFAULT_SCHEMA_VERSION);
    });

    it('does not throw when envelope is an array (object but not a record)', () => {
      expect(() =>
        resolveSchemaVersion({ envelope: ['not', 'a', 'record'] }),
      ).not.toThrow();
      expect(resolveSchemaVersion({ envelope: ['not', 'a', 'record'] })).toBe(
        DEFAULT_SCHEMA_VERSION,
      );
    });
  });

  it('exports a stable AMQP header key for transport-level mirroring', () => {
    expect(SCHEMA_VERSION_HEADER).toBe('x-schema-version');
  });
});
