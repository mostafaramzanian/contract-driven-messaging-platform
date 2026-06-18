import { z } from 'zod';
import { eventEnvelopeSchema } from '../envelope.schema';

/**
 * Payload for the v1 CreateMessageEvent.
 *
 * Deliberately mirrors the fields the gateway already sends today
 * (`subject`, `content`) rather than introducing new business fields.
 * `recipient` is included as optional because the underlying `messages`
 * table already has a nullable `recipient` column (see
 * apps/messaging/src/entities/message.entity.ts), even though the current
 * `/api/test-rabbit` flow does not set it.
 */
export const createMessageEventV1PayloadSchema = z.object({
  subject: z.string().min(1).max(255),
  content: z.string().min(1),
  recipient: z.string().max(255).optional(),
});

export type CreateMessageEventV1Payload = z.infer<
  typeof createMessageEventV1PayloadSchema
>;

export const createMessageEventV1Schema = eventEnvelopeSchema.extend({
  type: z.literal('CreateMessageEvent.v1'),
  payload: createMessageEventV1PayloadSchema,
});

export type CreateMessageEventV1 = z.infer<typeof createMessageEventV1Schema>;
