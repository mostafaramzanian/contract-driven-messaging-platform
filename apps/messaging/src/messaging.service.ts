import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { Message } from './entities/message.entity';
import { OutboxTransactionService } from './outbox/outbox-transaction.service';

/**
 * MessagingService (updated — Production-Readiness Stage)
 *
 * `handleMessageCreation` (unchanged, see its own doc comment) uses
 * `OutboxTransactionService.runWithOutboxEvents()` to guarantee that the
 * `Message` row and the `MessagePersisted` outbox event are committed
 * **atomically**.  A crash between the two writes is no longer possible —
 * both commit or both roll back.
 *
 * `handleMessageCreationIdempotent` is a new, additive sibling method
 * (added during the production-readiness review) that closes a separate,
 * previously-unaddressed gap: the idempotency-ledger write
 * (`IdempotencyService.checkAndMark`) was running as its OWN, separately
 * committed transaction, before this service's transactional write ever
 * started. A crash between those two transactions left an orphaned
 * idempotency row with no corresponding message — silent, permanent
 * message loss on redelivery. This method uses
 * `OutboxTransactionService.runIdempotentWithOutboxEvents()` to fold the
 * idempotency check into the SAME transaction as the message write and
 * the outbox event, exactly the same pattern that already protects the
 * message+outbox-event pair.
 *
 * `MessagingController.handleMessage` calls the new method;
 * `handleMessageCreation` is kept as-is (not deleted, not changed) both
 * to avoid touching its large existing test suite for a change unrelated
 * to what those tests verify, and because it remains a valid, simpler
 * building block for any future caller that doesn't need the idempotency
 * guarantee (e.g. an internal admin/backfill tool operating on already
 * deduplicated data).
 */
@Injectable()
export class MessagingService implements OnModuleInit {
  private readonly logger = new Logger(MessagingService.name);

  constructor(
    @InjectRepository(Message)
    private readonly messageRepository: Repository<Message>,
    private readonly outboxTransactionService: OutboxTransactionService,
  ) {}

  getHello(): string {
    return 'Welcome to the Messaging Showcase messaging service';
  }

  async onModuleInit() {
    try {
      await this.messageRepository.count();
      this.logger.log('Database connection established', MessagingService.name);
      this.logger.log(
        'Messaging service ready to receive RabbitMQ messages',
        MessagingService.name,
      );
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      this.logger.error(
        'Error during service initialization',
        stack,
        MessagingService.name,
      );
    }
  }

  async handleMessageCreation(
    data: { content?: string; subject?: string },
    correlationId?: string,
  ): Promise<Message> {
    return this.outboxTransactionService.runWithOutboxEvents(
      async (em) => {
        const newMessage = em.create(Message, {
          title: data.subject ?? 'Untitled',
          content: data.content ?? 'No content',
          sender: 'system-user',
        });
        const saved = await em.save(Message, newMessage);

        this.logger.log(
          `Message saved successfully with ID: ${saved.id}`,
          MessagingService.name,
          correlationId,
        );

        return saved;
      },
      [
        {
          eventType: 'MessagePersisted',
          payload: { messageId: undefined, title: data.subject ?? 'Untitled' },
          correlationId,
        },
      ],
    );
  }

  /**
   * Idempotent counterpart to `handleMessageCreation` — see this class's
   * top-level doc comment for the full rationale.
   *
   * @returns `{ duplicate: true }` if `eventId` was already processed
   *          (the message row was NOT touched); `{ duplicate: false,
   *          result }` with the persisted `Message` otherwise.
   */
  async handleMessageCreationIdempotent(
    data: { content?: string; subject?: string },
    eventId: string,
    eventType: string,
    correlationId?: string,
  ): Promise<{ duplicate: true } | { duplicate: false; result: Message }> {
    return this.outboxTransactionService.runIdempotentWithOutboxEvents(
      { eventId, eventType, correlationId },
      async (em) => {
        const newMessage = em.create(Message, {
          title: data.subject ?? 'Untitled',
          content: data.content ?? 'No content',
          sender: 'system-user',
        });
        const saved = await em.save(Message, newMessage);

        this.logger.log(
          `Message saved successfully with ID: ${saved.id} (eventId=${eventId})`,
          MessagingService.name,
          correlationId,
        );

        return saved;
      },
      [
        {
          eventType: 'MessagePersisted',
          payload: { messageId: undefined, title: data.subject ?? 'Untitled' },
          correlationId,
        },
      ],
    );
  }
}
