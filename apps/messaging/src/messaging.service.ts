import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { Message } from './entities/message.entity';

@Injectable()
export class MessagingService implements OnModuleInit {
  private readonly logger = new Logger(MessagingService.name);

  constructor(
    @InjectRepository(Message)
    private readonly messageRepository: Repository<Message>,
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
    try {
      const newMessage = this.messageRepository.create({
        title: data.subject ?? 'Untitled',
        content: data.content ?? 'No content',
        sender: 'system-user',
      });

      const savedMessage = await this.messageRepository.save(newMessage);
      this.logger.log(
        `Message saved successfully with ID: ${savedMessage.id}`,
        MessagingService.name,
        correlationId,
      );

      return savedMessage;
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      this.logger.error(
        'Failed to save message',
        stack,
        MessagingService.name,
        correlationId,
      );
      throw error;
    }
  }
}
