import {
  Controller,
  Get,
  Inject,
  OnModuleInit,
  Req,
  Logger,
  BadRequestException,
} from '@nestjs/common';
import type { Request } from 'express';
import { AppService } from './app.service';
import { ClientProxy } from '@nestjs/microservices';
import { CORRELATION_ID_HEADER } from '@app/common';
import {
  buildCreateMessageEventV1,
  validateEvent,
  CreateMessageEvent,
  EVENT_LIFECYCLE_PUBLISHER,
  type EventLifecyclePublisher,
} from '@app/contracts';

@Controller()
export class AppController implements OnModuleInit {
  private readonly logger = new Logger(AppController.name);

  constructor(
    private readonly appService: AppService,
    @Inject('MESSAGING_SERVICE') private readonly client: ClientProxy,
    @Inject(EVENT_LIFECYCLE_PUBLISHER)
    private readonly lifecyclePublisher: EventLifecyclePublisher,
  ) {}

  async onModuleInit() {
    try {
      await this.client.connect();
      this.logger.log('Connected to RabbitMQ', AppController.name);
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      this.logger.error(
        'Failed to connect to RabbitMQ',
        stack,
        AppController.name,
      );
    }
  }

  @Get()
  getRoot(@Req() req: Request) {
    const correlationId = req.headers[CORRELATION_ID_HEADER] as string;
    this.logger.log(
      'Root endpoint accessed',
      AppController.name,
      correlationId,
    );

    return {
      message: 'Welcome to the Messaging Showcase platform',
      status: 'active',
      endpoints: {
        api: '/api',
        testRabbit: '/api/test-rabbit',
      },
    };
  }

  @Get('test-rabbit')
  async sendTestMessage(@Req() req: Request) {
    const correlationId = req.headers[CORRELATION_ID_HEADER] as string;

    // CorrelationIdMiddleware always sets this header (generating a v4
    // UUID if the caller did not supply one), but a caller-supplied header
    // value could in principle be any string. Building the event below
    // and validating it against the contract is what actually enforces
    // the "correlationId must be a UUID" rule end to end, rather than
    // trusting the header blindly.
    const event = buildCreateMessageEventV1(
      {
        subject: 'System test message',
        content: 'Hello RabbitMQ!',
      },
      correlationId,
    );

    const result = validateEvent(CreateMessageEvent.name, event);

    if (!result.valid) {
      // Fail fast: an event that does not match its own contract is never
      // emitted onto RabbitMQ. Logged as a structured validation failure
      // per docs/observability.md.
      this.logger.error(
        `Refused to emit invalid ${CreateMessageEvent.name} event: ${JSON.stringify(result.errors)}`,
        undefined,
        AppController.name,
        correlationId,
      );
      await this.lifecyclePublisher.publish({
        stage: 'rejected',
        eventType: CreateMessageEvent.name,
        eventId: event.eventId,
        correlationId,
        errors: result.errors,
      });
      throw new BadRequestException({
        status: 'rejected',
        reason: 'event_contract_violation',
        eventType: CreateMessageEvent.name,
        eventId: event.eventId,
        errors: result.errors,
      });
    }

    try {
      this.client.emit(CreateMessageEvent.name, result.event);

      this.logger.log(
        `Validated ${CreateMessageEvent.name} event emitted to RabbitMQ (eventId=${result.event.eventId})`,
        AppController.name,
        correlationId,
      );

      await this.lifecyclePublisher.publish({
        stage: 'emitted',
        eventType: CreateMessageEvent.name,
        eventId: result.event.eventId,
        correlationId,
      });

      return {
        status: 'success',
        message: 'Message successfully sent to RabbitMQ queue',
        correlationId,
        eventId: result.event.eventId,
        eventType: CreateMessageEvent.name,
      };
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      this.logger.error(
        'Failed to send message to RabbitMQ',
        stack,
        AppController.name,
        correlationId,
      );
      throw error;
    }
  }
}
