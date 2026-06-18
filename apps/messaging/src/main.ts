import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { WinstonModule } from 'nest-winston';
import { createWinstonLogger } from '@app/common';
import { MessagingModule } from './messaging.module';

async function bootstrap() {
  const isProduction = process.env.NODE_ENV === 'production';
  const logLevel = process.env.LOG_LEVEL || 'info';

  const instance = createWinstonLogger(logLevel, isProduction);

  const rabbitUrl =
    process.env.RABBITMQ_URL || 'amqp://guest:guest@showcase-rabbitmq:5672';

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    MessagingModule,
    {
      transport: Transport.RMQ,
      options: {
        urls: [rabbitUrl],
        queue: 'messaging_queue',
        queueOptions: {
          durable: true,
        },
      },
    },
  );

  app.useLogger(WinstonModule.createLogger({ instance }));

  await app.listen();
}

void bootstrap();
