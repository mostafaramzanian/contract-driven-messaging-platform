import { Module, MiddlewareConsumer, NestModule } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { LoggerModule, CorrelationIdMiddleware } from '@app/common';
import { createEventLifecyclePublisherProvider } from '@app/contracts';
import { AppController } from './app.controller';
import { AppService } from './app.service';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    LoggerModule,
    ClientsModule.register([
      {
        name: 'MESSAGING_SERVICE',
        transport: Transport.RMQ,
        options: {
          urls: [
            process.env.RABBITMQ_URL ||
              'amqp://guest:guest@showcase-rabbitmq:5672',
          ],
          queue: 'messaging_queue',
        },
      },
    ]),
  ],
  controllers: [AppController],
  providers: [AppService, createEventLifecyclePublisherProvider('gateway')],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    consumer.apply(CorrelationIdMiddleware).forRoutes('*');
  }
}
