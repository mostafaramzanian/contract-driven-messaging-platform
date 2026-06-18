import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { LoggerModule } from '@app/common';
import { createEventLifecyclePublisherProvider } from '@app/contracts';
import { MessagingController } from './messaging.controller';
import { MessagingService } from './messaging.service';
import { Message } from './entities/message.entity';
import { MessagesModule } from './messages/messages.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    LoggerModule,
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        type: 'postgres',
        host: configService.get<string>('DB_HOST'),
        port: Number.parseInt(configService.get<string>('DB_PORT') ?? '5432'),
        username: configService.get<string>('DB_USERNAME'),
        password: configService.get<string>('DB_PASSWORD'),
        database: configService.get<string>('DB_NAME'),
        entities: [Message],
        synchronize: false,
        logging: false,
        retryAttempts: 5,
        retryDelay: 3000,
      }),
    }),
    TypeOrmModule.forFeature([Message]),
    MessagesModule,
  ],
  controllers: [MessagingController],
  providers: [
    MessagingService,
    createEventLifecyclePublisherProvider('messaging'),
  ],
  exports: [MessagingService],
})
export class MessagingModule {}
