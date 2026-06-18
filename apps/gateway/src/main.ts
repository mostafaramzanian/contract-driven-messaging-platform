import { NestFactory } from '@nestjs/core';
import { WinstonModule } from 'nest-winston';
import { createWinstonLogger } from '@app/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const isProduction = process.env.NODE_ENV === 'production';
  const logLevel = process.env.LOG_LEVEL ?? 'info';

  const instance = createWinstonLogger(logLevel, isProduction);

  const app = await NestFactory.create(AppModule, {
    logger: WinstonModule.createLogger({ instance }),
  });

  // CORS: in production only explicitly allowed origins are permitted
  const allowedOrigins = process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(',')
    : ['http://localhost:8080'];

  app.enableCors({
    origin: isProduction ? allowedOrigins : '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
  });

  app.setGlobalPrefix('api');

  const port = process.env.PORT ?? 3005;
  await app.listen(port, '0.0.0.0');
}

void bootstrap();
