import { Injectable, LoggerService as NestLoggerService } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Logger } from 'winston';
import { createWinstonLogger } from './logger.factory';

@Injectable()
export class LoggerService implements NestLoggerService {
  private readonly logger: Logger;

  constructor(private readonly configService: ConfigService) {
    const logLevel = this.configService.get<string>('LOG_LEVEL', 'info');
    const isProduction =
      this.configService.get<string>('NODE_ENV') === 'production';
    this.logger = createWinstonLogger(logLevel, isProduction);
  }

  log(message: string, context?: string, correlationId?: string) {
    this.logger.info(message, { context, correlationId });
  }

  error(
    message: string,
    trace?: string,
    context?: string,
    correlationId?: string,
  ) {
    this.logger.error(message, { trace, context, correlationId });
  }

  warn(message: string, context?: string, correlationId?: string) {
    this.logger.warn(message, { context, correlationId });
  }

  debug(message: string, context?: string, correlationId?: string) {
    this.logger.debug(message, { context, correlationId });
  }

  verbose(message: string, context?: string, correlationId?: string) {
    this.logger.verbose(message, { context, correlationId });
  }
}
