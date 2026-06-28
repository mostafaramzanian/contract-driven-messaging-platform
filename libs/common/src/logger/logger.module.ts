import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PinoLoggerService } from './pino-logger.service';

/**
 * LoggerModule (updated — now Pino-backed)
 *
 * Global module providing `PinoLoggerService` to the entire application.
 *
 * Because this module is `@Global()`, any module that imports the root
 * `AppModule` / `MessagingModule` automatically has access to
 * `PinoLoggerService` without needing to import `LoggerModule` itself.
 *
 * Migration note:
 *   The previous implementation used `nest-winston`.  The interface exposed
 *   by `PinoLoggerService` is a superset of the old `LoggerService`:
 *   - All existing call sites (`.log(msg, context?)`, `.error(msg, stack?,
 *     context?)`, `.warn(...)`, `.debug(...)`) continue to work unchanged.
 *   - New call sites can pass a `PinoBaseFields` object as the second
 *     argument to log `correlationId`, `eventId`, `messageId`, etc.
 *     as structured JSON fields rather than interpolated strings.
 */
@Global()
@Module({
  imports: [ConfigModule],
  providers: [PinoLoggerService],
  exports: [PinoLoggerService],
})
export class LoggerModule {}
