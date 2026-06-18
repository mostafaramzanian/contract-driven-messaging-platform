import { createLogger, format, transports, Logger } from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';

interface LogInfo {
  level: string;
  message: string;
  timestamp: string;
  context?: string;
  correlationId?: string;
  [key: string]: unknown;
}

function isLogInfo(info: Record<string, unknown>): info is LogInfo {
  return (
    typeof info.level === 'string' &&
    typeof info.message === 'string' &&
    typeof info.timestamp === 'string'
  );
}

export function createWinstonLogger(
  logLevel: string = 'info',
  isProduction: boolean = false,
): Logger {
  const fileFormat = format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.json(),
  );

  const consoleFormat = format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.colorize(),
    format.printf((info: Record<string, unknown>) => {
      if (!isLogInfo(info)) {
        return typeof info.message === 'string'
          ? info.message
          : 'Unknown message';
      }
      const { level, message, timestamp, context, correlationId } = info;
      const ctx = typeof context === 'string' ? `[${context}] ` : '';
      const cid =
        typeof correlationId === 'string' ? `[CID: ${correlationId}] ` : '';
      return `${timestamp} ${level}: ${ctx}${cid}${message}`;
    }),
  );

  return createLogger({
    level: logLevel,
    defaultMeta: {},
    transports: [
      new transports.Console({
        format: isProduction ? fileFormat : consoleFormat,
      }),
      new DailyRotateFile({
        filename: 'logs/application-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        format: fileFormat,
      }),
      new DailyRotateFile({
        filename: 'logs/error-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'error',
        format: fileFormat,
      }),
    ],
    exitOnError: false,
  });
}
