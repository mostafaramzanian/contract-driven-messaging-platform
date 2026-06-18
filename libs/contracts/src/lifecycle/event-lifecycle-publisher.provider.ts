import type { Provider } from '@nestjs/common';
import { Logger } from '@nestjs/common';
import { EventLifecyclePublisher } from './event-lifecycle-publisher';

export const EVENT_LIFECYCLE_PUBLISHER = 'EVENT_LIFECYCLE_PUBLISHER';

/**
 * Builds a NestJS provider for `EventLifecyclePublisher`, scoped to
 * `serviceName` (used to tag every record this process publishes) and the
 * RabbitMQ URL the rest of the app already connects to.
 *
 * Errors from the publisher are logged via Nest's `Logger` rather than
 * thrown, consistent with the publisher's own fail-soft design: lifecycle
 * tracing must never be able to break the app that hosts it.
 */
export function createEventLifecyclePublisherProvider(
  serviceName: string,
): Provider {
  return {
    provide: EVENT_LIFECYCLE_PUBLISHER,
    useFactory: (): EventLifecyclePublisher => {
      const logger = new Logger('EventLifecyclePublisher');
      const amqpUrl =
        process.env.RABBITMQ_URL || 'amqp://guest:guest@showcase-rabbitmq:5672';

      return new EventLifecyclePublisher(serviceName, amqpUrl, (error) => {
        const message = error instanceof Error ? error.message : String(error);
        logger.warn(
          `Event lifecycle publisher error (non-fatal, tracing only): ${message}`,
        );
      });
    },
  };
}
