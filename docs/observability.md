# Observability

## Correlation ID Generation

Correlation IDs are generated and managed by `CorrelationIdMiddleware` in
`libs/common/src/logger/correlation-id.middleware.ts`:

- For every incoming HTTP request, the middleware checks for an
  `x-correlation-id` request header.
- If the header is present, its value is reused.
- If the header is absent, a new UUID v4 is generated (via the `uuid`
  package).
- The resolved correlation ID is written back onto the request headers
  (`req.headers['x-correlation-id']`) and set as a response header
  (`res.setHeader('x-correlation-id', correlationId)`), so callers can read
  it back and reuse it for related requests.

The middleware is registered globally in `apps/gateway/src/app.module.ts`
via `consumer.apply(CorrelationIdMiddleware).forRoutes('*')`, so it runs for
every route on the gateway.

## Propagation

### Within the gateway (HTTP)

Within the gateway, the correlation ID is available on the request object.
`AppController` reads it directly from `req.headers[CORRELATION_ID_HEADER]`
where needed, and it can also be retrieved via the `@CorrelationId()`
parameter decorator (`libs/common/src/logger/correlation-id.decorator.ts`),
which wraps `createParamDecorator` and reads the same header from the
request.

### Across RabbitMQ

AMQP messages do not carry HTTP headers, so the correlation ID cannot be
propagated automatically across the RabbitMQ boundary. Instead, it is
included explicitly as the `correlationId` field of the event envelope
defined in `@app/contracts` (see `docs/architecture.md` for the full
schema):

```ts
const event = buildCreateMessageEventV1(
  { subject, content },
  correlationId,
);
// event.correlationId === correlationId
this.client.emit(CreateMessageEvent.name, event);
```

On the consuming side, `MessagingController.handleMessage` validates the
incoming payload against the same schema and reads `event.correlationId`
from the now-typed, contract-checked event, passing it through to the
service layer and to every log statement and event-lifecycle record for
that operation.

This means correlation ID propagation across services is now enforced by
schema validation rather than only by convention: `correlationId` is a
required, UUID-typed field on `eventEnvelopeSchema`, so an event missing it
(or carrying a non-UUID value) fails validation on both the producer and
consumer side and is never processed — see "Fail-fast enforcement" in
`docs/architecture.md`. It is still true that `@MessagePattern` handlers
are not HTTP routes and `CorrelationIdMiddleware` does not apply to them;
the enforcement on the consumer side comes from contract validation, not
from a shared middleware.

## Contract Validation Failures

When `validateEvent` rejects an event — on either the gateway (before
emitting) or the messaging service (after receiving) — the rejection is
logged as a single structured error message containing the event type and
the full list of validation errors (`{ path, message }` pairs), for
example:

```
Refused to emit invalid CreateMessageEvent.v1 event: [{"path":"correlationId","message":"Invalid UUID"}]
```

This is logged via `Logger.error`, so it appears in both the console
transport and the `logs/error-%DATE%.log` file described below, with the
same correlation ID metadata as any other log call. See
`docs/architecture.md` ("Fail-fast enforcement") for what happens to the
event itself after this log line — it is dropped, not retried.

## Event Lifecycle Logging

Independently of the structured validation-failure logs above, every event
that reaches `MessagingController.handleMessage` produces one
`Logger.log` line per stage it passes through on the consumer side
(`received`, `validated`, `persisted`, or `rejected` on failure), each
including the `eventId` so a single event's progress can be grepped out of
the logs even without using the event-lifecycle RabbitMQ exchange described
in `docs/architecture.md`. The gateway logs `emitted` (or the rejection)
the same way on the producer side.

The RabbitMQ-based lifecycle exchange (`event-lifecycle.test`,
test-only via `EVENT_LIFECYCLE_TRACING`) carries the same five stages as
structured JSON records rather than log lines, specifically so the
integration test can `await` a stage deterministically instead of grepping
logs or polling the database — see `docs/testing.md`.

## Logging Strategy

Both `apps/gateway` and `apps/messaging` bootstrap a Winston logger via
`createWinstonLogger` (`libs/common/src/logger/logger.factory.ts`) and wire
it into Nest via `nest-winston`'s `WinstonModule.createLogger`.

The factory configures:

- **Console transport**: in non-production (`NODE_ENV !== 'production'`),
  uses a colorized, human-readable format:
  `TIMESTAMP level: [Context] [CID: correlation-id] message`. In production,
  the console transport uses the same JSON format as the file transports.
- **Daily rotating file transport** (`logs/application-%DATE%.log`): all log
  levels, JSON format, gzip-compressed on rotation, max file size 20MB, 14
  day retention.
- **Daily rotating error file transport** (`logs/error-%DATE%.log`): same
  rotation policy, filtered to `error` level only.

The log level is controlled by the `LOG_LEVEL` environment variable
(default `info`).

Every log call accepts an optional `context` (typically the class name, via
`new Logger(SomeClass.name)`) and an optional `correlationId`, both of which
are included in the structured log output via `defaultMeta`/log metadata.
This means a single correlation ID can be grepped across both services' log
files to reconstruct the full path of a request.

**Current limitation**: the `logs/` directory is written inside each
container's filesystem and is not mounted as a Docker volume in
`docker-compose.yml`. Rotated log files therefore do not persist across
container restarts. For local debugging, the console transport (visible via
`docker-compose logs <service>`) is the primary source of truth.

## Troubleshooting Workflow

To trace a single request end-to-end:

1. Make the request to the gateway and note the `x-correlation-id` value
   returned in the response headers (or supply your own by setting that
   header on the request).
2. Search the gateway logs for that correlation ID:

   ```bash
   docker-compose logs gateway | grep "<correlation-id>"
   ```

    This should show the route being accessed and the
    `emit(CreateMessageEvent.name, ...)` call (or a "Refused to emit
    invalid..." line if the event failed contract validation, in which
    case it never reached RabbitMQ — see "Contract Validation Failures"
    above).
3. Search the messaging service logs for the same ID:

   ```bash
   docker-compose logs messaging | grep "<correlation-id>"
   ```

   This should show the message being received, processed, and the
   resulting `Message` ID on success, or an error with stack trace on
   failure.
4. If the correlation ID does not appear in the messaging service logs at
   all, check:
   - RabbitMQ management UI (`http://localhost:15672`) for the
     `messaging_queue` — messages stuck in the queue indicate the consumer
     is not running or not connected.
   - The messaging service's startup logs for "Database connection
     established" / "Messaging service ready to receive RabbitMQ messages"
     (logged in `MessagingService.onModuleInit`) to confirm it started
     successfully.
