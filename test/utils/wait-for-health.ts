import * as amqplib from 'amqplib';

const DEFAULT_POLL_INTERVAL_MS = 500;

/**
 * Polls `check` until it resolves without throwing, or rejects with a
 * clear timeout error. This is intentionally only used for infrastructure
 * readiness (is the gateway's HTTP server accepting connections yet? is
 * the broker accepting connections yet?) -- never for business-data
 * conditions like "has a row appeared". That distinction matters: waiting
 * for a dependency to finish starting is a normal, bounded readiness gate;
 * waiting for an application-level side effect with the same mechanism is
 * the DB-polling anti-pattern this test suite intentionally avoids
 * elsewhere (see test/utils/event-tracker.ts for the event-driven
 * alternative used for that).
 */
async function pollUntilReady(
  description: string,
  check: () => Promise<void>,
  timeoutMs: number,
  intervalMs: number = DEFAULT_POLL_INTERVAL_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      await check();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }

  const reason =
    lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(
    `Timed out after ${timeoutMs}ms waiting for ${description} to become ready. Last error: ${reason}`,
  );
}

/**
 * Waits for an HTTP server to accept connections and respond with any
 * status code (a connection refused/reset is the failure mode being
 * waited out; an actual HTTP response, even a 404, means the server is
 * up).
 */
export async function waitForHttpReady(
  url: string,
  timeoutMs = 30_000,
): Promise<void> {
  await pollUntilReady(
    `HTTP endpoint ${url}`,
    async () => {
      await fetch(url);
    },
    timeoutMs,
  );
}

/**
 * Waits for a RabbitMQ broker to accept AMQP connections.
 */
export async function waitForRabbitMqReady(
  amqpUrl: string,
  timeoutMs = 30_000,
): Promise<void> {
  await pollUntilReady(
    `RabbitMQ broker ${amqpUrl}`,
    async () => {
      const connection = await amqplib.connect(amqpUrl);
      await connection.close();
    },
    timeoutMs,
  );
}
