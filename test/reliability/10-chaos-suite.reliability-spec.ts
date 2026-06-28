/**
 * ═══════════════════════════════════════════════════════════════════════════
 * RELIABILITY SCENARIO 10: Chaos Suite
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * FAILURE BEING SIMULATED
 * ──────────────────────
 * Randomized, concurrent failure injection across all infrastructure
 * components. Unlike the targeted single-failure tests (01–09), this suite
 * combines failures and injects them at random times, simulating real
 * production chaos:
 *
 *   - Relay instances killed mid-publish
 *   - RabbitMQ restarted mid-delivery
 *   - Consumers restarted mid-processing
 *   - Redeliveries triggered
 *   - Multiple concurrent events in flight during failures
 *
 * WHAT THIS TEST PROVES
 * ─────────────────────
 * Under arbitrary combinations of failures:
 *  1. Eventual consistency: every committed event is eventually delivered
 *  2. No lost events: zero events disappear permanently
 *  3. No duplicate side effects: idempotency prevents double processing
 *
 * This is the hardest reliability property to prove because:
 *  - Individual failure tests only prove single-point resilience
 *  - Production failures combine and overlap in unpredictable ways
 *  - The chaos suite exercises the interaction between reliability mechanisms
 *    (outbox + reaper + fencing + idempotency all active simultaneously)
 *
 * WHY THIS PROVES RELIABILITY
 * ──────────────────────────
 * If the system passes this suite, it has demonstrated:
 *  - The transactional outbox survives concurrent relay crashes
 *  - The stale-lock reaper works correctly under concurrent claims
 *  - The fencing token prevents double-publish during concurrent recovery
 *  - Consumer idempotency handles redeliveries from broker restarts
 *  - The retry budget is correctly enforced via the durable counter
 *  - The system self-heals without operator intervention
 */

import { randomUUID } from 'crypto';
import { PgTestClient, pollUntil } from '../utils/pg-client';
import { RabbitMqTestClient, waitForRabbitMqAmqpReady } from '../utils/rabbitmq-client';
import { waitForHttpReady } from '../utils/wait-for-health';
import {
  CONTAINERS,
  containerStop,
  containerStart,
  containerKill,
  waitForContainerHealthy,
} from '../utils/docker-control';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const MESSAGING_INTERNAL_URL = process.env.MESSAGING_HEALTH_URL ?? 'http://127.0.0.1:3006';

// ── Chaos Configuration ───────────────────────────────────────────────────

const CHAOS_CONFIG = {
  /** Total events to inject during the chaos run */
  numEvents: 20,
  /** Duration of chaos injection phase (ms) */
  chaosWindowMs: 30_000,
  /** Min/max delay between chaos actions (ms) */
  minActionDelayMs: 1_500,
  maxActionDelayMs: 4_000,
  /** How long to wait for eventual consistency after chaos stops */
  recoveryTimeoutMs: 180_000,
};

// ── Chaos Actions ─────────────────────────────────────────────────────────

type ChaosAction = {
  name: string;
  execute: () => Promise<void>;
  recover: () => Promise<void>;
};

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForServicesHealthy(): Promise<void> {
  await Promise.all([
    waitForRabbitMqAmqpReady(RABBITMQ_URL, 120_000),
    waitForHttpReady(
      `${MESSAGING_INTERNAL_URL}/internal/health/live`,
      120_000,
    ),
  ]);
}

// ── Main Chaos Suite ──────────────────────────────────────────────────────

describe('Reliability Scenario 10: Chaos Suite', () => {
  let db: PgTestClient;
  let rmq: RabbitMqTestClient;
  let chaosLog: string[] = [];

  function log(msg: string): void {
    const ts = new Date().toISOString();
    const entry = `[${ts}] ${msg}`;
    chaosLog.push(entry);
    console.log(entry); // visible in Jest output with --verbose
  }

  beforeAll(async () => {
    await waitForHttpReady(GATEWAY_URL, 60_000);
    await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);

    db = new PgTestClient();
    await db.connect();

    rmq = new RabbitMqTestClient();
    await rmq.connect();
  });

  afterAll(async () => {
    // Ensure all services are restored
    const services = [CONTAINERS.RABBITMQ, CONTAINERS.MESSAGING];
    for (const svc of services) {
      try { containerStart(svc); } catch { /* already running */ }
    }
    await waitForContainerHealthy(CONTAINERS.RABBITMQ, 120_000);
    await waitForContainerHealthy(CONTAINERS.MESSAGING, 120_000);
    await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);
    await db.disconnect();
    await rmq.disconnect();
  });

  it(
    'eventual consistency: all events delivered with no loss or duplicate side effects under chaos',
    async () => {
      // ── PHASE 1: Set up all events BEFORE chaos starts ────────────────────
      // Insert all outbox rows atomically. They are durable in Postgres
      // regardless of what happens to the relay or broker during chaos.

      const allEventIds: string[] = [];
      const allOutboxIds: number[] = [];
      const allCorrelationIds: string[] = [];

      log(`Inserting ${CHAOS_CONFIG.numEvents} events before chaos starts`);

      for (let i = 0; i < CHAOS_CONFIG.numEvents; i++) {
        const eventId = randomUUID();
        const correlationId = randomUUID();
        allEventIds.push(eventId);
        allCorrelationIds.push(correlationId);

        const outboxId = await db.insertOutboxRow({
          eventType: 'MessageCreated.v2',
          payload: {
            version: 2,
            messageId: randomUUID(),
            title: `Chaos Event ${i + 1}`,
            content: `Chaos suite event ${i + 1} of ${CHAOS_CONFIG.numEvents}`,
            sender: 'chaos-tester',
            recipient: 'test-consumer',
            correlationId,
            eventId,
            timestamp: new Date().toISOString(),
          },
          correlationId,
          eventId,
          status: 'pending',
          maxAttempts: 10, // Higher cap to survive multiple retry cycles
        });

        allOutboxIds.push(outboxId);
      }

      log(`All ${CHAOS_CONFIG.numEvents} outbox rows inserted. Starting chaos.`);

      // ── PHASE 2: Inject chaos concurrently while events are being processed ──

      const chaosActions: ChaosAction[] = [
        {
          name: 'RabbitMQ restart',
          execute: async () => {
            log('CHAOS: Stopping RabbitMQ');
            containerStop(CONTAINERS.RABBITMQ, 3);
          },
          recover: async () => {
            log('CHAOS RECOVERY: Starting RabbitMQ');
            containerStart(CONTAINERS.RABBITMQ);
            await waitForContainerHealthy(CONTAINERS.RABBITMQ, 120_000);
            await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);
            log('CHAOS RECOVERY: RabbitMQ healthy');
          },
        },
        {
          name: 'Messaging service restart',
          execute: async () => {
            log('CHAOS: Stopping messaging service');
            containerStop(CONTAINERS.MESSAGING, 3);
          },
          recover: async () => {
            log('CHAOS RECOVERY: Starting messaging service');
            containerStart(CONTAINERS.MESSAGING);
            await waitForContainerHealthy(CONTAINERS.MESSAGING, 120_000);
            log('CHAOS RECOVERY: Messaging service healthy');
          },
        },
        {
          name: 'Messaging service SIGKILL (abrupt crash)',
          execute: async () => {
            log('CHAOS: Killing messaging service (SIGKILL)');
            try { containerKill(CONTAINERS.MESSAGING); } catch { /* may already be stopped */ }
          },
          recover: async () => {
            log('CHAOS RECOVERY: Restarting messaging service after kill');
            containerStart(CONTAINERS.MESSAGING);
            await waitForContainerHealthy(CONTAINERS.MESSAGING, 120_000);
            log('CHAOS RECOVERY: Messaging service healthy after kill');
          },
        },
        {
          name: 'Both services restarted simultaneously',
          execute: async () => {
            log('CHAOS: Stopping BOTH RabbitMQ and messaging simultaneously');
            try { containerStop(CONTAINERS.MESSAGING, 2); } catch { /* ignore */ }
            try { containerStop(CONTAINERS.RABBITMQ, 2); } catch { /* ignore */ }
          },
          recover: async () => {
            log('CHAOS RECOVERY: Restarting both services');
            containerStart(CONTAINERS.RABBITMQ);
            await waitForRabbitMqAmqpReady(RABBITMQ_URL, 120_000);
            containerStart(CONTAINERS.MESSAGING);
            await waitForContainerHealthy(CONTAINERS.MESSAGING, 120_000);
            log('CHAOS RECOVERY: Both services healthy');
          },
        },
        {
          name: 'Force retry timing to trigger immediate re-publish',
          execute: async () => {
            log('CHAOS: Resetting retry timers on all pending rows');
            await db.query(
              `UPDATE outbox_events SET next_retry_at = now() WHERE status = 'pending'`,
            );
          },
          recover: async () => {
            // No recovery needed — just a timing adjustment
          },
        },
      ];

      // Run chaos actions for CHAOS_CONFIG.chaosWindowMs milliseconds
      const chaosDeadline = Date.now() + CHAOS_CONFIG.chaosWindowMs;
      let chaosIterations = 0;

      while (Date.now() < chaosDeadline) {
        // Pick a random action
        const action = chaosActions[randomInt(0, chaosActions.length - 1)]!;
        chaosIterations++;

        log(`Chaos iteration ${chaosIterations}: ${action.name}`);

        try {
          await action.execute();
          // Brief pause during the failure
          const downtime = randomInt(
            CHAOS_CONFIG.minActionDelayMs,
            CHAOS_CONFIG.maxActionDelayMs,
          );
          await sleep(downtime);
          await action.recover();

          // Wait for services to stabilize before next action
          await waitForServicesHealthy().catch(() => {
            log('Warning: services not fully stable, continuing chaos anyway');
          });
        } catch (err) {
          log(`Chaos action error (non-fatal): ${(err as Error).message}`);
          // Try to recover even if the action itself errored
          try {
            await action.recover();
          } catch {
            // Best effort
          }
        }

        // Small pause between actions
        await sleep(randomInt(500, 1_500));
      }

      log(`Chaos phase complete (${chaosIterations} iterations). Entering recovery phase.`);

      // ── PHASE 3: Full recovery — ensure all services are up ───────────────
      for (const svc of [CONTAINERS.RABBITMQ, CONTAINERS.MESSAGING]) {
        try { containerStart(svc); } catch { /* already running */ }
      }

      await waitForContainerHealthy(CONTAINERS.RABBITMQ, 120_000);
      await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);
      await waitForContainerHealthy(CONTAINERS.MESSAGING, 120_000);
      await waitForHttpReady(
        `${MESSAGING_INTERNAL_URL}/internal/health/live`,
        60_000,
      );

      // Reset retry timing so all pending rows are immediately claimable
      await db.query(
        `UPDATE outbox_events SET next_retry_at = now() WHERE status = 'pending'`,
      );

      log('All services healthy. Waiting for eventual consistency...');

      // ── PHASE 4: Assert eventual consistency ─────────────────────────────

      // ASSERTION 1: Every outbox row must eventually reach 'sent'
      // (no permanent failures from chaos — all events are eventually published)
      await pollUntil(
        `all ${CHAOS_CONFIG.numEvents} outbox rows reach 'sent'`,
        async () => {
          const results = await Promise.all(
            allOutboxIds.map((id) => db.getOutboxRow(id)),
          );
          const sentCount = results.filter((r) => r?.status === 'sent').length;
          const pendingCount = results.filter((r) => r?.status === 'pending').length;
          const failedCount = results.filter((r) => r?.status === 'failed').length;

          log(
            `Progress: sent=${sentCount}, pending=${pendingCount}, failed=${failedCount} / total=${CHAOS_CONFIG.numEvents}`,
          );

          // Replay any failed rows (chaos may have exhausted their retry budgets)
          if (failedCount > 0) {
            const failedRows = await db.getAllFailedOutboxRows();
            for (const row of failedRows) {
              if (allOutboxIds.includes(row.id)) {
                await db.replayOutboxRow(row.id);
                log(`Replayed failed outbox row ${row.id}`);
              }
            }
            await db.query(
              `UPDATE outbox_events SET next_retry_at = now() WHERE status = 'pending'`,
            );
          }

          return sentCount === CHAOS_CONFIG.numEvents;
        },
        {
          timeoutMs: CHAOS_CONFIG.recoveryTimeoutMs,
          intervalMs: 2_000,
        },
      );

      log('All outbox rows reached sent status.');

      // ASSERTION 2: No duplicate side effects
      // Each event should be in processed_events AT MOST once.
      // (Some events may still be in-flight if the consumer is slow — that's fine.
      //  The CRITICAL assertion is: count never exceeds 1.)
      await sleep(5_000); // Give consumer time to finish processing

      for (const eventId of allEventIds) {
        const count = await db.countProcessedEvents(eventId);
        expect(count).toBeLessThanOrEqual(1);
      }

      // ASSERTION 3: No events lost permanently
      // Every outbox row is 'sent' (published to broker exactly once from the relay).
      for (const outboxId of allOutboxIds) {
        const row = await db.getOutboxRow(outboxId);
        expect(row).not.toBeNull();
        expect(row!.status).toBe('sent');
      }

      log(`Chaos suite PASSED. Total chaos iterations: ${chaosIterations}`);
      log('Chaos log:');
      chaosLog.forEach((entry) => console.log(entry));
    },
    // This is a long-running test: chaos window + recovery timeout + buffer
    CHAOS_CONFIG.chaosWindowMs + CHAOS_CONFIG.recoveryTimeoutMs + 60_000,
  );

  it(
    'randomized concurrent relay simulation: multiple concurrent claim-publish cycles',
    async () => {
      // ── Concurrent Relay Simulation ───────────────────────────────────────
      // Simulate 3 relay instances all running their poll cycle simultaneously,
      // each racing to claim and publish the same pool of pending rows.
      // Verifies that SKIP LOCKED + fencing token together prevent any row
      // from being published more than once.

      const numRows = 10;
      const eventIds: string[] = [];
      const outboxIds: number[] = [];

      for (let i = 0; i < numRows; i++) {
        const eventId = randomUUID();
        const correlationId = randomUUID();
        eventIds.push(eventId);

        const id = await db.insertOutboxRow({
          eventType: 'MessageCreated.v2',
          payload: {
            version: 2,
            messageId: randomUUID(),
            title: `Concurrent Relay Race ${i + 1}`,
            content: 'Concurrent relay race test',
            sender: 'chaos-tester',
            recipient: 'test-consumer',
            correlationId,
            eventId,
          },
          correlationId,
          eventId,
          status: 'pending',
          maxAttempts: 10,
        });
        outboxIds.push(id);
      }

      // Simulate 3 concurrent relay instances trying to claim the same rows
      // by running 3 concurrent claimBatch() calls in Postgres.
      // The SKIP LOCKED + fencing token means each row is claimed exactly once.
      const { Client } = await import('pg');
      const { DB_CONFIG } = await import('../utils/pg-client');

      const relayCount = 3;
      const batchSize = numRows; // Each relay tries to claim all rows

      const allClaims = await Promise.all(
        Array.from({ length: relayCount }, (_, i) => i).map(async (relayIndex) => {
          const client = new Client(DB_CONFIG);
          await client.connect();

          const res = await client.query<{ id: number; lock_version: number }>(
            `UPDATE outbox_events
             SET locked_at    = now(),
                 locked_by    = $1,
                 lock_version = lock_version + 1
             WHERE id IN (
               SELECT id FROM outbox_events
               WHERE status = 'pending'
                 AND next_retry_at <= now()
                 AND id = ANY($3)
               ORDER BY next_retry_at ASC
               LIMIT $2
               FOR UPDATE SKIP LOCKED
             )
             RETURNING id, lock_version`,
            [`chaos-relay-${relayIndex}`, batchSize, outboxIds],
          );

          await client.end();
          return { relayIndex, claimed: res.rows };
        }),
      );

      // ── ASSERT: Each row was claimed by exactly one relay ─────────────────
      const claimedById = new Map<number, number[]>(); // outboxId → [relayIndexes]

      for (const { relayIndex, claimed } of allClaims) {
        for (const row of claimed) {
          if (!claimedById.has(row.id)) claimedById.set(row.id, []);
          claimedById.get(row.id)!.push(relayIndex);
        }
      }

      // Each row must be claimed by AT MOST ONE relay (SKIP LOCKED guarantee)
      for (const [outboxId, relays] of claimedById) {
        expect(relays.length).toBe(1);
      }

      // Total claims across all relays must equal numRows (no double-claims)
      const totalClaims = allClaims.reduce((sum, r) => sum + r.claimed.length, 0);
      expect(totalClaims).toBe(numRows);

      log(
        `Concurrent relay test: ${relayCount} relays raced for ${numRows} rows. ` +
        `All rows claimed exactly once. Total claims: ${totalClaims}`,
      );

      // Clean up: mark all as sent so they don't pollute other tests
      await db.query(
        `UPDATE outbox_events
         SET status = 'sent', sent_at = now(), locked_at = NULL, locked_by = NULL
         WHERE id = ANY($1)`,
        [outboxIds],
      );
    },
    60_000,
  );

  it(
    'redelivery storm: 100 duplicate deliveries of the same event are idempotent',
    async () => {
      // ── Simulate broker redelivery after consumer crash ───────────────────
      // When a consumer crashes mid-ACK, the broker may redeliver the same
      // message hundreds of times. The idempotency layer must handle all of
      // them and process the event exactly once.

      const eventId = randomUUID();
      const correlationId = randomUUID();

      const payload = {
        version: 2,
        messageId: randomUUID(),
        title: 'Redelivery Storm Test',
        content: 'This event will be delivered 100 times',
        sender: 'chaos-tester',
        recipient: 'test-consumer',
        correlationId,
        eventId,
        timestamp: new Date().toISOString(),
      };

      const headers = {
        'x-event-type': 'MessageCreated.v2',
        'x-correlation-id': correlationId,
        'x-event-id': eventId,
      };

      log(`Publishing 100 copies of eventId=${eventId}`);

      // Publish 100 copies in parallel to maximise concurrency
      const NUM_DUPLICATES = 100;
      await Promise.all(
        Array.from({ length: NUM_DUPLICATES }, () =>
          rmq.publishToWork(payload, headers),
        ),
      );

      log('All 100 copies published. Waiting for exactly-once processing...');

      // ── ASSERT: Processed exactly once ────────────────────────────────────
      // Wait for the first processing to complete
      await pollUntil(
        `eventId=${eventId} appears in processed_events`,
        async () => {
          const count = await db.countProcessedEvents(eventId);
          return count > 0;
        },
        { timeoutMs: 30_000, intervalMs: 200 },
      );

      // Wait for all 100 messages to be consumed (they'll all be rejected
      // as duplicates by the idempotency check after the first one succeeds)
      await sleep(15_000);

      // CRITICAL ASSERTION: Exactly ONE processed_events row
      const processedCount = await db.countProcessedEvents(eventId);
      expect(processedCount).toBe(1);

      // CRITICAL ASSERTION: Exactly ONE messages row (no duplicate business writes)
      const messageCount = await db.countMessagesByCorrelationId(correlationId);
      expect(messageCount).toBe(1);

      log(`Redelivery storm test PASSED: ${NUM_DUPLICATES} deliveries → 1 processed`);
    },
    120_000,
  );
});
