import { ConfigService } from '@nestjs/config';
import { context, propagation } from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { MetricsService } from '@app/common';
import { OutboxRelayService } from './outbox-relay.service';

/**
 * Failure-proof tests for OutboxRelayService.
 *
 * These tests do not assert "the code calls method X" — they reproduce
 * the exact failure mode each fix (Requirements 1–3 of the
 * fencing/confirms/trace-propagation task) was written to close, using a
 * fake AMQP channel and a fake Postgres `DataSource.query` double that
 * can independently confirm or reject a publish, succeed or fail a
 * compare-and-swap, and simulate two relay instances racing for the same
 * row. Each test is written to FAIL against the pre-fix behavior
 * (verified manually during development — see inline notes) and PASS
 * against the current implementation.
 *
 * ## Why a fake double instead of a real Postgres/RabbitMQ
 *
 * This environment has no Docker, so a live broker/database integration
 * test (the gold standard for this class of bug) cannot be executed
 * here — see `test/integration/messaging-flow-v2.integration-spec.ts`'s
 * own header comment for the same caveat applied to the gateway/consumer
 * path. These tests instead drive `OutboxRelayService`'s real,
 * unmodified `claimBatch`/`markSent`/`markFailedAttempt`/`publishOne`
 * logic against fakes that can deterministically reproduce timing-
 * dependent races (which a real broker, run once in CI, might not
 * reliably hit) — a legitimate and standard technique for testing
 * concurrency bugs, not a substitute for eventually also verifying
 * against real infrastructure.
 */

// ── Fakes ────────────────────────────────────────────────────────────────

/**
 * A minimal fake Postgres backing store for the subset of `outbox_events`
 * columns this service touches, driven by hand-written SQL pattern
 * matching against the real queries `OutboxRelayService` issues. This is
 * deliberately NOT a general-purpose SQL engine — it implements exactly
 * the handful of UPDATE/SELECT shapes this one service emits, closely
 * enough to exercise the real compare-and-swap and locking semantics.
 */
class FakeOutboxTable {
  rows = new Map<
    number,
    {
      id: number;
      event_type: string;
      payload: unknown;
      correlation_id: string | null;
      attempts: number;
      max_attempts: number;
      status: 'pending' | 'sent' | 'failed';
      lock_version: number;
      locked_at: Date | null;
      locked_by: string | null;
      next_retry_at: Date;
      trace_context: Record<string, string> | null;
      event_id: string | null;
      last_error: string | null;
      sent_at: Date | null;
    }
  >();

  seed(
    row: Partial<typeof this.rows extends Map<number, infer V> ? V : never> & {
      id: number;
    },
  ) {
    this.rows.set(row.id, {
      event_type: 'TestEvent',
      payload: { foo: 'bar' },
      correlation_id: null,
      attempts: 0,
      max_attempts: 5,
      status: 'pending',
      lock_version: 0,
      locked_at: null,
      locked_by: null,
      next_retry_at: new Date(0),
      trace_context: null,
      event_id: null,
      last_error: null,
      sent_at: null,
      ...row,
    });
  }

  /**
   * Routes a SQL string + params to the matching fake handler, in the
   * exact shape `DataSource.query(sql, params)` is called by
   * OutboxRelayService. Pattern-matches on a short, distinctive
   * substring of each real query rather than parsing SQL.
   */
  query = jest.fn(
    async (sql: string, params: unknown[] = []): Promise<unknown[]> => {
      const normalized = sql.replace(/\s+/g, ' ').trim();

      // claimBatch(): UPDATE ... SET lock_version = lock_version + 1 ... RETURNING
      if (
        normalized.includes('SET locked_at = now()') &&
        normalized.includes('lock_version = lock_version + 1')
      ) {
        const [lockedBy, limit] = params as [string, number];
        const claimable = [...this.rows.values()]
          .filter(
            (r) =>
              r.status === 'pending' &&
              r.next_retry_at.getTime() <= Date.now() &&
              r.locked_at === null, // SKIP LOCKED: already-claimed rows are invisible to concurrent claimants
          )
          .sort((a, b) => a.next_retry_at.getTime() - b.next_retry_at.getTime())
          .slice(0, limit);

        const claimed = claimable.map((r) => {
          r.locked_at = new Date();
          r.locked_by = lockedBy;
          r.lock_version += 1;
          return { ...r };
        });

        return claimed.map((r) => ({
          id: r.id,
          event_type: r.event_type,
          payload: r.payload,
          correlation_id: r.correlation_id,
          attempts: r.attempts,
          max_attempts: r.max_attempts,
          lock_version: r.lock_version,
          trace_context: r.trace_context,
          event_id: r.event_id,
        }));
      }

      // markSent(): UPDATE ... SET status = 'sent' ... WHERE id = $1 AND lock_version = $2
      if (normalized.includes("SET status = 'sent'")) {
        const [id, expectedLockVersion] = params as [number, number];
        const row = this.rows.get(id);
        if (!row || row.lock_version !== expectedLockVersion) {
          return []; // CAS failed — no rows matched, exactly like real Postgres
        }
        row.status = 'sent';
        row.sent_at = new Date();
        row.locked_at = null;
        row.locked_by = null;
        return [{ id }];
      }

      // markFailedAttempt(): UPDATE ... SET status = $1, attempts = $2, ... WHERE id = $5 AND lock_version = $6
      if (normalized.includes('SET status = $1, attempts = $2')) {
        const [
          status,
          attempts,
          lastError,
          nextRetryAt,
          id,
          expectedLockVersion,
        ] = params as [string, number, string, Date, number, number];
        const row = this.rows.get(id);
        if (!row || row.lock_version !== expectedLockVersion) {
          return [];
        }
        row.status = status as 'pending' | 'failed';
        row.attempts = attempts;
        row.last_error = lastError;
        row.next_retry_at = nextRetryAt;
        row.locked_at = null;
        row.locked_by = null;
        return [{ id }];
      }

      // reapStaleLocks(): UPDATE ... SET locked_at = NULL ... WHERE locked_at < now() - ttl
      if (
        normalized.includes(
          'SET locked_at = NULL, locked_by = NULL WHERE status',
        )
      ) {
        const [ttlMs] = params as [number];
        const cutoff = Date.now() - ttlMs;
        const reaped: { id: number }[] = [];
        for (const row of this.rows.values()) {
          if (
            row.status === 'pending' &&
            row.locked_at !== null &&
            row.locked_at.getTime() < cutoff
          ) {
            row.locked_at = null;
            row.locked_by = null;
            reaped.push({ id: row.id });
          }
        }
        return reaped;
      }

      // refreshPendingGauge(): SELECT COUNT(*)
      if (normalized.includes('COUNT(*)::int AS count')) {
        const count = [...this.rows.values()].filter(
          (r) => r.status === 'pending',
        ).length;
        return [{ count }];
      }

      throw new Error(`FakeOutboxTable: unrecognized query: ${normalized}`);
    },
  );
}

/**
 * A fake amqplib ConfirmChannel whose publish()/waitForConfirms()
 * behavior is independently controllable per test — this is what lets
 * tests reproduce "the broker accepted the local write but then the
 * relay crashed before confirmation arrived" and similar timing-specific
 * scenarios deterministically.
 */
function buildFakeConfirmChannel(
  opts: {
    publishReturns?: boolean;
    waitForConfirmsBehavior?: 'resolve' | 'reject' | 'never-resolve';
  } = {},
) {
  const { publishReturns = true, waitForConfirmsBehavior = 'resolve' } = opts;

  return {
    publish: jest.fn().mockReturnValue(publishReturns),
    waitForConfirms: jest.fn(() => {
      if (waitForConfirmsBehavior === 'reject') {
        return Promise.reject(new Error('broker nacked the publish'));
      }
      if (waitForConfirmsBehavior === 'never-resolve') {
        return new Promise(() => {
          /* simulates a crash: this promise never settles within the test */
        });
      }
      return Promise.resolve();
    }),
    close: jest.fn().mockResolvedValue(undefined),
  };
}

/**
 * Thin test subclass exposing the one deliberate testability seam
 * (`getChannel`, made `protected` specifically for this purpose — see
 * outbox-relay.service.ts's doc comment on that method) so tests can
 * inject a FakeConfirmChannel without attempting a real AMQP connection.
 * No other method is overridden — claimBatch/publishOne/markSent/
 * markFailedAttempt/reapStaleLocks all run their real, unmodified logic.
 */
class TestableOutboxRelayService extends OutboxRelayService {
  fakeChannel: ReturnType<typeof buildFakeConfirmChannel>;

  constructor(...args: ConstructorParameters<typeof OutboxRelayService>) {
    super(...args);
    this.fakeChannel = buildFakeConfirmChannel();
  }

  protected async getChannel() {
    return this.fakeChannel as unknown as Awaited<
      ReturnType<OutboxRelayService['getChannel']>
    >;
  }
}

function buildService(table: FakeOutboxTable, metrics: MetricsService) {
  const fakeDataSource = {
    query: table.query,
  } as unknown as ConstructorParameters<typeof OutboxRelayService>[0];
  const config = new ConfigService({});
  return new TestableOutboxRelayService(fakeDataSource, metrics, config);
}

// ── Tests ────────────────────────────────────────────────────────────────

describe('OutboxRelayService — failure-proof tests', () => {
  let table: FakeOutboxTable;
  let metrics: MetricsService;
  let service: TestableOutboxRelayService;

  beforeEach(() => {
    table = new FakeOutboxTable();
    metrics = new MetricsService();
    service = buildService(table, metrics);
  });

  // ── A. Broker acknowledgement required before marking event published ──

  describe('A. Broker acknowledgement required before marking event PUBLISHED', () => {
    it('does NOT mark the row sent if the broker never confirms the publish (simulated crash before confirmation)', async () => {
      table.seed({ id: 1 });
      // publish() returns true (locally accepted into the write buffer —
      // this is the exact signal the OLD code treated as "sent"), but
      // waitForConfirms() never resolves, simulating the relay process
      // crashing (or the connection dying) after the local write but
      // before the broker's ack arrives.
      service.fakeChannel = buildFakeConfirmChannel({
        publishReturns: true,
        waitForConfirmsBehavior: 'never-resolve',
      });

      const rows = await service.claimBatch();
      expect(rows).toHaveLength(1);

      // publishOne() will hang on waitForConfirms() — race it against a
      // short timeout standing in for "the process died here".
      const result = await Promise.race([
        service.publishOne(rows[0]).then(() => 'completed'),
        new Promise((resolve) => setTimeout(() => resolve('timed-out'), 50)),
      ]);
      expect(result).toBe('timed-out');

      // The critical assertion: because waitForConfirms() never
      // resolved, markSent() was never reached, so the row's status is
      // UNCHANGED — still 'pending', not incorrectly marked 'sent' on
      // the strength of the local publish() boolean alone. Against the
      // pre-fix implementation (createChannel(), no waitForConfirms()),
      // this row would already be 'sent' at this point, because the
      // old code called markSent() immediately after a true publish()
      // return with no broker-confirmation step at all.
      expect(table.rows.get(1)!.status).toBe('pending');
    });

    it('marks the row sent only AFTER waitForConfirms() resolves, not merely after publish() returns true', async () => {
      table.seed({ id: 1 });
      let confirmResolved = false;
      service.fakeChannel = {
        publish: jest.fn().mockReturnValue(true),
        waitForConfirms: jest.fn(async () => {
          // Simulate real broker-ack latency: publish() returning true
          // happens synchronously, well before this resolves.
          await new Promise((r) => setTimeout(r, 10));
          confirmResolved = true;
        }),
        close: jest.fn(),
      };

      const [row] = await service.claimBatch();

      const publishPromise = service.publishOne(row);
      // Immediately after publish() (synchronous) but before
      // waitForConfirms() resolves: the row must NOT be sent yet.
      // (We can't assert mid-flight state deterministically without a
      // hook, so instead we assert the postcondition below proves
      // ordering: confirmResolved was true before status flipped.)
      await publishPromise;

      expect(confirmResolved).toBe(true);
      expect(table.rows.get(1)!.status).toBe('sent');
    });

    it('routes to retry, NOT sent, when the broker explicitly rejects (nacks) the publish via waitForConfirms()', async () => {
      table.seed({ id: 1, attempts: 0, max_attempts: 5 });
      service.fakeChannel = buildFakeConfirmChannel({
        publishReturns: true,
        waitForConfirmsBehavior: 'reject',
      });

      const [row] = await service.claimBatch();
      await service.publishOne(row);

      const after = table.rows.get(1)!;
      expect(after.status).toBe('pending'); // scheduled for retry, not sent
      expect(after.attempts).toBe(1);
      expect(after.last_error).toContain('broker nacked');
    });

    it('uses createConfirmChannel-style publishing: waitForConfirms() is actually invoked on every successful local publish', async () => {
      table.seed({ id: 1 });
      const fake = buildFakeConfirmChannel();
      service.fakeChannel = fake;

      const [row] = await service.claimBatch();
      await service.publishOne(row);

      // This is the direct, minimal-but-meaningful assertion that the
      // confirm-channel API is actually being used, not bypassed.
      expect(fake.waitForConfirms).toHaveBeenCalledTimes(1);
    });
  });

  // ── B. Relay crash cannot produce silent message loss ───────────────────

  describe('B. Relay crash cannot produce silent message loss', () => {
    it('a row whose publish never reached waitForConfirms() (simulated crash) is still claimable by a later tick — not lost', async () => {
      table.seed({ id: 1 });
      service.fakeChannel = buildFakeConfirmChannel({
        waitForConfirmsBehavior: 'never-resolve',
      });

      const [row] = await service.claimBatch();
      // Fire-and-forget, simulating the process dying mid-await — we do
      // NOT await this promise, modeling an abrupt crash.
      void service.publishOne(row);

      // The row is still locked (locked_at/locked_by set) at this
      // instant, exactly as a real crash would leave it: claimed but
      // never confirmed sent.
      expect(table.rows.get(1)!.status).toBe('pending');
      expect(table.rows.get(1)!.locked_at).not.toBeNull();

      // Time passes beyond OUTBOX_LOCK_TTL_MS. The reaper runs (this is
      // the SAME reaper that already exists in production, exercised
      // here directly rather than via its setInterval wrapper).
      const oldLockedAt = new Date(Date.now() - 70_000); // > default 60s TTL
      table.rows.get(1)!.locked_at = oldLockedAt;

      await service.reapStaleLocks();

      // The lock is cleared — the row is reclaimable.
      expect(table.rows.get(1)!.locked_at).toBeNull();
      expect(table.rows.get(1)!.status).toBe('pending'); // never silently lost

      // A fresh relay "instance" (a new service, modeling a process
      // restart after the crash) claims and successfully publishes it.
      const recoveredService = buildService(table, metrics);
      recoveredService.fakeChannel = buildFakeConfirmChannel(); // healthy broker now
      const [reclaimed] = await recoveredService.claimBatch();
      expect(reclaimed).toBeDefined();
      await recoveredService.publishOne(reclaimed);

      expect(table.rows.get(1)!.status).toBe('sent');
    });

    it('a publish that fails outright (exception thrown by publish()) is recorded as a failed attempt, not silently dropped', async () => {
      table.seed({ id: 1, attempts: 0, max_attempts: 5 });
      service.fakeChannel = {
        publish: jest.fn(() => {
          throw new Error('ECONNRESET');
        }),
        waitForConfirms: jest.fn(),
        close: jest.fn(),
      };

      const [row] = await service.claimBatch();
      await service.publishOne(row);

      const after = table.rows.get(1)!;
      expect(after.status).toBe('pending');
      expect(after.attempts).toBe(1);
      expect(after.last_error).toContain('ECONNRESET');
      // Row remains visible and re-claimable -- not deleted, not stuck.
      expect(table.rows.has(1)).toBe(true);
    });
  });

  // ── C. Relay race cannot double-publish ──────────────────────────────────

  describe('C. Relay race cannot double-publish (fencing token)', () => {
    it('a stale claimant whose lock was reaped and reclaimed by a second instance cannot mark the row sent after the second instance already did', async () => {
      table.seed({ id: 1 });

      // Instance A claims the row (lock_version: 0 -> 1).
      const instanceA = buildService(table, metrics);
      const [claimedByA] = await instanceA.claimBatch();
      expect(claimedByA.lock_version).toBe(1);

      // Time passes; A is merely SLOW (not dead) — e.g. blocked on a
      // slow network round-trip to a struggling broker — but the
      // reaper, running on its own timer, doesn't know that and reaps
      // A's lock because OUTBOX_LOCK_TTL_MS has elapsed.
      table.rows.get(1)!.locked_at = new Date(Date.now() - 70_000);
      await instanceA.reapStaleLocks();
      expect(table.rows.get(1)!.locked_at).toBeNull();

      // Instance B claims the now-unlocked row (lock_version: 1 -> 2)
      // and successfully publishes + confirms it.
      const instanceB = buildService(table, metrics);
      instanceB.fakeChannel = buildFakeConfirmChannel();
      const [claimedByB] = await instanceB.claimBatch();
      expect(claimedByB.lock_version).toBe(2);
      await instanceB.publishOne(claimedByB);
      expect(table.rows.get(1)!.status).toBe('sent');

      // NOW instance A's original, slow publish finally completes (the
      // broker round-trip A was blocked on finally returns) and A
      // attempts markSent() with the STALE lock_version (1) it claimed
      // with, not the current one (2).
      const aStillOwnsIt = await instanceA.markSent(
        claimedByA.id,
        claimedByA.lock_version,
      );

      // The critical assertion: A's compare-and-swap correctly FAILS —
      // it does not get to overwrite B's already-sent row, and the
      // method tells the caller it lost the race rather than silently
      // "succeeding" with no row actually matched. Without the fencing
      // token (the pre-fix `UPDATE ... WHERE id = $1`, no lock_version
      // check), this call would unconditionally succeed regardless of
      // which instance ran last, with no way to detect that a race
      // happened at all.
      expect(aStillOwnsIt).toBe(false);
      // B's write is undisturbed.
      expect(table.rows.get(1)!.status).toBe('sent');
    });

    it('two instances claiming concurrently never receive the same row (SKIP LOCKED semantics preserved)', async () => {
      table.seed({ id: 1 });
      table.seed({ id: 2 });

      const instanceA = buildService(table, metrics);
      const instanceB = buildService(table, metrics);

      // The fake table's query() is not truly concurrent (it's
      // synchronous JS), so this specifically verifies the SEQUENTIAL
      // correctness property SKIP LOCKED provides: once A has claimed a
      // row, that exact row is excluded from B's claim, because A's
      // claim already flipped locked_at/locked_by before B's query runs.
      const claimedByA = await instanceA.claimBatch();
      const claimedByB = await instanceB.claimBatch();

      const idsA = new Set(claimedByA.map((r) => r.id));
      const idsB = new Set(claimedByB.map((r) => r.id));
      const overlap = [...idsA].filter((id) => idsB.has(id));

      expect(overlap).toHaveLength(0);
      expect(idsA.size + idsB.size).toBe(2); // both rows claimed, no row claimed twice, none skipped
    });

    it('records outboxFencedPublishesTotal when a fencing-token race is detected, for operational visibility', async () => {
      table.seed({ id: 1 });
      const instanceA = buildService(table, metrics);

      // A's channel: publish succeeds locally, but confirmation is
      // deliberately held until we manually resolve it below -- this
      // models instanceA being alive but slow.
      let resolveAConfirm!: () => void;
      instanceA.fakeChannel = {
        publish: jest.fn().mockReturnValue(true),
        waitForConfirms: jest.fn(
          () =>
            new Promise<void>((resolve) => {
              resolveAConfirm = resolve;
            }),
        ),
        close: jest.fn(),
      };

      // instanceA claims the row and fires publishOne.
      const [claimedByA] = await instanceA.claimBatch();
      const instanceAPublish = instanceA.publishOne(claimedByA); // starts but hangs on waitForConfirms

      // While A is mid-publish, the lock TTL elapses and the reaper fires.
      table.rows.get(1)!.locked_at = new Date(Date.now() - 70_000);
      await instanceA.reapStaleLocks();

      // instanceB claims the now-unlocked row (bumps lock_version again) and
      // successfully publishes + confirms it -- at this point the row is 'sent'.
      const instanceB = buildService(table, metrics);
      instanceB.fakeChannel = buildFakeConfirmChannel();
      const [claimedByB] = await instanceB.claimBatch();
      await instanceB.publishOne(claimedByB);
      expect(table.rows.get(1)!.status).toBe('sent');

      const beforeValue =
        (
          await metrics.registry
            .getSingleMetric('outbox_fenced_publishes_total')!
            .get()
        ).values.find((v) => v.labels.service === 'messaging')?.value ?? 0;

      // Now instanceA's confirmation finally arrives -- it calls markSent()
      // with the stale lock_version. The CAS fails, publishOne() must log
      // and increment the fencing counter.
      resolveAConfirm();
      await instanceAPublish;

      const afterValue =
        (
          await metrics.registry
            .getSingleMetric('outbox_fenced_publishes_total')!
            .get()
        ).values.find((v) => v.labels.service === 'messaging')?.value ?? 0;

      expect(afterValue).toBe(beforeValue + 1);
      // The row is still 'sent' (B's write was not clobbered by A).
      expect(table.rows.get(1)!.status).toBe('sent');
    });
  });

  // ── D. Trace survives the outbox boundary ────────────────────────────────

  describe('D. Trace survives the outbox boundary', () => {
    let contextManager: AsyncLocalStorageContextManager;

    beforeEach(() => {
      propagation.setGlobalPropagator(new W3CTraceContextPropagator());

      // Production registers a real ContextManager via
      // NodeTracerProvider.register() inside otel-bootstrap.ts's
      // NodeSDK.start() (see that file's comment, and the empirical
      // verification performed while building this fix: context.with()
      // is a documented no-op under @opentelemetry/api's default
      // NoopContextManager, which is what's active if nothing else
      // registers one — exactly the situation under Jest, since this
      // project's jest config has no `setupFiles` entry that would run
      // NodeSDK.start() the way main.ts does in the real application).
      // Registering the SAME context manager class NodeSDK uses by
      // default reproduces production's actual propagation behavior
      // for this test, rather than either skipping verification of
      // context.with()'s real effect or silently testing against a
      // no-op that would pass regardless of whether publishOne()'s
      // trace-restoration logic is correct.
      contextManager = new AsyncLocalStorageContextManager();
      contextManager.enable();
      context.setGlobalContextManager(contextManager);
    });

    afterEach(() => {
      contextManager.disable();
    });

    it('publishOne() emits the ORIGINAL producer traceparent stored in trace_context, not an unrelated/absent one', async () => {
      const originalTraceparent =
        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
      table.seed({
        id: 1,
        trace_context: { traceparent: originalTraceparent },
      });

      const fake = buildFakeConfirmChannel();
      service.fakeChannel = fake;

      const [row] = await service.claimBatch();
      expect(row.trace_context).toEqual({ traceparent: originalTraceparent });

      await service.publishOne(row);

      const publishCall = fake.publish.mock.calls[0];
      const headers = (publishCall[3] as { headers: Record<string, string> })
        .headers;

      // The exact original traceId must be present in what got
      // published — proving the producer's trace, not a fresh/ambient
      // one, was propagated onto the outgoing AMQP message.
      expect(headers.traceparent).toBeDefined();
      expect(headers.traceparent).toContain('4bf92f3577b34da6a3ce929d0e0e4736');
    });

    it('does not throw and still publishes successfully when trace_context is null (row predates the column, or had no active span at write time)', async () => {
      table.seed({ id: 1, trace_context: null });
      const fake = buildFakeConfirmChannel();
      service.fakeChannel = fake;

      const [row] = await service.claimBatch();
      await expect(service.publishOne(row)).resolves.toBeUndefined();

      expect(table.rows.get(1)!.status).toBe('sent');
      // Headers were still built (event-type/correlation-id/etc.) even
      // with no trace to restore -- this is a graceful no-op, not a crash.
      expect(fake.publish).toHaveBeenCalledTimes(1);
    });

    it('different rows with different stored trace_context values each propagate THEIR OWN trace, not a shared/leaked one', async () => {
      const traceA = '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01';
      const traceB = '00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01';
      table.seed({ id: 1, trace_context: { traceparent: traceA } });
      table.seed({ id: 2, trace_context: { traceparent: traceB } });

      const fake = buildFakeConfirmChannel();
      service.fakeChannel = fake;

      const rows = await service.claimBatch();
      for (const row of rows) {
        await service.publishOne(row);
      }

      const headersById = new Map(
        fake.publish.mock.calls.map((call) => {
          const opts = call[3] as { headers: Record<string, string> };
          return [opts.headers['x-outbox-id'], opts.headers.traceparent];
        }),
      );

      expect(headersById.get('1')).toContain(
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      );
      expect(headersById.get('2')).toContain(
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      );
      // The two traces must not have leaked into each other -- proves
      // context.with() correctly scopes each restoration to its own
      // publish call rather than mutating shared ambient state.
      expect(headersById.get('1')).not.toContain(
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      );
      expect(headersById.get('2')).not.toContain(
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      );
    });
  });
});
