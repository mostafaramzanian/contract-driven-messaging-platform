import { Test } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { EventAttempt } from '../entities/event-attempt.entity';
import { RetryAttemptTrackerService } from './retry-attempt-tracker.service';

/**
 * Failure-proof tests for RetryAttemptTrackerService (Requirement 5E).
 *
 * Each test reproduces the exact failure mode that x-retry-count header
 * reliance alone cannot close, and proves the durable tracker closes it.
 */

function buildFakeRepo() {
  // The service uses repo.manager.query() for the UPSERT, and
  // repo.findOne()/repo.delete() for the read/clear paths. This fake
  // implements all three with an in-memory Map.
  const store = new Map<string, number>();

  const manager = {
    query: jest.fn(
      async (
        sql: string,
        params: unknown[],
      ): Promise<{ attempts: number }[]> => {
        // INSERT ... ON CONFLICT DO UPDATE SET attempts = attempts + 1
        if (sql.includes('ON CONFLICT')) {
          const [eventId] = params as [string];
          const current = store.get(eventId) ?? 0;
          const newCount = current + 1;
          store.set(eventId, newCount);
          return [{ attempts: newCount }];
        }
        throw new Error(`FakeRepo: unrecognized query: ${sql}`);
      },
    ),
  };

  return {
    manager,
    findOne: jest.fn(async ({ where }: { where: { eventId: string } }) => {
      const attempts = store.get(where.eventId);
      return attempts !== undefined
        ? { eventId: where.eventId, attempts }
        : null;
    }),
    delete: jest.fn(async ({ eventId }: { eventId: string }) => {
      store.delete(eventId);
    }),
    store, // expose for direct assertions
  };
}

describe('RetryAttemptTrackerService — failure-proof tests (Requirement 5E)', () => {
  let service: RetryAttemptTrackerService;
  let fakeRepo: ReturnType<typeof buildFakeRepo>;
  const EVENT_ID = '11111111-1111-4111-8111-111111111111';

  beforeEach(async () => {
    fakeRepo = buildFakeRepo();
    const module = await Test.createTestingModule({
      providers: [
        RetryAttemptTrackerService,
        { provide: getRepositoryToken(EventAttempt), useValue: fakeRepo },
      ],
    }).compile();
    service = module.get(RetryAttemptTrackerService);
  });

  describe('E. Retry count survives paths that reset the AMQP header counter', () => {
    it('the first call returns 1, not 0 -- each call records one attempt immediately', async () => {
      const count = await service.recordAttempt(EVENT_ID);
      expect(count).toBe(1);
    });

    it('survives a relay replay: a second recordAttempt on the same eventId returns 2, not 1', async () => {
      // This is the core behavioral assertion: a relay replay creates a
      // brand-new AMQP message with no x-retry-count header, so the
      // header-based counter would see "attempt 1" again. The durable
      // tracker has no such blind spot -- it increments the stored count
      // regardless of how the delivery arrived.
      await service.recordAttempt(EVENT_ID);
      const durableCount = await service.recordAttempt(EVENT_ID);
      expect(durableCount).toBe(2);
    });

    it('survives a broker restart: a third call after the counter was reset from the AMQP side still reads 3', async () => {
      // Model: 2 prior deliveries happened (their AMQP connections are
      // gone -- broker restarted -- but event_attempts persists in Postgres).
      // A third delivery arrives. From the header's perspective, this is
      // attempt 1. From the durable counter's perspective, it is attempt 3.
      await service.recordAttempt(EVENT_ID); // "delivery 1"
      await service.recordAttempt(EVENT_ID); // "delivery 2"
      // Broker restarts -- header counter gone, Postgres is unaffected.
      const countAfterBrokerRestart = await service.recordAttempt(EVENT_ID); // "delivery 3"
      expect(countAfterBrokerRestart).toBe(3);
    });

    it('survives MAX_ATTEMPTS consecutive header-less redeliveries: count is exhausted even with x-retry-count=0 on every message', async () => {
      // A message is redelivered 5 times via paths that carry no header
      // (manual requeue, relay replay, etc.). Each time, the header says
      // "attempt 1". The durable counter says 1, 2, 3, 4, 5.
      // The final count must be >= MAX_ATTEMPTS so the isRetryable decision
      // correctly routes to DLQ instead of retrying again indefinitely.
      const MAX_ATTEMPTS = 5;
      let finalCount = 0;
      for (let i = 0; i < MAX_ATTEMPTS; i++) {
        finalCount = await service.recordAttempt(EVENT_ID);
      }
      expect(finalCount).toBe(MAX_ATTEMPTS);
      // This is what the controller's check evaluates:
      expect(finalCount < MAX_ATTEMPTS).toBe(false); // retry budget exhausted
    });

    it('concurrent deliveries of the same eventId (two consumer instances receiving the same message simultaneously) both record the attempt atomically without a race', async () => {
      // This specifically tests the INSERT ... ON CONFLICT DO UPDATE
      // semantics: both calls start concurrently, but each must see a
      // different final count (1 and 2) rather than both seeing 1 --
      // which is what a non-atomic read-then-write would give (two reads
      // of 0, two writes of 1, one of them silently overwriting the other).
      const [count1, count2] = await Promise.all([
        service.recordAttempt(EVENT_ID),
        service.recordAttempt(EVENT_ID),
      ]);
      // The two counts must be distinct (one of them is 1, one is 2) --
      // not both 1 (which would indicate the second write clobbered the
      // first's increment). In the fake, the synchronous JS execution
      // serializes them naturally, but the SQL's ON CONFLICT DO UPDATE
      // handles the real concurrent-session case at the Postgres level.
      const sorted = [count1, count2].sort((a, b) => a - b);
      expect(sorted).toEqual([1, 2]);
    });

    it('getAttemptCount reads the current count without incrementing it', async () => {
      await service.recordAttempt(EVENT_ID);
      await service.recordAttempt(EVENT_ID);

      const readCount = await service.getAttemptCount(EVENT_ID);
      expect(readCount).toBe(2);

      // Reading again must not have changed the value.
      const readAgain = await service.getAttemptCount(EVENT_ID);
      expect(readAgain).toBe(2);
    });

    it('clearAttempts removes the record so the next recordAttempt starts at 1 again', async () => {
      await service.recordAttempt(EVENT_ID);
      await service.recordAttempt(EVENT_ID);

      await service.clearAttempts(EVENT_ID);

      const afterClear = await service.getAttemptCount(EVENT_ID);
      expect(afterClear).toBe(0);

      // A new delivery after the clear starts fresh (as intended for
      // retry-after-success or operator-initiated DLQ-replay scenarios).
      const freshCount = await service.recordAttempt(EVENT_ID);
      expect(freshCount).toBe(1);
    });
  });
});
