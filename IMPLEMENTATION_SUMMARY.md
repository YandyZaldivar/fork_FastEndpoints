# Implementation Summary: Distributed Job Queues

This document provides a high-level overview of the distributed job queue implementation in FastEndpoints.

## What Was Implemented

The implementation adds support for distributed job processing across horizontally scaled applications (multiple pods/instances). It provides two coordination strategies:

### 1. Row-Level Locking (PostgreSQL FOR UPDATE SKIP LOCKED)
- Each worker locks specific job rows using database-native mechanisms
- Multiple workers process different jobs concurrently
- Highest throughput and concurrency
- Requires PostgreSQL, SQL Server, or Oracle

### 2. Global Distributed Lock (Redis/PostgreSQL Advisory Locks)
- One worker locks the entire queue using external coordination
- Simpler implementation
- Lower concurrency but easier to reason about
- Works with any database

## Key Components

### New Interface Members in `IJobStorageProvider<TStorageRecord>`

1. **`TryAcquireLockAsync(string queueId, CancellationToken ct)`**
   - Called before fetching jobs from storage
   - Returns `true` if lock acquired, `false` otherwise
   - Default implementation returns `true` (no locking)

2. **`TryReleaseLockAsync(string queueId, CancellationToken ct)`**
   - Called after processing batch to release lock
   - Returns `true` if successfully released
   - Default implementation returns `true` (no-op)

3. **`OnLeaseHeartbeatAsync(TStorageRecord record, CancellationToken ct)`**
   - Called periodically for each pending job during processing
   - Used to renew job leases (extend `LockExpiresOn`)
   - Default implementation does nothing (no-op)

### New Properties in `IJobStorageRecord`

1. **`DateTime? LockExpiresOn`**
   - Timestamp when the current lock expires
   - Set when a job is acquired by a worker
   - If expired, job becomes available for other workers
   - Null if job is not currently locked

2. **`string? LockedBy`**
   - Identifier of the worker that locked the job
   - Typically machine name or pod ID
   - Used for observability and debugging
   - Null if job is not currently locked

### Configuration in `JobQueueOptions`

1. **`TimeSpan LeaseHeartbeatDelay`**
   - How often to call `OnLeaseHeartbeatAsync`
   - Default: 30 seconds
   - Should be 1/3 to 1/2 of lease duration

2. **`LimitsFor<TCommand>(..., TimeSpan leaseHeartbeatDelay)`**
   - Overload that accepts per-command heartbeat delay
   - Allows different heartbeat rates for different job types

## How It Works

### Job Processing Flow

```
1. Worker calls TryAcquireLockAsync(queueId)
   ├─ Success: Continue to step 2
   └─ Failure: Wait and retry

2. Worker calls GetNextBatchAsync(parameters)
   └─ Query includes: LockExpiresOn IS NULL OR LockExpiresOn <= NOW()
   └─ For row-level locking: Use FOR UPDATE SKIP LOCKED
   └─ Set LockExpiresOn and LockedBy on acquired jobs

3. Worker starts processing jobs in parallel
   └─ Simultaneously, heartbeat task runs every LeaseHeartbeatDelay
   └─ Heartbeat calls OnLeaseHeartbeatAsync for each pending job
   └─ Renew LockExpiresOn to prevent expiration

4. When jobs complete:
   └─ Set IsComplete = true
   └─ Clear LockExpiresOn and LockedBy

5. Worker calls TryReleaseLockAsync(queueId)
```

### Fault Tolerance

**If worker crashes before acquiring lock:**
- No problem - lock was never acquired

**If worker crashes after acquiring lock but before processing:**
- Row-level locking: Jobs were locked in DB, leases expire naturally
- Global locking: Redis/advisory lock expires automatically

**If worker crashes during processing:**
- Leases expire (no more heartbeats)
- Other workers pick up the jobs
- Job is retried from the beginning

**If job processing exceeds lease duration:**
- Without heartbeat: Lease expires, another worker may pick it up (duplicate processing risk)
- With heartbeat: Lease is renewed, job continues processing safely

## Example Scenarios

### Scenario 1: 3 Pods Processing Email Jobs (Row-Level Locking)

```
Initial State:
- 100 email jobs in queue
- 3 pods running (Pod-A, Pod-B, Pod-C)

Step 1: All pods query for jobs
- Pod-A gets jobs 1-10 (locked with FOR UPDATE SKIP LOCKED)
- Pod-B gets jobs 11-20 (skips 1-10 as they're locked)
- Pod-C gets jobs 21-30 (skips 1-20 as they're locked)

Step 2: All pods process concurrently
- Each pod processes 10 jobs in parallel
- Heartbeat task renews leases every 30 seconds

Step 3: Jobs complete
- Pods mark jobs as complete
- Query for next batch (jobs 31-60)
```

### Scenario 2: 3 Pods Processing Email Jobs (Global Lock)

```
Initial State:
- 100 email jobs in queue
- 3 pods running (Pod-A, Pod-B, Pod-C)

Step 1: Pods compete for global lock
- Pod-A acquires Redis lock for "email-queue"
- Pod-B fails to acquire lock, waits
- Pod-C fails to acquire lock, waits

Step 2: Pod-A processes jobs
- Gets jobs 1-10 from database
- Processes them in parallel
- Pod-B and Pod-C remain blocked

Step 3: Pod-A releases lock
- Pod-B acquires lock
- Gets jobs 11-20
- Pod-C waits

(Process repeats with pods taking turns)
```

## Implementation Checklist

To implement distributed job queues:

- [ ] Add `LockExpiresOn` and `LockedBy` properties to storage record
- [ ] Implement `TryAcquireLockAsync` (or leave default for row-level locking)
- [ ] Implement `TryReleaseLockAsync` (or leave default for row-level locking)
- [ ] Implement `OnLeaseHeartbeatAsync` to renew leases
- [ ] Update `GetNextBatchAsync` to:
  - Filter by `LockExpiresOn IS NULL OR LockExpiresOn <= NOW()`
  - Use `FOR UPDATE SKIP LOCKED` for row-level locking
  - Set `LockExpiresOn` and `LockedBy` on acquired jobs
- [ ] Configure `LeaseHeartbeatDelay` in `JobQueueOptions`
- [ ] Add database indexes on `(QueueID, IsComplete, ExecuteAfter, LockExpiresOn)`

## Files Modified

### Core Implementation
- `Src/Library/Messaging/Jobs/IJobStorageProvider.cs` - Added lock methods
- `Src/Library/Messaging/Jobs/IJobStorageRecord.cs` - Added lock properties
- `Src/Library/Messaging/Jobs/JobQueue.cs` - Added lock acquisition and heartbeat logic
- `Src/Library/Messaging/Jobs/JobQueueOptions.cs` - Added lease heartbeat configuration

### Tests
- `Web/[Features]/TestCases/Messaging/JobQueueTest/TestStorageProvider.cs` - Reference implementation
- `Tests/IntegrationTests/FastEndpoints/MessagingTests/JobQueueTests.cs` - Integration tests

## Documentation Files

- **`PR_DESCRIPTION.md`** - Concise PR description for GitHub
- **`DISTRIBUTED_JOB_QUEUES.md`** - Complete user documentation with examples
- **`IMPLEMENTATION_SUMMARY.md`** - This file - developer-focused implementation overview

## Benefits

1. **Horizontal Scalability**: Run multiple instances safely
2. **Fault Tolerance**: Automatic recovery from worker crashes
3. **No Duplicate Processing**: Locks prevent concurrent execution
4. **Flexible Deployment**: Choose locking strategy based on needs
5. **Backward Compatible**: Default implementations mean no breaking changes

## Next Steps

1. Review `PR_DESCRIPTION.md` for the PR text
2. Review `DISTRIBUTED_JOB_QUEUES.md` for complete user documentation
3. Share examples with users who need distributed job queues
4. Consider adding these docs to the official FastEndpoints documentation website

## Notes

- All new interface methods have default implementations (no breaking changes)
- Existing job queue code works without modification
- Row-level locking provides better throughput but requires database support
- Global locking is simpler but limits concurrency to one worker per queue
- Lease duration should be tuned based on typical job execution time
- Heartbeat interval should be 1/3 to 1/2 of lease duration
