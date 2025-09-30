# Pull Request: Distributed Job Queue Support

## Overview

This PR adds distributed job queue support to FastEndpoints, enabling horizontally scaled applications to safely coordinate job processing across multiple instances without duplicate execution.

## Implementation

The implementation provides a lease-based locking mechanism with two deployment strategies:

### 1. Row-Level Locking (PostgreSQL FOR UPDATE SKIP LOCKED)
- Uses database-native row locks for maximum concurrency
- Multiple workers process different jobs simultaneously
- Each worker independently selects and locks specific job rows
- Ideal for high-volume queues with many workers

### 2. Global Distributed Lock (Redis/PostgreSQL Advisory Locks)
- Uses external lock coordination (Redis) or database advisory locks (PostgreSQL)
- Only one worker processes the queue at a time
- Simpler implementation with lower concurrency
- Ideal for lower-volume queues or stricter ordering requirements

## Key Features

### Lease-Based Job Locking
Jobs use time-based leases to prevent duplicate processing:
- **`LockExpiresOn`**: Timestamp when the current lock expires
- **`LockedBy`**: Identifier of the worker holding the lock
- **Heartbeat Mechanism**: Periodic lease renewals for long-running jobs

If a worker crashes, leases naturally expire and other workers can acquire the jobs.

### New Interface Methods

#### `IJobStorageProvider<TStorageRecord>`

```csharp
/// <summary>
/// Attempt to acquire an exclusive lock for processing jobs in the specified queue.
/// </summary>
Task<bool> TryAcquireLockAsync(string queueId, CancellationToken ct);

/// <summary>
/// Release the exclusive lock for the specified queue.
/// </summary>
Task<bool> TryReleaseLockAsync(string queueId, CancellationToken ct);

/// <summary>
/// Called periodically to signal that the lease for a job is still active.
/// </summary>
ValueTask OnLeaseHeartbeatAsync(TStorageRecord record, CancellationToken ct);
```

#### `IJobStorageRecord`

```csharp
/// <summary>
/// The date/time when the current lock will expire.
/// </summary>
DateTime? LockExpiresOn { get; set; }

/// <summary>
/// Identifies the worker that has locked the job for processing.
/// </summary>
string? LockedBy { get; set; }
```

### Configuration Options

#### `JobQueueOptions`

```csharp
/// <summary>
/// Specifies the interval for sending heartbeat signals to maintain job leases.
/// Default: 30 seconds
/// </summary>
TimeSpan LeaseHeartbeatDelay { get; set; }

/// <summary>
/// Specify execution limits including lease heartbeat delay per command type.
/// </summary>
void LimitsFor<TCommand>(int maxConcurrency, TimeSpan timeLimit, TimeSpan leaseHeartbeatDelay);
```

## Example Implementation: PostgreSQL Row-Level Locking

```csharp
public class PostgresJobStorageProvider : IJobStorageProvider<JobRecord>, IJobResultProvider
{
    private readonly JobDbContext _db;
    private readonly string _instanceId;
    
    public PostgresJobStorageProvider(JobDbContext db)
    {
        _db = db;
        _instanceId = Environment.MachineName;
    }
    
    public async Task<IEnumerable<JobRecord>> GetNextBatchAsync(
        PendingJobSearchParams<JobRecord> parameters)
    {
        var leaseDuration = TimeSpan.FromMinutes(5);
        var now = DateTime.UtcNow;
        
        using var transaction = await _db.Database.BeginTransactionAsync();
        
        // Use PostgreSQL's FOR UPDATE SKIP LOCKED for row-level locking
        var jobs = await _db.Jobs
            .FromSqlRaw(@"
                SELECT * FROM ""Jobs""
                WHERE ""QueueID"" = {0}
                  AND ""IsComplete"" = FALSE
                  AND ""ExecuteAfter"" <= {1}
                  AND ""ExpireOn"" >= {1}
                  AND (""LockExpiresOn"" IS NULL OR ""LockExpiresOn"" <= {1})
                FOR UPDATE SKIP LOCKED
                LIMIT {2}",
                parameters.QueueID, now, parameters.Limit)
            .ToListAsync();
        
        // Acquire locks
        foreach (var job in jobs)
        {
            job.LockExpiresOn = now.Add(leaseDuration);
            job.LockedBy = _instanceId;
        }
        
        await _db.SaveChangesAsync();
        await transaction.CommitAsync();
        
        return jobs;
    }
    
    public async ValueTask OnLeaseHeartbeatAsync(JobRecord record, CancellationToken ct)
    {
        // Renew lease only if we still own it
        var leaseDuration = TimeSpan.FromMinutes(5);
        
        await _db.Database.ExecuteSqlRawAsync(@"
            UPDATE ""Jobs""
            SET ""LockExpiresOn"" = {0}
            WHERE ""Id"" = {1}
              AND ""LockedBy"" = {2}
              AND ""LockExpiresOn"" > {3}",
            DateTime.UtcNow.Add(leaseDuration),
            record.Id,
            _instanceId,
            DateTime.UtcNow);
    }
}
```

## Example Implementation: Redis Global Lock

```csharp
public class RedisJobStorageProvider : IJobStorageProvider<JobRecord>, IJobResultProvider
{
    private readonly JobDbContext _db;
    private readonly IConnectionMultiplexer _redis;
    private readonly string _instanceId;
    
    public async Task<bool> TryAcquireLockAsync(string queueId, CancellationToken ct)
    {
        var lockKey = $"jobqueue:lock:{queueId}";
        var db = _redis.GetDatabase();
        
        // Acquire Redis lock with SET NX (set if not exists)
        return await db.StringSetAsync(
            lockKey, 
            _instanceId, 
            TimeSpan.FromSeconds(30),
            When.NotExists);
    }
    
    public async Task<bool> TryReleaseLockAsync(string queueId, CancellationToken ct)
    {
        var lockKey = $"jobqueue:lock:{queueId}";
        var db = _redis.GetDatabase();
        
        // Release only if we own the lock
        var script = @"
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end";
        
        var result = await db.ScriptEvaluateAsync(
            script,
            new RedisKey[] { lockKey },
            new RedisValue[] { _instanceId });
        
        return (int)result == 1;
    }
    
    public async Task<IEnumerable<JobRecord>> GetNextBatchAsync(
        PendingJobSearchParams<JobRecord> parameters)
    {
        // Simple query - global lock ensures no conflicts
        return await _db.Jobs
            .Where(j => j.QueueID == parameters.QueueID)
            .Where(j => !j.IsComplete)
            .Where(j => j.ExecuteAfter <= DateTime.UtcNow)
            .Where(j => j.ExpireOn >= DateTime.UtcNow)
            .OrderBy(j => j.ExecuteAfter)
            .Take(parameters.Limit)
            .ToListAsync();
    }
}
```

## Configuration

```csharp
// Register storage provider
services.AddDbContext<JobDbContext>(options =>
    options.UseNpgsql(connectionString));

services.AddSingleton<IJobStorageProvider<JobRecord>, PostgresJobStorageProvider>();
// or
services.AddSingleton<IJobStorageProvider<JobRecord>, RedisJobStorageProvider>();

// Configure job queues
app.UseJobQueues(options =>
{
    options.MaxConcurrency = 10;
    options.ExecutionTimeLimit = TimeSpan.FromMinutes(10);
    options.LeaseHeartbeatDelay = TimeSpan.FromSeconds(30);
    options.StorageProbeDelay = TimeSpan.FromSeconds(60);
    
    // Per-command configuration
    options.LimitsFor<SendEmailCommand>(
        maxConcurrency: 5,
        timeLimit: TimeSpan.FromMinutes(2),
        leaseHeartbeatDelay: TimeSpan.FromSeconds(20));
});
```

## Usage

### Queue a Job

```csharp
var command = new SendEmailCommand
{
    To = "user@example.com",
    Subject = "Welcome",
    Body = "Welcome to our service!"
};

var trackingId = await command.QueueJobAsync(
    executeAfter: DateTime.UtcNow.AddMinutes(5),
    expireOn: DateTime.UtcNow.AddHours(24),
    ct: cancellationToken);
```

### Track Job Progress

```csharp
var jobTracker = serviceProvider.GetRequiredService<IJobTracker<SendEmailCommand>>();

while (true)
{
    var result = await jobTracker.GetJobResultAsync<EmailResult>(trackingId);
    if (result != null) break;
    await Task.Delay(1000);
}
```

### Cancel a Job

```csharp
await jobTracker.CancelJobAsync(trackingId, cancellationToken);
```

## Benefits

1. **Fault Tolerance**: If a worker crashes, leases expire and jobs are automatically picked up by other workers
2. **No Duplicate Processing**: Locking mechanisms ensure each job is processed exactly once
3. **Flexible Deployment**: Choose between row-level or global locks based on your needs
4. **Scalability**: Row-level locking allows multiple workers to process jobs concurrently
5. **Observability**: Track which worker is processing each job via `LockedBy` field

## Testing

The implementation includes comprehensive tests in `Tests/IntegrationTests/FastEndpoints/MessagingTests/JobQueueTests.cs`:
- Job creation and validation
- Job execution across multiple instances
- Job cancellation
- Job results tracking
- Lease management

## Documentation

See `DISTRIBUTED_JOB_QUEUES.md` for complete documentation including:
- Detailed implementation guides for both locking strategies
- Complete code examples
- Best practices and troubleshooting
- Comparison of row-level vs global locks

## Breaking Changes

None. All new methods have default implementations:
- `TryAcquireLockAsync()` defaults to returning `true`
- `TryReleaseLockAsync()` defaults to returning `true`  
- `OnLeaseHeartbeatAsync()` defaults to no-op

Existing job queue implementations continue to work without modification. The new features are opt-in.

## Migration Guide

To enable distributed job processing:

1. Add `LockExpiresOn` and `LockedBy` properties to your job storage record
2. Implement `TryAcquireLockAsync`, `TryReleaseLockAsync`, and `OnLeaseHeartbeatAsync` in your storage provider
3. Update `GetNextBatchAsync` to filter by expired leases
4. Configure `LeaseHeartbeatDelay` in `JobQueueOptions`

See `DISTRIBUTED_JOB_QUEUES.md` for complete examples.
