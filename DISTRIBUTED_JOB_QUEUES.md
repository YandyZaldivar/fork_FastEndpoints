# Distributed Job Queues in FastEndpoints

This document describes the distributed job queue implementation in FastEndpoints, which allows horizontally scaled applications to safely process jobs across multiple instances without duplicate processing.

## Overview

The distributed job queue feature enables multiple application instances (pods/containers) to coordinate job processing using either:

1. **Row-Level Locking** - Using database-native mechanisms like PostgreSQL's `FOR UPDATE SKIP LOCKED`
2. **Global Distributed Locks** - Using external locking systems like Redis or PostgreSQL advisory locks

## Core Concepts

### Lease-Based Job Locking

Jobs use a lease-based locking mechanism to prevent duplicate processing:

- **LockExpiresOn**: Timestamp when the current lock expires
- **LockedBy**: Identifier of the worker that acquired the lock
- **LeaseHeartbeat**: Periodic signals to renew the lease during long-running jobs

If a worker crashes, the lease naturally expires and other workers can pick up the job.

### Job Storage Record

Implement `IJobStorageRecord` to define your job storage entity:

```csharp
public class JobRecord : IJobStorageRecord, IHasCommandType, IJobResultStorage
{
    public string QueueID { get; set; } = default!;
    public Guid TrackingID { get; set; }
    public object Command { get; set; } = default!;
    public string CommandType { get; set; } = default!;
    public DateTime ExecuteAfter { get; set; }
    public DateTime ExpireOn { get; set; }
    public bool IsComplete { get; set; }
    
    // Distributed locking fields
    public DateTime? LockExpiresOn { get; set; }
    public string? LockedBy { get; set; }
    
    // Optional: For commands that return results
    public object? Result { get; set; }
}
```

## Use Case 1: Row-Level Locking with PostgreSQL

This approach uses PostgreSQL's `FOR UPDATE SKIP LOCKED` to let each worker acquire exclusive locks on specific job rows. Workers skip rows that are already locked by other workers.

### Entity Framework Core Implementation

```csharp
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class JobRecord : IJobStorageRecord, IHasCommandType, IJobResultStorage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string QueueID { get; set; } = default!;
    public Guid TrackingID { get; set; }
    
    [NotMapped]
    public object Command { get; set; } = default!;
    
    // Serialized command storage
    public byte[] CommandData { get; set; } = Array.Empty<byte>();
    
    public string CommandType { get; set; } = default!;
    public DateTime ExecuteAfter { get; set; }
    public DateTime ExpireOn { get; set; }
    public bool IsComplete { get; set; }
    public DateTime? LockExpiresOn { get; set; }
    public string? LockedBy { get; set; }
    
    [NotMapped]
    public object? Result { get; set; }
    
    public byte[]? ResultData { get; set; }
    
    // Custom serialization methods
    public TCommand GetCommand<TCommand>() where TCommand : class, ICommandBase
    {
        if (Command is TCommand cmd)
            return cmd;
        
        // Deserialize from CommandData using your preferred serializer (MessagePack, JSON, etc.)
        return MessagePackSerializer.Deserialize<TCommand>(CommandData);
    }
    
    public void SetCommand<TCommand>(TCommand command) where TCommand : class, ICommandBase
    {
        Command = command;
        CommandData = MessagePackSerializer.Serialize(command);
    }
    
    public TResult? GetResult<TResult>()
    {
        if (Result is TResult res)
            return res;
        
        if (ResultData == null)
            return default;
        
        return MessagePackSerializer.Deserialize<TResult>(ResultData);
    }
    
    public void SetResult<TResult>(TResult result)
    {
        Result = result;
        ResultData = MessagePackSerializer.Serialize(result);
    }
}

public class JobDbContext : DbContext
{
    public DbSet<JobRecord> Jobs { get; set; } = default!;
    
    public JobDbContext(DbContextOptions<JobDbContext> options) : base(options) { }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<JobRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => new { e.QueueID, e.IsComplete, e.ExecuteAfter, e.LockExpiresOn });
            entity.HasIndex(e => e.TrackingID).IsUnique();
        });
    }
}

public class PostgresJobStorageProvider : IJobStorageProvider<JobRecord>, IJobResultProvider
{
    private readonly JobDbContext _db;
    private readonly string _instanceId;
    
    public PostgresJobStorageProvider(JobDbContext db)
    {
        _db = db;
        _instanceId = Environment.MachineName; // Or use Kubernetes Pod UID
    }
    
    public async Task StoreJobAsync(JobRecord r, CancellationToken ct)
    {
        _db.Jobs.Add(r);
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task<IEnumerable<JobRecord>> GetNextBatchAsync(
        PendingJobSearchParams<JobRecord> parameters)
    {
        var ct = parameters.CancellationToken;
        var leaseDuration = TimeSpan.FromMinutes(5); // Lease duration
        var now = DateTime.UtcNow;
        var lockExpiresAt = now.Add(leaseDuration);
        
        // Start transaction for row-level locking
        using var transaction = await _db.Database.BeginTransactionAsync(ct);
        
        try
        {
            // Use raw SQL to leverage PostgreSQL's FOR UPDATE SKIP LOCKED
            var jobs = await _db.Jobs
                .FromSqlRaw(@"
                    SELECT * 
                    FROM ""Jobs""
                    WHERE ""QueueID"" = {0}
                      AND ""IsComplete"" = FALSE
                      AND ""ExecuteAfter"" <= {1}
                      AND ""ExpireOn"" >= {1}
                      AND (""LockExpiresOn"" IS NULL OR ""LockExpiresOn"" <= {1})
                    ORDER BY ""ExecuteAfter""
                    FOR UPDATE SKIP LOCKED
                    LIMIT {2}",
                    parameters.QueueID,
                    now,
                    parameters.Limit)
                .ToListAsync(ct);
            
            // Acquire locks by updating lock fields
            foreach (var job in jobs)
            {
                job.LockExpiresOn = lockExpiresAt;
                job.LockedBy = _instanceId;
            }
            
            await _db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            
            return jobs;
        }
        catch
        {
            await transaction.RollbackAsync(ct);
            throw;
        }
    }
    
    public async ValueTask OnLeaseHeartbeatAsync(JobRecord record, CancellationToken ct)
    {
        // Renew the lease only if this instance still owns it
        var leaseDuration = TimeSpan.FromMinutes(5);
        var newExpiry = DateTime.UtcNow.Add(leaseDuration);
        
        await _db.Database.ExecuteSqlRawAsync(@"
            UPDATE ""Jobs""
            SET ""LockExpiresOn"" = {0}
            WHERE ""Id"" = {1}
              AND ""LockedBy"" = {2}
              AND ""LockExpiresOn"" > {3}",
            newExpiry,
            record.Id,
            _instanceId,
            DateTime.UtcNow,
            ct);
    }
    
    public async Task MarkJobAsCompleteAsync(JobRecord r, CancellationToken ct)
    {
        r.IsComplete = true;
        r.LockExpiresOn = null;
        r.LockedBy = null;
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task CancelJobAsync(Guid trackingId, CancellationToken ct)
    {
        await _db.Jobs
            .Where(j => j.TrackingID == trackingId)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(j => j.IsComplete, true),
                ct);
    }
    
    public async Task OnHandlerExecutionFailureAsync(
        JobRecord r, 
        Exception exception, 
        CancellationToken ct)
    {
        // Reschedule failed jobs to retry later
        r.ExecuteAfter = DateTime.UtcNow.AddMinutes(5);
        r.LockExpiresOn = null;
        r.LockedBy = null;
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task PurgeStaleJobsAsync(StaleJobSearchParams<JobRecord> parameters)
    {
        var ct = parameters.CancellationToken;
        
        // Delete completed jobs older than 24 hours
        await _db.Jobs
            .Where(j => j.IsComplete && j.ExpireOn < DateTime.UtcNow.AddDays(-1))
            .ExecuteDeleteAsync(ct);
        
        // Move or log expired incomplete jobs
        var expiredJobs = await _db.Jobs
            .Where(j => !j.IsComplete && j.ExpireOn < DateTime.UtcNow)
            .ToListAsync(ct);
        
        foreach (var job in expiredJobs)
        {
            // Log or move to dead letter queue
            Console.WriteLine($"Job {job.TrackingID} expired without completing");
        }
        
        // Delete expired incomplete jobs
        await _db.Jobs
            .Where(j => !j.IsComplete && j.ExpireOn < DateTime.UtcNow)
            .ExecuteDeleteAsync(ct);
    }
    
    // IJobResultProvider implementation
    public async Task StoreJobResultAsync<TResult>(
        Guid trackingId, 
        TResult result, 
        CancellationToken ct)
    {
        var job = await _db.Jobs.FirstOrDefaultAsync(j => j.TrackingID == trackingId, ct);
        if (job != null)
        {
            job.SetResult(result);
            await _db.SaveChangesAsync(ct);
        }
    }
    
    public async Task<TResult?> GetJobResultAsync<TResult>(Guid trackingId, CancellationToken ct)
    {
        var job = await _db.Jobs.FirstOrDefaultAsync(j => j.TrackingID == trackingId, ct);
        return job?.GetResult<TResult>();
    }
}
```

### Configuration

```csharp
// Program.cs or Startup.cs
services.AddDbContext<JobDbContext>(options =>
    options.UseNpgsql(connectionString));

services.AddSingleton<IJobStorageProvider<JobRecord>, PostgresJobStorageProvider>();
services.AddSingleton<IJobResultProvider, PostgresJobStorageProvider>();

// In your app builder configuration
app.UseJobQueues(options =>
{
    options.MaxConcurrency = 10; // Per queue
    options.ExecutionTimeLimit = TimeSpan.FromMinutes(10);
    options.LeaseHeartbeatDelay = TimeSpan.FromSeconds(30); // How often to renew leases
    options.StorageProbeDelay = TimeSpan.FromSeconds(60); // How often to check for new jobs
});
```

## Use Case 2: Global Distributed Lock

This approach uses a global lock for the entire queue. Only one worker can process jobs from a queue at a time. This is simpler but less concurrent than row-level locking.

### Redis-Based Global Lock Implementation

```csharp
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class RedisJobStorageProvider : IJobStorageProvider<JobRecord>, IJobResultProvider
{
    private readonly JobDbContext _db;
    private readonly IConnectionMultiplexer _redis;
    private readonly string _instanceId;
    private readonly TimeSpan _lockTimeout = TimeSpan.FromSeconds(30);
    
    public RedisJobStorageProvider(JobDbContext db, IConnectionMultiplexer redis)
    {
        _db = db;
        _redis = redis;
        _instanceId = $"{Environment.MachineName}:{Guid.NewGuid()}";
    }
    
    public async Task StoreJobAsync(JobRecord r, CancellationToken ct)
    {
        _db.Jobs.Add(r);
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task<bool> TryAcquireLockAsync(string queueId, CancellationToken ct)
    {
        var lockKey = $"jobqueue:lock:{queueId}";
        var db = _redis.GetDatabase();
        
        // Try to acquire lock with SET NX (set if not exists)
        return await db.StringSetAsync(
            lockKey, 
            _instanceId, 
            _lockTimeout,
            When.NotExists);
    }
    
    public async Task<bool> TryReleaseLockAsync(string queueId, CancellationToken ct)
    {
        var lockKey = $"jobqueue:lock:{queueId}";
        var db = _redis.GetDatabase();
        
        // Only release if we own the lock
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
        // Simple query without row-level locking
        // The global lock ensures only one worker fetches jobs
        var match = parameters.Match.Compile();
        
        var jobs = await _db.Jobs
            .Where(j => j.QueueID == parameters.QueueID)
            .Where(j => !j.IsComplete)
            .Where(j => j.ExecuteAfter <= DateTime.UtcNow)
            .Where(j => j.ExpireOn >= DateTime.UtcNow)
            .OrderBy(j => j.ExecuteAfter)
            .Take(parameters.Limit)
            .ToListAsync(parameters.CancellationToken);
        
        return jobs;
    }
    
    public ValueTask OnLeaseHeartbeatAsync(JobRecord record, CancellationToken ct)
    {
        // Not needed with global lock, but could extend Redis lock here if needed
        return ValueTask.CompletedTask;
    }
    
    public async Task MarkJobAsCompleteAsync(JobRecord r, CancellationToken ct)
    {
        r.IsComplete = true;
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task CancelJobAsync(Guid trackingId, CancellationToken ct)
    {
        await _db.Jobs
            .Where(j => j.TrackingID == trackingId)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(j => j.IsComplete, true),
                ct);
    }
    
    public async Task OnHandlerExecutionFailureAsync(
        JobRecord r, 
        Exception exception, 
        CancellationToken ct)
    {
        // Reschedule failed jobs
        r.ExecuteAfter = DateTime.UtcNow.AddMinutes(5);
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task PurgeStaleJobsAsync(StaleJobSearchParams<JobRecord> parameters)
    {
        var ct = parameters.CancellationToken;
        
        // Delete completed jobs
        await _db.Jobs
            .Where(j => j.IsComplete && j.ExpireOn < DateTime.UtcNow.AddDays(-1))
            .ExecuteDeleteAsync(ct);
        
        // Delete expired jobs
        await _db.Jobs
            .Where(j => !j.IsComplete && j.ExpireOn < DateTime.UtcNow)
            .ExecuteDeleteAsync(ct);
    }
    
    // IJobResultProvider implementation
    public async Task StoreJobResultAsync<TResult>(
        Guid trackingId, 
        TResult result, 
        CancellationToken ct)
    {
        var job = await _db.Jobs.FirstOrDefaultAsync(j => j.TrackingID == trackingId, ct);
        if (job != null)
        {
            job.SetResult(result);
            await _db.SaveChangesAsync(ct);
        }
    }
    
    public async Task<TResult?> GetJobResultAsync<TResult>(Guid trackingId, CancellationToken ct)
    {
        var job = await _db.Jobs.FirstOrDefaultAsync(j => j.TrackingID == trackingId, ct);
        return job?.GetResult<TResult>();
    }
}
```

### PostgreSQL Advisory Lock Implementation

```csharp
public class PostgresAdvisoryLockJobStorageProvider : IJobStorageProvider<JobRecord>, IJobResultProvider
{
    private readonly JobDbContext _db;
    private long? _currentLockId;
    
    public PostgresAdvisoryLockJobStorageProvider(JobDbContext db)
    {
        _db = db;
    }
    
    public async Task StoreJobAsync(JobRecord r, CancellationToken ct)
    {
        _db.Jobs.Add(r);
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task<bool> TryAcquireLockAsync(string queueId, CancellationToken ct)
    {
        // Convert queue ID to a numeric lock ID
        var lockId = GetLockId(queueId);
        
        // Try to acquire PostgreSQL advisory lock
        var result = await _db.Database
            .ExecuteSqlRawAsync("SELECT pg_try_advisory_lock({0})", lockId);
        
        if (result > 0)
        {
            _currentLockId = lockId;
            return true;
        }
        
        return false;
    }
    
    public async Task<bool> TryReleaseLockAsync(string queueId, CancellationToken ct)
    {
        if (_currentLockId.HasValue)
        {
            await _db.Database
                .ExecuteSqlRawAsync("SELECT pg_advisory_unlock({0})", _currentLockId.Value);
            _currentLockId = null;
            return true;
        }
        
        return false;
    }
    
    private long GetLockId(string queueId)
    {
        // Convert string to consistent numeric ID
        return Math.Abs(queueId.GetHashCode());
    }
    
    public async Task<IEnumerable<JobRecord>> GetNextBatchAsync(
        PendingJobSearchParams<JobRecord> parameters)
    {
        // Simple query - global lock ensures no conflicts
        var jobs = await _db.Jobs
            .Where(j => j.QueueID == parameters.QueueID)
            .Where(j => !j.IsComplete)
            .Where(j => j.ExecuteAfter <= DateTime.UtcNow)
            .Where(j => j.ExpireOn >= DateTime.UtcNow)
            .OrderBy(j => j.ExecuteAfter)
            .Take(parameters.Limit)
            .ToListAsync(parameters.CancellationToken);
        
        return jobs;
    }
    
    // ... rest of the methods similar to Redis implementation
}
```

### Configuration for Global Lock

```csharp
// With Redis
services.AddSingleton<IConnectionMultiplexer>(sp =>
    ConnectionMultiplexer.Connect("localhost:6379"));

services.AddSingleton<IJobStorageProvider<JobRecord>, RedisJobStorageProvider>();
services.AddSingleton<IJobResultProvider, RedisJobStorageProvider>();

// Or with PostgreSQL Advisory Locks
services.AddSingleton<IJobStorageProvider<JobRecord>, PostgresAdvisoryLockJobStorageProvider>();
services.AddSingleton<IJobResultProvider, PostgresAdvisoryLockJobStorageProvider>();

app.UseJobQueues(options =>
{
    options.MaxConcurrency = 10;
    options.ExecutionTimeLimit = TimeSpan.FromMinutes(10);
});
```

## Comparison: Row-Level vs Global Locks

### Row-Level Locking (PostgreSQL FOR UPDATE SKIP LOCKED)

**Pros:**
- Higher concurrency - multiple workers process different jobs simultaneously
- Better throughput for high-volume queues
- Each worker independently selects jobs

**Cons:**
- Requires database support (PostgreSQL, SQL Server, Oracle)
- More complex implementation
- Requires lease heartbeat mechanism

**Best for:**
- High-volume job queues
- When you have many workers
- PostgreSQL or similar databases

### Global Distributed Lock (Redis/Advisory Locks)

**Pros:**
- Simpler implementation
- Works with any database
- Clear single point of coordination

**Cons:**
- Lower concurrency - only one worker processes the queue at a time
- Potential bottleneck for high-volume queues
- Lock timeout management required

**Best for:**
- Lower-volume queues
- Simpler deployment scenarios
- When using databases without row-level locking
- When you need strict ordering guarantees

## Usage Examples

### Queueing a Job

```csharp
public class SendEmailCommand : ICommand
{
    public string To { get; set; } = default!;
    public string Subject { get; set; } = default!;
    public string Body { get; set; } = default!;
}

public class SendEmailHandler : ICommandHandler<SendEmailCommand>
{
    private readonly IEmailService _emailService;
    
    public SendEmailHandler(IEmailService emailService)
    {
        _emailService = emailService;
    }
    
    public async Task ExecuteAsync(SendEmailCommand command, CancellationToken ct)
    {
        await _emailService.SendAsync(command.To, command.Subject, command.Body, ct);
    }
}

// Queue the job
var command = new SendEmailCommand
{
    To = "user@example.com",
    Subject = "Welcome",
    Body = "Welcome to our service!"
};

var trackingId = await command.QueueJobAsync(
    executeAfter: DateTime.UtcNow.AddMinutes(5), // Delay execution
    expireOn: DateTime.UtcNow.AddHours(24),      // Expire if not completed
    ct: cancellationToken);
```

### Tracking Job Progress

```csharp
// For commands that return results
public class ProcessOrderCommand : ICommand<OrderResult>
{
    public Guid OrderId { get; set; }
}

public class OrderResult : IJobResult
{
    public bool Success { get; set; }
    public string Message { get; set; } = default!;
}

// Queue and track
var command = new ProcessOrderCommand { OrderId = orderId };
var trackingId = await command.QueueJobAsync(ct: cancellationToken);

// Poll for result
var jobTracker = serviceProvider.GetRequiredService<IJobTracker<ProcessOrderCommand>>();

while (true)
{
    var result = await jobTracker.GetJobResultAsync<OrderResult>(trackingId, cancellationToken);
    
    if (result != null)
    {
        Console.WriteLine($"Order processed: {result.Message}");
        break;
    }
    
    await Task.Delay(1000, cancellationToken);
}
```

### Cancelling Jobs

```csharp
var jobTracker = serviceProvider.GetRequiredService<IJobTracker<SendEmailCommand>>();
await jobTracker.CancelJobAsync(trackingId, cancellationToken);
```

## Best Practices

1. **Choose the Right Locking Strategy**
   - Use row-level locking for high-volume queues with multiple workers
   - Use global locks for simpler scenarios or when database doesn't support row-level locking

2. **Set Appropriate Timeouts**
   - Lease duration should be longer than typical job execution time
   - Heartbeat interval should be 1/3 to 1/2 of lease duration
   - Execution time limit should be set based on job requirements

3. **Handle Failures Gracefully**
   - Implement retry logic in `OnHandlerExecutionFailureAsync`
   - Use exponential backoff for retries
   - Move permanently failed jobs to a dead-letter queue

4. **Monitor and Debug**
   - Log lock acquisitions and releases
   - Track lease expirations and renewals
   - Monitor job execution times and failure rates

5. **Database Indexes**
   - Index on `(QueueID, IsComplete, ExecuteAfter, LockExpiresOn)` for efficient queries
   - Index on `TrackingID` for fast lookups

6. **Scalability**
   - Row-level locking scales better with more workers
   - Global locks limit parallelism but provide simplicity
   - Consider partitioning queues by job type or priority

## Troubleshooting

### Jobs Not Being Processed

1. Check that `UseJobQueues()` is called in application startup
2. Verify storage provider is correctly registered
3. Check `ExecuteAfter` and `ExpireOn` timestamps
4. Verify database connectivity

### Duplicate Job Processing

1. Ensure `TryAcquireLockAsync` is properly implemented
2. Check that `LockExpiresOn` is being set correctly
3. Verify lease heartbeat is working
4. Check for clock skew between servers

### Jobs Stuck in Processing

1. Increase `LeaseHeartbeatDelay` if jobs are long-running
2. Check that heartbeat implementation is updating `LockExpiresOn`
3. Verify workers aren't crashing without releasing locks
4. Monitor expired leases in your logs

## Summary

FastEndpoints' distributed job queue implementation provides flexible options for coordinating job processing across multiple instances. Choose row-level locking for maximum concurrency and throughput, or global locks for simplicity. Both approaches prevent duplicate processing and handle worker failures gracefully through lease-based locking.
