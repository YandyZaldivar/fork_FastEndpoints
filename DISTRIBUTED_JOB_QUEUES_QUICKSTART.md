# Quick Start: Distributed Job Queues

This guide helps you quickly set up distributed job queues in FastEndpoints for horizontally scaled applications.

## Prerequisites

- FastEndpoints with job queue support
- PostgreSQL database (for row-level locking example)
- OR Redis (for global lock example)
- Entity Framework Core (for storage)

## Choose Your Approach

### Option 1: Row-Level Locking (Recommended for High Volume)
✅ Multiple workers process jobs concurrently  
✅ Maximum throughput  
❌ Requires PostgreSQL/SQL Server/Oracle  

### Option 2: Global Lock (Simpler)
✅ Simple implementation  
✅ Works with any database  
❌ One worker processes queue at a time  

---

## Quick Start: Row-Level Locking with PostgreSQL

### Step 1: Define Your Job Record

```csharp
using Microsoft.EntityFrameworkCore;
using MessagePack;

public class JobRecord : IJobStorageRecord, IHasCommandType, IJobResultStorage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string QueueID { get; set; } = default!;
    public Guid TrackingID { get; set; }
    public string CommandType { get; set; } = default!;
    public DateTime ExecuteAfter { get; set; }
    public DateTime ExpireOn { get; set; }
    public bool IsComplete { get; set; }
    
    // Distributed locking properties
    public DateTime? LockExpiresOn { get; set; }
    public string? LockedBy { get; set; }
    
    // Serialized command storage
    [NotMapped]
    public object Command { get; set; } = default!;
    public byte[] CommandData { get; set; } = Array.Empty<byte>();
    
    // Serialized result storage (optional)
    [NotMapped]
    public object? Result { get; set; }
    public byte[]? ResultData { get; set; }
    
    public TCommand GetCommand<TCommand>() where TCommand : class, ICommandBase
    {
        if (Command is TCommand cmd) return cmd;
        return MessagePackSerializer.Deserialize<TCommand>(CommandData);
    }
    
    public void SetCommand<TCommand>(TCommand command) where TCommand : class, ICommandBase
    {
        Command = command;
        CommandData = MessagePackSerializer.Serialize(command);
    }
    
    public TResult? GetResult<TResult>()
    {
        if (Result is TResult res) return res;
        if (ResultData == null) return default;
        return MessagePackSerializer.Deserialize<TResult>(ResultData);
    }
    
    public void SetResult<TResult>(TResult result)
    {
        Result = result;
        ResultData = MessagePackSerializer.Serialize(result);
    }
}
```

### Step 2: Create DbContext

```csharp
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
```

### Step 3: Implement Storage Provider

```csharp
public class PostgresJobStorageProvider : IJobStorageProvider<JobRecord>, IJobResultProvider
{
    private readonly JobDbContext _db;
    private readonly string _instanceId;
    private readonly TimeSpan _leaseDuration = TimeSpan.FromMinutes(5);
    
    public PostgresJobStorageProvider(JobDbContext db)
    {
        _db = db;
        _instanceId = Environment.MachineName;
    }
    
    public async Task StoreJobAsync(JobRecord r, CancellationToken ct)
    {
        _db.Jobs.Add(r);
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task<IEnumerable<JobRecord>> GetNextBatchAsync(
        PendingJobSearchParams<JobRecord> parameters)
    {
        var now = DateTime.UtcNow;
        var lockExpiresAt = now.Add(_leaseDuration);
        
        using var transaction = await _db.Database.BeginTransactionAsync(parameters.CancellationToken);
        
        // Use PostgreSQL's FOR UPDATE SKIP LOCKED
        var jobs = await _db.Jobs
            .FromSqlRaw(@"
                SELECT * FROM ""Jobs""
                WHERE ""QueueID"" = {0}
                  AND ""IsComplete"" = FALSE
                  AND ""ExecuteAfter"" <= {1}
                  AND ""ExpireOn"" >= {1}
                  AND (""LockExpiresOn"" IS NULL OR ""LockExpiresOn"" <= {1})
                ORDER BY ""ExecuteAfter""
                FOR UPDATE SKIP LOCKED
                LIMIT {2}",
                parameters.QueueID, now, parameters.Limit)
            .ToListAsync(parameters.CancellationToken);
        
        // Acquire locks
        foreach (var job in jobs)
        {
            job.LockExpiresOn = lockExpiresAt;
            job.LockedBy = _instanceId;
        }
        
        await _db.SaveChangesAsync(parameters.CancellationToken);
        await transaction.CommitAsync(parameters.CancellationToken);
        
        return jobs;
    }
    
    public async ValueTask OnLeaseHeartbeatAsync(JobRecord record, CancellationToken ct)
    {
        // Renew lease only if we still own it
        await _db.Database.ExecuteSqlRawAsync(@"
            UPDATE ""Jobs""
            SET ""LockExpiresOn"" = {0}
            WHERE ""Id"" = {1} AND ""LockedBy"" = {2} AND ""LockExpiresOn"" > {3}",
            DateTime.UtcNow.Add(_leaseDuration),
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
            .ExecuteUpdateAsync(s => s.SetProperty(j => j.IsComplete, true), ct);
    }
    
    public async Task OnHandlerExecutionFailureAsync(JobRecord r, Exception ex, CancellationToken ct)
    {
        // Reschedule failed jobs
        r.ExecuteAfter = DateTime.UtcNow.AddMinutes(5);
        r.LockExpiresOn = null;
        r.LockedBy = null;
        await _db.SaveChangesAsync(ct);
    }
    
    public async Task PurgeStaleJobsAsync(StaleJobSearchParams<JobRecord> parameters)
    {
        var ct = parameters.CancellationToken;
        await _db.Jobs
            .Where(j => j.IsComplete && j.ExpireOn < DateTime.UtcNow.AddDays(-1))
            .ExecuteDeleteAsync(ct);
    }
    
    public async Task StoreJobResultAsync<TResult>(Guid trackingId, TResult result, CancellationToken ct)
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

### Step 4: Register Services

```csharp
// Program.cs
var builder = WebApplication.CreateBuilder(args);

// Add FastEndpoints
builder.Services.AddFastEndpoints();

// Add database
builder.Services.AddDbContext<JobDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

// Add job storage provider
builder.Services.AddSingleton<IJobStorageProvider<JobRecord>, PostgresJobStorageProvider>();
builder.Services.AddSingleton<IJobResultProvider, PostgresJobStorageProvider>();

var app = builder.Build();

// Use FastEndpoints
app.UseFastEndpoints();

// Use Job Queues
app.UseJobQueues(options =>
{
    options.MaxConcurrency = 10;
    options.ExecutionTimeLimit = TimeSpan.FromMinutes(10);
    options.LeaseHeartbeatDelay = TimeSpan.FromSeconds(30);
    options.StorageProbeDelay = TimeSpan.FromSeconds(60);
});

app.Run();
```

### Step 5: Create and Run Migrations

```bash
dotnet ef migrations add AddJobQueueSupport
dotnet ef database update
```

### Step 6: Use It!

```csharp
// Define a command
public class SendEmailCommand : ICommand
{
    public string To { get; set; } = default!;
    public string Subject { get; set; } = default!;
    public string Body { get; set; } = default!;
}

// Define a handler
public class SendEmailHandler : ICommandHandler<SendEmailCommand>
{
    public async Task ExecuteAsync(SendEmailCommand cmd, CancellationToken ct)
    {
        // Send email logic
        await Task.Delay(1000, ct); // Simulate work
        Console.WriteLine($"Email sent to {cmd.To}");
    }
}

// Queue a job
var command = new SendEmailCommand
{
    To = "user@example.com",
    Subject = "Welcome",
    Body = "Welcome to our service!"
};

var trackingId = await command.QueueJobAsync();
```

---

## Quick Start: Global Lock with Redis

### Step 1-2: Same as Row-Level Locking

Use the same `JobRecord` and `JobDbContext` from the row-level example.

### Step 3: Implement Storage Provider with Redis Lock

```csharp
using StackExchange.Redis;

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
    
    public async Task<bool> TryAcquireLockAsync(string queueId, CancellationToken ct)
    {
        var lockKey = $"jobqueue:lock:{queueId}";
        var db = _redis.GetDatabase();
        
        return await db.StringSetAsync(lockKey, _instanceId, _lockTimeout, When.NotExists);
    }
    
    public async Task<bool> TryReleaseLockAsync(string queueId, CancellationToken ct)
    {
        var lockKey = $"jobqueue:lock:{queueId}";
        var db = _redis.GetDatabase();
        
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
    
    // StoreJobAsync, GetNextBatchAsync, etc. - similar to PostgreSQL example
    // but without FOR UPDATE SKIP LOCKED (global lock handles concurrency)
}
```

### Step 4: Register Services with Redis

```csharp
// Program.cs
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
    ConnectionMultiplexer.Connect("localhost:6379"));

builder.Services.AddDbContext<JobDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddSingleton<IJobStorageProvider<JobRecord>, RedisJobStorageProvider>();
builder.Services.AddSingleton<IJobResultProvider, RedisJobStorageProvider>();

// Rest is the same as row-level locking example
```

---

## Testing Multiple Instances

### Local Testing with Multiple Processes

```bash
# Terminal 1
dotnet run --urls=http://localhost:5001

# Terminal 2
dotnet run --urls=http://localhost:5002

# Terminal 3
dotnet run --urls=http://localhost:5003
```

Queue jobs from any instance and observe all instances processing them without duplicates.

### Docker Compose Example

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
  
  redis:
    image: redis:7
    ports:
      - "6379:6379"
  
  worker1:
    build: .
    environment:
      ConnectionStrings__DefaultConnection: "Host=postgres;Database=jobs;Username=postgres;Password=password"
      REDIS_CONNECTION: "redis:6379"
    depends_on:
      - postgres
      - redis
  
  worker2:
    build: .
    environment:
      ConnectionStrings__DefaultConnection: "Host=postgres;Database=jobs;Username=postgres;Password=password"
      REDIS_CONNECTION: "redis:6379"
    depends_on:
      - postgres
      - redis
```

---

## Verification

### Check if Jobs Are Being Processed

```sql
-- View active jobs
SELECT "TrackingID", "QueueID", "IsComplete", "LockExpiresOn", "LockedBy"
FROM "Jobs"
WHERE "IsComplete" = FALSE
ORDER BY "ExecuteAfter";

-- View completed jobs
SELECT COUNT(*), "LockedBy"
FROM "Jobs"
WHERE "IsComplete" = TRUE
GROUP BY "LockedBy";
```

### Monitor in Application Logs

Look for:
- Lock acquisitions: "Acquired lock for queue: {queueId}"
- Job processing: "Processing job: {trackingId}"
- Lease renewals: "Renewed lease for job: {trackingId}"

---

## Common Issues

### Jobs Not Processing
- Check `UseJobQueues()` is called in Program.cs
- Verify database connection
- Check `ExecuteAfter` is not in the future

### Duplicate Processing
- Ensure `FOR UPDATE SKIP LOCKED` is in your SQL
- Verify lease heartbeat is working
- Check `LockExpiresOn` is being set correctly

### Stuck Jobs
- Increase lease duration if jobs take longer
- Check worker instances are still running
- Verify heartbeat is renewing leases

---

## Next Steps

- Read **DISTRIBUTED_JOB_QUEUES.md** for complete documentation
- Read **IMPLEMENTATION_SUMMARY.md** for implementation details
- Tune `LeaseHeartbeatDelay` based on your job execution times
- Add monitoring and alerting for failed jobs
- Implement retry logic with exponential backoff

## Support

For issues or questions:
1. Check the comprehensive documentation in `DISTRIBUTED_JOB_QUEUES.md`
2. Review the implementation summary in `IMPLEMENTATION_SUMMARY.md`
3. Check the test implementation in `Web/[Features]/TestCases/Messaging/JobQueueTest/TestStorageProvider.cs`
