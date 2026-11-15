# Runbooks

One runbook per alert. Each follows the same shape: **what it means**, **first checks**, **diagnose**, **mitigate**, **follow up**. SQL assumes `psql "$DATABASE_URL"`.

| Alert | Runbook |
|---|---|
| `QueueOldestJobTooOld`, `QueueOldestJobCritical`, `JobPickupSLO*`, `NoWorkersUp` | [queue-backlog.md](queue-backlog.md) |
| `DeadLetterQueueNotEmpty`, `DeadLetterQueueGrowing` | [dead-letter-queue.md](dead-letter-queue.md) |
| `HighRetryRate`, `JobSuccessSLOFastBurn` | [high-failure-rate.md](high-failure-rate.md) |
| `WorkerCrashesDetected` | [worker-crashes.md](worker-crashes.md) |
| `NoSchedulerLeader`, `MultipleSchedulerLeaders`, `CronDuplicatePrevented` | [scheduler-leader.md](scheduler-leader.md) |
| `APIHighLatency`, `APIAvailabilitySLOFastBurn` | [api-errors.md](api-errors.md) |
| `PostgresUnreachableFromAPI` | [database-down.md](database-down.md) |
