# cmd

Four thin `main` packages: load config, wire dependencies, handle signals. All behaviour is in [internal](../internal/README.md).

| Command | Role | Scales |
|---|---|---|
| [api](api/README.md) | HTTP API | horizontally (HPA) |
| [worker](worker/README.md) | claims and runs jobs; also runs the reaper and retention sweeper | horizontally (KEDA on queue depth) |
| [scheduler](scheduler/README.md) | fires cron schedules | 2+ replicas, one leads |
| [jobctl](jobctl/README.md) | migrations, API keys, limits, API client | one-shot (the migration Job) |

`make build` writes them to `./bin`; `make docker-build` builds four distroless images from one Dockerfile.
