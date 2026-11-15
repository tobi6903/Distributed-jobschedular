# deploy/tempo

[tempo.yml](tempo.yml): a single-node Tempo with local storage that accepts **OTLP** over HTTP (4318) and gRPC (4317). The API, worker and scheduler export spans there when `OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318` is set (the Compose file sets it).

A job's life is one trace even though the API and the worker never share memory: the API stores the W3C `traceparent` in the job row and the worker restores it. Verified on the running stack: one trace with five spans across two containers (`POST /jobs` -> `queue.enqueue` -> `job.claim` / `job.execute` -> `job.complete`).

Grafana's Tempo datasource links traces to logs; its health-check button reports "Method not implemented", a quirk of that endpoint, since trace queries work.
