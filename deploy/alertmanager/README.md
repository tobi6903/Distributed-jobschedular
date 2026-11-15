# deploy/alertmanager

[alertmanager.yml](alertmanager.yml): routing, grouping and inhibition.

- **Routing by severity**: `critical` goes to the `pager` receiver with a short `group_wait` and hourly repeats; `info` repeats daily; everything else goes to `default`.
- **Receivers are webhooks to a placeholder** (`alert-sink`). Point them at Slack, PagerDuty or Opsgenie, and keep credentials in the environment or a secret manager, never in this file.
- **Inhibition**: if the API is down (`PostgresUnreachableFromAPI`) its latency and availability alerts are suppressed; a critical backlog alert suppresses the warning-level one for the same queue.

Validate with `amtool check-config deploy/alertmanager/alertmanager.yml` (part of `make validate`).
