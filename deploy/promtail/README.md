# deploy/promtail

[promtail.yml](promtail.yml): ships container logs from the Docker socket to [Loki](../loki/README.md). Containers are discovered through the Docker API (`docker_sd_configs`) and labelled with their Compose `service` name; the JSON `level` field becomes a label.

Needs `/var/run/docker.sock` mounted read-only (the Compose file does this). In Kubernetes, use your cluster's log shipper (Promtail/Alloy DaemonSet) with the same labelling.
