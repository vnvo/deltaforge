# Production Quick Start

A minimal, production-shaped deployment on Kubernetes using the bundled Helm chart. It uses the [supported single-instance topology](deployment-support.md#topology-single-owner-per-source), persistent storage, referenced secrets, readiness/liveness probes, explicit resource limits, and the [preflight command](deployment-support.md#deployment-preflight) as a deployment gate.

This is a starting point, not a configuration reference - see [Configuration](configuration.md) for every option and the [Capacity & Resource Envelope](capacity-envelope.md) for sizing.

## Prerequisites

- A Kubernetes cluster with a default StorageClass (for the checkpoint volume).
- A reachable source database, already provisioned per [Supported Deployment Envelope → Required privileges](deployment-support.md#required-privileges). For PostgreSQL, create the publication (DeltaForge creates and owns the slot).
- Source (and sink) credentials in a Kubernetes Secret - referenced, never inlined in the config.

## 1. Create the credentials Secret

Keep secrets out of the pipeline config; the config references them with `${VAR}`.

```bash
kubectl create secret generic mysql-creds \
  --from-literal=MYSQL_USER=cdc_user \
  --from-literal=MYSQL_PASSWORD=s3cret
```

## 2. Write a values file

```yaml
# values.prod.yaml
replicaCount: 1                     # single instance (required)

pipeline:
  config: |
    apiVersion: deltaforge/v1
    kind: Pipeline
    metadata:
      name: orders
      tenant: default
    spec:
      source:
        type: mysql
        config:
          id: orders-src
          dsn: "mysql://mysql-primary:3306/orders"
          credentials:                # typed secret references (protected resolution)
            username:
              provider: env
              location: MYSQL_USER
            password:
              provider: env
              location: MYSQL_PASSWORD
          tables: ["orders.*"]
      sinks:
        - type: kafka
          config:
            id: orders-kafka
            brokers: kafka:9092
            topic: cdc.orders
            exactly_once: true
      commit_policy:
        mode: required
      journal:
        enabled: true               # DLQ on, so a poison event never blocks

secrets:
  existingSecrets:                  # referenced, not created from values
    - name: mysql-creds

persistence:
  enabled: true                    # durable checkpoints + DLQ (SQLite on a PVC)
  size: 5Gi

preflight:
  enabled: true                    # gate the deploy on preflight (default)

resources:
  requests: { cpu: 250m, memory: 512Mi }
  limits:   { cpu: "2",  memory: 2Gi }   # no aggregate memory cap exists; size per Capacity Envelope
```

Probes (`/health` liveness, `/ready` readiness) and the single-instance StatefulSet come from the chart defaults; you do not need to set them.

The deployment is gated on preflight automatically: with `preflight.enabled: true` the chart runs `deltaforge preflight` as an initContainer - with the same config, secrets, and `--storage-*` settings as the pipeline container - before the pipeline starts. A failing check fails the pod, so a misconfigured deployment never begins streaming, and it re-runs on every rollout.

## 3. Install

`helm install` creates the ConfigMap and Secret wiring and runs the preflight initContainer as the authoritative deployment gate:

```bash
helm install orders ./deploy/helm/deltaforge -f values.prod.yaml
```

## 4. Verify

```bash
# The preflight initContainer must complete before the app container starts.
kubectl get pods -l app.kubernetes.io/instance=orders
kubectl logs orders-0 -c preflight        # the preflight report (the deploy gate)

# Readiness gates traffic; liveness restarts a wedged process:
kubectl get pod orders-0 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'

# Health/metrics:
kubectl port-forward svc/orders 8080:8080 &
curl -fsS localhost:8080/ready
```

If preflight fails, the pod stays in `Init` and `kubectl logs orders-0 -c preflight` shows the hard errors to fix before the pipeline can start.

## Notes

- **Single instance is required.** The guarantee that only one process holds the state store comes from `replicaCount: 1`, the StatefulSet's one-pod-at-a-time behavior, and the documented prohibition against a second installation against the same store - **not** from `ReadWriteOnce` (RWO restricts volume attachment to one node, but multiple pods on that node could still mount it). Do not scale the replica count or point a second install at the same volume - see the [single-owner rule](deployment-support.md#topology-single-owner-per-source).
- **Persistent storage.** SQLite checkpoints and the DLQ live on the PVC. Treat the state volume as durable production data. Back it up using the documented SQLite-safe procedure. If it is lost, stop the pipeline and recover the state or explicitly reinitialize/re-snapshot; do not assume automatic checkpoint recovery (a lost volume loses the durable checkpoint, and exact recovery may be impossible once source logs expire). For a shared store, use the `postgres` storage backend (still single-instance).
- **Upgrades.** A single-replica StatefulSet terminates the old pod before starting the new one; the preflight initContainer re-gates every rollout.
