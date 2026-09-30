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
          dsn: "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql-primary:3306/orders"
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

## 3. Gate the deploy on preflight

Validate before rolling anything out (same checks the initContainer runs):

```bash
kubectl run deltaforge-preflight --rm -i --restart=Never \
  --image=ghcr.io/vnvo/deltaforge:latest \
  --overrides='{"spec":{"containers":[{"name":"p","image":"ghcr.io/vnvo/deltaforge:latest",
    "args":["preflight","/etc/deltaforge/pipeline.yaml"],
    "envFrom":[{"secretRef":{"name":"mysql-creds"}}],
    "volumeMounts":[{"name":"c","mountPath":"/etc/deltaforge"}]}],
    "volumes":[{"name":"c","configMap":{"name":"orders-config"}}]}}'
```

In the chart this is automatic: an initContainer runs `deltaforge preflight` before the pipeline container starts, so a misconfigured deployment fails the pod instead of streaming. Point it at the **same** `--storage-*` settings as the deployment (the chart does this) so the slot-ownership check sees the real ownership records.

## 4. Install

```bash
helm install orders ./deploy/helm/deltaforge -f values.prod.yaml
```

## 5. Verify

```bash
# The preflight initContainer must complete before the app starts:
kubectl get pods -l app.kubernetes.io/instance=orders
kubectl logs orders-0 -c preflight        # preflight report

# Readiness gates traffic; liveness restarts a wedged process:
kubectl get pod orders-0 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'

# Health/metrics:
kubectl port-forward svc/orders 8080:8080 &
curl -fsS localhost:8080/ready
```

## Notes

- **Single instance is required.** The StatefulSet runs `replicaCount: 1` and the checkpoint volume is `ReadWriteOnce`, so at most one process ever holds the state store. Do not scale the replica count or point a second install at the same volume - see the [single-owner rule](deployment-support.md#topology-single-owner-per-source).
- **Persistent storage.** SQLite checkpoints and the DLQ live on the PVC; losing it rewinds to the last durable checkpoint (at-least-once re-delivery, deduped on event `id`). For a shared store, use the `postgres` storage backend (still single-instance).
- **Upgrades.** A single-replica StatefulSet terminates the old pod before starting the new one; the preflight initContainer re-gates every rollout.
