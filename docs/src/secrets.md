# Secrets & Credentials

DeltaForge resolves every credential in a deployment - source database, sinks,
schema registry, and the storage backend - from **references** rather than inline
values. A reference names *where* a secret lives (an environment variable, a file, or
a Vault KV path); the runtime reads it at startup, holds it in a zeroized buffer, and
hands it straight to the client that needs it. The secret never enters the parsed
config, so it cannot leak through the status API, logs, or `Debug` output.

Inline secrets and `${ENV}` expansion still work for backward compatibility, but they
are **deprecated**. Prefer references for anything sensitive.

## Reference shape

A reference is a small object:

```yaml
{ provider: file, location: /run/secrets/db/password }
```

| Field | Required | Description |
|-------|----------|-------------|
| `provider` | Yes | `env`, `file`, or `vault`. |
| `location` | Yes | Env var name, absolute file path, or `<mount>/<path>` for Vault. |
| `selector` | Vault only | Field within a structured record (e.g. the KV key). |
| `version` | No | Provider-specific immutable version pin (Vault KV version). |
| `purpose` | No | Non-secret label for diagnostics (e.g. `source-password`). |

## Providers

### Environment variables (`env`)

```yaml
{ provider: env, location: DF_PG_PASSWORD }
```

Covers Kubernetes `env.valueFrom.secretKeyRef`, which projects a Secret key into an
environment variable.

### Files (`file`)

```yaml
{ provider: file, location: /run/secrets/db/password }
```

The path must be absolute. By default the file must be a regular file (not a symlink),
which suits operator-mounted secrets.

### Kubernetes projected volumes

A projected `Secret` volume mounts each key as a symlink into a versioned directory.
To follow those symlinks safely, set a **trusted root** so the resolved target must
stay within the mounted volume:

- **Sources and sinks:** set `projected_file_root` under `spec.secrets` (see
  [Pipeline-level secret providers](#pipeline-level-secret-providers)). A source file
  rotation trigger's `trusted_root` is honored as a fallback.
- **Storage backend:** set `secret_trusted_root` on the storage config.

```yaml
storage:
  backend: postgres
  dsn: postgres://pg.internal:5432/deltaforge      # password-less base
  secret_trusted_root: /var/run/secrets
  credentials:
    username: { provider: file, location: /var/run/secrets/store/username }
    password: { provider: file, location: /var/run/secrets/store/password }
```

### HashiCorp Vault KV (`vault`)

A Vault reference points at a KV v2 record; `location` is `<mount>/<path>` (the KV v2
`data/` segment is added automatically, so do not include it) and `selector` picks the
field:

```yaml
{ provider: vault, location: secret/deltaforge/pg, selector: password }
```

Vault KV references require a Vault connection. Configure it in the scope that owns the
reference: the **pipeline** (`spec.secrets`) for source and sink references, and the
**storage** config for the storage backend. The two are independent.

```yaml
storage:
  backend: postgres
  dsn: postgres://pg.internal:5432/deltaforge
  vault:
    address: https://vault.internal:8200
    auth:
      method: kubernetes
      role: deltaforge-storage
      jwt_path: /var/run/secrets/kubernetes.io/serviceaccount/token
  dsn_secret: { provider: vault, location: secret/deltaforge/store, selector: dsn }
```

The runner must be built with the `vault` feature; otherwise a Vault-referenced
credential fails closed at startup with a clear error.

### Pipeline-level secret providers

`spec.secrets` assembles the single resolver shared by **both** source and sink
credential resolution. Configure it here whenever any source or sink reference needs
Vault or a Kubernetes projected volume - the source does not need to use credential
rotation for a sink's Vault or projected-file reference to resolve.

```yaml
spec:
  secrets:
    # Enables `vault` references on the source and any sink.
    vault:
      address: https://vault.internal:8200
      auth:
        method: kubernetes
        role: deltaforge-pipeline
        jwt_path: /var/run/secrets/kubernetes.io/serviceaccount/token
    # Enables projected-volume (symlink) `file` references for the source and any sink.
    projected_file_root: /var/run/secrets
```

When `spec.secrets` is absent, the resolver falls back to the source's rotation
configuration (backward compatible): a source `file` rotation trigger supplies the
projected-file root, and a source `vault` trigger supplies the Vault connection.

## Where references apply

### Source (PostgreSQL / MySQL)

Three mutually exclusive forms:

```yaml
source:
  type: postgres
  config:
    dsn_secret: { provider: vault, location: secret/pg, selector: dsn }   # whole DSN
```

or a password-less base DSN plus credential references:

```yaml
    dsn: postgres://pg.internal:5432/orders
    credentials:
      username: { provider: env,  location: DF_PG_USER }
      password: { provider: file, location: /run/secrets/pg/password }
```

### Storage backend

`dsn_secret` (whole DSN) **or** a password-less `dsn` plus `credentials`
(`username`/`password`). Resolved before the backend is opened.

### Sinks

| Sink | Reference fields |
|------|------------------|
| Kafka | `secret_refs` map (any client-conf key, e.g. `sasl.password`) |
| Redis | `uri_secret` (whole URI) **or** `credentials` (`username`/`password`) on a password-less `uri` |
| NATS | `username_ref`, `password_ref`, `token_ref` |
| HTTP | `secret_refs` map (any header name) |
| S3 | `access_key_id_ref`, `secret_access_key_ref`, `session_token_ref` |
| ClickHouse | `user_ref`, `password_ref` |
| Elasticsearch | `basic`: `username_ref`, `password_ref`; `apikey`: `api_key_ref` |

For Kafka and HTTP, a key set both inline and in `secret_refs` is rejected - never
silently overridden.

### Schema registry (Avro)

Any sink using `encoding: { type: avro }` resolves the registry's basic-auth password
from a reference:

```yaml
encoding:
  type: avro
  schema_registry_url: https://schema-registry.internal:8081
  username: sr-user
  password_ref: { provider: vault, location: secret/sr, selector: password }
```

### S3 and ambient AWS identity

Omitting all S3 keys is a first-class, no-static-secret mode: the sink uses the
ambient AWS credential chain (IAM instance/role, environment, or profile). Provide
`access_key_id_ref` and `secret_access_key_ref` **together** or not at all; a partial
pair is rejected.

## Rules the runtime enforces

All of these fail **closed at startup**, before any network connection:

- A reference that resolves to nothing, an empty value, or invalid UTF-8 is an error.
- Setting both an inline value and its reference for the same field is rejected.
- Partial credential sets (a username without a password, one S3 key without the
  other) are rejected.
- A base DSN that already embeds a password cannot also take credential references.
- Storage credential resolution happens before the backend is opened; source and sink
  resolution happens before any client is constructed.

## Redaction

Resolved secrets are held in zeroized buffers and never written back into the
serializable config. Inline secrets that remain in config for compatibility are
redacted everywhere they could surface - the status API, the pipeline `Debug`
representation, and logs all show `***REDACTED***` in place of secret values.

## Rotation is restart-required

This baseline does **not** hot-swap credentials in a live client. Rotating any
referenced credential (updating the Secret, file, or Vault KV value) takes effect when
the affected pipeline or the runner restarts. Plan rotations as a rolling restart.

> Zero-downtime rotation of dynamic Vault database credentials for PostgreSQL and MySQL
> sources is designed as a separate capability and is not part of this baseline.

## Complete example: no inline secrets

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-cdc
  tenant: retail
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pg.internal:5432/orders
      publication: df_pub
      slot: df_slot
      tables: [public.orders]
      credentials:
        username: { provider: env,  location: DF_PG_USER }
        password: { provider: file, location: /run/secrets/pg/password }
  sinks:
    - type: kafka
      config:
        id: kafka
        brokers: broker.internal:9092
        topic: orders.events
        secret_refs:
          sasl.password: { provider: file, location: /run/secrets/kafka/sasl_password }
        encoding:
          type: avro
          schema_registry_url: https://schema-registry.internal:8081
          username: sr-user
          password_ref: { provider: file, location: /run/secrets/sr/password }
    - type: s3
      config:
        id: archive
        bucket: cdc-archive
        prefix: orders/
        # no keys: uses the pod's IAM role (ambient AWS identity)
```
