# HTTP/Webhook sink

The HTTP sink delivers CDC events via HTTP POST (or PUT) to any URL — internal services, serverless functions, third-party APIs, webhooks.

## When to use HTTP

HTTP is the universal integration point. Use it when:

- Your consumer doesn't run Kafka/NATS/Redis
- You need to call a REST API or webhook on every database change
- You want the simplest possible setup (DeltaForge + any HTTP server)
- You're integrating with serverless functions (AWS Lambda, Cloud Functions)

### Pros and cons

| Pros | Cons |
|------|------|
| Works with any HTTP server | Higher latency than message queues |
| No infrastructure dependencies | No built-in replay (consumer must be idempotent) |
| Simple auth (Bearer, Basic, headers) | No consumer groups or partitioning |
| URL templates for per-table routing | One request per event (unless batch mode) |

## Configuration

<table>
<tr>
<td>

```yaml
sinks:
  - type: http
    config:
      id: my-webhook
      url: "https://api.example.com/events"
      method: POST
      headers:
        Authorization: "Bearer ${API_TOKEN}"
        X-Source: deltaforge
      batch_mode: false
      required: true
      send_timeout_secs: 10
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `url` | string | — | Target URL (supports `${path}` templates) |
| `method` | string | `POST` | HTTP method (`POST` or `PUT`) |
| `headers` | map | `{}` | Static headers (values support `${ENV_VAR}` expansion) |
| `batch_mode` | bool | `false` | Send JSON array instead of per-event requests |
| `required` | bool | `true` | Gates checkpoints |
| `send_timeout_secs` | int | `10` | Per-request timeout |
| `batch_timeout_secs` | int | `30` | Batch timeout |
| `connect_timeout_secs` | int | `5` | TCP connection timeout |

</td>
</tr>
</table>

## Authentication

All auth is handled via the `headers` map. Values support `${ENV_VAR}` shell expansion.

```yaml
# Bearer token
headers:
  Authorization: "Bearer ${API_TOKEN}"

# Basic auth
headers:
  Authorization: "Basic ${BASIC_AUTH_B64}"

# Custom API key
headers:
  X-API-Key: "${MY_API_KEY}"

# HMAC signature (computed externally, injected via env)
headers:
  X-Signature: "${WEBHOOK_SIGNATURE}"
```

## URL templates

Route events to different URLs based on event fields:

```yaml
# Per-table endpoint
url: "https://api.example.com/cdc/${source.table}"
# → https://api.example.com/cdc/orders
# → https://api.example.com/cdc/customers

# Per-database endpoint
url: "https://${source.db}.api.internal/events"

# Static URL (most common)
url: "https://api.example.com/webhook"
```

## Batch mode

By default, the sink sends one HTTP request per event. Enable `batch_mode: true` to send a JSON array of events in a single request:

```yaml
# Per-event mode (default): one POST per event
batch_mode: false
# Body: {"id": "...", "op": "c", "after": {...}}

# Batch mode: one POST with JSON array
batch_mode: true
# Body: [{"id": "...", "op": "c", ...}, {"id": "...", "op": "u", ...}]
```

Batch mode reduces HTTP overhead but means the consumer must handle arrays.

## Retry behavior

| Condition | Behavior |
|-----------|----------|
| 2xx response | Success |
| 408, 429 | Retry with backoff (100ms → 10s, 3 attempts) |
| 5xx | Retry with backoff |
| Connection error | Retry with backoff |
| Timeout | Retry with backoff |
| 401, 403 | Auth error — fail immediately, no retry |
| Other 4xx | Permanent failure — fail batch |

## Failure modes

| Failure | Symptoms | DeltaForge behavior | Resolution |
|---------|----------|---------------------|------------|
| **Endpoint unavailable** | Connection refused | Retries with backoff; blocks checkpoint | Restore endpoint |
| **Authentication failure** | 401/403 response | Fails fast, no retry | Fix credentials in headers |
| **Rate limited** | 429 response | Retries with backoff | Reduce throughput or increase rate limit |
| **Timeout** | Request exceeds `send_timeout_secs` | Retries | Increase timeout or fix slow endpoint |
| **URL template error** | Template resolves to empty | Event → DLQ (if enabled) | Fix template or event data |

## Consuming events

### Node.js / Express

```javascript
app.post('/webhook', (req, res) => {
  const event = req.body;
  console.log(`${event.op} on ${event.source.table}: ${JSON.stringify(event.after)}`);
  res.sendStatus(200);
});
```

### Python / Flask

```python
@app.route('/webhook', methods=['POST'])
def webhook():
    event = request.json
    process(event)
    return '', 200
```

### Go

```go
http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
    var event Event
    json.NewDecoder(r.Body).Decode(&event)
    process(event)
    w.WriteHeader(http.StatusOK)
})
```

## Notes

- Connection pooling is automatic — `reqwest` reuses TCP connections to the same host
- The `Content-Type` header is set to `application/json` by default
- At-least-once delivery: on crash, events may be re-sent. Consumers should be idempotent.
- For per-event dedup, use the `id` field in the event payload (UUID v7, stable across replays)
