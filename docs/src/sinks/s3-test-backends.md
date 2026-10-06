# S3 test backends

DeltaForge's S3 integration tests run against two emulated backends. Each
proves something different, and neither proves the sink works against
real AWS S3.

| Backend | Proves | Suites |
|---|---|---|
| RustFS 1.0.1, pinned by image digest in `crates/s3-test-server` | **Portable S3-server behavior:** what any S3-compatible server must do for DeltaForge to be correct on it | `sinks/lib-s3-server` (the contract below), `runner/s3_e2e_tests`, `sinks/s3_server_test` |
| MiniStack 1.4.9, pinned by image digest in `crates/runner/tests/ministack` | **The AWS-shaped path:** AWS-style endpoints, credentials and responses, through the production legacy rolling and durable_v2 sinks | `runner/s3_ministack_canary`, `runner/s3_ministack_durable_canary` |

Real-AWS qualification remains a separate, eventual step. An emulator
passing does not show that AWS behaves the same way under load, across
regions, or with its own consistency and rate limits.

## The contract

`crates/sinks/src/s3/s3_server_contract.rs` checks the S3 behavior
DeltaForge relies on. A server is acceptable as a test backend only while
every check passes:

- **Conditional PUT**, checked on the wire:
  - `If-None-Match: *` on an existing key returns 412 `PreconditionFailed`;
  - `If-Match` with a wrong or stale ETag returns 412;
  - `If-Match` on a missing key returns 404 `NoSuchKey`;
  - a rejected PUT writes nothing.
- **The production conditional path** (`ObjectStoreConditional`):
  create-only, CAS, conflict on a stale or missing ETag, and a capability
  probe that passes and leaves no trace.
- **Stable ETags**: the PUT response, HEAD, GET, LIST and the wire `ETag`
  header agree across repeated reads, and a read never invalidates the
  ETag that the durable_v2 HEAD CAS uses.
- **Basic object operations**: PUT, GET, HEAD, LIST and DELETE. DELETE of a
  missing key returns 204.
- **Multipart uploads**:
  - create, upload parts and complete produces the exact bytes;
  - an aborted upload is never visible and no open upload remains.
- **Zero-byte objects**, written both unconditionally and create-only.
- **Read-after-write visibility** for a write, an overwrite and a delete.
- **Persistence across a server restart**: bytes and ETags survive, and
  conditional writes still hold.
- **Concurrent conditional writers**: exactly one winner, for 16 writers
  over 20 rounds, both for create-only and for CAS. The final object and
  its ETag are the winner's.

The suite is `#[ignore]`d and starts its own container:

```text
cargo test -p sinks --lib -- --include-ignored --test-threads=1 s3_server_contract
```

The env-gated live durable_v2 matrix in `s3_server_it` runs against any
server, the pinned RustFS included. See
[Durable acknowledgements](s3-durable-acks.md).

## Changing the pinned image

The image, credentials, readiness check and container setup live only in
`crates/s3-test-server`. To move to a new RustFS release:

1. Pin the new release by tag and digest.
2. Run the contract suite, `runner/s3_e2e_tests` and `sinks/s3_server_test`.
3. Adopt it only if every check passes unchanged. If a check fails, record
   the exact request, the expected AWS behavior and the actual response.
   Do not relax the assertion.

The manual chaos stack (`docker-compose.chaos.yml`) still uses its own MinIO
service. It is not part of either gate tier.
