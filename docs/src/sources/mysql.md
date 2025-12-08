# MySQL source

DeltaForge tails the MySQL binlog to capture row-level changes.

- 🛡️ **Replication-safe**: built for MySQL first, using binlog subscriptions with table filters.

## Configuration

Set `spec.source.type` to `mysql` and provide a config object:

- `id` (string): logical identifier for metrics and logging.
- `dsn` (string): MySQL connection string with replication privileges.
- `tables` (array<string>): fully qualified tables to subscribe to.

### Example
```yaml
source:
  type: mysql
  config:
    id: orders-mysql
    dsn: ${MYSQL_DSN}
    tables:
      - shop.orders
      - shop.order_items
```

## Notes

- Table filters are applied before events are handed to processors and sinks.
- Batches respect `respect_source_tx` to avoid splitting a single binlog transaction across multiple commits.
