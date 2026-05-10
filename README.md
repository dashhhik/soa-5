# Smart Warehouse

Event-driven warehouse state management with Kafka, Schema Registry, a Go stateful consumer, Cassandra, DLQ, Prometheus, and Grafana.

## Run

Start the full stack:

```bash
docker compose up --build
```

Run the compose E2E suite:

```bash
make test
```

Useful endpoints:

| Service | URL |
| --- | --- |
| WMS producer | http://localhost:18080 |
| Consumer health | http://localhost:18081/health |
| Consumer metrics | http://localhost:18081/metrics |
| Schema Registry | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000, `admin/admin` |
| Cassandra CQL | `localhost:9042` |

Kafka topics:

- `warehouse-events`
- `warehouse-events-dlq`

Consumer group:

- `warehouse-state-consumer`

## Architecture

`wms-producer` accepts JSON events over HTTP, registers Avro schemas in Schema Registry, and publishes Confluent Avro messages to Kafka. `wms-consumer` reads with manual offset commits, updates Cassandra state, publishes invalid/problematic events to DLQ, and exposes health and Prometheus metrics.

Offset commit happens only after one of these outcomes:

- the event was applied to Cassandra;
- the event was identified as duplicate or stale and recorded as processed;
- the event was published to `warehouse-events-dlq`.

## Cassandra Model

Keyspace:

```sql
CREATE KEYSPACE warehouse
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
```

Tables are query-first and denormalized:

| Table | Query |
| --- | --- |
| `inventory_by_product_zone` | exact product state in one zone: `product_id`, `zone_id` |
| `inventory_by_product` | aggregate product totals across all zones |
| `inventory_by_zone` | all products in a zone |
| `orders_by_id` | order status by `order_id` |
| `order_items_by_order` | order lines by `order_id` |
| `processed_events` | idempotency by `event_id` |
| `event_history` | audit by product and event timestamp |
| `entity_versions` | timestamp guard for out-of-order events |

Writes use Cassandra logged batches and `QUORUM`, so one warehouse event updates all related denormalized tables atomically from the consumer point of view. Reads use `ONE` for demo/API speed; this is faster and remains available during node loss, while `QUORUM` reads would provide stronger read-after-write consistency at higher latency.

## Event Contract

Subject: `warehouse-events-value`

Canonical fields:

- `event_id`
- `event_type`
- `event_version`
- `timestamp`
- `product_id`
- `zone_id`
- `from_zone_id`
- `to_zone_id`
- `quantity`
- `order_id`
- `items`
- `supplier_id`

Supported event types:

- `PRODUCT_RECEIVED`
- `PRODUCT_SHIPPED`
- `PRODUCT_MOVED`
- `PRODUCT_RESERVED`
- `PRODUCT_RELEASED`
- `INVENTORY_COUNTED`
- `ORDER_CREATED`
- `ORDER_COMPLETED`

Schema evolution:

1. V1 has all canonical fields except `supplier_id`.
2. V2 adds `supplier_id` as `["null", "string"]` with default `null`.
3. Producer and consumer set Schema Registry compatibility to `BACKWARD`.
4. Consumer decodes by writer schema ID, so V1 and V2 messages can coexist in `warehouse-events`.
5. For V2 `PRODUCT_RECEIVED`, `supplier_id` is written to Cassandra. For V1 it stays `null`.

Show registered versions:

```bash
curl http://localhost:8081/subjects/warehouse-events-value/versions
```

## Demo Events

Publish an event:

```bash
curl -sS -X POST http://localhost:18080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_type": "PRODUCT_RECEIVED",
    "event_version": 2,
    "timestamp": "2026-05-10T12:00:00Z",
    "product_id": "SKU-001",
    "zone_id": "ZONE-A",
    "quantity": 100,
    "supplier_id": "SUP-001"
  }'
```

Query Cassandra:

```bash
docker exec cassandra-1 cqlsh -e "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"
docker exec cassandra-1 cqlsh -e "SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"
docker exec cassandra-1 cqlsh -e "SELECT * FROM warehouse.inventory_by_zone WHERE zone_id='ZONE-A';"
```

## Defense Scenarios

1. Basic cycle:
   - Send `PRODUCT_RECEIVED`, `PRODUCT_RESERVED`, `PRODUCT_MOVED`, `PRODUCT_SHIPPED`, `ORDER_CREATED`, `ORDER_COMPLETED`.
   - Check `inventory_by_product_zone`, `inventory_by_product`, and `inventory_by_zone`.

2. Idempotency:
   - Send the same event twice with the same `event_id`.
   - Check that quantities are unchanged after the second delivery.

3. Denormalized consistency:
   - Send one `PRODUCT_RECEIVED`.
   - Check all three inventory tables contain matching state.

4. Out-of-order events:
   - Send events at `12:00` and `12:05`.
   - Send a later-arriving event with timestamp `12:02`.
   - Check `processed_events.status = 'STALE'` and inventory is not overwritten.

5. DLQ:
   - Use `/events/unsafe` to publish an invalid event that the producer would normally reject:

```bash
curl -sS -X POST http://localhost:18080/events/unsafe \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "22222222-2222-2222-2222-222222222222",
    "event_type": "PRODUCT_SHIPPED",
    "event_version": 2,
    "timestamp": "2026-05-10T12:10:00Z",
    "product_id": "SKU-DLQ",
    "zone_id": "ZONE-A",
    "quantity": -5
  }'
```

   - Read DLQ:

```bash
make dlq
```

6. Cassandra cluster failure:

```bash
docker exec cassandra-1 nodetool status
docker stop cassandra-2
# publish another valid event
docker start cassandra-2
docker exec cassandra-1 nodetool status
```

With RF=3 and write CL=`QUORUM`, writes continue with one node down. With CL=`ALL`, the same write would fail while one replica is unavailable.

7. Monitoring:

```bash
curl http://localhost:18081/health
curl http://localhost:18081/metrics
```

Open Grafana at http://localhost:3000. The provisioned dashboard has panels for consumer lag, throughput, and Cassandra write errors. Prometheus also loads an alert for `consumer_lag > 5`.

8. Schema evolution:
   - Send V1 `PRODUCT_RECEIVED` without `supplier_id`.
   - Send V2 `PRODUCT_RECEIVED` with `supplier_id`.
   - Check Cassandra: V1 row has `supplier_id = null`, V2 row stores the supplier.
   - Check Schema Registry versions with the command above.

## Development

Local checks:

```bash
go test ./...
(cd tests && go test ./... -run '^$')
docker compose config
```
