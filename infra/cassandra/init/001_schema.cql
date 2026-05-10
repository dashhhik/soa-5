CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
  product_id text,
  zone_id text,
  available_quantity int,
  reserved_quantity int,
  supplier_id text,
  updated_at timestamp,
  last_event_id text,
  PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
  product_id text PRIMARY KEY,
  total_available int,
  total_reserved int,
  updated_at timestamp,
  last_event_id text
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
  zone_id text,
  product_id text,
  available_quantity int,
  reserved_quantity int,
  supplier_id text,
  updated_at timestamp,
  last_event_id text,
  PRIMARY KEY ((zone_id), product_id)
);

CREATE TABLE IF NOT EXISTS warehouse.orders_by_id (
  order_id text PRIMARY KEY,
  status text,
  created_at timestamp,
  updated_at timestamp,
  last_event_id text
);

CREATE TABLE IF NOT EXISTS warehouse.order_items_by_order (
  order_id text,
  product_id text,
  zone_id text,
  quantity int,
  created_at timestamp,
  last_event_id text,
  PRIMARY KEY ((order_id), product_id, zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.processed_events (
  event_id text PRIMARY KEY,
  event_type text,
  event_timestamp timestamp,
  processed_at timestamp,
  status text,
  error_reason text
);

CREATE TABLE IF NOT EXISTS warehouse.event_history (
  product_id text,
  event_timestamp timestamp,
  event_id text,
  event_type text,
  status text,
  order_id text,
  zone_id text,
  from_zone_id text,
  to_zone_id text,
  quantity int,
  supplier_id text,
  PRIMARY KEY ((product_id), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS warehouse.entity_versions (
  entity_id text PRIMARY KEY,
  last_event_timestamp timestamp,
  last_event_id text
);
