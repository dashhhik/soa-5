package warehouse

const WarehouseEventSchemaV1 = `{
  "type": "record",
  "name": "WarehouseEvent",
  "namespace": "smart.warehouse",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "event_version", "type": "int", "default": 1},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "product_id", "type": "string", "default": ""},
    {"name": "zone_id", "type": "string", "default": ""},
    {"name": "from_zone_id", "type": "string", "default": ""},
    {"name": "to_zone_id", "type": "string", "default": ""},
    {"name": "quantity", "type": "int", "default": 0},
    {"name": "order_id", "type": "string", "default": ""},
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "zone_id", "type": "string"},
            {"name": "quantity", "type": "int"}
          ]
        }
      },
      "default": []
    }
  ]
}`

const WarehouseEventSchemaV2 = `{
  "type": "record",
  "name": "WarehouseEvent",
  "namespace": "smart.warehouse",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "event_version", "type": "int", "default": 2},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "product_id", "type": "string", "default": ""},
    {"name": "zone_id", "type": "string", "default": ""},
    {"name": "from_zone_id", "type": "string", "default": ""},
    {"name": "to_zone_id", "type": "string", "default": ""},
    {"name": "quantity", "type": "int", "default": 0},
    {"name": "order_id", "type": "string", "default": ""},
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "zone_id", "type": "string"},
            {"name": "quantity", "type": "int"}
          ]
        }
      },
      "default": []
    },
    {"name": "supplier_id", "type": ["null", "string"], "default": null}
  ]
}`
