package warehouse

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/linkedin/goavro/v2"
)

type AvroCodec struct {
	registry *SchemaRegistryClient
	subject  string
	mu       sync.RWMutex
	bySchema map[int]*goavro.Codec
	v1       *goavro.Codec
	v2       *goavro.Codec
	v1ID     int
	v2ID     int
}

func NewAvroCodec(registry *SchemaRegistryClient, subject string) (*AvroCodec, error) {
	v1, err := goavro.NewCodec(WarehouseEventSchemaV1)
	if err != nil {
		return nil, err
	}
	v2, err := goavro.NewCodec(WarehouseEventSchemaV2)
	if err != nil {
		return nil, err
	}
	return &AvroCodec{
		registry: registry,
		subject:  subject,
		bySchema: make(map[int]*goavro.Codec),
		v1:       v1,
		v2:       v2,
	}, nil
}

func (c *AvroCodec) EnsureRegistered(ctx context.Context) error {
	if err := c.registry.SetCompatibility(ctx, c.subject, "BACKWARD"); err != nil {
		return err
	}
	v1ID, err := c.registry.Register(ctx, c.subject, WarehouseEventSchemaV1)
	if err != nil {
		return err
	}
	v2ID, err := c.registry.Register(ctx, c.subject, WarehouseEventSchemaV2)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.v1ID = v1ID
	c.v2ID = v2ID
	c.bySchema[v1ID] = c.v1
	c.bySchema[v2ID] = c.v2
	c.mu.Unlock()
	return nil
}

func (c *AvroCodec) Encode(event Event) ([]byte, error) {
	return c.encode(event, true)
}

func (c *AvroCodec) EncodeUnchecked(event Event) ([]byte, error) {
	return c.encode(event, false)
}

func (c *AvroCodec) encode(event Event, validate bool) ([]byte, error) {
	event = event.WithDefaults()
	if validate {
		if err := event.Validate(); err != nil {
			return nil, err
		}
	}
	c.mu.RLock()
	schemaID := c.v2ID
	codec := c.v2
	if event.EventVersion == 1 {
		schemaID = c.v1ID
		codec = c.v1
	}
	c.mu.RUnlock()
	if schemaID == 0 {
		return nil, fmt.Errorf("schema ids are not registered")
	}

	native := eventToNative(event)
	if event.EventVersion == 1 {
		delete(native, "supplier_id")
	}
	payload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	out.Grow(5 + len(payload))
	out.WriteByte(0)
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], uint32(schemaID))
	out.Write(id[:])
	out.Write(payload)
	return out.Bytes(), nil
}

func (c *AvroCodec) Decode(ctx context.Context, payload []byte) (Event, int, error) {
	if len(payload) < 5 {
		return Event{}, 0, fmt.Errorf("payload too short for Confluent Avro envelope")
	}
	if payload[0] != 0 {
		return Event{}, 0, fmt.Errorf("invalid Confluent Avro magic byte: %d", payload[0])
	}
	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	codec, err := c.codecForID(ctx, schemaID)
	if err != nil {
		return Event{}, schemaID, err
	}
	native, remaining, err := codec.NativeFromBinary(payload[5:])
	if err != nil {
		return Event{}, schemaID, err
	}
	if len(remaining) != 0 {
		return Event{}, schemaID, fmt.Errorf("trailing bytes after Avro payload: %d", len(remaining))
	}
	record, ok := native.(map[string]interface{})
	if !ok {
		return Event{}, schemaID, fmt.Errorf("Avro payload is not a record")
	}
	event, err := nativeToEvent(record)
	if err != nil {
		return Event{}, schemaID, err
	}
	if event.EventVersion == 0 {
		event.EventVersion = 1
	}
	return event, schemaID, nil
}

func (c *AvroCodec) codecForID(ctx context.Context, id int) (*goavro.Codec, error) {
	c.mu.RLock()
	codec := c.bySchema[id]
	c.mu.RUnlock()
	if codec != nil {
		return codec, nil
	}
	schema, err := c.registry.SchemaByID(ctx, id)
	if err != nil {
		return nil, err
	}
	codec, err = goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.bySchema[id] = codec
	c.mu.Unlock()
	return codec, nil
}

func eventToNative(event Event) map[string]interface{} {
	items := make([]interface{}, 0, len(event.Items))
	for _, item := range event.Items {
		items = append(items, map[string]interface{}{
			"product_id": item.ProductID,
			"zone_id":    item.ZoneID,
			"quantity":   int32(item.Quantity),
		})
	}
	var supplier interface{}
	if event.SupplierID != nil {
		supplier = goavro.Union("string", *event.SupplierID)
	}
	return map[string]interface{}{
		"event_id":      event.EventID,
		"event_type":    string(event.EventType),
		"event_version": int32(event.EventVersion),
		"timestamp":     int64(event.Timestamp),
		"product_id":    event.ProductID,
		"zone_id":       event.ZoneID,
		"from_zone_id":  event.FromZoneID,
		"to_zone_id":    event.ToZoneID,
		"quantity":      int32(event.Quantity),
		"order_id":      event.OrderID,
		"items":         items,
		"supplier_id":   supplier,
	}
}

func nativeToEvent(record map[string]interface{}) (Event, error) {
	event := Event{
		EventID:      stringField(record, "event_id"),
		EventType:    EventType(stringField(record, "event_type")),
		EventVersion: intField(record, "event_version"),
		Timestamp:    TimestampMillis(int64Field(record, "timestamp")),
		ProductID:    stringField(record, "product_id"),
		ZoneID:       stringField(record, "zone_id"),
		FromZoneID:   stringField(record, "from_zone_id"),
		ToZoneID:     stringField(record, "to_zone_id"),
		Quantity:     intField(record, "quantity"),
		OrderID:      stringField(record, "order_id"),
		Items:        orderItems(record["items"]),
	}
	if raw, ok := record["supplier_id"]; ok && raw != nil {
		switch value := raw.(type) {
		case map[string]interface{}:
			if s, ok := value["string"].(string); ok {
				event.SupplierID = &s
			}
		case string:
			event.SupplierID = &value
		}
	}
	return event, nil
}

func (e Event) JSONMap() map[string]interface{} {
	body, _ := json.Marshal(e)
	var out map[string]interface{}
	_ = json.Unmarshal(body, &out)
	return out
}

func stringField(record map[string]interface{}, key string) string {
	value, _ := record[key].(string)
	return value
}

func intField(record map[string]interface{}, key string) int {
	switch value := record[key].(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func int64Field(record map[string]interface{}, key string) int64 {
	switch value := record[key].(type) {
	case int64:
		return value
	case int32:
		return int64(value)
	case int:
		return int64(value)
	case float64:
		return int64(value)
	default:
		return 0
	}
}

func orderItems(raw interface{}) []OrderItem {
	itemsRaw, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	items := make([]OrderItem, 0, len(itemsRaw))
	for _, itemRaw := range itemsRaw {
		itemRecord, ok := itemRaw.(map[string]interface{})
		if !ok {
			continue
		}
		items = append(items, OrderItem{
			ProductID: stringField(itemRecord, "product_id"),
			ZoneID:    stringField(itemRecord, "zone_id"),
			Quantity:  intField(itemRecord, "quantity"),
		})
	}
	return items
}
