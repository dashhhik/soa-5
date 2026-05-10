package warehouse

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testCodec(t *testing.T) *AvroCodec {
	t.Helper()
	codec, err := NewAvroCodec(nil, "warehouse-events-value")
	if err != nil {
		t.Fatal(err)
	}
	codec.v1ID = 101
	codec.v2ID = 102
	codec.bySchema[101] = codec.v1
	codec.bySchema[102] = codec.v2
	return codec
}

func TestAvroDecodesV1WithoutSupplier(t *testing.T) {
	codec := testCodec(t)
	event := Event{
		EventID:      uuid.NewString(),
		EventType:    EventProductReceived,
		EventVersion: 1,
		Timestamp:    TimestampMillis(time.Now().UnixMilli()),
		ProductID:    "SKU-1",
		ZoneID:       "ZONE-A",
		Quantity:     10,
	}

	payload, err := codec.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	got, schemaID, err := codec.Decode(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if schemaID != 101 {
		t.Fatalf("schema id = %d, want 101", schemaID)
	}
	if got.SupplierID != nil {
		t.Fatalf("supplier_id = %v, want nil", *got.SupplierID)
	}
	if got.EventVersion != 1 {
		t.Fatalf("event_version = %d, want 1", got.EventVersion)
	}
}

func TestAvroDecodesV2Supplier(t *testing.T) {
	codec := testCodec(t)
	supplier := "SUP-001"
	event := Event{
		EventID:      uuid.NewString(),
		EventType:    EventProductReceived,
		EventVersion: 2,
		Timestamp:    TimestampMillis(time.Now().UnixMilli()),
		ProductID:    "SKU-2",
		ZoneID:       "ZONE-A",
		Quantity:     10,
		SupplierID:   &supplier,
	}

	payload, err := codec.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	got, schemaID, err := codec.Decode(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if schemaID != 102 {
		t.Fatalf("schema id = %d, want 102", schemaID)
	}
	if got.SupplierID == nil || *got.SupplierID != supplier {
		t.Fatalf("supplier_id = %v, want %s", got.SupplierID, supplier)
	}
}
