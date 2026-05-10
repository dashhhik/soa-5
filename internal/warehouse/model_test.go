package warehouse

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventValidationRejectsInvalidQuantity(t *testing.T) {
	event := Event{
		EventID:      uuid.NewString(),
		EventType:    EventProductShipped,
		EventVersion: 2,
		Timestamp:    TimestampMillis(time.Now().UnixMilli()),
		ProductID:    "SKU-1",
		ZoneID:       "ZONE-A",
		Quantity:     -5,
	}

	err := event.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	var validation ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validation.Code != "VALIDATION_ERROR" {
		t.Fatalf("unexpected code: %s", validation.Code)
	}
}

func TestTouchedEntitiesDeduplicatesOrderProducts(t *testing.T) {
	event := Event{
		EventType: EventOrderCreated,
		OrderID:   "ORD-1",
		Items: []OrderItem{
			{ProductID: "SKU-1", ZoneID: "A", Quantity: 1},
			{ProductID: "SKU-1", ZoneID: "B", Quantity: 2},
		},
	}

	got := touchedEntities(event)
	want := []string{"order:ORD-1", "product:SKU-1"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
