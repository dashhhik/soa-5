package warehouse

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventProductReceived  EventType = "PRODUCT_RECEIVED"
	EventProductShipped   EventType = "PRODUCT_SHIPPED"
	EventProductMoved     EventType = "PRODUCT_MOVED"
	EventProductReserved  EventType = "PRODUCT_RESERVED"
	EventProductReleased  EventType = "PRODUCT_RELEASED"
	EventInventoryCounted EventType = "INVENTORY_COUNTED"
	EventOrderCreated     EventType = "ORDER_CREATED"
	EventOrderCompleted   EventType = "ORDER_COMPLETED"
)

type TimestampMillis int64

func (t *TimestampMillis) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return errors.New("timestamp is required")
	}
	if data[0] == '"' {
		var raw string
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		parsed, err := parseTimestamp(raw)
		if err != nil {
			return err
		}
		*t = TimestampMillis(parsed.UnixMilli())
		return nil
	}
	var millis int64
	if err := json.Unmarshal(data, &millis); err != nil {
		return err
	}
	*t = TimestampMillis(millis)
	return nil
}

func (t TimestampMillis) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(t), 10)), nil
}

func (t TimestampMillis) Time() time.Time {
	return time.UnixMilli(int64(t)).UTC()
}

type OrderItem struct {
	ProductID string `json:"product_id"`
	ZoneID    string `json:"zone_id"`
	Quantity  int    `json:"quantity"`
}

type Event struct {
	EventID      string          `json:"event_id"`
	EventType    EventType       `json:"event_type"`
	EventVersion int             `json:"event_version,omitempty"`
	Timestamp    TimestampMillis `json:"timestamp"`
	ProductID    string          `json:"product_id,omitempty"`
	ZoneID       string          `json:"zone_id,omitempty"`
	FromZoneID   string          `json:"from_zone_id,omitempty"`
	ToZoneID     string          `json:"to_zone_id,omitempty"`
	Quantity     int             `json:"quantity,omitempty"`
	OrderID      string          `json:"order_id,omitempty"`
	Items        []OrderItem     `json:"items,omitempty"`
	SupplierID   *string         `json:"supplier_id,omitempty"`
}

type ValidationError struct {
	Code    string
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}

func (e Event) WithDefaults() Event {
	if e.EventID == "" {
		e.EventID = uuid.NewString()
	}
	if e.EventVersion == 0 {
		e.EventVersion = 2
	}
	if e.Timestamp == 0 {
		e.Timestamp = TimestampMillis(time.Now().UTC().UnixMilli())
	}
	return e
}

func (e Event) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return validation("VALIDATION_ERROR", "event_id is required")
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return validation("VALIDATION_ERROR", "event_id must be a valid UUID")
	}
	if !validEventType(e.EventType) {
		return validation("VALIDATION_ERROR", fmt.Sprintf("unsupported event_type %q", e.EventType))
	}
	if e.EventVersion != 1 && e.EventVersion != 2 {
		return validation("VALIDATION_ERROR", "event_version must be 1 or 2")
	}
	if e.Timestamp <= 0 {
		return validation("VALIDATION_ERROR", "timestamp must be positive unix millis or RFC3339")
	}

	switch e.EventType {
	case EventProductReceived:
		return validateProductZoneQuantity(e.ProductID, e.ZoneID, e.Quantity, true)
	case EventProductShipped, EventProductReserved, EventProductReleased:
		return validateProductZoneQuantity(e.ProductID, e.ZoneID, e.Quantity, true)
	case EventInventoryCounted:
		return validateProductZoneQuantity(e.ProductID, e.ZoneID, e.Quantity, false)
	case EventProductMoved:
		if strings.TrimSpace(e.ProductID) == "" {
			return validation("VALIDATION_ERROR", "product_id is required")
		}
		if strings.TrimSpace(e.FromZoneID) == "" {
			return validation("VALIDATION_ERROR", "from_zone_id is required")
		}
		if strings.TrimSpace(e.ToZoneID) == "" {
			return validation("VALIDATION_ERROR", "to_zone_id is required")
		}
		if e.FromZoneID == e.ToZoneID {
			return validation("VALIDATION_ERROR", "from_zone_id and to_zone_id must be different")
		}
		if e.Quantity <= 0 {
			return validation("VALIDATION_ERROR", fmt.Sprintf("invalid quantity: %d (must be positive)", e.Quantity))
		}
	case EventOrderCreated:
		if strings.TrimSpace(e.OrderID) == "" {
			return validation("VALIDATION_ERROR", "order_id is required")
		}
		if len(e.Items) == 0 {
			return validation("VALIDATION_ERROR", "order items are required")
		}
		for i, item := range e.Items {
			if err := validateProductZoneQuantity(item.ProductID, item.ZoneID, item.Quantity, true); err != nil {
				return validation("VALIDATION_ERROR", fmt.Sprintf("invalid order item %d: %v", i, err))
			}
		}
	case EventOrderCompleted:
		if strings.TrimSpace(e.OrderID) == "" {
			return validation("VALIDATION_ERROR", "order_id is required")
		}
	}
	return nil
}

func validateProductZoneQuantity(productID, zoneID string, quantity int, strictlyPositive bool) error {
	if strings.TrimSpace(productID) == "" {
		return validation("VALIDATION_ERROR", "product_id is required")
	}
	if strings.TrimSpace(zoneID) == "" {
		return validation("VALIDATION_ERROR", "zone_id is required")
	}
	if strictlyPositive && quantity <= 0 {
		return validation("VALIDATION_ERROR", fmt.Sprintf("invalid quantity: %d (must be positive)", quantity))
	}
	if !strictlyPositive && quantity < 0 {
		return validation("VALIDATION_ERROR", fmt.Sprintf("invalid quantity: %d (must be non-negative)", quantity))
	}
	return nil
}

func validation(code, msg string) ValidationError {
	return ValidationError{Code: code, Message: msg}
}

func validEventType(v EventType) bool {
	switch v {
	case EventProductReceived, EventProductShipped, EventProductMoved, EventProductReserved,
		EventProductReleased, EventInventoryCounted, EventOrderCreated, EventOrderCompleted:
		return true
	default:
		return false
	}
}

func parseTimestamp(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z07:00",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp format: %q", value)
}
