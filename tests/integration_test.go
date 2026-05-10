package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type event struct {
	EventID      string      `json:"event_id"`
	EventType    string      `json:"event_type"`
	EventVersion int         `json:"event_version,omitempty"`
	Timestamp    string      `json:"timestamp"`
	ProductID    string      `json:"product_id,omitempty"`
	ZoneID       string      `json:"zone_id,omitempty"`
	FromZoneID   string      `json:"from_zone_id,omitempty"`
	ToZoneID     string      `json:"to_zone_id,omitempty"`
	Quantity     int         `json:"quantity,omitempty"`
	OrderID      string      `json:"order_id,omitempty"`
	Items        []orderItem `json:"items,omitempty"`
	SupplierID   *string     `json:"supplier_id,omitempty"`
}

type orderItem struct {
	ProductID string `json:"product_id"`
	ZoneID    string `json:"zone_id"`
	Quantity  int    `json:"quantity"`
}

type cfg struct {
	ProducerURL       string
	ConsumerURL       string
	SchemaRegistryURL string
	KafkaBrokers      []string
	DLQTopic          string
	CassandraHosts    []string
	CassandraKeyspace string
}

func TestWarehouseE2E(t *testing.T) {
	config := loadConfig()
	session := openCassandra(t, config)
	defer session.Close()

	waitHTTP(t, config.ProducerURL+"/health", 3*time.Minute)
	waitHTTP(t, config.ConsumerURL+"/health", 3*time.Minute)

	t.Run("basic warehouse cycle", func(t *testing.T) {
		basicWarehouseCycle(t, config, session)
	})
	t.Run("idempotency", func(t *testing.T) {
		idempotencyScenario(t, config, session)
	})
	t.Run("denormalized consistency", func(t *testing.T) {
		consistencyScenario(t, config, session)
	})
	t.Run("out of order event", func(t *testing.T) {
		outOfOrderScenario(t, config, session)
	})
	t.Run("dlq", func(t *testing.T) {
		dlqScenario(t, config, session)
	})
	t.Run("monitoring", func(t *testing.T) {
		monitoringScenario(t, config)
	})
	t.Run("schema evolution", func(t *testing.T) {
		schemaEvolutionScenario(t, config, session)
	})
}

func basicWarehouseCycle(t *testing.T, config cfg, session *gocql.Session) {
	product := "SKU-BASIC-" + shortID()
	orderID := "ORD-" + shortID()
	ts := time.Now().UTC()

	postEvent(t, config.ProducerURL+"/events", received(product, "ZONE-A", 100, ts, nil, 2))
	waitInventory(t, session, product, "ZONE-A", 100, 0, 2*time.Minute)

	postEvent(t, config.ProducerURL+"/events", productEvent("PRODUCT_RESERVED", product, "ZONE-A", 30, ts.Add(time.Minute)))
	waitInventory(t, session, product, "ZONE-A", 70, 30, 2*time.Minute)

	postEvent(t, config.ProducerURL+"/events", event{
		EventID:      uuid.NewString(),
		EventType:    "PRODUCT_MOVED",
		EventVersion: 2,
		Timestamp:    ts.Add(2 * time.Minute).Format(time.RFC3339Nano),
		ProductID:    product,
		FromZoneID:   "ZONE-A",
		ToZoneID:     "ZONE-B",
		Quantity:     20,
	})
	waitInventory(t, session, product, "ZONE-A", 50, 30, 2*time.Minute)
	waitInventory(t, session, product, "ZONE-B", 20, 0, 2*time.Minute)

	postEvent(t, config.ProducerURL+"/events", productEvent("PRODUCT_SHIPPED", product, "ZONE-A", 10, ts.Add(3*time.Minute)))
	waitInventory(t, session, product, "ZONE-A", 40, 30, 2*time.Minute)

	postEvent(t, config.ProducerURL+"/events", event{
		EventID:      uuid.NewString(),
		EventType:    "ORDER_CREATED",
		EventVersion: 2,
		Timestamp:    ts.Add(4 * time.Minute).Format(time.RFC3339Nano),
		OrderID:      orderID,
		Items:        []orderItem{{ProductID: product, ZoneID: "ZONE-A", Quantity: 15}},
	})
	waitInventory(t, session, product, "ZONE-A", 25, 45, 2*time.Minute)

	postEvent(t, config.ProducerURL+"/events", event{
		EventID:      uuid.NewString(),
		EventType:    "ORDER_COMPLETED",
		EventVersion: 2,
		Timestamp:    ts.Add(5 * time.Minute).Format(time.RFC3339Nano),
		OrderID:      orderID,
	})
	waitInventory(t, session, product, "ZONE-A", 25, 30, 2*time.Minute)
	waitTotal(t, session, product, 45, 30, 2*time.Minute)
}

func idempotencyScenario(t *testing.T, config cfg, session *gocql.Session) {
	product := "SKU-DUP-" + shortID()
	ev := received(product, "ZONE-A", 50, time.Now().UTC(), nil, 2)
	postEvent(t, config.ProducerURL+"/events", ev)
	waitInventory(t, session, product, "ZONE-A", 50, 0, 2*time.Minute)
	postEvent(t, config.ProducerURL+"/events", ev)
	time.Sleep(3 * time.Second)
	assertInventory(t, session, product, "ZONE-A", 50, 0)
}

func consistencyScenario(t *testing.T, config cfg, session *gocql.Session) {
	product := "SKU-CONSISTENT-" + shortID()
	postEvent(t, config.ProducerURL+"/events", received(product, "ZONE-A", 100, time.Now().UTC(), nil, 2))
	waitInventory(t, session, product, "ZONE-A", 100, 0, 2*time.Minute)
	waitTotal(t, session, product, 100, 0, 2*time.Minute)
	waitZone(t, session, "ZONE-A", product, 100, 0, 2*time.Minute)
}

func outOfOrderScenario(t *testing.T, config cfg, session *gocql.Session) {
	product := "SKU-ORDER-" + shortID()
	base := time.Now().UTC().Add(30 * time.Minute)
	postEvent(t, config.ProducerURL+"/events", received(product, "ZONE-A", 100, base, nil, 2))
	waitInventory(t, session, product, "ZONE-A", 100, 0, 2*time.Minute)
	postEvent(t, config.ProducerURL+"/events", productEvent("PRODUCT_SHIPPED", product, "ZONE-A", 20, base.Add(5*time.Minute)))
	waitInventory(t, session, product, "ZONE-A", 80, 0, 2*time.Minute)

	stale := received(product, "ZONE-A", 50, base.Add(2*time.Minute), nil, 2)
	postEvent(t, config.ProducerURL+"/events", stale)
	waitProcessedStatus(t, session, stale.EventID, "STALE", 2*time.Minute)
	assertInventory(t, session, product, "ZONE-A", 80, 0)
}

func dlqScenario(t *testing.T, config cfg, session *gocql.Session) {
	product := "SKU-DLQ-" + shortID()
	bad := productEvent("PRODUCT_SHIPPED", product, "ZONE-A", -5, time.Now().UTC())
	postEvent(t, config.ProducerURL+"/events/unsafe", bad)
	waitDLQ(t, config, bad.EventID, 2*time.Minute)

	postEvent(t, config.ProducerURL+"/events", received(product, "ZONE-A", 5, time.Now().UTC().Add(time.Minute), nil, 2))
	waitInventory(t, session, product, "ZONE-A", 5, 0, 2*time.Minute)
}

func monitoringScenario(t *testing.T, config cfg) {
	waitHTTP(t, config.ConsumerURL+"/health", time.Minute)
	body := httpGet(t, config.ConsumerURL+"/metrics")
	for _, metric := range []string{"consumer_lag", "events_processed_total", "event_processing_duration_seconds", "cassandra_write_errors_total"} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics output missing %s", metric)
		}
	}
}

func schemaEvolutionScenario(t *testing.T, config cfg, session *gocql.Session) {
	v1Product := "SKU-V1-" + shortID()
	postEvent(t, config.ProducerURL+"/events", received(v1Product, "ZONE-A", 10, time.Now().UTC(), nil, 1))
	waitInventory(t, session, v1Product, "ZONE-A", 10, 0, 2*time.Minute)
	if supplier := supplierID(t, session, v1Product, "ZONE-A"); supplier != "" {
		t.Fatalf("V1 supplier_id = %q, want empty/null", supplier)
	}

	v2Product := "SKU-V2-" + shortID()
	supplier := "SUP-001"
	postEvent(t, config.ProducerURL+"/events", received(v2Product, "ZONE-A", 10, time.Now().UTC().Add(time.Minute), &supplier, 2))
	waitInventory(t, session, v2Product, "ZONE-A", 10, 0, 2*time.Minute)
	if got := supplierID(t, session, v2Product, "ZONE-A"); got != supplier {
		t.Fatalf("V2 supplier_id = %q, want %q", got, supplier)
	}

	body := httpGet(t, config.SchemaRegistryURL+"/subjects/warehouse-events-value/versions")
	if !strings.Contains(body, "1") || !strings.Contains(body, "2") {
		t.Fatalf("expected Schema Registry versions 1 and 2, got %s", body)
	}
}

func received(product, zone string, quantity int, ts time.Time, supplier *string, version int) event {
	return event{
		EventID:      uuid.NewString(),
		EventType:    "PRODUCT_RECEIVED",
		EventVersion: version,
		Timestamp:    ts.UTC().Format(time.RFC3339Nano),
		ProductID:    product,
		ZoneID:       zone,
		Quantity:     quantity,
		SupplierID:   supplier,
	}
}

func productEvent(eventType, product, zone string, quantity int, ts time.Time) event {
	return event{
		EventID:      uuid.NewString(),
		EventType:    eventType,
		EventVersion: 2,
		Timestamp:    ts.UTC().Format(time.RFC3339Nano),
		ProductID:    product,
		ZoneID:       zone,
		Quantity:     quantity,
	}
}

func loadConfig() cfg {
	return cfg{
		ProducerURL:       env("PRODUCER_URL", "http://wms-producer:8080"),
		ConsumerURL:       env("CONSUMER_URL", "http://wms-consumer:8081"),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		KafkaBrokers:      split(env("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")),
		DLQTopic:          env("KAFKA_DLQ_TOPIC", "warehouse-events-dlq"),
		CassandraHosts:    split(env("CASSANDRA_HOSTS", "cassandra-1")),
		CassandraKeyspace: env("CASSANDRA_KEYSPACE", "warehouse"),
	}
}

func openCassandra(t *testing.T, config cfg) *gocql.Session {
	t.Helper()
	cluster := gocql.NewCluster(config.CassandraHosts...)
	cluster.Keyspace = config.CassandraKeyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 10 * time.Second
	deadline := time.Now().Add(3 * time.Minute)
	var last error
	for time.Now().Before(deadline) {
		session, err := cluster.CreateSession()
		if err == nil {
			return session
		}
		last = err
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("connect Cassandra: %v", last)
	return nil
}

func postEvent(t *testing.T, url string, ev event) {
	t.Helper()
	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	resp, respBody := postJSON(t, url, body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("POST %s returned %s: %s", url, resp.Status, strings.TrimSpace(string(respBody)))
	}
}

func postJSON(t *testing.T, url string, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, respBody
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
			last = resp.Status
		} else if err != nil {
			last = err.Error()
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timeout waiting for %s: %s", url, last)
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("GET %s returned %s: %s", url, resp.Status, strings.TrimSpace(string(body)))
	}
	return string(body)
}

func waitInventory(t *testing.T, session *gocql.Session, product, zone string, available, reserved int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		gotAvailable, gotReserved, err := loadInventory(session, product, zone)
		if err == nil && gotAvailable == available && gotReserved == reserved {
			return
		}
		if err != nil {
			last = err.Error()
		} else {
			last = fmt.Sprintf("available=%d reserved=%d", gotAvailable, gotReserved)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timeout waiting inventory %s/%s available=%d reserved=%d, last=%s", product, zone, available, reserved, last)
}

func assertInventory(t *testing.T, session *gocql.Session, product, zone string, available, reserved int) {
	t.Helper()
	gotAvailable, gotReserved, err := loadInventory(session, product, zone)
	if err != nil {
		t.Fatal(err)
	}
	if gotAvailable != available || gotReserved != reserved {
		t.Fatalf("inventory %s/%s = available=%d reserved=%d, want available=%d reserved=%d", product, zone, gotAvailable, gotReserved, available, reserved)
	}
}

func loadInventory(session *gocql.Session, product, zone string) (int, int, error) {
	var available, reserved int
	err := session.Query(
		"SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?",
		product, zone,
	).Scan(&available, &reserved)
	return available, reserved, err
}

func waitTotal(t *testing.T, session *gocql.Session, product string, available, reserved int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var gotAvailable, gotReserved int
		err := session.Query("SELECT total_available, total_reserved FROM inventory_by_product WHERE product_id = ?", product).Scan(&gotAvailable, &gotReserved)
		if err == nil && gotAvailable == available && gotReserved == reserved {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timeout waiting total %s available=%d reserved=%d", product, available, reserved)
}

func waitZone(t *testing.T, session *gocql.Session, zone, product string, available, reserved int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var gotAvailable, gotReserved int
		err := session.Query("SELECT available_quantity, reserved_quantity FROM inventory_by_zone WHERE zone_id = ? AND product_id = ?", zone, product).Scan(&gotAvailable, &gotReserved)
		if err == nil && gotAvailable == available && gotReserved == reserved {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timeout waiting zone %s product %s", zone, product)
}

func waitProcessedStatus(t *testing.T, session *gocql.Session, eventID, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var got string
		err := session.Query("SELECT status FROM processed_events WHERE event_id = ?", eventID).Scan(&got)
		if err == nil && got == status {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timeout waiting processed_events status %s for %s", status, eventID)
}

func supplierID(t *testing.T, session *gocql.Session, product, zone string) string {
	t.Helper()
	var supplier *string
	err := session.Query("SELECT supplier_id FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?", product, zone).Scan(&supplier)
	if err != nil && err != gocql.ErrNotFound {
		t.Fatal(err)
	}
	if supplier == nil {
		return ""
	}
	return *supplier
}

func waitDLQ(t *testing.T, config cfg, eventID string, timeout time.Duration) {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.KafkaBrokers,
		Topic:       config.DLQTopic,
		GroupID:     "warehouse-e2e-" + shortID(),
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("timeout waiting DLQ event %s: %v", eventID, err)
		}
		if strings.Contains(string(msg.Value), eventID) {
			_ = reader.CommitMessages(context.Background(), msg)
			return
		}
		_ = reader.CommitMessages(context.Background(), msg)
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func shortID() string {
	return strings.Split(uuid.NewString(), "-")[0]
}
