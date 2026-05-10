package warehouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type Inventory struct {
	ProductID   string
	ZoneID      string
	Available   int
	Reserved    int
	SupplierID  *string
	UpdatedAt   time.Time
	LastEventID string
}

type ProductTotal struct {
	ProductID string
	Available int
	Reserved  int
}

type OrderRecord struct {
	OrderID string
	Status  string
}

type Repository struct {
	session          *gocql.Session
	readConsistency  gocql.Consistency
	writeConsistency gocql.Consistency
}

func NewRepository(hosts []string, keyspace string) (*Repository, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 15 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("dc1"))
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &Repository{
		session:          session,
		readConsistency:  gocql.One,
		writeConsistency: gocql.Quorum,
	}, nil
}

func (r *Repository) Close() {
	if r != nil && r.session != nil {
		r.session.Close()
	}
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.session.Query("SELECT now() FROM system.local").WithContext(ctx).Consistency(gocql.One).Exec()
}

func (r *Repository) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	var existing string
	err := r.session.Query(
		"SELECT event_id FROM processed_events WHERE event_id = ?",
		eventID,
	).WithContext(ctx).Consistency(r.readConsistency).Scan(&existing)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (r *Repository) LoadInventory(ctx context.Context, productID, zoneID string) (Inventory, error) {
	inv := Inventory{ProductID: productID, ZoneID: zoneID}
	err := r.session.Query(
		"SELECT available_quantity, reserved_quantity, supplier_id, updated_at, last_event_id FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?",
		productID, zoneID,
	).WithContext(ctx).Consistency(r.readConsistency).Scan(&inv.Available, &inv.Reserved, &inv.SupplierID, &inv.UpdatedAt, &inv.LastEventID)
	if errors.Is(err, gocql.ErrNotFound) {
		return inv, nil
	}
	return inv, err
}

func (r *Repository) LoadProductTotal(ctx context.Context, productID string) (ProductTotal, error) {
	total := ProductTotal{ProductID: productID}
	err := r.session.Query(
		"SELECT total_available, total_reserved FROM inventory_by_product WHERE product_id = ?",
		productID,
	).WithContext(ctx).Consistency(r.readConsistency).Scan(&total.Available, &total.Reserved)
	if errors.Is(err, gocql.ErrNotFound) {
		return total, nil
	}
	return total, err
}

func (r *Repository) LoadOrder(ctx context.Context, orderID string) (OrderRecord, error) {
	order := OrderRecord{OrderID: orderID}
	err := r.session.Query(
		"SELECT status FROM orders_by_id WHERE order_id = ?",
		orderID,
	).WithContext(ctx).Consistency(r.readConsistency).Scan(&order.Status)
	if errors.Is(err, gocql.ErrNotFound) {
		return order, nil
	}
	return order, err
}

func (r *Repository) LoadOrderItems(ctx context.Context, orderID string) ([]OrderItem, error) {
	iter := r.session.Query(
		"SELECT product_id, zone_id, quantity FROM order_items_by_order WHERE order_id = ?",
		orderID,
	).WithContext(ctx).Consistency(r.readConsistency).Iter()
	var items []OrderItem
	var item OrderItem
	for iter.Scan(&item.ProductID, &item.ZoneID, &item.Quantity) {
		items = append(items, item)
		item = OrderItem{}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return items, nil
}

func (r *Repository) LastEntityTimestamp(ctx context.Context, entityID string) (time.Time, bool, error) {
	var ts time.Time
	err := r.session.Query(
		"SELECT last_event_timestamp FROM entity_versions WHERE entity_id = ?",
		entityID,
	).WithContext(ctx).Consistency(r.readConsistency).Scan(&ts)
	if errors.Is(err, gocql.ErrNotFound) {
		return time.Time{}, false, nil
	}
	return ts, err == nil, err
}

func (r *Repository) NewBatch(ctx context.Context) *gocql.Batch {
	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Cons = r.writeConsistency
	return batch
}

func (r *Repository) ExecuteBatch(batch *gocql.Batch) error {
	return r.session.ExecuteBatch(batch)
}

func (r *Repository) AddInventory(batch *gocql.Batch, inv Inventory) {
	batch.Query(
		`INSERT INTO inventory_by_product_zone
		 (product_id, zone_id, available_quantity, reserved_quantity, supplier_id, updated_at, last_event_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		inv.ProductID, inv.ZoneID, inv.Available, inv.Reserved, inv.SupplierID, inv.UpdatedAt, inv.LastEventID,
	)
	batch.Query(
		`INSERT INTO inventory_by_zone
		 (zone_id, product_id, available_quantity, reserved_quantity, supplier_id, updated_at, last_event_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		inv.ZoneID, inv.ProductID, inv.Available, inv.Reserved, inv.SupplierID, inv.UpdatedAt, inv.LastEventID,
	)
}

func (r *Repository) AddProductTotal(batch *gocql.Batch, total ProductTotal, updatedAt time.Time, lastEventID string) {
	batch.Query(
		`INSERT INTO inventory_by_product
		 (product_id, total_available, total_reserved, updated_at, last_event_id)
		 VALUES (?, ?, ?, ?, ?)`,
		total.ProductID, total.Available, total.Reserved, updatedAt, lastEventID,
	)
}

func (r *Repository) AddOrder(batch *gocql.Batch, orderID, status string, event Event) {
	batch.Query(
		`INSERT INTO orders_by_id (order_id, status, created_at, updated_at, last_event_id)
		 VALUES (?, ?, ?, ?, ?)`,
		orderID, status, event.Timestamp.Time(), event.Timestamp.Time(), event.EventID,
	)
}

func (r *Repository) AddOrderStatus(batch *gocql.Batch, orderID, status string, event Event) {
	batch.Query(
		`UPDATE orders_by_id SET status = ?, updated_at = ?, last_event_id = ? WHERE order_id = ?`,
		status, event.Timestamp.Time(), event.EventID, orderID,
	)
}

func (r *Repository) AddOrderItem(batch *gocql.Batch, orderID string, item OrderItem, event Event) {
	batch.Query(
		`INSERT INTO order_items_by_order
		 (order_id, product_id, zone_id, quantity, created_at, last_event_id)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		orderID, item.ProductID, item.ZoneID, item.Quantity, event.Timestamp.Time(), event.EventID,
	)
}

func (r *Repository) MarkProcessed(batch *gocql.Batch, event Event, status, reason string) {
	batch.Query(
		`INSERT INTO processed_events
		 (event_id, event_type, event_timestamp, processed_at, status, error_reason)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		event.EventID, string(event.EventType), event.Timestamp.Time(), time.Now().UTC(), status, reason,
	)
}

func (r *Repository) AddHistory(batch *gocql.Batch, event Event, status string) {
	productID := event.ProductID
	if productID == "" {
		productID = "ORDER:" + event.OrderID
	}
	batch.Query(
		`INSERT INTO event_history
		 (product_id, event_timestamp, event_id, event_type, status, order_id, zone_id, from_zone_id, to_zone_id, quantity, supplier_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		productID, event.Timestamp.Time(), event.EventID, string(event.EventType), status,
		event.OrderID, event.ZoneID, event.FromZoneID, event.ToZoneID, event.Quantity, event.SupplierID,
	)
}

func (r *Repository) AddVersion(batch *gocql.Batch, entityID string, event Event) {
	batch.Query(
		`INSERT INTO entity_versions (entity_id, last_event_timestamp, last_event_id)
		 VALUES (?, ?, ?)`,
		entityID, event.Timestamp.Time(), event.EventID,
	)
}

func productEntity(productID string) string {
	return fmt.Sprintf("product:%s", productID)
}

func orderEntity(orderID string) string {
	return fmt.Sprintf("order:%s", orderID)
}
