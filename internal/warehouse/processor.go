package warehouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type ProcessStatus string

const (
	ProcessApplied   ProcessStatus = "APPLIED"
	ProcessDuplicate ProcessStatus = "DUPLICATE"
	ProcessStale     ProcessStatus = "STALE"
)

type ProcessResult struct {
	Status ProcessStatus
	Reason string
}

type Processor struct {
	repo *Repository
}

func NewProcessor(repo *Repository) *Processor {
	return &Processor{repo: repo}
}

func (p *Processor) Process(ctx context.Context, event Event) (ProcessResult, error) {
	event = event.WithDefaults()
	if err := event.Validate(); err != nil {
		return ProcessResult{}, err
	}
	processed, err := p.repo.IsProcessed(ctx, event.EventID)
	if err != nil {
		return ProcessResult{}, err
	}
	if processed {
		return ProcessResult{Status: ProcessDuplicate, Reason: "event_id already processed"}, nil
	}
	stale, reason, err := p.isStale(ctx, event)
	if err != nil {
		return ProcessResult{}, err
	}
	if stale {
		batch := p.repo.NewBatch(ctx)
		p.repo.MarkProcessed(batch, event, string(ProcessStale), reason)
		p.repo.AddHistory(batch, event, string(ProcessStale))
		if err := p.repo.ExecuteBatch(batch); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Status: ProcessStale, Reason: reason}, nil
	}

	batch := p.repo.NewBatch(ctx)
	switch event.EventType {
	case EventProductReceived:
		if err := p.applyReceived(ctx, batch, event); err != nil {
			return ProcessResult{}, err
		}
	case EventProductShipped:
		if err := p.applyAvailableReserved(ctx, batch, event, -event.Quantity, 0); err != nil {
			return ProcessResult{}, err
		}
	case EventProductReserved:
		if err := p.applyAvailableReserved(ctx, batch, event, -event.Quantity, event.Quantity); err != nil {
			return ProcessResult{}, err
		}
	case EventProductReleased:
		if err := p.applyRelease(ctx, batch, event); err != nil {
			return ProcessResult{}, err
		}
	case EventInventoryCounted:
		if err := p.applyCounted(ctx, batch, event); err != nil {
			return ProcessResult{}, err
		}
	case EventProductMoved:
		if err := p.applyMoved(ctx, batch, event); err != nil {
			return ProcessResult{}, err
		}
	case EventOrderCreated:
		if err := p.applyOrderCreated(ctx, batch, event); err != nil {
			return ProcessResult{}, err
		}
	case EventOrderCompleted:
		if err := p.applyOrderCompleted(ctx, batch, event); err != nil {
			return ProcessResult{}, err
		}
	default:
		return ProcessResult{}, validation("VALIDATION_ERROR", fmt.Sprintf("unsupported event_type %q", event.EventType))
	}

	p.repo.MarkProcessed(batch, event, string(ProcessApplied), "")
	p.repo.AddHistory(batch, event, string(ProcessApplied))
	for _, entityID := range touchedEntities(event) {
		p.repo.AddVersion(batch, entityID, event)
	}
	if err := p.repo.ExecuteBatch(batch); err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Status: ProcessApplied}, nil
}

func (p *Processor) isStale(ctx context.Context, event Event) (bool, string, error) {
	eventTime := event.Timestamp.Time()
	for _, entityID := range touchedEntities(event) {
		last, ok, err := p.repo.LastEntityTimestamp(ctx, entityID)
		if err != nil {
			return false, "", err
		}
		if ok && eventTime.Before(last) {
			return true, fmt.Sprintf("event timestamp %s is older than %s for %s", eventTime.Format(time.RFC3339), last.Format(time.RFC3339), entityID), nil
		}
	}
	return false, "", nil
}

func (p *Processor) applyReceived(ctx context.Context, batch *gocql.Batch, event Event) error {
	inv, total, err := p.loadInventoryAndTotal(ctx, event.ProductID, event.ZoneID)
	if err != nil {
		return err
	}
	inv.Available += event.Quantity
	if event.SupplierID != nil {
		inv.SupplierID = event.SupplierID
	}
	total.Available += event.Quantity
	p.writeInventoryAndTotal(batch, inv, total, event)
	return nil
}

func (p *Processor) applyAvailableReserved(ctx context.Context, batch *gocql.Batch, event Event, availableDelta, reservedDelta int) error {
	inv, total, err := p.loadInventoryAndTotal(ctx, event.ProductID, event.ZoneID)
	if err != nil {
		return err
	}
	if availableDelta < 0 && inv.Available < -availableDelta {
		return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("insufficient available quantity for %s in %s: have %d need %d", event.ProductID, event.ZoneID, inv.Available, -availableDelta))
	}
	inv.Available += availableDelta
	inv.Reserved += reservedDelta
	total.Available += availableDelta
	total.Reserved += reservedDelta
	p.writeInventoryAndTotal(batch, inv, total, event)
	return nil
}

func (p *Processor) applyRelease(ctx context.Context, batch *gocql.Batch, event Event) error {
	inv, total, err := p.loadInventoryAndTotal(ctx, event.ProductID, event.ZoneID)
	if err != nil {
		return err
	}
	if inv.Reserved < event.Quantity {
		return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("insufficient reserved quantity for %s in %s: have %d need %d", event.ProductID, event.ZoneID, inv.Reserved, event.Quantity))
	}
	inv.Reserved -= event.Quantity
	inv.Available += event.Quantity
	total.Reserved -= event.Quantity
	total.Available += event.Quantity
	p.writeInventoryAndTotal(batch, inv, total, event)
	return nil
}

func (p *Processor) applyCounted(ctx context.Context, batch *gocql.Batch, event Event) error {
	inv, total, err := p.loadInventoryAndTotal(ctx, event.ProductID, event.ZoneID)
	if err != nil {
		return err
	}
	delta := event.Quantity - inv.Available
	inv.Available = event.Quantity
	total.Available += delta
	p.writeInventoryAndTotal(batch, inv, total, event)
	return nil
}

func (p *Processor) applyMoved(ctx context.Context, batch *gocql.Batch, event Event) error {
	from, total, err := p.loadInventoryAndTotal(ctx, event.ProductID, event.FromZoneID)
	if err != nil {
		return err
	}
	if from.Available < event.Quantity {
		return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("insufficient available quantity for move: have %d need %d", from.Available, event.Quantity))
	}
	to, err := p.repo.LoadInventory(ctx, event.ProductID, event.ToZoneID)
	if err != nil {
		return err
	}
	from.Available -= event.Quantity
	to.Available += event.Quantity
	if to.SupplierID == nil {
		to.SupplierID = from.SupplierID
	}
	p.writeInventory(batch, from, event)
	p.writeInventory(batch, to, event)
	p.repo.AddProductTotal(batch, total, event.Timestamp.Time(), event.EventID)
	return nil
}

func (p *Processor) applyOrderCreated(ctx context.Context, batch *gocql.Batch, event Event) error {
	order, err := p.repo.LoadOrder(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if order.Status != "" {
		return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("order %s already exists", event.OrderID))
	}
	inventories := map[string]Inventory{}
	totals := map[string]ProductTotal{}
	for _, item := range event.Items {
		key := inventoryKey(item.ProductID, item.ZoneID)
		inv, ok := inventories[key]
		if !ok {
			loaded, err := p.repo.LoadInventory(ctx, item.ProductID, item.ZoneID)
			if err != nil {
				return err
			}
			inv = loaded
		}
		if inv.Available < item.Quantity {
			return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("insufficient available quantity for order %s item %s/%s: have %d need %d", event.OrderID, item.ProductID, item.ZoneID, inv.Available, item.Quantity))
		}
		total, ok := totals[item.ProductID]
		if !ok {
			loaded, err := p.repo.LoadProductTotal(ctx, item.ProductID)
			if err != nil {
				return err
			}
			total = loaded
		}
		inv.Available -= item.Quantity
		inv.Reserved += item.Quantity
		total.Available -= item.Quantity
		total.Reserved += item.Quantity
		inventories[key] = inv
		totals[item.ProductID] = total
		p.repo.AddOrderItem(batch, event.OrderID, item, event)
	}
	for _, inv := range inventories {
		p.writeInventory(batch, inv, event)
	}
	for _, total := range totals {
		p.repo.AddProductTotal(batch, total, event.Timestamp.Time(), event.EventID)
	}
	p.repo.AddOrder(batch, event.OrderID, "CREATED", event)
	return nil
}

func (p *Processor) applyOrderCompleted(ctx context.Context, batch *gocql.Batch, event Event) error {
	order, err := p.repo.LoadOrder(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if order.Status == "" {
		return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("order %s does not exist", event.OrderID))
	}
	if order.Status == "COMPLETED" {
		p.repo.AddOrderStatus(batch, event.OrderID, "COMPLETED", event)
		return nil
	}
	items, err := p.repo.LoadOrderItems(ctx, event.OrderID)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("order %s has no items", event.OrderID))
	}
	inventories := map[string]Inventory{}
	totals := map[string]ProductTotal{}
	for _, item := range items {
		key := inventoryKey(item.ProductID, item.ZoneID)
		inv, ok := inventories[key]
		if !ok {
			loaded, err := p.repo.LoadInventory(ctx, item.ProductID, item.ZoneID)
			if err != nil {
				return err
			}
			inv = loaded
		}
		if inv.Reserved < item.Quantity {
			return validation("BUSINESS_RULE_ERROR", fmt.Sprintf("insufficient reserved quantity for order %s item %s/%s: have %d need %d", event.OrderID, item.ProductID, item.ZoneID, inv.Reserved, item.Quantity))
		}
		total, ok := totals[item.ProductID]
		if !ok {
			loaded, err := p.repo.LoadProductTotal(ctx, item.ProductID)
			if err != nil {
				return err
			}
			total = loaded
		}
		inv.Reserved -= item.Quantity
		total.Reserved -= item.Quantity
		inventories[key] = inv
		totals[item.ProductID] = total
	}
	for _, inv := range inventories {
		p.writeInventory(batch, inv, event)
	}
	for _, total := range totals {
		p.repo.AddProductTotal(batch, total, event.Timestamp.Time(), event.EventID)
	}
	p.repo.AddOrderStatus(batch, event.OrderID, "COMPLETED", event)
	return nil
}

func (p *Processor) loadInventoryAndTotal(ctx context.Context, productID, zoneID string) (Inventory, ProductTotal, error) {
	inv, err := p.repo.LoadInventory(ctx, productID, zoneID)
	if err != nil {
		return Inventory{}, ProductTotal{}, err
	}
	total, err := p.repo.LoadProductTotal(ctx, productID)
	if err != nil {
		return Inventory{}, ProductTotal{}, err
	}
	return inv, total, nil
}

func (p *Processor) writeInventoryAndTotal(batch *gocql.Batch, inv Inventory, total ProductTotal, event Event) {
	p.writeInventory(batch, inv, event)
	p.repo.AddProductTotal(batch, total, event.Timestamp.Time(), event.EventID)
}

func (p *Processor) writeInventory(batch *gocql.Batch, inv Inventory, event Event) {
	inv.UpdatedAt = event.Timestamp.Time()
	inv.LastEventID = event.EventID
	p.repo.AddInventory(batch, inv)
}

func touchedEntities(event Event) []string {
	seen := map[string]struct{}{}
	var entities []string
	add := func(entityID string) {
		if strings.TrimSpace(entityID) == "" {
			return
		}
		if _, ok := seen[entityID]; ok {
			return
		}
		seen[entityID] = struct{}{}
		entities = append(entities, entityID)
	}
	switch event.EventType {
	case EventOrderCreated:
		add(orderEntity(event.OrderID))
		for _, item := range event.Items {
			add(productEntity(item.ProductID))
		}
	case EventOrderCompleted:
		add(orderEntity(event.OrderID))
	default:
		add(productEntity(event.ProductID))
	}
	return entities
}

func inventoryKey(productID, zoneID string) string {
	return productID + "|" + zoneID
}
