package producer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type PublisherConfig struct {
	Brokers           []string
	Topic             string
	SchemaRegistryURL string
}

type Publisher struct {
	writer   *kafka.Writer
	registry *SchemaRegistryClient
	logger   *slog.Logger
	topic    string
	schema   string
	schemaMu sync.Mutex
	schemaID int
}

func NewPublisher(cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers must not be empty")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, errors.New("kafka topic must not be empty")
	}
	if strings.TrimSpace(cfg.SchemaRegistryURL) == "" {
		return nil, errors.New("schema registry url must not be empty")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}

	return &Publisher{
		writer:   writer,
		registry: NewSchemaRegistryClient(cfg.SchemaRegistryURL),
		logger:   logger,
		topic:    cfg.Topic,
		schema:   movieEventSchema(),
	}, nil
}

func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func (p *Publisher) Publish(ctx context.Context, event MovieEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}

	return retryWithBackoff(ctx, 5, 100*time.Millisecond, func() error {
		schemaID, err := p.ensureSchemaID(ctx)
		if err != nil {
			return err
		}

		value, err := encodeConfluentMovieEvent(schemaID, event)
		if err != nil {
			return err
		}

		msg := kafka.Message{
			Key:   []byte(event.UserID),
			Value: value,
			Time:  event.Timestamp.Time(),
		}
		if err := p.writer.WriteMessages(ctx, msg); err != nil {
			return err
		}

		p.logger.Info("event published",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"timestamp", event.Timestamp,
			"topic", p.topic,
			"partition_key", event.UserID,
		)
		return nil
	})
}

func (p *Publisher) ensureSchemaID(ctx context.Context) (int, error) {
	p.schemaMu.Lock()
	defer p.schemaMu.Unlock()

	if p.schemaID != 0 {
		return p.schemaID, nil
	}

	id, err := p.registry.Register(ctx, p.topic+"-value", p.schema)
	if err != nil {
		return 0, err
	}
	p.schemaID = id
	return id, nil
}

func retryWithBackoff(ctx context.Context, attempts int, initialDelay time.Duration, fn func() error) error {
	if attempts <= 0 {
		attempts = 1
	}
	delay := initialDelay
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			if attempt == attempts {
				break
			}
			jitter := time.Duration(rand.Int63n(int64(delay / 2)))
			sleep := delay + jitter
			select {
			case <-ctx.Done():
				return fmt.Errorf("publish cancelled: %w", ctx.Err())
			case <-time.After(sleep):
			}
			delay *= 2
			if delay > 2*time.Second {
				delay = 2 * time.Second
			}
			continue
		}
		return nil
	}
	return lastErr
}
