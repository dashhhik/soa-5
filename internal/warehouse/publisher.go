package warehouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type PublisherConfig struct {
	Brokers           []string
	Topic             string
	SchemaRegistryURL string
}

type Publisher struct {
	writer *kafka.Writer
	codec  *AvroCodec
	topic  string
	logger *slog.Logger
}

func NewPublisher(ctx context.Context, cfg PublisherConfig, logger *slog.Logger) (*Publisher, error) {
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
	registry := NewSchemaRegistryClient(cfg.SchemaRegistryURL)
	codec, err := NewAvroCodec(registry, cfg.Topic+"-value")
	if err != nil {
		return nil, err
	}
	if err := retry(ctx, 30, 2*time.Second, codec.EnsureRegistered); err != nil {
		return nil, err
	}
	return &Publisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
		},
		codec:  codec,
		topic:  cfg.Topic,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, event Event) (Event, error) {
	return p.publish(ctx, event, true)
}

func (p *Publisher) PublishUnsafe(ctx context.Context, event Event) (Event, error) {
	return p.publish(ctx, event, false)
}

func (p *Publisher) publish(ctx context.Context, event Event, validate bool) (Event, error) {
	event = event.WithDefaults()
	var (
		value []byte
		err   error
	)
	if validate {
		value, err = p.codec.Encode(event)
	} else {
		value, err = p.codec.EncodeUnchecked(event)
	}
	if err != nil {
		return event, err
	}
	key := event.ProductID
	if key == "" {
		key = event.OrderID
	}
	if key == "" {
		key = event.EventID
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  event.Timestamp.Time(),
	}); err != nil {
		return event, err
	}
	p.logger.Info("warehouse event published",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"event_version", event.EventVersion,
		"topic", p.topic,
		"partition_key", key,
	)
	return event, nil
}

func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func retry(ctx context.Context, attempts int, delay time.Duration, fn func(context.Context) error) error {
	var last error
	for i := 0; i < attempts; i++ {
		if err := fn(ctx); err != nil {
			last = err
			select {
			case <-ctx.Done():
				return fmt.Errorf("retry cancelled: %w", ctx.Err())
			case <-time.After(delay):
			}
			continue
		}
		return nil
	}
	return last
}
