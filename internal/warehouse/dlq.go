package warehouse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

type DLQPublisher struct {
	writer *kafka.Writer
}

type DLQMessage struct {
	OriginalEvent interface{} `json:"original_event"`
	ErrorReason   string      `json:"error_reason"`
	ErrorCode     string      `json:"error_code"`
	FailedAt      time.Time   `json:"failed_at"`
	KafkaMetadata KafkaMeta   `json:"kafka_metadata"`
}

type KafkaMeta struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

func NewDLQPublisher(brokers []string, topic string) *DLQPublisher {
	return &DLQPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *DLQPublisher) Publish(ctx context.Context, event *Event, raw []byte, reason string, err error, meta KafkaMeta) error {
	code := "PROCESSING_ERROR"
	var validation ValidationError
	if errors.As(err, &validation) {
		code = validation.Code
	}
	var original interface{}
	key := "decode-error"
	if event != nil {
		original = event.JSONMap()
		key = event.EventID
	} else {
		original = map[string]string{"raw_base64": base64.StdEncoding.EncodeToString(raw)}
	}
	payload, marshalErr := json.Marshal(DLQMessage{
		OriginalEvent: original,
		ErrorReason:   reason,
		ErrorCode:     code,
		FailedAt:      time.Now().UTC(),
		KafkaMetadata: meta,
	})
	if marshalErr != nil {
		return marshalErr
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func (p *DLQPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
