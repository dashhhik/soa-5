package warehouse

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type ConsumerMetrics struct {
	Registry             *prometheus.Registry
	ConsumerLag          *prometheus.GaugeVec
	EventsProcessed      *prometheus.CounterVec
	ProcessingDuration   prometheus.Histogram
	CassandraWriteErrors prometheus.Counter
}

func NewConsumerMetrics() *ConsumerMetrics {
	registry := prometheus.NewRegistry()
	m := &ConsumerMetrics{
		Registry: registry,
		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "consumer_lag",
			Help: "Kafka consumer lag by topic partition.",
		}, []string{"topic", "partition"}),
		EventsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Warehouse events successfully processed by event type.",
		}, []string{"event_type"}),
		ProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "Duration of warehouse event processing.",
			Buckets: prometheus.DefBuckets,
		}),
		CassandraWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cassandra_write_errors_total",
			Help: "Total Cassandra write errors in the warehouse consumer.",
		}),
	}
	registry.MustRegister(m.ConsumerLag, m.EventsProcessed, m.ProcessingDuration, m.CassandraWriteErrors)
	return m
}

func (m *ConsumerMetrics) SetLag(topic string, partition int, value float64) {
	m.ConsumerLag.WithLabelValues(topic, strconv.Itoa(partition)).Set(value)
}
