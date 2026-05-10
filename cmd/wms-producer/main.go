package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"smart-warehouse/internal/warehouse"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := warehouse.NewProducerApp(ctx, warehouse.ProducerConfig{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		KafkaBrokers:      splitAndTrim(env("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:        env("KAFKA_TOPIC", "warehouse-events"),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
	}, logger)
	if err != nil {
		logger.Error("failed to initialize producer", "error", err)
		os.Exit(1)
	}
	if err := app.Run(ctx); err != nil {
		logger.Error("producer stopped", "error", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
