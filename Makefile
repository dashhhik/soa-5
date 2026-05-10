.PHONY: build-up test down config cql nodetool dlq topics

COMPOSE ?= docker compose

build-up:
	DOCKER_BUILDKIT=0 COMPOSE_BAKE=false $(COMPOSE) up --build

test:
	DOCKER_BUILDKIT=0 COMPOSE_BAKE=false $(COMPOSE) --profile test up --build --abort-on-container-exit --exit-code-from tests tests

down:
	$(COMPOSE) down --remove-orphans

config:
	$(COMPOSE) config

cql:
	docker exec -it cassandra-1 cqlsh

nodetool:
	docker exec cassandra-1 nodetool status

topics:
	$(COMPOSE) exec kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --list

dlq:
	$(COMPOSE) exec kafka-1 kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic warehouse-events-dlq --from-beginning --timeout-ms 10000
