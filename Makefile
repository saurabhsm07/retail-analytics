.PHONY: help up down submit-script-master-example submit-script-client-example
help:
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-30s %s\n", $$1, $$2}'

.PHONY: up
up: ## Start all containers
	docker compose up -d

.PHONY: down
down: ## Stop all containers
	docker compose down

.PHONY: start-client-session
start-client-session: ## Start Spark client session
	@echo "Starting Spark client session..."
	docker exec -it spark-client /bin/bash

.PHONY: spark-submit-master-example
spark-submit-master-example: ## Submit example script to Spark master
	@echo "Submitting example script to Spark master..."
	docker-compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077  ./examples/sample.py

.PHONY: spark-run-client-example
spark-run-client-example: ## Submit example script to Spark client
	@echo "Submitting example script to Spark client..."
	docker exec spark-client python3 ./examples/sample.py
