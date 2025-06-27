.PHONY: help up down submit-script-master-example submit-script-client-example
help:
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-30s %s\n", $$1, $$2}'


.PHONY: build
build: ## Build Docker images
	docker compose build

	
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

.PHONY: start-master-session
start-master-session: ## Start Spark master session
	@echo "Starting Spark master session..."
	docker exec -it spark-master /bin/bash

.PHONY: spark-submit-master-example
spark-submit-master-example: ## Submit example script to Spark master
	@echo "Submitting example script to Spark master..."
	docker-compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077  ./examples/sample.py

.PHONY: spark-run-client-example
spark-run-client-example: ## Submit example script to Spark client
	@echo "Submitting example script to Spark client..."
	docker exec spark-client python3 ./examples/sample.py

.PHONY: sql-shell
sql-shell: ## Start an SQL shell in master container
	docker exec -it spark-master /opt/spark/bin/spark-sql
