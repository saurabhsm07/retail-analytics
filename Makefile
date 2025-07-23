ifndef MODULE_NAME
MODULE_NAME = ./src/examples/sample.py
endif

.PHONY: help up down submit-script-master-example submit-script-client-example
help:
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-30s %s\n", $$1, $$2}'


.PHONY: build
build: ## Build Docker images
	docker compose build

.PHONY: build-package
build-package: ## Build Python package
	pipenv run python -m build
	python setup.py bdist_egg
	
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

.PHONY: start-yarn-session
start-yarn-session: ## Start YARN ResourceManager session
	@echo "Starting YARN ResourceManager session..."
	docker exec -it resourcemanager /bin/bash

.PHONY: start-namenode-session
start-namenode-session: ## Start Hadoop NameNode session
	@echo "Starting Hadoop NameNode session..."
	docker exec -it namenode /bin/bash

# YARN-based Spark Submit Commands
.PHONY: init-hdfs
init-hdfs: ## Initialize HDFS directories and upload files
	@echo "Creating HDFS directories..."
	docker exec namenode hdfs dfs -mkdir -p /user/root
	docker exec namenode hdfs dfs -mkdir -p /spark-logs
	docker exec namenode hdfs dfs -mkdir -p /app-logs
	docker exec namenode hdfs dfs -mkdir -p /shared/dist
	@echo "Uploading files to HDFS..."
	docker exec namenode hdfs dfs -put -f /opt/shared/dist/* /shared/dist/ 2>/dev/null || echo "No dist files to upload"
	docker exec namenode hdfs dfs -put -f /opt/shared/requirements.txt /shared/ 2>/dev/null || echo "No requirements.txt to upload"
	@echo "HDFS initialization complete"

.PHONY: spark-submit-yarn
spark-submit-yarn: ## Submit Spark job to YARN cluster
	@echo "Submitting Spark job to YARN..."
	docker exec spark-client /opt/spark/bin/spark-submit \
									--master yarn \
									--deploy-mode cluster \
									--py-files hdfs://namenode:9000/shared/dist/retail_analytics-0.1.0-py3.12.egg \
									--archives hdfs://namenode:9000/shared/dist/venv.tar.gz#venv \
									--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python \
									--conf spark.yarn.executorEnv.PYSPARK_PYTHON=./venv/bin/python \
									hdfs://namenode:9000/shared/${MODULE_NAME}

.PHONY: spark-submit-yarn-local
spark-submit-yarn-local: ## Submit Spark job to YARN with local files
	@echo "Submitting Spark job to YARN with local files..."
	docker exec spark-client /opt/spark/bin/spark-submit \
									--master yarn \
									--deploy-mode client \
									--py-files /opt/shared/dist/retail_analytics-0.1.0-py3.12.egg \
									--archives /opt/shared/dist/venv.tar.gz#venv \
									--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python \
									--conf spark.yarn.executorEnv.PYSPARK_PYTHON=./venv/bin/python \
									/opt/shared/${MODULE_NAME}

# Utility Commands for YARN/Hadoop
.PHONY: check-hdfs
check-hdfs: ## Check HDFS status and files
	@echo "HDFS Status:"
	docker exec namenode hdfs dfs -ls /
	@echo "\nShared files:"
	docker exec namenode hdfs dfs -ls /shared/ 2>/dev/null || echo "No shared directory"

.PHONY: check-yarn
check-yarn: ## Check YARN cluster status
	@echo "YARN Node Status:"
	docker exec resourcemanager yarn node -list
	@echo "\nYARN Application Status:"
	docker exec resourcemanager yarn application -list

.PHONY: yarn-logs
yarn-logs: ## Get YARN application logs (requires APPLICATION_ID)
	@echo "Getting logs for application: $(APPLICATION_ID)"
	docker exec resourcemanager yarn logs -applicationId $(APPLICATION_ID)

.PHONY: build-and-upload
build-and-upload: ## Build package and upload to HDFS
	make build-package
	make init-hdfs
	@echo "Package built and uploaded to HDFS"

.PHONY: spark-run-client-example
spark-run-client-example: ## Submit example script to Spark client
	@echo "Submitting example script to Spark client..."
	docker exec spark-client python3 ./examples/sample.py

.PHONY: yarn-ui
yarn-ui: ## Open YARN Resource Manager UI
	@echo "YARN Resource Manager UI: http://localhost:8088"
	@echo "NameNode UI: http://localhost:9870"
	@echo "History Server UI: http://localhost:8188"

.PHONY: sql-shell
sql-shell: ## Start an SQL shell in spark client
	docker exec -it spark-client /opt/spark/bin/spark-sql --master yarn --deploy-mode client
