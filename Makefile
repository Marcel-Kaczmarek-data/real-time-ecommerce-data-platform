# Real-Time E-Commerce Data Platform - developer commands.
# Windows without make: use `./make.ps1 <target>` (same targets).
#
#   make demo            # everything: infra -> data -> pipelines -> dashboard
#   SCALE=full make seed # reference-scale data (100k customers / 10k products)

SHELL := /bin/bash
COMPOSE := docker compose
RUN := $(COMPOSE) run --rm --no-deps platform
SCALE ?= lite
HISTORY_DAYS ?= 120
STREAM_DAYS ?= 7
PROFILES_UP := --profile dashboard --profile monitoring

.PHONY: help setup build up up-all down reset seed stream stream-service batch transform silver gold quality \
        reconcile publish test test-unit test-integration test-e2e lint format typecheck benchmark dashboard \
        screenshots demo status logs airflow

help:  ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## Create .env from the template and build images
	@test -f .env || cp .env.example .env
	$(COMPOSE) build platform

build:  ## Rebuild platform and Airflow images
	$(COMPOSE) build platform
	$(COMPOSE) --profile orchestration build airflow-init

up:  ## Start core services (Kafka, Schema Registry, Postgres, S3, reference API) + dashboard + monitoring
	$(COMPOSE) $(PROFILES_UP) up -d

up-all:  ## Start everything incl. continuous streaming, producer and Airflow
	$(COMPOSE) $(PROFILES_UP) --profile processing --profile orchestration up -d

airflow:  ## Start Airflow (webserver on http://localhost:8088)
	$(COMPOSE) --profile orchestration up -d

down:  ## Stop all services (keeps data volumes)
	$(COMPOSE) --profile '*' down

reset:  ## Stop all services and DELETE all data volumes (asks for confirmation)
	@read -p "This deletes Kafka, Postgres, object-storage and checkpoint volumes. Type 'yes': " ans; \
	  [ "$$ans" = "yes" ] && $(COMPOSE) --profile '*' down -v || echo "aborted"

seed:  ## Generate master data + legacy history, create topics and register schemas
	$(RUN) python -m data_generator.cli --scale $(SCALE) seed --history-days $(HISTORY_DAYS) --stream-days $(STREAM_DAYS)
	$(RUN) python -m ingestion.kafka_producers.admin bootstrap

stream:  ## Publish the event backlog to Kafka and drain it into Bronze (bounded run)
	$(RUN) python -m ingestion.kafka_producers.run catchup --sync-ops-db --metrics-port 0
	$(RUN) python -m pipelines.bronze.stream_kafka_to_bronze --once

stream-service:  ## Run producer + Bronze streaming continuously (profile: processing)
	$(COMPOSE) --profile processing up -d event-producer spark-streaming

batch:  ## Batch ingestion of all sources (PostgreSQL, files, REST)
	$(RUN) python -m ingestion.batch.run --all

silver:  ## Bronze -> Silver
	$(RUN) python -m pipelines.silver.run

gold:  ## Silver -> Gold (dimensions, facts, marts), guarded by the DQ gate
	$(RUN) python -m pipelines.gold.run

quality:  ## Run all data-quality suites (report: data_quality/reports/latest.md)
	$(RUN) python -m data_quality.run --fail-on never

reconcile:  ## Financial reconciliation
	$(RUN) python -m pipelines.gold.reconciliation

publish:  ## Publish Gold to the PostgreSQL analytics schema (Power BI)
	$(RUN) python -m pipelines.gold.publish_serving

transform: silver gold quality reconcile  ## Silver + Gold + DQ + reconciliation

test: test-unit  ## Unit tests incl. Spark tests (in the platform container) with coverage
test-unit:
	$(RUN) python -m pytest tests/unit tests/data_quality -q -p no:cacheprovider \
	  --cov --cov-report=term-missing:skip-covered --cov-report=xml:coverage.xml

test-integration:  ## Integration tests against running services
	$(COMPOSE) run --rm platform python -m pytest tests/integration -m integration -q -p no:cacheprovider

test-e2e:  ## End-to-end test: event -> Kafka -> Bronze -> Silver -> Gold -> metric
	$(COMPOSE) run --rm platform python -m pytest tests/end_to_end -m e2e -q -p no:cacheprovider

lint:  ## Ruff lint + format check + mypy + sqlfluff
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) mypy data_generator ingestion pipelines data_quality serving
	$(RUN) sqlfluff lint data_models/marts --dialect sparksql

format:  ## Auto-format
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

typecheck:
	$(RUN) mypy data_generator ingestion pipelines data_quality serving

benchmark:  ## Load benchmarks (EVENTS=100000|1000000|10000000)
	$(COMPOSE) run --rm platform python -m benchmarks.run --events $${EVENTS:-100000}

dashboard:  ## Start the Streamlit dashboard on http://localhost:8501
	$(COMPOSE) --profile dashboard up -d streamlit

screenshots:  ## Capture dashboard screenshots (needs Playwright + a browser on the host)
	python scripts/capture_screenshots.py --url http://localhost:8501 --out dashboards/screenshots

status:  ## Row counts of every lakehouse table
	$(RUN) python scripts/lakehouse_status.py

logs:
	$(COMPOSE) logs -f --tail=100

demo: setup  ## Full local demo: infra, data, streaming + batch, transformations, dashboard
	$(COMPOSE) up -d
	$(MAKE) seed stream batch transform publish
	$(MAKE) dashboard
	@echo ""
	@echo "Dashboard:  http://localhost:8501"
	@echo "Grafana:    http://localhost:3001   (profile monitoring: make up)"
	@echo "Airflow:    http://localhost:8088   (make airflow)"
