# ponytail: simplest thing that works — no venv management, no auto-deps.
# Add `venv` and `install` targets when Python version drift becomes a problem.

COMPOSE := docker compose -f source/deployment/compose.yaml
PYTHON  := python
PYTHONPATH_SRC := PYTHONPATH=source
CRAWLER := cd source && $(PYTHONPATH_SRC) $(PYTHON) main.py crawler --platform tokopedia

.PHONY: up down crawl smoke test test-all lint clean

# --- infra ---
start:
	@bash start.sh

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

# --- crawl ---
crawl:
	$(CRAWLER) --mode scrape --type search-product --keyword "$(KEYWORD)" --pretty

# --- smoke test (full pipeline: crawl → Kafka → verify) ---
smoke:
	@echo "=== Smoke test: crawl → Kafka ==="
	cd source && $(PYTHONPATH_SRC) $(PYTHON) library/setup_infra.py
	$(CRAWLER) --mode full --type search-product --keyword "$(KEYWORD)" -d kafka -o tokopedia.products.raw --bootstrap-servers localhost:9092
	@echo "=== Smoke test: verify topic ==="
	docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:29092 --topic tokopedia.products.raw --time -1

# --- tests ---
test:
	cd source && $(PYTHONPATH_SRC) $(PYTHON) -m pytest tests/ -v

test-assets:
	cd assets && PYTHONPATH=. $(PYTHON) -m pytest tests/ -v

test-pipeline:
	docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo pytest pipeline/tests/ -v"

test-all: test test-assets test-pipeline

# --- lint ---
lint:
	ruff check source/ pipeline/ assets/

lint-fix:
	ruff check --fix source/ pipeline/ assets/

# --- clean ---
deploy:
	@bash deploy.sh

rollback:
	@bash deploy.sh --rollback

clean:
	$(COMPOSE) down -v
