PYTHON ?= python
export PYTHONPATH := src
DATE ?= 2024-06-01
DAYS ?= 1

.PHONY: install test lint run report airflow-up airflow-down

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	pytest -v

lint:
	ruff check .

run:            ## run the pipeline without Airflow: make run DATE=2024-06-01 DAYS=7
	$(PYTHON) -m climate_elt.pipeline --date $(DATE) --days $(DAYS)

report:
	$(PYTHON) -m climate_elt.report

airflow-up:     ## Airflow UI on http://localhost:8080
	mkdir -p data && docker compose up -d

airflow-down:
	docker compose down
