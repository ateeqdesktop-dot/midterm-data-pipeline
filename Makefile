PYTHON ?= python3
export PYTHONPATH := src

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest -q

lint:
	ruff check src tests

format-check:
	ruff format --check src tests
	$(PYTHON) -m compileall -q src tests

sample:
	$(PYTHON) src/create_small_sample.py --input data/sample_orders.csv --output data/generated_sample.csv --rows 5

run:
	$(PYTHON) src/main.py --input data/sample_orders.csv --backend memory --reports reports/results.json --check-idempotency

clean:
	rm -f data/generated_sample.csv reports/results.json
