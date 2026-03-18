.PHONY: ci-check test e2e-test lint

ci-check:
	@bash scripts/ci-check.sh

test:
	python -m pytest tests/ -v

e2e-test:
	python -m pytest tests/test_e2e_golden.py -v

lint:
	python -m pytest tests/ -v --tb=short
