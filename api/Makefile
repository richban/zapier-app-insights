.PHONY: help install dev run test lint format clean

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync

dev: ## Install with dev dependencies
	uv sync --all-extras

run: ## Run development server
	uv run uvicorn zapier_insights.main:app --reload --port 8000

run-prod: ## Run production server
	uv run uvicorn zapier_insights.main:app --host 0.0.0.0 --port 8000

test: ## Run tests
	uv run pytest -v

test-cov: ## Run tests with coverage
	uv run pytest --cov=zapier_insights --cov-report=html --cov-report=term

lint: ## Run linter
	uv run ruff check .

format: ## Format code
	uv run ruff format .

typecheck: ## Run type checker
	uv run mypy src/

check: format lint typecheck test ## Run all checks

clean: ## Clean cache files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf htmlcov/ .coverage
