# UCM TFM Lakehouse Project Makefile

# Variables
DOCKER_IMAGE_NAME = ucm-tfm-lakehouse
DOCKER_TAG = latest
DOCKERFILE_PATH = Dockerfile
DOCKER_CONTEXT = .
TEST_PATHS = tests/ test/

# Default target
.DEFAULT_GOAL := help

# Help target
.PHONY: help
help: ## Show this help message
	@echo "UCM TFM Lakehouse Project"
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Test command
.PHONY: test
test: ## Run all tests using pytest
	@echo "Running tests..."
	pytest $(TEST_PATHS) -v --tb=short

# Test with coverage
.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	pytest $(TEST_PATHS) -v --tb=short --cov=src --cov-report=html --cov-report=term

# Build Docker image
.PHONY: build
build: ## Build Docker image with latest tag (default: telegram bot)
	@echo "Building Docker image: $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)"
	docker build -f $(DOCKERFILE_PATH) -t $(DOCKER_IMAGE_NAME):$(DOCKER_TAG) $(DOCKER_CONTEXT)
	@echo "Docker image built successfully: $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)"

# Build with specific tag
.PHONY: build-tag
build-tag: ## Build Docker image with custom tag (usage: make build-tag TAG=your-tag)
	@if [ -z "$(TAG)" ]; then echo "Error: TAG is required. Usage: make build-tag TAG=your-tag"; exit 1; fi
	@echo "Building Docker image: $(DOCKER_IMAGE_NAME):$(TAG)"
	docker build -f $(DOCKERFILE_PATH) -t $(DOCKER_IMAGE_NAME):$(TAG) $(DOCKER_CONTEXT)
	@echo "Docker image built successfully: $(DOCKER_IMAGE_NAME):$(TAG)"

# Clean up
.PHONY: clean
clean: ## Clean up Python cache files and pytest cache
	@echo "Cleaning up..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup completed"

# Install dependencies
.PHONY: install
install: ## Install project dependencies using uv
	@echo "Installing dependencies..."
	uv sync

# Run the bot locally
.PHONY: run-bot
run-bot: ## Run the Telegram bot locally
	@echo "Starting Telegram bot..."
	uv run -m src.databricks_telegram_bot.main

# Docker operations
.PHONY: docker-run
docker-run: ## Run the Docker container (default: telegram bot)
	@echo "Running Docker container with telegram bot..."
	docker run --rm -it \
		-e PYTHONPATH=$$PYTHONPATH \
		-e DATABRICKS_WORKSPACE_URL=$$DATABRICKS_WORKSPACE_URL \
		-e DATABRICKS_ACCESS_TOKEN=$$DATABRICKS_ACCESS_TOKEN \
		-e DATABRICKS_CATALOG=$$DATABRICKS_CATALOG \
		-e DATABRICKS_SCHEMA=$$DATABRICKS_SCHEMA \
		-e TELEGRAM_BOT_TOKEN=$$TELEGRAM_BOT_TOKEN \
		-e TELEGRAM_ALLOWED_USERS=$$TELEGRAM_ALLOWED_USERS \
		-e LOG_LEVEL=$$LOG_LEVEL \
		-e MAX_QUERY_TIMEOUT=$$MAX_QUERY_TIMEOUT \
		$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)

.PHONY: docker-run-module
docker-run-module: ## Run Docker container with custom module (usage: make docker-run-module MODULE=your.module)
	@if [ -z "$(MODULE)" ]; then echo "Error: MODULE is required. Usage: make docker-run-module MODULE=your.module"; exit 1; fi
	@echo "Running Docker container with module: $(MODULE)"
	docker run --rm -it \
		-e PYTHONPATH=$$PYTHONPATH \
		-e DATABRICKS_WORKSPACE_URL=$$DATABRICKS_WORKSPACE_URL \
		-e DATABRICKS_ACCESS_TOKEN=$$DATABRICKS_ACCESS_TOKEN \
		-e DATABRICKS_CATALOG=$$DATABRICKS_CATALOG \
		-e DATABRICKS_SCHEMA=$$DATABRICKS_SCHEMA \
		-e TELEGRAM_BOT_TOKEN=$$TELEGRAM_BOT_TOKEN \
		-e TELEGRAM_ALLOWED_USERS=$$TELEGRAM_ALLOWED_USERS \
		-e LOG_LEVEL=$$LOG_LEVEL \
		-e MAX_QUERY_TIMEOUT=$$MAX_QUERY_TIMEOUT \
		-e ENTRYPOINT_MODULE=$(MODULE) \
		$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)

.PHONY: docker-run-bot
docker-run-bot: ## Run the Docker container with telegram bot
	@echo "Running Docker container with telegram bot..."
	docker run --rm -it \
		-e PYTHONPATH=$$PYTHONPATH \
		-e DATABRICKS_WORKSPACE_URL=$$DATABRICKS_WORKSPACE_URL \
		-e DATABRICKS_ACCESS_TOKEN=$$DATABRICKS_ACCESS_TOKEN \
		-e DATABRICKS_CATALOG=$$DATABRICKS_CATALOG \
		-e DATABRICKS_SCHEMA=$$DATABRICKS_SCHEMA \
		-e TELEGRAM_BOT_TOKEN=$$TELEGRAM_BOT_TOKEN \
		-e TELEGRAM_ALLOWED_USERS=$$TELEGRAM_ALLOWED_USERS \
		-e LOG_LEVEL=$$LOG_LEVEL \
		-e MAX_QUERY_TIMEOUT=$$MAX_QUERY_TIMEOUT \
		-e ENTRYPOINT_MODULE=src.databricks_telegram_bot.main \
		$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)