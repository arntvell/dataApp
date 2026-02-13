.PHONY: help build up down logs test clean sync init-db

help: ## Show this help message
	@echo "DataApp - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the Docker containers
	docker-compose build

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

logs: ## Show logs from all services
	docker-compose logs -f

test: ## Test the setup
	python scripts/test_setup.py

sync: ## Run data synchronization
	python scripts/run_sync.py

init-db: ## Initialize database tables
	python scripts/init_db.py

clean: ## Clean up containers and volumes
	docker-compose down -v
	docker system prune -f

restart: ## Restart all services
	docker-compose restart

status: ## Show service status
	docker-compose ps

shell: ## Open shell in backend container
	docker-compose exec backend /bin/bash

db-shell: ## Open PostgreSQL shell
	docker-compose exec postgres psql -U dataapp -d dataapp



