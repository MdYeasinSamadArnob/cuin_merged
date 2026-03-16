.PHONY: help install dev test lint format build clean

# Default target
help:
	@echo "CUIN v2 - Available Commands"
	@echo "============================="
	@echo "make install      - Install all dependencies (backend + frontend)"
	@echo "make dev          - Start development servers"
	@echo "make test         - Run all tests"
	@echo "make lint         - Lint all code"
	@echo "make format       - Format all code"
	@echo "make build        - Build for production"
	@echo "make clean        - Clean build artifacts"
	@echo "make docker-up    - Start all services with Docker"
	@echo "make docker-down  - Stop all Docker services"
	@echo "make db-up        - Start databases only"
	@echo "make db-migrate   - Run database migrations"

# Installation
install: install-backend install-frontend

install-backend:
	@echo "📦 Installing backend dependencies..."
	cd backend && python3 -m venv venv
	cd backend && . venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
	@echo "✅ Backend dependencies installed"

install-frontend:
	@echo "📦 Installing frontend dependencies..."
	cd frontend && npm install
	@echo "✅ Frontend dependencies installed"

# Development
dev: dev-backend dev-frontend

dev-backend:
	@echo "🚀 Starting backend server..."
	cd backend && . venv/bin/activate && uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

dev-frontend:
	@echo "🚀 Starting frontend server..."
	cd frontend && npm run dev

# Testing
test: test-backend test-frontend

test-backend:
	@echo "🧪 Running backend tests..."
	cd backend && . venv/bin/activate && pytest tests/ -v

test-frontend:
	@echo "🧪 Running frontend tests..."
	cd frontend && npm test

# Linting
lint: lint-backend lint-frontend

lint-backend:
	@echo "🔍 Linting backend..."
	cd backend && . venv/bin/activate && ruff check .

lint-frontend:
	@echo "🔍 Linting frontend..."
	cd frontend && npm run lint

# Formatting
format: format-backend format-frontend

format-backend:
	@echo "✨ Formatting backend..."
	cd backend && . venv/bin/activate && ruff format .

format-frontend:
	@echo "✨ Formatting frontend..."
	cd frontend && npm run format

# Build
build: build-backend build-frontend

build-backend:
	@echo "🏗️ Building backend..."
	cd backend && . venv/bin/activate && pip install build && python -m build

build-frontend:
	@echo "🏗️ Building frontend..."
	cd frontend && npm run build

# Clean
clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf backend/venv
	rm -rf backend/__pycache__
	rm -rf backend/.pytest_cache
	rm -rf backend/.ruff_cache
	rm -rf backend/.mypy_cache
	rm -rf backend/dist
	rm -rf backend/build
	rm -rf backend/*.egg-info
	rm -rf frontend/node_modules
	rm -rf frontend/.next
	rm -rf frontend/out
	@echo "✅ Clean complete"

# Docker
docker-up:
	@echo "🐳 Starting all services..."
	docker-compose up --build -d

docker-down:
	@echo "🐳 Stopping all services..."
	docker-compose down

db-up:
	@echo "🗄️ Starting databases..."
	docker-compose up -d postgres neo4j

db-migrate:
	@echo "🗄️ Running migrations..."
	cd backend && . venv/bin/activate && alembic upgrade head
