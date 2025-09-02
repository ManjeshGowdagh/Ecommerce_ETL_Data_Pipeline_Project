# ETL Pipeline Makefile
# Provides convenient commands for common pipeline operations

.PHONY: help setup install test validate run clean deploy

# Default target
help:
	@echo "E-commerce ETL Pipeline Commands"
	@echo "================================"
	@echo ""
	@echo "Setup and Installation:"
	@echo "  make setup     - Set up development environment"
	@echo "  make install   - Install Python dependencies"
	@echo ""
	@echo "Development:"
	@echo "  make validate  - Validate pipeline configuration"
	@echo "  make test      - Run all tests"
	@echo "  make run       - Execute the full pipeline"
	@echo "  make sample    - Generate sample data"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean     - Clean generated files"
	@echo "  make lint      - Run code quality checks"
	@echo ""
	@echo "Deployment:"
	@echo "  make deploy    - Deploy to production (requires configuration)"

# Setup development environment
setup:
	@echo "Setting up development environment..."
	python scripts/setup_environment.py
	cp .env.example .env
	@echo "✅ Setup completed. Edit .env file as needed."

# Install dependencies
install:
	@echo "Installing Python dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed."

# Validate pipeline
validate:
	@echo "Validating pipeline configuration..."
	python scripts/validate_pipeline.py

# Run tests
test:
	@echo "Running test suite..."
	python scripts/run_tests.py

# Generate sample data
sample:
	@echo "Generating sample data..."
	python scripts/generate_sample_data.py

# Run the pipeline
run:
	@echo "Executing ETL pipeline..."
	python run_pipeline.py

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	rm -rf data/processed/*
	rm -rf reports/*
	rm -rf logs/*
	rm -rf spark-warehouse/*
	rm -rf tmp/*
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -name "*.pyc" -delete
	@echo "✅ Cleanup completed."

# Code quality checks
lint:
	@echo "Running code quality checks..."
	@command -v flake8 >/dev/null 2>&1 || { echo "Installing flake8..."; pip install flake8; }
	flake8 src/ --max-line-length=100 --exclude=__pycache__
	@echo "✅ Linting completed."

# Deploy to production (placeholder)
deploy:
	@echo "Deploying to production..."
	@echo "⚠️  Configure deployment settings in scripts/deploy.sh"
	@echo "This would typically involve:"
	@echo "  1. Building Docker images"
	@echo "  2. Uploading to cluster"
	@echo "  3. Running deployment scripts"
	@echo "  4. Validating deployment"

# Development workflow
dev: clean sample validate test
	@echo "✅ Development workflow completed successfully!"

# CI/CD pipeline simulation
ci: install lint test validate
	@echo "✅ CI/CD pipeline completed successfully!"