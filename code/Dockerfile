# Use Python 3.13 slim image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=src \
  ENTRYPOINT_MODULE=src.databricks_telegram_bot.main

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  gcc \
  g++ \
  curl \
  git \
  && rm -rf /var/lib/apt/lists/*

# Install uv for faster dependency management
RUN pip install --no-cache-dir uv

# Copy dependency files first for better caching
COPY pyproject.toml uv.lock ./

# Install Python dependencies using uv
RUN uv sync --frozen

# Copy the entire project
COPY src src

# Create logs directory
RUN mkdir -p logs

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser \
  && chown -R appuser:appuser /app
USER appuser

# Use the environment variable to determine what module to run
CMD ["sh", "-c", "uv run -m $ENTRYPOINT_MODULE"]
