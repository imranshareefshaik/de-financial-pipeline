# Base image: Python 3.11/3.12 slim Debian
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install OpenJDK runtime for PySpark and curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Copy dependency specifications
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source and configuration
COPY config/ config/
COPY src/ src/

# Create necessary data directories
RUN mkdir -p data/raw data/processed data/analytics

# Default entrypoint: Run the full pipeline orchestrator
ENTRYPOINT ["python", "-m", "src.pipeline_runner"]
