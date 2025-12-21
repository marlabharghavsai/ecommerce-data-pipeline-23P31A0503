# -------------------------------
# Base Image (lightweight)
# -------------------------------
FROM python:3.10-slim

# -------------------------------
# Environment Variables
# -------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# -------------------------------
# System Dependencies
# -------------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Working Directory
# -------------------------------
WORKDIR /app

# -------------------------------
# Install Python Dependencies
# (layer caching optimized)
# -------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -------------------------------
# Copy Application Code
# -------------------------------
COPY . .

# -------------------------------
# Default Command
# -------------------------------
CMD ["python", "scripts/pipeline_orchestrator.py"]
