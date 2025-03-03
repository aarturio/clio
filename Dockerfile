# Use a slim Python base image
FROM python:3.11-slim

# Set working directory in the container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy only the dependency files first (for caching efficiency)
COPY pyproject.toml uv.lock ./

# Install dependencies with uv, using the lockfile for reproducibility
RUN uv sync --frozen --no-cache

# Copy the rest of your FastAPI app code
COPY app ./app

# Ensure the virtual environment is activated and uvicorn is available
ENV PATH="/app/.venv/bin:$PATH"

# Command to run the FastAPI app
CMD ["uvicorn", "clio.main:app", "--host", "0.0.0.0", "--port", "8000"]