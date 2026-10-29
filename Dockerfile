FROM python:3.11-slim

WORKDIR /app

# Install build essentials if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set permissions for Hugging Face Spaces non-root user (UID 1000)
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH \
    DASHBOARD_PORT=7860 \
    DASHBOARD_HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1

EXPOSE 7860

CMD ["python", "run.py"]
