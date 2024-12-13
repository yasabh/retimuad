# Base Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy application code to the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default entrypoint to allow running producer or consumer
ENTRYPOINT ["python"]
