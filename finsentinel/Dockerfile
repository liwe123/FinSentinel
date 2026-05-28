FROM bitnami/spark:3.5.0

USER root

# Set environment variables
ENV PYTHONPATH=/app

WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy project files
COPY src/ /app/src/
COPY config/ /app/config/
COPY tests/ /app/tests/
COPY notebooks/ /app/notebooks/

CMD ["sleep", "infinity"]
