FROM bitnami/spark:3.5.0

USER root

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy project files
COPY src/ /app/src/
COPY config/ /app/config/
COPY tests/ /app/tests/
COPY notebooks/ /app/notebooks/

WORKDIR /app

CMD ["/opt/spark/sbin/start-master.sh"]
