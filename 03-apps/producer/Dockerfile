FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python faker
COPY producer_orders.py .
CMD ["python", "producer_orders.py"]
