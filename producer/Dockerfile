FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY transaction_generator.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "transaction_generator.py"]
