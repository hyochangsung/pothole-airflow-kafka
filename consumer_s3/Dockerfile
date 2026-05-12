FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer_s3.py .

CMD ["python", "consumer_s3.py"]
