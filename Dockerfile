FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

# Run the broker (entrypoint will be updated once broker is implemented)
# CMD ["python", "broker.py"]
