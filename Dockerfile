FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

VOLUME ["/app/data"]
EXPOSE 8081
CMD ["telegram-worker", "serve"]

