"""Konfigurasi aplikasi message handling berbasis Celery dan RabbitMQ."""

import os

# TCP gateway yang menerima request dari client.py.
BROKER_HOST = os.getenv("APP_BROKER_HOST", "127.0.0.1")
BROKER_PORT = int(os.getenv("APP_BROKER_PORT", "8000"))

# Kredensial RabbitMQ untuk docker-compose.yml.
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "app")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "app_password")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))

CELERY_BROKER_URL = os.getenv(
    "CELERY_BROKER_URL",
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//",
)
# RPC backend menggunakan RabbitMQ yang sama; tidak membutuhkan Redis tambahan.
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "rpc://")

FANOUT_EXCHANGE_NAME = "messages.fanout"
TOPIC_EXCHANGE_NAME = "messages.topic"

# Routing key Topic harus tepat sama dengan salah satu key berikut.
TOPIC_ROUTES = {
    "app.short": "SHORT",
    "app.long": "LONG",
}
TOPIC_QUEUE_BY_KEY = {
    "app.short": "topic.short",
    "app.long": "topic.long",
}

# Setiap worker hanya mengonsumsi antrean yang ditugaskan kepadanya.
WORKER_QUEUES = {
    "W1": ["fanout.w1", "topic.short"],
    "W2": ["fanout.w2", "topic.long"],
    "W3": ["fanout.w3", "wildcard.events"],
}

# Batas waktu menunggu hasil untuk skema Topic dan Wildcard.
TASK_RESULT_TIMEOUT_SECONDS = int(os.getenv("TASK_RESULT_TIMEOUT_SECONDS", "45"))
SOCKET_TIMEOUT_SECONDS = TASK_RESULT_TIMEOUT_SECONDS + 10
MAX_MESSAGE_BYTES = 1_000_000
APP_TYPES = ("SHORT", "LONG")
