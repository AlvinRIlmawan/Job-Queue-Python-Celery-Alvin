"""Inisialisasi Celery dan deklarasi exchange, queue, serta binding RabbitMQ."""

from celery import Celery
from kombu import Exchange, Queue, binding

from config import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    FANOUT_EXCHANGE_NAME,
    TOPIC_EXCHANGE_NAME,
)

# Fan-Out: satu pesan dipublikasikan ke exchange yang meneruskannya ke seluruh
# queue yang terikat pada exchange tersebut.
FANOUT_EXCHANGE = Exchange(FANOUT_EXCHANGE_NAME, type="fanout", durable=True)

# Topic dan Wildcard memakai topic exchange RabbitMQ.
TOPIC_EXCHANGE = Exchange(TOPIC_EXCHANGE_NAME, type="topic", durable=True)

app = Celery(
    "python_message_handling",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=["tasks"],
)

app.conf.update(
    task_queues=(
        # Tiga antrean fanout. Ketiganya terikat pada exchange fanout yang sama.
        Queue("fanout.w1", exchange=FANOUT_EXCHANGE, routing_key="", durable=True),
        Queue("fanout.w2", exchange=FANOUT_EXCHANGE, routing_key="", durable=True),
        Queue("fanout.w3", exchange=FANOUT_EXCHANGE, routing_key="", durable=True),

        # Topic: hanya menerima routing key yang cocok secara tepat.
        Queue("topic.short", exchange=TOPIC_EXCHANGE, routing_key="app.short", durable=True),
        Queue("topic.long", exchange=TOPIC_EXCHANGE, routing_key="app.long", durable=True),

        # Wildcard: dua binding dipasang pada SATU queue.
        # '*' cocok dengan tepat satu segmen; '#' cocok dengan nol atau lebih segmen.
        # RabbitMQ tetap menaruh satu salinan pesan pada queue ini walau lebih dari
        # satu binding pada queue yang sama cocok.
        Queue(
            "wildcard.events",
            exchange=TOPIC_EXCHANGE,
            routing_key="",
            bindings=[
                binding(TOPIC_EXCHANGE, routing_key="app.*"),
                binding(TOPIC_EXCHANGE, routing_key="app.#"),
            ],
            durable=True,
        ),
    ),
    task_default_queue="topic.short",
    task_default_exchange=TOPIC_EXCHANGE_NAME,
    task_default_exchange_type="topic",
    task_default_routing_key="app.short",
    task_create_missing_queues=False,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_send_sent_event=True,
    worker_send_task_events=True,
    worker_enable_remote_control=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    result_expires=3600,
    timezone="Asia/Jakarta",
    enable_utc=True,
)
