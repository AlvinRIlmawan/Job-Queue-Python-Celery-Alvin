"""Task Celery yang dieksekusi oleh worker W1, W2, dan W3."""

import logging
import socket

from celery_app import app

logger = logging.getLogger(__name__)


def _current_worker_id(task_request) -> str:
    """Ambil nama worker dari hostname Celery, misalnya W1@nama-komputer."""
    hostname = getattr(task_request, "hostname", None) or socket.gethostname()
    return hostname.split("@", 1)[0]


def _process_payload(payload: dict, worker_id: str) -> dict:
    """Logika bisnis yang sebelumnya berada pada worker.py."""
    app_type = str(payload.get("app_type", "")).upper()
    data = payload.get("data", "")

    try:
        if app_type == "SHORT":
            result = f"Echo dari {worker_id}: {data}"
        elif app_type == "LONG":
            try:
                number = int(data)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "Untuk aplikasi LONG, data harus berupa bilangan bulat n."
                ) from exc

            if not 1 <= number <= 2_000_000:
                raise ValueError(
                    "Nilai n untuk LONG harus berada pada rentang 1..2000000."
                )

            total = number * (number + 1) * (2 * number + 1) // 6
            result = f"Jumlah kuadrat 1..{number} = {total}"
        else:
            raise ValueError("app_type harus SHORT atau LONG.")

        status = "SUCCESS"
    except ValueError as exc:
        result = str(exc)
        status = "ERROR"

    response = {
        "message_type": "RESPONSE",
        "request_id": payload.get("request_id"),
        "app_id": payload.get("app_id"),
        "scheme": payload.get("scheme"),
        "routing_key": payload.get("routing_key"),
        "worker_id": worker_id,
        "status": status,
        "result": result,
    }

    logger.info(
        "worker=%s scheme=%s routing_key=%s app_id=%s status=%s",
        worker_id,
        payload.get("scheme"),
        payload.get("routing_key"),
        payload.get("app_id"),
        status,
    )
    return response


@app.task(bind=True, name="tasks.process_message", acks_late=True)
def process_message(self, payload: dict) -> dict:
    """Memproses satu pesan yang diterima melalui salah satu queue Celery."""
    worker_id = _current_worker_id(self.request)
    return _process_payload(payload, worker_id)
