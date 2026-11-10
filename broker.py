"""TCP gateway sekaligus publisher Celery untuk skema Fan-Out, Topic, Wildcard."""

import argparse
import re
import socket
import threading
import uuid

from celery.exceptions import TimeoutError as CeleryTimeoutError

from celery_app import FANOUT_EXCHANGE, TOPIC_EXCHANGE
from config import (
    APP_TYPES,
    BROKER_HOST,
    BROKER_PORT,
    SOCKET_TIMEOUT_SECONDS,
    TASK_RESULT_TIMEOUT_SECONDS,
    TOPIC_QUEUE_BY_KEY,
    TOPIC_ROUTES,
    WORKER_QUEUES,
)
from protocol import encode_message, recv_message
from tasks import process_message


ROUTING_KEY_PATTERN = re.compile(r"^app(?:\.[A-Za-z0-9_-]+)+$")


def send_error(conn: socket.socket, message: str, app_id=None, request_id=None):
    response = {
        "message_type": "RESPONSE",
        "request_id": request_id,
        "app_id": app_id,
        "worker_id": None,
        "status": "ERROR",
        "result": message,
    }
    conn.sendall(encode_message(response))


def _validate_request(request: dict) -> tuple[str, str, str | None, dict]:
    app_id = request.get("app_id")
    if not isinstance(app_id, str) or not app_id.strip():
        raise ValueError("app_id wajib berupa teks yang tidak kosong.")

    scheme = str(request.get("scheme", "")).strip().upper()
    if scheme not in {"FANOUT", "TOPIC", "WILDCARD"}:
        raise ValueError("scheme harus FANOUT, TOPIC, atau WILDCARD.")

    app_type = str(request.get("app_type", "")).upper()
    if app_type not in APP_TYPES:
        raise ValueError("app_type harus SHORT atau LONG.")
    if "data" not in request:
        raise ValueError("Field data wajib diisi.")

    routing_key = request.get("routing_key")
    if scheme == "TOPIC":
        if not isinstance(routing_key, str) or routing_key not in TOPIC_ROUTES:
            raise ValueError(
                "Topic menerima routing_key app.short atau app.long."
            )
        expected_type = TOPIC_ROUTES[routing_key]
        if app_type != expected_type:
            raise ValueError(
                f"Routing key {routing_key} harus menggunakan app_type {expected_type}."
            )
    elif scheme == "WILDCARD":
        if not isinstance(routing_key, str) or not ROUTING_KEY_PATTERN.fullmatch(routing_key):
            raise ValueError(
                "Routing key Wildcard harus berupa app.<segmen>[.<segmen>...], "
                "tanpa karakter * atau #. Contoh: app.status atau app.device.status."
            )
    else:
        routing_key = None

    task_payload = {
        "request_id": str(uuid.uuid4()),
        "app_id": app_id.strip(),
        "scheme": scheme,
        "routing_key": routing_key,
        "app_type": app_type,
        "data": request["data"],
    }
    return scheme, app_id.strip(), routing_key, task_payload


def publish_message(request: dict) -> dict:
    """Validasi dan publikasikan pesan ke exchange RabbitMQ melalui Celery."""
    scheme, app_id, routing_key, payload = _validate_request(request)
    request_id = payload["request_id"]

    if scheme == "FANOUT":
        # Satu publikasi ke fanout exchange akan disalin oleh RabbitMQ ke
        # setiap queue yang terikat: fanout.w1, fanout.w2, dan fanout.w3.
        async_result = process_message.apply_async(
            args=[payload],
            exchange=FANOUT_EXCHANGE,
            routing_key="",
            task_id=request_id,
            queue="fanout.w1",
            ignore_result=True,
        )
        return {
            "message_type": "RESPONSE",
            "request_id": request_id,
            "app_id": app_id,
            "scheme": scheme,
            "routing_key": None,
            "task_id": async_result.id,
            "worker_id": ", ".join(WORKER_QUEUES.keys()),
            "status": "PUBLISHED",
            "result": (
                "Pesan berhasil dipublikasikan ke exchange Fan-Out. "
                "Setiap worker menerima salinan pesan; periksa log worker atau Flower."
            ),
        }

    # Topic dan Wildcard menggunakan topic exchange. Perbedaannya terletak
    # pada binding antrean yang sudah didefinisikan di celery_app.py.
    queue_name = (
        TOPIC_QUEUE_BY_KEY[routing_key]
        if scheme == "TOPIC"
        else "wildcard.events"
    )
    async_result = process_message.apply_async(
        args=[payload],
        queue=queue_name,
        exchange=TOPIC_EXCHANGE,
        routing_key=routing_key,
        task_id=request_id,
    )

    try:
        response = async_result.get(
            timeout=TASK_RESULT_TIMEOUT_SECONDS,
            propagate=True,
        )
    except CeleryTimeoutError as exc:
        raise TimeoutError(
            f"Worker belum mengirim hasil dalam {TASK_RESULT_TIMEOUT_SECONDS} detik. "
            "Pastikan worker yang sesuai sedang aktif."
        ) from exc

    if not isinstance(response, dict):
        raise RuntimeError("Worker mengembalikan respons dengan format tidak dikenal.")
    response["task_id"] = async_result.id
    return response


def handle_client(conn: socket.socket, address):
    with conn:
        conn.settimeout(SOCKET_TIMEOUT_SECONDS)
        try:
            request = recv_message(conn)
            if request is None:
                return

            response = publish_message(request)
            print(
                f"[BROKER] client={address} task_id={response.get('task_id')} "
                f"scheme={response.get('scheme')} "
                f"routing_key={response.get('routing_key')} "
                f"status={response.get('status')}",
                flush=True,
            )
            conn.sendall(encode_message(response))
        except (ValueError, TimeoutError, RuntimeError) as exc:
            try:
                send_error(
                    conn,
                    str(exc),
                    app_id=request.get("app_id") if "request" in locals() else None,
                )
            except OSError:
                pass
            print(f"[BROKER] Request ditolak/gagal dari {address}: {exc}", flush=True)
        except (OSError, ConnectionError) as exc:
            print(f"[BROKER] Kesalahan koneksi dari {address}: {exc}", flush=True)
        except Exception as exc:  # Catat kegagalan publisher/Celery yang tidak terduga.
            try:
                send_error(conn, f"Gagal memublikasikan pesan: {exc}")
            except OSError:
                pass
            print(f"[BROKER] Kesalahan internal: {exc}", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="TCP gateway untuk message handling menggunakan Celery/RabbitMQ."
    )
    parser.add_argument("--host", default=BROKER_HOST)
    parser.add_argument("--port", type=int, default=BROKER_PORT)
    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen()
        print(
            f"[BROKER] TCP gateway aktif pada {args.host}:{args.port}",
            flush=True,
        )
        print("[BROKER] Publisher Celery siap menggunakan RabbitMQ.", flush=True)

        try:
            while True:
                conn, address = server.accept()
                threading.Thread(
                    target=handle_client,
                    args=(conn, address),
                    daemon=True,
                ).start()
        except KeyboardInterrupt:
            print("\n[BROKER] Gateway dihentikan.", flush=True)


if __name__ == "__main__":
    main()
