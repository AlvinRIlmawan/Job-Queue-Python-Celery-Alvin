"""Client interaktif untuk mengirim pesan ke TCP gateway broker."""

import argparse
import socket

from config import BROKER_HOST, BROKER_PORT, SOCKET_TIMEOUT_SECONDS
from protocol import encode_message, recv_message


def send_request(
    host: str,
    port: int,
    app_id: str,
    scheme: str,
    app_type: str,
    data,
    routing_key: str | None = None,
) -> dict:
    request = {
        "message_type": "REQUEST",
        "app_id": app_id,
        "scheme": scheme.upper(),
        "app_type": app_type.upper(),
        "routing_key": routing_key,
        "data": data,
    }

    with socket.create_connection(
        (host, port), timeout=SOCKET_TIMEOUT_SECONDS
    ) as client:
        client.settimeout(SOCKET_TIMEOUT_SECONDS)
        client.sendall(encode_message(request))
        response = recv_message(client)
        if response is None:
            raise ConnectionError("Broker menutup koneksi tanpa respons.")
        return response


def main():
    parser = argparse.ArgumentParser(
        description="Client simulasi message handling Celery."
    )
    parser.add_argument("--host", default=BROKER_HOST)
    parser.add_argument("--port", type=int, default=BROKER_PORT)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument(
        "--scheme",
        choices=["FANOUT", "TOPIC", "WILDCARD"],
        help="Skema pesan; jika tidak diisi akan ditanyakan secara interaktif.",
    )
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count harus minimal 1.")

    print("=== Client Message Handling dengan Celery ===")
    print("Skema: FANOUT | TOPIC | WILDCARD")
    print("Tipe SHORT: data teks, misalnya Halo")
    print("Tipe LONG : angka 1..2000000, menghitung jumlah kuadrat 1..n")
    print("Ketik 'q' pada Application ID untuk keluar.\n")

    for index in range(args.count):
        try:
            app_id = input(f"[{index + 1}/{args.count}] Application ID: ").strip()
            if app_id.lower() == "q":
                break
            if not app_id:
                print("Application ID tidak boleh kosong.\n")
                continue

            scheme = args.scheme or input(
                "Skema (FANOUT/TOPIC/WILDCARD): "
            ).strip().upper()
            if scheme not in {"FANOUT", "TOPIC", "WILDCARD"}:
                print("Skema tidak valid.\n")
                continue

            routing_key = None
            if scheme == "TOPIC":
                routing_key = input(
                    "Routing key (app.short/app.long): "
                ).strip()
            elif scheme == "WILDCARD":
                routing_key = input(
                    "Routing key (contoh app.status/app.device.status): "
                ).strip()

            app_type = input("Jenis aplikasi (SHORT/LONG): ").strip().upper()
            if app_type not in {"SHORT", "LONG"}:
                print("Jenis aplikasi harus SHORT atau LONG.\n")
                continue

            data = input("Data request: ").strip()
            if app_type == "LONG":
                try:
                    data = int(data)
                except ValueError:
                    print("Data LONG harus berupa bilangan bulat.\n")
                    continue

            response = send_request(
                args.host,
                args.port,
                app_id,
                scheme,
                app_type,
                data,
                routing_key,
            )

            print("\n--- Respons dari broker/worker ---")
            print(f"Task ID    : {response.get('task_id') or response.get('request_id')}")
            print(f"Application: {response.get('app_id')}")
            print(f"Skema      : {response.get('scheme', scheme)}")
            print(f"Routing key: {response.get('routing_key') or '-'}")
            print(f"Worker     : {response.get('worker_id')}")
            print(f"Status     : {response.get('status')}")
            print(f"Hasil      : {response.get('result')}\n")
        except (OSError, ValueError, ConnectionError) as exc:
            print(f"Gagal mengirim request: {exc}\n")


if __name__ == "__main__":
    main()
