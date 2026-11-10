
"""Launcher untuk menjalankan satu Celery worker (W1, W2, atau W3)."""

import argparse
import subprocess
import sys
from pathlib import Path

from config import WORKER_QUEUES


def main():
    parser = argparse.ArgumentParser(
        description="Jalankan satu worker Celery sesuai pembagian queue."
    )

    parser.add_argument(
        "--id",
        required=True,
        choices=sorted(WORKER_QUEUES)
    )

    parser.add_argument(
        "--pool",
        choices=["solo", "prefork", "threads"],
        default="solo",
        help="Pool solo paling mudah dipakai lintas OS; prefork cocok untuk Linux."
    )

    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--loglevel", default="INFO")

    args = parser.parse_args()

    if args.concurrency < 1:
        parser.error("--concurrency harus minimal 1.")

    # Menentukan queue berdasarkan ID worker
    queues = ",".join(WORKER_QUEUES[args.id])

    # Menentukan direktori utama project
    project_dir = Path(__file__).resolve().parent

    # Menyusun perintah Celery
    command = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "celery_app:app",
        "worker",
        "--loglevel",
        args.loglevel,
        "--queues",
        queues,
        "--hostname",
        f"{args.id}@%h",
        "--pool",
        args.pool,
        "--concurrency",
        str(args.concurrency),
        "--events",
    ]

    print(
        f"[LAUNCHER] Worker {args.id} mengonsumsi queue: {queues}",
        flush=True
    )

    print(
        f"[LAUNCHER] Python interpreter: {sys.executable}",
        flush=True
    )

    print(
        f"[LAUNCHER] Project directory: {project_dir}",
        flush=True
    )

    print(
        f"[LAUNCHER] Menjalankan Celery dengan argumen: {command!r}",
        flush=True
    )

    try:
        # Menjalankan Celery menggunakan subprocess
        result = subprocess.run(
            command,
            cwd=project_dir,
            check=False
        )

        return result.returncode

    except FileNotFoundError as error:
        print(
            f"[ERROR] File atau executable tidak ditemukan: {error}",
            flush=True
        )
        return 1

    except KeyboardInterrupt:
        print(
            f"\n[LAUNCHER] Worker {args.id} dihentikan.",
            flush=True
        )
        return 130


if __name__ == "__main__":
    sys.exit(main())
