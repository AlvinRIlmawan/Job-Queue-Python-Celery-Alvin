"""Protokol JSON newline-delimited untuk komunikasi client dan TCP gateway."""

import json

from config import MAX_MESSAGE_BYTES


def encode_message(message: dict) -> bytes:
    """Ubah dictionary menjadi JSON UTF-8 yang diakhiri newline."""
    return (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")


def recv_message(sock) -> dict | None:
    """Baca satu pesan JSON hingga newline; satu pesan untuk setiap koneksi."""
    buffer = bytearray()

    while True:
        chunk = sock.recv(4096)
        if not chunk:
            if not buffer:
                return None
            raise ConnectionError("Koneksi ditutup sebelum pesan lengkap diterima.")

        newline_index = chunk.find(b"\n")
        if newline_index >= 0:
            buffer.extend(chunk[:newline_index])
            break

        buffer.extend(chunk)
        if len(buffer) > MAX_MESSAGE_BYTES:
            raise ValueError("Pesan terlalu besar (maksimum 1 MB).")

    try:
        message = json.loads(buffer.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Pesan JSON tidak valid: {exc}") from exc

    if not isinstance(message, dict):
        raise ValueError("Pesan JSON harus berupa objek.")
    return message
