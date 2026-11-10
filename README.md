# Message Handling Python dengan Celery, RabbitMQ, dan Flower

## 1. Gambaran umum

Proyek ini mengubah komunikasi worker yang sebelumnya memakai socket TCP langsung menjadi message handling asinkron dengan **Celery**. **RabbitMQ** bertindak sebagai message broker AMQP, sedangkan **Flower** dipakai untuk memantau aktivitas task dan worker melalui web.

Alur aplikasi:

```text
Client (client.py)
       |
       | JSON over TCP
       v
TCP Gateway / Publisher (broker.py)
       |
       | Celery task
       v
RabbitMQ Exchange
       |
       +---- Queue W1 ---- Celery Worker W1
       +---- Queue W2 ---- Celery Worker W2
       +---- Queue W3 ---- Celery Worker W3

Flower: http://localhost:5555
RabbitMQ Management: http://localhost:15672
```

Gateway TCP dipertahankan agar antarmuka client dari proyek awal tetap sederhana. Penerusan pesan di belakang gateway dilakukan oleh Celery dan RabbitMQ.

## 2. Tiga skema message handling

### A. Fan-Out

- Exchange: `messages.fanout` dengan tipe `fanout`.
- Binding: `fanout.w1`, `fanout.w2`, dan `fanout.w3` terikat pada exchange yang sama.
- Satu publikasi menghasilkan salinan pesan pada ketiga antrean; W1, W2, dan W3 memproses pesan yang sama.
- Karena satu task ID dikirim ke beberapa antrean, hasil task tidak disimpan (`ignore_result=True`). Status `PUBLISHED` berarti pesan sudah diterbitkan, bukan berarti ketiga worker sudah selesai. Periksa log worker atau Flower untuk melihat eksekusinya.

### B. Topic

- Exchange: `messages.topic` dengan tipe `topic`.
- Routing key harus cocok tepat dengan binding antrean.

| Routing key | Queue | Worker | Tipe aplikasi |
|---|---|---|---|
| `app.short` | `topic.short` | W1 | `SHORT` |
| `app.long` | `topic.long` | W2 | `LONG` |

Contoh `app.short` hanya diterima antrean `topic.short`; `app.long` hanya diterima `topic.long`.

### C. Wildcard

- Menggunakan exchange `messages.topic`.
- Queue `wildcard.events` memiliki dua binding: `app.*` dan `app.#`.
- Kedua binding dipasang pada queue yang sama dan queue tersebut dikonsumsi W3.

| Binding | Makna | Contoh routing key yang cocok |
|---|---|---|
| `app.*` | `*` mencocokkan tepat satu segmen | `app.status`, `app.alert` |
| `app.#` | `#` mencocokkan nol atau lebih segmen | `app.status`, `app.device.status`, `app.sensor.room.temperature` |

Contoh:
- `app.status` cocok dengan `app.*` dan `app.#`, tetapi hanya masuk satu kali ke queue `wildcard.events`.
- `app.device.status` hanya cocok dengan `app.#`.

Karakter `*` dan `#` digunakan pada binding, bukan diketik sebagai bagian routing key pesan. Client mengirim routing key aktual seperti `app.device.status`.

## 3. Pembagian antrean worker

| Worker | Queue yang dikonsumsi |
|---|---|
| W1 | `fanout.w1`, `topic.short` |
| W2 | `fanout.w2`, `topic.long` |
| W3 | `fanout.w3`, `wildcard.events` |

Satu worker dapat mengonsumsi lebih dari satu queue. Dengan pembagian ini, ketiga worker menerima semua pesan Fan-Out, sedangkan pesan Topic/Wildcard diteruskan sesuai binding.

## 4. Persyaratan

- Python 3.10 atau lebih baru.
- Docker Desktop atau Docker Engine dengan Docker Compose.
- Paket Python pada `requirements.txt`.

## 5. Instalasi dan menjalankan aplikasi

Jalankan perintah dari direktori proyek.

### Langkah 1 — Instal paket Python

```bash
python -m venv myenv
```

Aktifkan virtual environment:

Windows PowerShell:
```powershell
myenv\Scripts\Activate.ps1
```

Linux/macOS:
```bash
source myenv/bin/activate
```

Kemudian:
```bash
pip install -r requirements.txt
```

### Langkah 2 — Jalankan RabbitMQ

```bash
docker compose up -d rabbitmq
```

Periksa status container:
```bash
docker compose ps
```

RabbitMQ Management dapat dibuka pada `http://localhost:15672`.

Kredensial konfigurasi proyek:
- Username: `app`
- Password: `app_password`

Ganti kredensial tersebut sebelum dipakai pada lingkungan produksi. Jika diganti, samakan variabel `RABBITMQ_USER` dan `RABBITMQ_PASSWORD` pada environment Python dengan konfigurasi container.

### Langkah 3 — Jalankan tiga worker

Buka tiga terminal pada direktori proyek dan virtual environment yang sama.

Terminal 1:
```bash
python worker.py --id W1
```

Terminal 2:
```bash
python worker.py --id W2
```

Terminal 3:
```bash
python worker.py --id W3
```

Launcher memakai pool `solo` secara default agar mudah dijalankan pada Windows, Linux, dan macOS. Pada Linux, Anda dapat mencoba pool `prefork`, misalnya:
```bash
python worker.py --id W1 --pool prefork --concurrency 2
```

### Langkah 4 — Jalankan TCP gateway

Terminal 4:
```bash
python broker.py
```

Gateway aktif secara default pada `127.0.0.1:8000`.

### Langkah 5 — Jalankan Flower

Terminal 5:
```bash
celery -A celery_app:app flower --port=5555
```

Buka dashboard Flower pada `http://localhost:5555`. Flower menampilkan worker, task, status, dan riwayat eksekusi.

### Langkah 6 — Jalankan client

Terminal 6:
```bash
python client.py
```

Untuk mengirim beberapa request:
```bash
python client.py --count 5
```

Atau tentukan skema melalui argumen:
```bash
python client.py --scheme FANOUT
python client.py --scheme TOPIC
python client.py --scheme WILDCARD
```

## 6. Contoh pengujian

### Fan-Out

Input:
```text
Application ID: BROADCAST-001
Skema: FANOUT
Jenis aplikasi: SHORT
Data request: Halo semua worker
```

Hasil yang diharapkan: broker menampilkan status `PUBLISHED`, lalu log W1, W2, dan W3 masing-masing menunjukkan pemrosesan pesan yang sama.

### Topic

Uji pertama:
```text
Application ID: TOPIC-001
Skema: TOPIC
Routing key: app.short
Jenis aplikasi: SHORT
Data request: Pesan untuk W1
```

Hasil yang diharapkan: hanya W1 memproses pesan dari queue `topic.short`.

Uji kedua:
```text
Application ID: TOPIC-002
Skema: TOPIC
Routing key: app.long
Jenis aplikasi: LONG
Data request: 100
```

Hasil yang diharapkan: hanya W2 memproses pesan dari queue `topic.long`.

### Wildcard

Uji `*` dan `#` sekaligus:
```text
Application ID: WILD-001
Skema: WILDCARD
Routing key: app.status
Jenis aplikasi: SHORT
Data request: Uji wildcard satu segmen
```

Routing key tersebut cocok dengan `app.*` dan `app.#`, dan diproses satu kali oleh W3.

Uji `#` untuk beberapa segmen:
```text
Application ID: WILD-002
Skema: WILDCARD
Routing key: app.device.status
Jenis aplikasi: SHORT
Data request: Uji wildcard bertingkat
```

Routing key tersebut cocok dengan `app.#`, lalu diproses oleh W3.

## 7. Variabel environment (opsional)

Konfigurasi default dapat diubah tanpa mengedit kode:

- `APP_BROKER_HOST`, `APP_BROKER_PORT`: alamat TCP gateway.
- `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_HOST`, `RABBITMQ_PORT`: koneksi RabbitMQ.
- `CELERY_BROKER_URL`: URL broker Celery penuh, jika ingin menggantikan komposisi URL dari variabel RabbitMQ.
- `CELERY_RESULT_BACKEND`: default `rpc://`.
- `TASK_RESULT_TIMEOUT_SECONDS`: batas waktu tunggu hasil Topic/Wildcard, default 45 detik.

## 8. Catatan penting

1. Jalankan RabbitMQ terlebih dahulu, kemudian worker, gateway, dan client. Flower dapat dijalankan setelah worker aktif.
2. Jika pesan Topic/Wildcard tidak memiliki queue binding yang cocok, RabbitMQ tidak akan mengirim pesan tersebut ke worker. Gateway membatasi Topic pada `app.short` dan `app.long`, serta memvalidasi format routing key Wildcard.
3. Fan-Out adalah pola broadcast. Client menerima konfirmasi publikasi, bukan hasil gabungan dari ketiga worker.
4. Topic dan Wildcard menunggu hasil satu worker dan mengembalikannya ke client. Pastikan worker yang mengonsumsi queue tujuan aktif.
5. Worker Celery mengirim task events agar aktivitasnya dapat terlihat oleh Flower.
6. Hentikan proses Python dengan `Ctrl+C`; RabbitMQ dapat dihentikan dengan `docker compose down`. Data volume RabbitMQ tetap tersimpan kecuali volume dihapus secara eksplisit.

## 9. File proyek

- `broker.py`: TCP gateway dan publisher Celery.
- `client.py`: client pengirim request.
- `worker.py`: launcher Celery untuk W1/W2/W3.
- `tasks.py`: fungsi pemrosesan task.
- `celery_app.py`: deklarasi Celery, exchange, queue, dan binding.
- `config.py`: konfigurasi koneksi dan pembagian queue.
- `protocol.py`: protokol JSON untuk komunikasi client-gateway.
- `docker-compose.yml`: layanan RabbitMQ.
- `requirements.txt`: dependensi Python.
