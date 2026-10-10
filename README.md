# Industrial Edge Telemetry & Streaming Anomaly Detection Platform

[![CI Pipeline](https://github.com/emirhankaya-AFK/industrial-iot-telemetry-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/emirhankaya-AFK/industrial-iot-telemetry-platform/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-009688.svg?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.40%2B-FF4B4B.svg?logo=streamlit)](https://streamlit.io/)
[![Redis Streams](https://img.shields.io/badge/Redis-Streams%207.0%2B-DC382D.svg?logo=redis)](https://redis.io/)
[![Protocol Buffers](https://img.shields.io/badge/Protobuf-3.0-4285F4.svg?logo=google)](https://protobuf.dev/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## English

### Purpose
Industrial-grade edge telemetry ingestion and real-time streaming anomaly detection platform for mission-critical cyber‑physical systems (AC induction motors, high‑speed CNC spindles, centrifugal pumps). It ingests high‑frequency sensor streams over MQTT and HTTP, validates payloads via Protocol Buffers (Proto3) and JSON, enforces HMAC‑SHA256 authentication, buffers streams with Redis Streams consumer groups, detects anomalies using an ensemble (EWMA, robust MAD Z‑Score, cross‑sensor correlation) with an Isolation Forest baseline, debounces alerts, and dispatches incident webhooks with exponential backoff and DLQ containment.

### Verified Features
- MQTT broker (Mosquitto) and FastAPI HTTP gateway ingestion
- Production HMAC‑SHA256 token authentication with clock‑drift rejection and unknown device quarantining
- Redis Streams (industrial:telemetry) with consumer groups, XREADGROUP/XACK/XCLAIM, backpressure drop handling, and in‑memory fallback
- Streaming anomaly ensemble:
  - EWMA with pre‑update mean/variance
  - Robust rolling Z‑Score via Median Absolute Deviation (MAD)
  - Multi‑sensor correlation fusion (vibration, current, temperature)
  - Isolation Forest offline comparative baseline
- Alert debouncing, exponential backoff webhook dispatcher, Dead‑Letter Queue (DLQ) with manual retry
- Streamlit‑based SCADA console: live strip charts, fault injection studio, incident triage, DLQ inspector
- FastAPI REST API with health check, device registry, telemetry ingestion, anomaly/alert queries, metrics, and DLQ endpoint
- Code quality enforced by Ruff

### Stack
- Python 3.11+
- FastAPI (backend API)
- Streamlit (dashboard)
- Redis 7.0+ (streams) with optional in‑memory fallback
- Mosquitto (MQTT broker)
- Protocol Buffers (proto3) for payload validation
- Docker & Docker Compose (optional containerized deployment)
- Ruff (linting)
- pytest (testing)

### Setup & Usage
**Local development**
1. Clone the repo and `cd industrial-iot-telemetry-platform`
2. Create and activate a virtual environment (`python -m venv .venv`; activate per OS)
3. Install dependencies: `pip install -r requirements.txt`
4. Run linting: `ruff check .`
5. Run tests: `pytest tests/ -v`
6. (Optional) Run benchmark: `python tests/evaluation/run_benchmark.py`
7. Start API: `uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload`
8. Start dashboard: `streamlit run dashboard/app.py --server.port 8501`

**Docker Compose**
1. Copy `.env.example` to `.env` and set a secure `IOT_AUTH_SECRET`
2. Launch: `docker compose up --build -d`
3. Access API docs at `http://localhost:8000/docs`
4. Access dashboard at `http://localhost:8501`
5. Health check: `http://localhost:8000/health`

### Testing
- Linting with Ruff (`ruff check .`)
- Unit and integration test suite via `pytest tests/ -v` (36 passing; 2 live‑service tests opt‑in)
- Quantitative benchmark via `tests/evaluation/run_benchmark.py` (results shown in the README are hardware‑dependent samples)

### Limitations
- Benchmark figures are from a 2026‑09‑26 local Windows sample and vary with hardware.
- Live Redis/MQTT behavior is exercised only by opt‑in integration tests; the reported latency measures processing time, not network latency.
- When Redis is unavailable the system transparently falls back to an in‑memory stream engine.
- The platform is provided as‑is; production deployment should follow applicable safety and security practices for industrial control systems.

### License
MIT License – see the LICENSE file for details.

## Türkçe

### Amaç
Görev açısından kritik siber‑fiziksel sistemler (AC indüksiyon motorları, yüksek hızlı CNC mil çubukları, sentrifüj pompaları) için endüstriyel seviyede kenar telemetri alımı ve gerçek zamanlı akış anomali tespiti platformu. MQTT ve HTTP üzerinden yüksek frekanslı sensör akışlarını alır, Protobuf (Proto3) ve JSON ile yükü doğrular, HMAC‑SHA256 kimlik doğrulamasını zorunlu kılar, Redis Streams tüketici gruplarıyla akışı tamponlar, EWMA, sağlam MAD Z‑Skoru ve çapraşlı sensör korelasyonundan oluşan bir ensemble ile anomali tespiti yapar, Isolation Forest ile referans modeli karşılaştırır, uyarıları geciktirir ve exponansiyel geri çekilmeyle webhook gönderimi ve DLQ tutar.

### Doğrulanmış Özellikler
- Mosquitto MQTT brokeru ve FastAPI HTTP ağ geçidi alımı
- Üretim seviyesinde HMAC‑SHA256 jeton kimlik doğrulaması, saat sapması reddi ve inconn cihaz karantina
- Redis Streams (industrial:telemetry) tüketici grupları, XREADGROUP/XACK/XCLAIM, basınç gerileme düşme işleme ve bellek içi yedek
- Akış anomali ensemble'i:
  - EWMA (ön‑güncelleme ortalama/varyans)
  - Robust kayan Z‑Skoru (Median Mutlak Sapma – MAD)
  - Çoklu sensör korelasyon birleştirme (titreme, akım, sıcaklık)
  - Isolation Forest çevrimdışı referans modeli
- Uyarı geciktirme, exponansiyel geri çekimli webhook dağıtıcı, manuel deneme için DLQ
- Streamlit tabanlı SCADA konsolu: canlı şerit grafikleri, falt enjeksiyon stüdyosu, olay triajı, DLQ denetçisi
- FastAPI REST API: sağlık kontrolü, cihaz kayıtları, telemetri alımı, anomali/alert sorguları, metrikler ve DLQ uç noktası
- Kod kalitesi Ruff ile zorunlu tutulur

### Yığın
- Python 3.11+
- FastAPI (arka uç API)
- Streamlit (kontrol paneli)
- Redis 7.0+ (akışlar) ve opsiyonel bellek içi yedek
- Mosquitto (MQTT brokeri)
- Protocol Buffers (proto3) yük doğrulama
- Docker & Docker Compose (isteğe bağlı konteyner dağıtımı)
- Ruff (kod denetimi)
- pytest (test)

### Kurulum ve Kullanım
**Yerel geliştirme**
1. Depoyu klonlayın ve `industrial-iot-telemetry-platform` klasörüne geçin
2. Sanal ortam oluşturun ve etkinleştirin (`python -m venv .venv`; işletim sistemine göre)
3. Bağımlılıkları yükleyin: `pip install -r requirements.txt`
4. Kod denetimini çalıştırın: `ruff check .`
5. Testleri çalıştırın: `pytest tests/ -v`
6. (İsteğe bağlı) Benchmarkı çalıştırın: `python tests/evaluation/run_benchmark.py`
7. API'yi başlatın: `uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload`
8. Kontrol panelini başlatın: `streamlit run dashboard/app.py --server.port 8501`

**Docker Compose**
1. `.env.example` dosyasını `.env` olarak kopyalayın ve güvenli bir `IOT_AUTH_SECRET` belirleyin
2. Yığını başlatın: `docker compose up --build -d`
3. API dokümantasyonuna erişin: `http://localhost:8000/docs`
4. Kontrol paneline erişin: `http://localhost:8501`
5. Sağlık kontrolü: `http://localhost:8000/health`

### Test
- Ruff ile kod denetimi (`ruff check .`)
- `pytest tests/ -v` ile birim ve entegrasyon testleri (36 başarılı; 2 canlı‑hizmet testi opsiyonel)
- `tests/evaluation/run_benchmark.py` ile niceliksiz referans (README'de gösterilen sonuçlar donanıma bağlı örneklerdir)

### Sınırlamalar
- Benchmark sonuçları 2026‑09‑26 yerel Windows ortamından alınmıştır ve donanıma göre değişebilir.
- Canlı Redis/MQTT davranışı sadece opsiyonel entegrasyon testleriyle ölçülür; bildirilen gecikme işlem süresini, ağ gecikmesini içermez.
- Redis kullanılamadığında sistem şeffaf bir bellek içi akış motoruna geçer.
- Platform "olduğu gibi" sağlanır; üretim ortamında kullanım için endüstriyel kontrol sistemlerine uygun güvenlik ve güvenlik uygulamalarının izlenmesi gerekir.

### Lisans
MIT Lisansı – ayrıntılar için LICENSE dosyasına bakın.

## Deutsch

### Zweck
Industrie‑taugliche Edge‑Telemetrie‑Erfassung und Echtzeit‑Streaming‑Anomalie‑Erkennung für unternehmenskritische cyber‑physische Systeme (Wechselstrom‑Induktionsmotoren, Hochgeschwindigkeits‑CNC‑Spindeln, Zentrifugalpumpen). Die Plattform nimmt hochfrequente Sensordaten über MQTT und HTTP entgegen, validiert Payloads mittels Protocol Buffers (Proto3) und JSON, erzwingt HMAC‑SHA256‑Authentifizierung, puffert Streams mit Redis‑Streams‑Consumer‑Groups, erkennt Anomalien mittels eines Ensembles (EWMA, robuster MAD‑Z‑Score, Kreuz‑Sensorkorrelation) mit einem Isolation‑Forest‑Baseline als Referenz, entprellt Warnungen und sendet Ereignis‑Webhooks mit exponentiellem Backoff und DLQ‑Speicherung.

### Verifizierte Features
- MQTT‑Broker (Mosquitto) und FastAPI‑HTTP‑Gateway‑Aufnahme
- Produktions‑HMAC‑SHA256‑Token‑Authentifizierung mit Takt‑Drift‑Abwehr und Quarantäne unbekannter Geräte
- Redis Streams (industrial:telemetry) mit Consumer‑Groups, XREADGROUP/XACK/XCLAIM, Back‑Pressure‑Drop‑Behandlung und In‑Memory‑Fallback
- Streaming‑Anomalie‑Ensemble:
  - EWMA (mit Vor‑Update‑Mittelwert/Varianz)
  - Robuster rollierender Z‑Score mittels Median‑Absoluter‑Abweichung (MAD)
  - Multi‑Sensor‑Korrelationsfusion (Vibration, Strom, Temperatur)
  - Isolation‑Forest als Offline‑Vergleichsmodell
- Warnungsentprellung, Webhook‑Dispatcher mit exponentiellem Backoff, Dead‑Letter‑Queue (DLQ) mit manueller Wiederholung
- Streamlit‑basierte SCADA‑Konsole: Live‑Strip‑Charts, Fault‑Injection‑Studio, Ereignis‑Triage, DLQ‑Inspector
- FastAPI‑REST‑API: Gesundheitscheck, Geräteregister, Telemetrie‑Aufnahme, Anomalie/‑Abfragen, Metriken und DLQ‑Endpunkt
- Code‑Qualitätssicherung durch Ruff

### Stack
- Python 3.11+
- FastAPI (Backend‑API)
- Streamlit (Dashboard)
- Redis 7.0+ (Streams) mit optionalem In‑Memory‑Fallback
- Mosquitto (MQTT‑Broker)
- Protocol Buffers (proto3) für Payload‑Validierung
- Docker & Docker Compose (optionaler Container‑Deployment)
- Ruff (Linting)
- pytest (Testing)

### Einrichtung & Nutzung
**Lokale Entwicklung**
1. Repository klonen und in das Verzeichnis `industrial-iot-telemetry-platform` wechseln
2. Virtuelle Umgebung anlegen und aktivieren (`python -m venv .venv`; Aktivierung je nach Betriebssystem)
3. Abhängigkeiten installieren: `pip install -r requirements.txt`
4. Linting ausführen: `ruff check .`
5. Tests ausführen: `pytest tests/ -v`
6. (Optional) Benchmark ausführen: `python tests/evaluation/run_benchmark.py`
7. API starten: `uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload`
8. Dashboard starten: `streamlit run dashboard/app.py --server.port 8501`

**Docker Compose**
1. `.env.example` nach `.env` kopieren und einen sicheren `IOT_AUTH_SECRET` setzen
2. Stack starten: `docker compose up --build -d`
3. API‑Dokumentation aufrufen: `http://localhost:8000/docs`
4. Dashboard aufrufen: `http://localhost:8501`
5. Gesundheitscheck: `http://localhost:8000/health`

### Test
- Linting mit Ruff (`ruff check .`)
- Einheit‑ und Integrations‑Testsuite mittels `pytest tests/ -v` (36 erfolgreich; 2 Live‑Service‑Tests optional)
- Quantitativer Benchmark über `tests/evaluation/run_benchmark.py` (die im README gezeigten Werte sind hardwareabhängige Beispiele)

### Einschränkungen
- Benchmark‑Zahlen stammen aus einer Windows‑Messung vom 2026‑09‑26 und sind hardwareabhängig.
- Das Verhalten von Live‑Redis/MQTT wird nur durch optionale Integrations‑Tests geprüft; die angegebenen Latenzen beinhalten nur die Verarbeitungszeit, nicht die Netzwerklatenz.
- Falls Redis nicht verfügbar ist, fällt das System auf einen In‑Memory‑Stream‑Engine zurück.
- Die Plattform wird „wie besehen“ bereitgestellt; für den Produktiveinsatz sollten branchenübliche Sicherheits‑ und Schutzpraktiken für Industrie‑Steuerungssysteme befolgt werden.

### Lizenz
MIT‑Lizenz – siehe die LICENSE‑Datei für Details.
