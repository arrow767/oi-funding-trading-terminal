# Production deployment — the.hosting Aurum JP (или любой KVM с Ubuntu 24.04)

Полный runbook для оси под 10–50k одновременных пользователей.
Заточен под `Aurum` от **the.hosting** (Япония, 8 vCore / 12 GB RAM /
150 GB NVMe), но любой KVM-сервер с Ubuntu 22.04+ подойдёт.

Время на всё: **~90 минут**, из них ~15 минут на cargo build.

---

## Что мы выкатываем

```
                            (Internet)
                                │
                                ▼
                         ┌─────────────┐
                         │   nginx     │   ← TLS, rate-limit, cache, WS upgrade
                         │ :80,443,    │
                         │  50051      │
                         └──────┬──────┘
                                │ (docker network, plain TCP)
                                ▼
                         ┌─────────────┐
                         │   oi-api    │   ← REST + native WS + gRPC + Prometheus
                         │  :8080      │
                         └──┬──────┬───┘
                            │      │
                ┌───────────┘      └────────────┐
                ▼                                ▼
         ┌────────────┐                  ┌──────────────┐
         │  Redis     │                  │  ClickHouse  │
         │  pub/sub   │                  │  storage     │
         └────────────┘                  └──────────────┘
                ▲
                │ broadcast publish
                │
         ┌──────┴──────┐
         │ oi-collector │   ← 9 exchanges REST + 4 live WS + funding sweep
         └──────────────┘
```

Один Redis pub/sub subscriber **внутри** `oi-api` фанаутит на
`broadcast::Sender` → все WS-клиенты подписываются на канал.
Без этого 50k WS = 50k Redis-коннектов; с этим = 1 Redis-коннект,
сколько бы клиентов ни подключилось.

---

## Этап 0 — что у тебя должно быть

* SSH-доступ к серверу (IP + ssh-ключ или пароль).
* Домен с A-записью на IP сервера (например `oi.yourdomain.com`).
* `git` локально, чтобы запушить если будут правки.

Если домена нет — можно деплоить без TLS на голый IP, но **не делай так
в production**: bearer-токен пойдёт plaintext'ом.

---

## Этап 1 — первый вход + non-root user (~5 мин)

```bash
ssh root@<IP>

# 1.1 Обновление
apt update && apt upgrade -y

# 1.2 Non-root пользователь
adduser oi              # задай сильный пароль
usermod -aG sudo oi
mkdir -p /home/oi/.ssh
cp /root/.ssh/authorized_keys /home/oi/.ssh/
chown -R oi:oi /home/oi/.ssh
chmod 700 /home/oi/.ssh && chmod 600 /home/oi/.ssh/authorized_keys
```

**Проверь во второй SSH-сессии что `ssh oi@<IP>` работает.** Если да:

```bash
# 1.3 Лочим root + пароли
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl reload ssh
```

---

## Этап 2 — Firewall (~1 мин)

```bash
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp        # SSH
ufw allow 80/tcp        # HTTP (для Let's Encrypt http-01 + редирект)
ufw allow 443/tcp       # HTTPS (REST + WS)
ufw allow 50051/tcp     # gRPC (h2 over TLS)
ufw --force enable
ufw status
```

Только эти 4 порта смотрят наружу — ClickHouse, Redis, oi-api на
8080, метрики на 9090/9091 живут в docker network и снаружи
недоступны.

---

## Этап 3 — Kernel tuning под 50k connections (~2 мин)

Скопируй файлы тюнинга **с твоего ноута** на сервер. Из репо:

```bash
# на ноуте:
scp deploy/tuning/sysctl-high-concurrency.conf oi@<IP>:/tmp/
scp deploy/tuning/limits.conf oi@<IP>:/tmp/

# на сервере (как oi):
sudo cp /tmp/sysctl-high-concurrency.conf /etc/sysctl.d/99-oi.conf
sudo cp /tmp/limits.conf /etc/security/limits.d/99-oi.conf
sudo sysctl --system 2>&1 | tail -20    # подхватить настройки сейчас

# swap off (важно для CH)
sudo swapoff -a
sudo sed -i '/ swap / s/^/#/' /etc/fstab
```

Релогиньтесь как `oi` чтобы limits.conf подхватился:
```bash
exit
ssh oi@<IP>
ulimit -n           # должно быть 262144
```

---

## Этап 4 — Docker (~3 мин)

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker oi
exit
ssh oi@<IP>          # перезайти чтобы docker группа подхватилась
docker compose version
```

---

## Этап 5 — Клонировать код (~1 мин)

```bash
cd ~
git clone https://github.com/arrow767/oi-funding-trading-terminal.git oi
cd oi
ls deploy/
```

Должно быть видно: `Dockerfile`, `docker-compose.yml`, `nginx/`, `tuning/`,
`api.toml`, `collector.toml`.

---

## Этап 6 — TLS-cert через certbot (~5 мин)

Сначала открой `80/tcp` (уже сделано в этапе 2). Затем:

```bash
sudo apt install -y certbot
sudo certbot certonly --standalone -d oi.yourdomain.com \
    --agree-tos --no-eff-email -m you@yourdomain.com
# Сертификаты лягут в /etc/letsencrypt/live/oi.yourdomain.com/
```

**Замени `oi.yourdomain.com` в `deploy/nginx/nginx.conf`** во всех трёх
местах (`server_name`, `ssl_certificate`, `ssl_certificate_key`):

```bash
sed -i 's/oi\.example\.com/oi.yourdomain.com/g' deploy/nginx/nginx.conf
```

Auto-renewal через certbot's systemd timer уже работает. Hook чтобы
nginx подхватил новые серты:

```bash
sudo tee /etc/letsencrypt/renewal-hooks/deploy/oi-nginx.sh <<'EOF'
#!/bin/bash
docker exec oi-nginx nginx -s reload || true
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/oi-nginx.sh
```

---

## Этап 7 — Конфиги (~3 мин)

```bash
# 7.1 Bearer-токен (сохрани его — он нужен будет терминалу)
openssl rand -base64 32 > ~/oi-token.txt
TOKEN=$(cat ~/oi-token.txt)
echo "Token: $TOKEN"

# 7.2 Включить auth в api.toml
nano deploy/api.toml
```

Поставь:
```toml
[tls]
enabled = false        # TLS терминируется в nginx, не в oi-api!

[auth]
enabled = true
tokens = ["<TOKEN из ~/oi-token.txt>"]
```

**Важно**: TLS делает nginx, поэтому **в api.toml `[tls] enabled = false`**.
Это намеренно — nginx делает session resumption, OCSP stapling, HTTP/2,
HTTP/3 (если включишь). Делать TLS дважды бессмысленно.

```bash
# 7.3 Включить WAL в collector.toml
nano deploy/collector.toml
```
```toml
[wal]
enabled = true
dir = "/var/lib/oi/wal"
```

---

## Этап 8 — Сборка образов (~15 мин)

```bash
cd ~/oi
docker compose -f deploy/docker-compose.yml build 2>&1 | tail -20
```

В отдельной сессии можешь следить за CPU/памятью:
```bash
docker stats --no-stream
```

Финиш: `Finished release ...` → `naming to oi-collector` + `oi-api`.

---

## Этап 9 — Запуск (~3 мин до первой минутки)

```bash
# 9.1 Стартуем хранилище
docker compose -f deploy/docker-compose.yml up -d clickhouse redis

# 9.2 Ждём CH (схема применится автоматом)
docker compose -f deploy/docker-compose.yml logs -f clickhouse \
    | grep -m1 "Ready for connections"
# Когда увидел — Ctrl-C

# 9.3 Проверяем таблицы
docker exec oi-clickhouse clickhouse-client --query "SHOW TABLES FROM oi"
# Должно показать: instruments, oi_minute, oi_hour, funding_minute,
# funding_event, mv_oi_minute_to_hour

# 9.4 Стартуем остальное (collector + api + nginx)
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps
```

Все 5 сервисов должны быть `Up` (clickhouse-backup можешь оставить
выключенным пока — `docker compose stop clickhouse-backup`).

```bash
# 9.5 Логи коллектора — первая минутка через ~90 сек
docker compose -f deploy/docker-compose.yml logs -f collector
```

Ищи последовательно:
- `instruments discovered count=...` × 9 (по каждой бирже)
- `ws connected` для Bybit/OKX/Bitget/Hyperliquid
- `minute flushed wrote=...` после первой :02 минутки
- `funding sweep starting`

---

## Этап 10 — Smoke test (~2 мин)

```bash
TOKEN=$(cat ~/oi-token.txt)

# 10.1 Health (внутри сервера, прямо в nginx)
curl -k https://localhost/health/ready
# {"clickhouse":"ok","redis":"ok"}

# 10.2 С указанием домена (проверка TLS)
curl https://oi.yourdomain.com/health/ready

# 10.3 Latest OI bar (нужен токен)
curl -H "Authorization: Bearer $TOKEN" \
  https://oi.yourdomain.com/v1/oi/latest/binance/BTCUSDT | jq

# 10.4 Funding rate
curl -H "Authorization: Bearer $TOKEN" \
  https://oi.yourdomain.com/v1/funding/latest/binance/BTCUSDT | jq

# 10.5 WebSocket (использует wscat — установи `npm i -g wscat`):
wscat -c "wss://oi.yourdomain.com/ws/v1/oi/subscribe?token=$TOKEN&instruments=binance:BTCUSDT,bybit:BTCUSDT"
# Должны полететь JSON фреймы с обновлениями
```

Без токена WS должен отвечать `401`:
```bash
wscat -c "wss://oi.yourdomain.com/ws/v1/oi/subscribe"
# error: Unexpected server response: 401
```

---

## Этап 11 — Мониторинг (опционально, ~5 мин)

Метрики oi-api/oi-collector доступны только в docker-сети
(`oi-api:9091/metrics`, `oi-collector:9090/metrics`). Чтобы посмотреть
с сервера:

```bash
docker exec oi-nginx wget -qO- http://api:9091/metrics | head -20
docker exec oi-nginx wget -qO- http://collector:9090/metrics | head -20
```

Если хочешь Grafana снаружи — раскомментируй блок `prometheus` +
`grafana` в `deploy/docker-compose.yml` (или скопируй из
`deploy/observability/`) и проложи туннель:

```bash
# на ноуте:
ssh -L 3000:localhost:3000 oi@<IP>
# открой http://localhost:3000
```

---

## Этап 12 — Backups (опционально)

```bash
# DO Spaces / Backblaze B2 / любой S3-совместимый
cat > ~/oi/.env <<'EOF'
BACKUP_STORAGE=s3
BACKUP_S3_BUCKET=oi-backups
BACKUP_S3_REGION=ap-northeast-1
BACKUP_S3_ACCESS_KEY=<from your S3 provider>
BACKUP_S3_SECRET_KEY=<from your S3 provider>
EOF

docker compose -f deploy/docker-compose.yml --env-file .env up -d clickhouse-backup

# daily cron
crontab -e
# добавить:
# 0 3 * * *  docker exec oi-ch-backup clickhouse-backup create_remote daily_$(date +\%Y\%m\%d)
```

---

## Что если ляжет / тормозит

### Симптом: WS клиенты получают `1011 internal error`

Redis pub/sub broadcaster не подключился. Проверь:
```bash
docker exec oi-redis redis-cli ping
docker logs oi-api | grep "broadcaster"
# Должно быть "ws broadcaster connected"
```

### Симптом: 50k клиентов подключились но многие висят

ulimits не подхватились в контейнерах:
```bash
docker exec oi-api bash -c 'ulimit -n'    # должно быть 200000
docker exec oi-nginx sh -c 'ulimit -n'    # должно быть 200000
```
Если меньше — `docker compose down && up -d` чтобы пересоздать с
правильными ulimits из compose.

### Симптом: `oi_wal_pending_files` растёт

ClickHouse не успевает писать. Проверь:
```bash
docker stats oi-clickhouse        # CPU / RAM
docker logs oi-clickhouse | tail -50
df -h                              # диск не забит?
```

### Симптом: `oi_api_ws_lagged_total` растёт

WS-клиенты слишком медленные, не успевают читать. Это **не баг**,
а сигнал — кто-то на медленном интернете. Если массово (500+/мин) —
увеличь `BROADCAST_CAPACITY` в `crates/oi-api/src/ws.rs` с 4096 до
8192-16384 и пересобери.

### Симптом: `429 Too Many Requests` от nginx

Кто-то долбит. Логи:
```bash
docker exec oi-nginx tail -100 /var/log/nginx/access.log | grep 429
```
Если легитимный пользователь — повышай `rate=50r/s` в
`nginx.conf` секции `limit_req_zone`. Если нет — это работающая
защита.

---

## Когда упрёшься в потолок одного сервера

Один CCX-класса узел (≤8 vCPU / 12-16 GB) выдержит:
- **~10–15k concurrent WS** комфортно
- **~5k req/sec REST** (cached)
- **~1k req/sec REST** (uncached Range queries)

При росте за 15k WS / 5k QPS — **horizontal scale**:

1. **Scale oi-api**: `docker compose up -d --scale api=3`. nginx
   уже умеет round-robin'ить через `upstream oi_api_rest`. Просто
   в `nginx.conf` добавь больше `server oi-api-2:8080;` строк (или
   используй DNS round-robin).
2. **Add second oi-api host**: ставь второй сервер (тот же образ),
   правишь `upstream` в nginx.conf чтобы туда роутить.
3. **Add Redis replica**: master/replica для read-only API queries,
   master продолжает принимать pub/sub publishes от коллектора.
4. **При 100k+ → CDN edge** для `/v1/oi/latest/*` (Cloudflare Pro
   принимает API trafic за $20/мес).

Подробнее в [docs/scaling.md] — добавлю когда упрёмся.

---

## Cheatsheet — Day-2 операции

```bash
cd ~/oi

# Обновить версию
git pull
docker compose -f deploy/docker-compose.yml build api collector
docker compose -f deploy/docker-compose.yml up -d --force-recreate api collector

# Лог конкретного сервиса
docker compose -f deploy/docker-compose.yml logs -f --tail=100 api
docker compose -f deploy/docker-compose.yml logs -f --tail=100 collector
docker compose -f deploy/docker-compose.yml logs -f --tail=100 nginx

# Состояние WS broadcaster (счётчик подключённых клиентов)
docker exec oi-api wget -qO- http://localhost:9091/metrics \
    | grep oi_api_ws_connections

# Состояние WAL
docker exec oi-collector wget -qO- http://localhost:9090/metrics \
    | grep oi_wal

# Прямой запрос в CH
docker exec oi-clickhouse clickhouse-client --query \
  "SELECT exchange, count(), max(bucket_ts) FROM oi.oi_minute GROUP BY exchange"

# Перезагрузка nginx (после правок nginx.conf)
docker exec oi-nginx nginx -s reload

# Резерв-копия "right now"
docker exec oi-ch-backup clickhouse-backup create manual_$(date +%Y%m%d_%H%M)
```
