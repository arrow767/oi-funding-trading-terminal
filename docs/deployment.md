# Production deployment on DigitalOcean

End-to-end plan from "I have a DO account" to "my terminal sees live
OI/funding/events". Each step is copy-pasteable; nothing here assumes
prior context.

Estimated time: **~90 minutes** (most of it cargo build).

## 0. Prerequisites (local machine, ~10 min)

* DigitalOcean account with billing set up.
* An SSH public key on file with DO (Settings → Security → SSH keys).
* `doctl` installed locally (optional, for scripted provisioning):
  ```sh
  brew install doctl   # or: snap install doctl
  doctl auth init      # paste API token
  ```
* A domain name pointed at DO nameservers (optional, for TLS via
  Let's Encrypt). Without one, you can still run plain-HTTP on the
  public IP or reverse-proxy through Cloudflare.
* The repo cloned locally:
  ```sh
  git clone <YOUR_REMOTE> trading-terminal-oi
  cd trading-terminal-oi
  ```

## 1. Provision the droplet (~5 min)

The recommended baseline (see `docs/sizing.md` for the breakdown):

* Droplet: `s-4vcpu-16gb-amd` (Premium AMD, 4 vCPU, 16 GB RAM, 200 GB SSD).
* Block storage: 250 GB attached, mounted at `/var/lib/clickhouse`.
* Region: `fra1` or `ams3` (closest to most exchange APIs from the EU;
  `sfo3` if you're West-coast US-bound).

```sh
# Create droplet
doctl compute droplet create oi-prod \
  --image ubuntu-24-04-x64 \
  --size s-4vcpu-16gb-amd \
  --region fra1 \
  --ssh-keys "$(doctl compute ssh-key list --no-header | awk '{print $1}' | head -1)" \
  --enable-monitoring \
  --enable-ipv6 \
  --wait

# Capture the public IP
DROPLET_IP=$(doctl compute droplet list oi-prod --no-header --format PublicIPv4)
echo "Droplet IP: $DROPLET_IP"

# Block storage volume (250 GB, named oi-data)
doctl compute volume create oi-data \
  --size 250GiB \
  --region fra1 \
  --fs-type xfs

# Attach
DROPLET_ID=$(doctl compute droplet list oi-prod --no-header --format ID)
VOL_ID=$(doctl compute volume list oi-data --no-header --format ID)
doctl compute volume-action attach $VOL_ID $DROPLET_ID
```

If using the web UI: Create → Droplets → same parameters; then
Volumes → Create → attach to the droplet.

## 2. SSH in and harden the OS (~10 min)

```sh
ssh root@$DROPLET_IP
```

On the droplet:

```sh
# 2.1 Update everything
apt update && apt upgrade -y

# 2.2 Create a non-root user
adduser oi   # set a strong password
usermod -aG sudo oi
mkdir -p /home/oi/.ssh
cp /root/.ssh/authorized_keys /home/oi/.ssh/
chown -R oi:oi /home/oi/.ssh
chmod 700 /home/oi/.ssh && chmod 600 /home/oi/.ssh/authorized_keys

# 2.3 Disable root SSH and password login
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl reload ssh

# 2.4 Firewall — only SSH, and the API ports we'll expose
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 50051/tcp  # gRPC
ufw allow 8080/tcp   # REST
ufw --force enable

# 2.5 Mount the Block Storage at /var/lib/clickhouse
mkdir -p /var/lib/clickhouse
echo '/dev/disk/by-id/scsi-0DO_Volume_oi-data /var/lib/clickhouse xfs defaults,noatime,nodiratime,discard 0 2' \
  | tee -a /etc/fstab
mount -a
df -h /var/lib/clickhouse

# 2.6 ulimits for ClickHouse (it asks for 262144 file descriptors)
cat <<'EOF' >> /etc/security/limits.conf
*  soft  nofile  262144
*  hard  nofile  262144
EOF

# 2.7 Disable swap (CH doesn't like it; OOM-kill is a cleaner failure mode)
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab
```

Reconnect as the `oi` user from now on:
```sh
ssh oi@$DROPLET_IP
```

## 3. Install Docker (~3 min)

```sh
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
# Re-login so the docker group takes effect
exit
ssh oi@$DROPLET_IP
docker compose version    # verify v2
```

## 4. Pull the code, build the binaries (~15 min)

```sh
git clone <YOUR_REMOTE> trading-terminal-oi
cd trading-terminal-oi
```

The `deploy/Dockerfile` is a multi-stage build that produces minimal
runtime images for `oi-collector` and `oi-api`. Building everything
locally on the droplet:

```sh
docker compose -f deploy/docker-compose.yml build
```

First build ~12–15 minutes on a 4 vCPU box. Subsequent builds reuse
cached layers and finish in seconds.

## 5. Generate secrets + edit configs (~5 min)

```sh
# Bearer token for API auth (rotate periodically)
TOKEN=$(openssl rand -base64 32)
echo "API bearer token: $TOKEN"   # save somewhere — terminal needs it
```

Edit `deploy/api.toml`:
```toml
[clickhouse]
url      = "http://clickhouse:8123"
database = "oi"
user     = "default"
password = ""

[redis]
url = "redis://redis:6379"

grpc_addr = "0.0.0.0:50051"
rest_addr = "0.0.0.0:8080"

[tls]
enabled = true
cert_path = "/etc/oi/tls/server.crt"
key_path  = "/etc/oi/tls/server.key"

[auth]
enabled = true
tokens  = ["<TOKEN_FROM_OPENSSL_ABOVE>"]
```

Edit `deploy/collector.toml` (defaults are fine; turn on WAL):
```toml
[wal]
enabled = true
dir = "/var/lib/oi/wal"

[exchanges]
enabled = []   # empty = all 9
```

## 6. TLS cert (~10 min, skip if running plaintext)

If you have a domain pointing at the droplet (`oi.example.com`):

```sh
sudo apt install -y certbot
sudo certbot certonly --standalone -d oi.example.com
# certs land in /etc/letsencrypt/live/oi.example.com/
sudo mkdir -p /etc/oi/tls
sudo cp /etc/letsencrypt/live/oi.example.com/fullchain.pem /etc/oi/tls/server.crt
sudo cp /etc/letsencrypt/live/oi.example.com/privkey.pem  /etc/oi/tls/server.key
sudo chown -R $USER:$USER /etc/oi
```

Auto-renew via certbot's systemd timer is already enabled by default.
Add a hook to copy renewed certs and restart the API:
```sh
sudo tee /etc/letsencrypt/renewal-hooks/deploy/oi-api.sh <<'EOF'
#!/bin/bash
DOMAIN="oi.example.com"
cp /etc/letsencrypt/live/$DOMAIN/fullchain.pem /etc/oi/tls/server.crt
cp /etc/letsencrypt/live/$DOMAIN/privkey.pem  /etc/oi/tls/server.key
chown -R oi:oi /etc/oi
docker restart oi-api || true
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/oi-api.sh
```

If running plaintext: leave `[tls] enabled = false` in api.toml and
skip this section.

## 7. First boot (~5 min)

```sh
cd ~/trading-terminal-oi
docker compose -f deploy/docker-compose.yml up -d clickhouse redis
# Wait for CH to be ready
docker compose -f deploy/docker-compose.yml logs -f clickhouse | grep "Ready for connections"
# Ctrl-C once you see it. Schema is auto-applied from migrations/001_schema.sql
# (the docker-compose mounts the migrations dir into CH's init-db hook).

# Now bring up the rest
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps
docker compose -f deploy/docker-compose.yml logs -f --tail=50 collector
```

You should see, within ~60 s:
* `instruments discovered count=...` for each enabled exchange.
* `minute flushed wrote=...` after the first :02 boundary.
* WS handlers connecting (`ws connected`).
* Funding sweep starting on its 30-min cadence.

## 8. Smoke-test (~3 min)

From the droplet:
```sh
# Health
curl -k https://localhost:8080/health/ready
# {"clickhouse":"ok","redis":"ok"}

# Latest OI bar (replace bearer token)
curl -k -H "Authorization: Bearer $TOKEN" \
  https://localhost:8080/v1/oi/latest/binance/BTCUSDT
# JSON with samples >= 1, native_open/high/low/close set

# Latest funding rate
curl -k -H "Authorization: Bearer $TOKEN" \
  https://localhost:8080/v1/funding/latest/binance/BTCUSDT

# Settlement events (might be empty for ~30min after first start —
# the sweep cadence). Check oi.funding_event in CH directly:
docker exec -it oi-clickhouse clickhouse-client \
  --query "SELECT exchange, count(), max(settlement_ts) FROM oi.funding_event GROUP BY exchange"
```

From your laptop (replace domain/IP):
```sh
curl -H "Authorization: Bearer $TOKEN" \
  https://oi.example.com:8080/health/ready
```

## 9. Observability (~10 min, optional but recommended)

```sh
mkdir -p deploy/observability_data/grafana
sudo chown -R 472:0 deploy/observability_data/grafana
```

Add to `deploy/docker-compose.yml` (or use a separate compose file
under `deploy/observability/`):

```yaml
  prometheus:
    image: prom/prometheus:v2.55.0
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./observability/prometheus-alerts.yml:/etc/prometheus/rules/oi.yml:ro
    ports: ["127.0.0.1:9096:9090"]

  grafana:
    image: grafana/grafana:11.4.0
    environment:
      GF_SECURITY_ADMIN_PASSWORD: <SET_A_PASSWORD>
    volumes:
      - ./observability_data/grafana:/var/lib/grafana
    ports: ["127.0.0.1:3000:3000"]
    depends_on: [prometheus]
```

```sh
docker compose -f deploy/docker-compose.yml up -d prometheus grafana
# Tunnel from your laptop to access Grafana:
ssh -L 3000:localhost:3000 oi@$DROPLET_IP
# Open http://localhost:3000
# Add Prometheus datasource: http://prometheus:9090
# Import deploy/observability/grafana-dashboard.json
```

Alerts in `deploy/observability/prometheus-alerts.yml` fire on:
* OI ingest stalled (no writes 3+ min)
* WAL backlog >5 min (warn) / >30 min (critical)
* Lease flapping
* WS reconnect storm
* API p95 latency >250 ms

## 10. Backups (~10 min, optional)

The `clickhouse-backup` sidecar in `deploy/docker-compose.yml` is
already wired. To enable S3 storage (DO Spaces works as S3-compatible):

```sh
# Create a Spaces bucket via DO UI or doctl, then:
cat > .env <<'EOF'
BACKUP_STORAGE=s3
BACKUP_S3_BUCKET=oi-backups
BACKUP_S3_REGION=fra1
BACKUP_S3_ACCESS_KEY=<from DO Spaces keys>
BACKUP_S3_SECRET_KEY=<from DO Spaces keys>
EOF

docker compose -f deploy/docker-compose.yml --env-file .env restart clickhouse-backup

# Schedule a daily backup via cron
crontab -e
# Add:  0 3 * * *  docker exec oi-ch-backup clickhouse-backup create_remote daily_$(date +\%Y\%m\%d)
```

## 11. Day-2 ops cheatsheet

```sh
# Update to a new version
cd ~/trading-terminal-oi
git pull
docker compose -f deploy/docker-compose.yml build --no-cache collector api
docker compose -f deploy/docker-compose.yml up -d --force-recreate collector api

# Resync a missed minute after a short outage
docker exec -it oi-collector oi-collector resync \
  --exchange all --minutes 5

# Tail collector logs filtered to one exchange
docker compose -f deploy/docker-compose.yml logs -f collector | grep '"exchange":"binance"'

# Inspect WAL backlog
docker exec -it oi-collector ls -la /var/lib/oi/wal | wc -l
curl -s http://localhost:9090/metrics | grep oi_wal_oldest_pending_age_seconds

# Check lease holder when failover is on
docker exec -it oi-redis redis-cli GET oi:lease:writer
```

## 12. Hardening checklist before going live

* [ ] Change Grafana admin password (default `admin/admin`).
* [ ] Generate a unique bearer token per terminal client; remove the
      default after verifying.
* [ ] Verify TLS cert chain reaches your terminal's trust store
      (`openssl s_client -connect oi.example.com:50051`).
* [ ] Set up DO snapshots (UI → Backups, ~$0.05/GB/month) on top of
      `clickhouse-backup` — covers OS-level corruption.
* [ ] Confirm `ulimit -n` inside `oi-clickhouse` ≥ 262144:
      `docker exec oi-clickhouse bash -c 'ulimit -n'`.
* [ ] Run an HA failover drill if `[failover] enabled = true`:
      `docker stop oi-collector` → watch the standby promote within
      ~15 s (lease TTL).

## Costs at a glance

* Droplet `s-4vcpu-16gb-amd`: **$84/mo**.
* Block Storage 250 GB: **$25/mo**.
* DO Spaces (backups): **~$5/mo** for the first 250 GB.
* Domain (Namecheap/Cloudflare): **~$1/mo** amortised.

**Total: ~$115/mo** for a single-node production deployment. HA
two-node ≈ $240–300/mo (see `docs/sizing.md`).

## Troubleshooting

* **`oi-clickhouse` keeps restarting** — almost always file
  descriptors. Verify `docker exec oi-clickhouse bash -c 'ulimit -n'`
  ≥ 262144. Re-mount `/etc/security/limits.conf` if not.
* **`oi-collector` says `OI fetch failed` for one exchange** — check
  the venue's status page. Our error taxonomy will say `ratelimit`,
  `auth`, `schema`, or `http` — each has a different fix path
  (see `docs/exchange-notes.md`).
* **Disk filling up** — the OHLC tables have a 400d TTL but a heavy
  prefix on small disks. Either raise the volume or drop the TTL in
  `migrations/001_schema.sql` and re-create the tables.
* **`/health/ready` returns 503** — payload tells you which probe
  failed. Most often Redis ran out of memory; bump `maxmemory` in
  the compose file.
