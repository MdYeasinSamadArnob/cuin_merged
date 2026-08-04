# CUIN v2 - Control Plane

**CUIN v2** is an advanced **Entity Resolution (ER)** and **Identity Graph** platform. It links customer records from different systems (like CRM, Loans, Website) to create a "Single Customer View".

---

## 🐳 Quick Start (Docker — Recommended)
The entire stack — Postgres, Redis, Apache Doris, the backend API, and the frontend dashboard — builds and runs from one place: `infra/`.

### 1. Prerequisites
**Docker** and **Docker Compose**. That's it — no local Python/Node install needed.

### 2. One-time host setup for Apache Doris
**Apache Doris** is the pipeline's execution engine — required, not optional. Doris's storage node refuses to start unless the **host** kernel allows enough memory-mapped areas — this is a real Linux kernel setting, not something Docker Compose can set on your behalf:
```bash
echo 'vm.max_map_count=2000000' | sudo tee /etc/sysctl.d/99-doris.conf
sudo sysctl --system
```
Do this once per host, before the first `docker compose up`. If you skip it, every other service comes up fine and only the `doris` container will crash-loop — `docker compose logs doris` will show the exact kernel-parameter error if you forget.

### 3. Configure (first time only)
```bash
cp infra/.env.example infra/.env
```
Open `infra/.env` and set:
*   `PUBLIC_HOST` — the address you'll type into your **browser** to reach the app (e.g. `localhost`, or this machine's LAN IP if you're accessing it from another device). This gets baked into the frontend at build time — see the comment in the file.
*   `BACKEND_UID` / `BACKEND_GID` — set to your host user's `id -u` / `id -g` if you hit `Permission denied` errors on the backend writing to `backend/data/` (a bind-mount ownership mismatch, not a bug in the app).
*   Ports, if any of the defaults (`8110`, `30011`, `5436`, `6381`, `18080`, `15540`, `8130`, `9130`, `8040`, `9050`) are already taken on your machine.

### 4. Up
```bash
cd infra
docker compose up --build -d
```
First build takes a few minutes (installing backend/frontend dependencies, and Doris's FE+BE cold start is slow — give it ~90s before it reports healthy); subsequent runs reuse the Docker layer cache and start in seconds. Watch progress with `docker compose logs -f`.

*   **Dashboard**: `http://<PUBLIC_HOST>:30011`
*   **API Docs**: `http://<PUBLIC_HOST>:8110/docs`
*   **Health Check**: `http://<PUBLIC_HOST>:8110/health/ready`
*   **PgAdmin**: `http://<PUBLIC_HOST>:18080` (Email: `admin@cuin.com`, Pass: `password123`, or whatever you set in `.env`)
*   **RedisInsight**: `http://<PUBLIC_HOST>:15540`
*   **Doris** (MySQL protocol, for direct SQL access): `mysql -h <PUBLIC_HOST> -P 9130 -uroot`

Check everything came up healthy:
```bash
docker compose ps
```

### 5. Down
```bash
cd infra
docker compose down
```
This stops and removes the containers but **keeps your data** — Postgres and pipeline run artifacts live in bind-mounted host folders (`../data/`, `../backend/data/`), and Doris's data lives in named Docker volumes (`doris_fe_meta`, `doris_be_storage`) — neither is inside a container that gets deleted, so nothing is lost. Run `docker compose up -d` again later and you're back where you left off.

To also wipe the data volumes (start completely fresh):
```bash
docker compose down -v   # -v also removes Doris's named volumes
rm -rf ../data/postgres ../backend/data/runs
```

### Rebuilding after a code change
```bash
docker compose up --build -d backend    # backend code changed
docker compose up --build -d frontend   # frontend code changed (re-bakes NEXT_PUBLIC_* too)
```

---

## 🏦 One Command, Any Environment (Local or Restricted/Banking)

The exact same command brings the stack up whether you're on a laptop or deploying inside a locked-down bank environment (no privileged containers, no hardcoded secrets):
```bash
cd infra && cp -n .env.example .env && docker compose up --build -d
```
`cp -n` only creates `.env` from the template if it doesn't already exist, so re-running this is safe and never clobbers environment-specific values an ops team has set.

Why this one command works in both places:
*   **No privileged containers.** Every service — including RedisInsight, which previously ran with `privileged: true` purely to write its bind-mounted data folder — now runs under standard container permissions. Passes Pod Security Standards "restricted" / OPA / Kyverno-style admission policies without exceptions.
*   **No hardcoded secrets.** Every port, credential, and hostname comes from `infra/.env` (see step 3 above) — set real values for a bank deployment, keep the defaults for local; the command itself never changes.
*   **Runs as a non-root, fixed UID/GID** (`BACKEND_UID`/`BACKEND_GID` in `.env`) rather than requiring root inside the container.

Two things worth being upfront about, honestly, rather than papering over:
*   **The `vm.max_map_count` host kernel setting from step 2 is mandatory everywhere, including restricted hosts.** Apache Doris is the only pipeline engine — there is no in-process fallback that avoids this requirement. If the host genuinely cannot grant `sudo` to run that one `sysctl` command, that's a real blocker to raise with the platform/ops team before deployment, not something this command can work around.
*   `docker compose up --build` still needs the base images (`postgres:16-alpine`, `redis:7-alpine`, `apache/doris:*`, `python`, `node`) to be reachable — either from the internet or a private registry mirror. A fully air-gapped host needs those images pre-pulled/pushed to an internal registry first; that's an infrastructure decision for the bank's ops team, not something a single command can paper over.

---

## 🧑‍💻 Alternative: Run Without Docker (Local Dev)
Useful for fast iteration on backend/frontend code without rebuilding containers each time.

### 1. Start infra only
```bash
docker-compose up -d --build
```
This starts Postgres (`5435`), Redis (`6380`), PgAdmin (`18080`), and RedisInsight (`15540`) from the **root** `docker-compose.yml` (infra services only — no backend/frontend containers).

This root compose file does **not** include Doris — but Doris is the pipeline's only execution engine, so it's still required even in this "fast local iteration" mode. Do the `vm.max_map_count` host setup from step 2 above, then start just the `doris` service from the full stack's compose file: `cd infra && docker compose up -d doris`. No further configuration needed — `backend/api/config.py`'s defaults (`DORIS_HOST=127.0.0.1`, `DORIS_MYSQL_PORT=9130`, `DORIS_HTTP_PORT=8130`) already match the host ports `infra/docker-compose.yml` publishes Doris on.

### 2. Run the backend
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```
*   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
*   *Tables are created automatically on startup.*

### 3. Run the frontend
```bash
cd frontend
npm install
npm run dev
```
*   **Dashboard**: [http://localhost:3000](http://localhost:3000) (redirects to `/dashboard`)
    *   Talks to the backend via `NEXT_PUBLIC_API_URL` (defaults to `http://localhost:8000`). Copy `frontend/.env.example` to `.env.local` if you need to point it elsewhere.

### Makefile shortcuts
```bash
make install      # installs backend (venv) + frontend (npm) deps
make docker-up    # starts Postgres, Redis, PgAdmin, RedisInsight
make dev          # runs backend (uvicorn :8000) + frontend (next :3000) concurrently
```
Other targets: `make test`, `make lint`, `make format`, `make db-migrate`, `make docker-down`, `make clean`. Run `make help` to list them all.

### Stopping (local dev mode)
```bash
docker-compose down      # or: make docker-down
# Ctrl+C the backend and frontend terminals
```

---

## 🎮 How to Demo (The "Wow" Flow)
1.  Go to **Upload** page. Drag & Drop a CSV (e.g., `challenging_er_200.csv`).
2.  Go to **Pipeline** page ([http://localhost:3000/pipeline](http://localhost:3000/pipeline)).
3.  Click **"Start New Run"**. Watch the animations as it processes!
4.  Go to **Graph** page ([http://localhost:3000/graph](http://localhost:3000/graph)).
5.  Click on any node to see the **"Bank Manager 360"** view (Risk Score, Balance, etc.).

---

## 🧠 System Architecture
*   **Ingest**: Loads the source dataset into Apache Doris; CSVs also land in Postgres (`customers_norm` table).
*   **Matching**: Deterministic, rule-based blocking and confidence scoring (Ruleset v2) compiled to Doris SQL — see the Settings page's rule editor.
*   **Graph**: Entity clusters and relationships are queried straight from Postgres (`entities`/`entity_members`) and rendered by the Identity Graph 360 dashboard.
*   **Auto-Healing**: on startup, `db_init.py` checks and repairs the database schema.

---

## ⚠️ Troubleshooting

### Doris container keeps restarting / never reports healthy
Almost always the host kernel setting from step 2 above. Check:
```bash
cat /proc/sys/vm/max_map_count   # must be >= 2000000
docker compose logs doris | tail -30
```
If the log mentions `vm.max_map_count`, set it (see step 2) and `docker compose restart doris`. Doris is the pipeline's only execution engine — there is no fallback that works without it, so this setting genuinely has to be applied, even on a restricted host (raise it with whoever manages the host if you don't have `sudo` yourself).

### Doris reports healthy but the first pipeline run fails ("Failed to find enough backend")
A transient race on a **freshly-created** Doris deployment (empty volumes) — the BE can report itself healthy on its HTTP endpoint slightly before it's finished registering its storage/disk info with the FE. Wait ~30 seconds after `docker compose ps` first shows `doris` healthy, then retry. Only seen on a brand new deployment (fresh volumes); does not recur on subsequent runs against the same Doris instance.

### Port already in use
On a shared machine, another project may already be using one of this stack's ports (`5433`, `8000`, `3000`, `8110`, `30011`, etc.). If `docker-compose up` or a dev server fails with `port is already allocated` / `address already in use`:
1.  Find the offending process/container: `ss -ltnp | grep :<port>` or `docker ps --format '{{.Names}}: {{.Ports}}'`.
2.  Either stop it, or remap **only your local copy**: edit the host-side port in `docker-compose.yml` (e.g. `"5433:5432"` → `"5435:5432"`) and update `backend/.env`'s `DATABASE_URL` (and `frontend/.env.local`'s `NEXT_PUBLIC_API_URL` if you also moved the backend port) to match.
3.  Don't commit personal port remaps — keep them local.

---

## 🛠️ Debugging & Database Access

### 🐘 Access PostgreSQL
If you need to peek at the raw data (tables like `runs`, `customers_norm`, `audit_events`), you have 3 options:

#### Option A: PgAdmin (Browser UI) - *Recommended*
1.  Go to **[http://localhost:18080](http://localhost:18080)**.
2.  **Login**: `admin@cuin.com` | `password123`.
3.  **Add Server** (if not present):
    *   **Host**: `postgres` (internal docker hostname)
    *   **Username**: `admin`
    *   **Password**: `password123`
    *   **Maintenance DB**: `cuin_db`
4.  Browse: `Servers > CUIN > Databases > cuin_db > Schemas > public > Tables`.

#### Option B: Terminal (CLI)
Jump directly into the container:
```bash
docker exec -it cuin-postgres psql -U admin -d cuin_db
```
*Useful commands:* `\dt` (list tables), `select * from runs;`, `\q` (quit).

#### Option C: External Tool (DBeaver / DataGrip)
Connect from your host machine:
*   **Host**: `localhost`
*   **Port**: `5433` (note the custom port!)
*   **User/Pass**: `admin` / `password123`
*   **Database**: `cuin_db`
