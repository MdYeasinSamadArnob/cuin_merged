# CUIN v2 - Control Plane

**CUIN v2** is an advanced **Entity Resolution (ER)** and **Identity Graph** platform. It links customer records from different systems (like CRM, Loans, Website) to create a "Single Customer View".

---

## 🚀 Quick Start (For Interns)
Follow these steps to get the system running in 5 minutes!

### 1. Prerequisites
You need **Docker** and **Docker Compose** installed. That's it!

### 2. Start the Infrastructure
Open your terminal in this folder and run:
```bash
sudo docker-compose up -d --build
```
This starts:
*   **Postgres** (Database) - Port 5433
*   **Neo4j** (Graph Database) - Port 7474
*   **Redis** (Cache) - Port 6380
*   **Management UIs**:
    *   PgAdmin (Postgres UI): [http://localhost:18080](http://localhost:18080) (Email: `admin@cuin.com`, Pass: `password123`)
    *   RedisInsight: [http://localhost:15540](http://localhost:15540)

### 3. Run the Backend (API)
The backend manages the logic. Open a new terminal:
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```
*   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
*   **Health Check**: [http://localhost:8000/health/ready](http://localhost:8000/health/ready)
    *   *Note: Tables are created automatically on startup!*

### 4. Run the Frontend (Dashboard)
The visualized dashboard. Open another terminal:
```bash
cd frontend
npm install
npm run dev
```
*   **Dashboard**: [http://localhost:3001](http://localhost:3001) (redirects to `/dashboard`)
    *   *Note: the dev server runs on port **3001**, not 3000 — see `frontend/package.json`.*
    *   The frontend talks to the backend via `NEXT_PUBLIC_API_URL` (defaults to `http://localhost:8000`). Copy `frontend/.env.example` to `.env.local` if you need to point it elsewhere.

---

## ⚡ One-Line Alternative (Makefile)
A `Makefile` wraps the same steps:
```bash
make install      # installs backend (venv) + frontend (npm) deps
make docker-up    # starts Postgres, Neo4j, Redis, PgAdmin, RedisInsight
make dev          # runs backend (uvicorn :8000) + frontend (next :3001) concurrently
```
Other useful targets: `make test`, `make lint`, `make format`, `make db-migrate`, `make docker-down`, `make clean`. Run `make help` to list them all.

### Stopping everything
```bash
docker-compose down      # or: make docker-down
# Ctrl+C the backend and frontend terminals
```

---

## 🎮 How to Demo (The "Wow" Flow)
1.  Go to **Upload** page. Drag & Drop a CSV (e.g., `challenging_er_200.csv`).
2.  Go to **Pipeline** page ([http://localhost:3001/pipeline](http://localhost:3001/pipeline)).
3.  Click **"Start New Run"**. Watch the animations as it processes!
4.  Go to **Graph** page ([http://localhost:3001/graph](http://localhost:3001/graph)).
5.  Click on any node to see the **"Bank Manager 360"** view (Risk Score, Balance, etc.).

---

## 🧠 System Architecture
*   **Ingest**: Loads CSVs into Postgres (`customers_norm` table).
*   **Matching**: Uses **Splink** (Probabilistic Matching) to find duplicates.
*   **Graph**: Projects the results into **Neo4j** for visualization.
*   **Auto-Healing**: on startup, `db_init.py` checks and repairs the database schema.

---

## ⚠️ Troubleshooting

### Port already in use
On a shared machine, another project may already be using one of this stack's ports (`5433`, `7474`, `7687`, `8000`, `3001`, etc.). If `docker-compose up` or a dev server fails with `port is already allocated` / `address already in use`:
1.  Find the offending process/container: `ss -ltnp | grep :<port>` or `docker ps --format '{{.Names}}: {{.Ports}}'`.
2.  Either stop it, or remap **only your local copy**: edit the host-side port in `docker-compose.yml` (e.g. `"5433:5432"` → `"5435:5432"`) and update `backend/.env`'s `DATABASE_URL`/`NEO4J_URI` (and `frontend/.env.local`'s `NEXT_PUBLIC_API_URL` if you also moved the backend port) to match.
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
