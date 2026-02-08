# CUIN v2 Control Plane - Backend Service

This is the FastAPI-based backend for the CUIN v2 Identity Intelligence Platform.

## Prerequisites

- **Python**: 3.9+
- **PostgreSQL**: Running locally or accessible via URL
- **Neo4j**: (Optional but recommended) Running locally or accessible via URI

## Local Development Setup

1.  **Navigate to the backend directory**:
    ```bash
    cd backend
    ```

2.  **Create and activate a virtual environment** (if not already done):
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuration**:
    -   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    -   Edit `.env` to match your local database credentials (Postgres, Neo4j).

## Running the Server

### Development Mode (Hot Reload)
This will auto-reload the server when you make code changes.

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

*Note: If `uvicorn` is not in your path, use local venv binary:*
```bash
./venv/bin/uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### Production Mode
For stable execution without reload monitoring.

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Running via Docker

1.  **Build the image**:
    ```bash
    docker build -t cuin-backend .
    ```

2.  **Run the container**:
    ```bash
    docker run -p 8000:8000 --env-file .env cuin-backend
    ```

## API Documentation

Once the server is running, interactive API docs are available at:
-   **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
-   **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

## Troubleshooting

-   **"Command not found: uvicorn"**: Ensure your virtual environment is activated (`source venv/bin/activate`).
-   **Database connection error**: Check `DATABASE_URL` in `.env` and ensure Postgres is running.
-   **Neo4j connection error**: Check `NEO4J_URI` and credentials in `.env`.
