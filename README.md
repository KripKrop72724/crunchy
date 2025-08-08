# crunchy

Backend service for uploading massive Excel files, tracking progress via Redis, and ingesting into DuckDB. A simple React frontend is included for uploading files and monitoring progress in real time.

## Features
- **Streaming uploads** of arbitrarily large Excel files without exhausting memory.
- **Background ingestion** into DuckDB using the Excel extension.
- **Redis-backed progress tracking** with job polling endpoint.
- **API key authentication** on protected endpoints.
- **Health check** endpoint.
- Packaged with **Docker** and orchestrated via **Docker Compose**.
- Simple **React frontend** for authenticated uploads with real-time progress via WebSockets.

## Configuration
Create a `.env` file (see `.env.example`):

```env
API_KEY=changeme
UPLOAD_DIR=/data/uploads
DB_PATH=/data/analytics.duckdb
REDIS_URL=redis://redis:6379/0
```

## Running with Docker Compose
```bash
docker-compose up --build
```
The API will be available at `http://localhost:8000` and the frontend at `http://localhost:5173`.

Open the frontend in your browser, supply the API key, select a file, and click **Upload**. Upload progress and row ingestion counts will update live using the WebSocket status API.

## API
### Upload Excel file
`POST /upload`

Headers:
- `X-API-Key`: API key from `.env`.

Body: `multipart/form-data` with `file` field.

Response:
```json
{"job_id": "<uuid>"}
```

### Check status
`GET /status/{job_id}` with `X-API-Key` header.

Response example:
```json
{
  "status": "uploading",
  "uploaded": 5242880,
  "total": 10485760,
  "table": null,
  "error": null
}
```

### Health
`GET /health`

Returns `{ "status": "ok" }`.

## DuckDB Excel Extension
The container installs and loads the DuckDB `excel` extension at startup, enabling `read_excel_auto` for ingestion.

## Development
Install dependencies and run tests:
```bash
pip install -r requirements.txt
pytest
```

Frontend tests use Vitest:
```bash
cd frontend
npm install
npm test
```
