# crunchy

Backend service for uploading massive Excel files, tracking progress via Redis, and ingesting into DuckDB. A simple React frontend is included for uploading files and monitoring progress in real time.

## Features
- **Streaming uploads** of arbitrarily large Excel files without exhausting memory.
- **Background ingestion** into DuckDB using the Excel extension.
- **Redis-backed progress tracking** with job polling endpoint.
- **API key authentication** on protected endpoints.
- **Health check** endpoint.
- Packaged with **Docker** and orchestrated via **Docker Compose**.
 - Simple **React frontend** for authenticated uploads, real-time progress, and filtering uploaded data via the query APIs.

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

Open the frontend in your browser, supply the API key, select a file, and click **Upload**. When the upload finishes the first few rows are displayed and you can add filters to refine the results via the new query endpoints. Upload progress and row ingestion counts update live using the WebSocket status API.

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

### List tables
`GET /tables` with `X-API-Key` header.

Returns:
```json
{"tables": ["import_123", ...]}
```

### List columns for a table
`GET /tables/{table}/columns` with `X-API-Key` header.

Returns:
```json
{"columns": ["id", "name", ...]}
```

### Query table with filters
`POST /tables/{table}/query` with `X-API-Key` header.

Body example:
```json
{
  "filters": [
    {"column": "age", "op": "gte", "value": 30},
    {"column": "country", "op": "eq", "value": "UAE"}
  ],
  "logical_operator": "AND",
  "order_by": {"column": "age", "direction": "desc"},
  "limit": 100,
  "offset": 0,
  "fields": ["id", "age", "country"]
}
```

Response:
```json
{"rows": [{"id": 1, "age": 42, "country": "UAE"}], "total": 1}
```

Supported filter operators include:

- `eq`, `neq`, `lt`, `lte`, `gt`, `gte`
- `like` (case-sensitive contains)
- `ilike` (case-insensitive contains)
- `ieq` (case-insensitive equals)
- `in`, `between`, `in_range`
- `is_null`, `is_not_null`

Examples:

- Case-insensitive contains:

  ```json
  { "column": "name", "op": "ilike", "value": "smith" }
  ```

  generates `WHERE "name" ILIKE '%smith%'`

- Case-insensitive equals:

  ```json
  { "column": "status", "op": "ieq", "value": "ACTIVE" }
  ```

  generates `WHERE LOWER("status") = LOWER('ACTIVE')`

Repeated queries are cached in Redis for speed, and specifying `fields` trims unused columns to reduce I/O.

### Stream query results
`POST /tables/{table}/stream` with `X-API-Key` header.

This returns an `application/x-ndjson` stream where each line is a JSON object representing a row. Example:

```
curl -N -H "X-API-Key: changeme" -X POST \
  http://localhost:8000/tables/mytable/stream \
  -d '{"limit":1000,"offset":0}'
```

```
{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}
```

Use this for exporting large result sets without loading them all into memory.

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
