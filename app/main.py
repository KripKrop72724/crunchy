import uuid
from pathlib import Path
import asyncio
import json
from functools import lru_cache

import aiofiles
import duckdb
import redis
from rq import Queue
from typing import Any, List, Literal, Optional
from pydantic import BaseModel, conint
from fastapi import (
    Depends,
    FastAPI,
    File,
    Header,
    HTTPException,
    Request,
    UploadFile,
    WebSocket,
)
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .config import API_KEY, DB_PATH, REDIS_URL, UPLOAD_DIR

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
queue = Queue(connection=redis_client)


# ---------------------------------------------------------------------------
# Utility & startup
# ---------------------------------------------------------------------------

def verify_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


@app.on_event("startup")
def startup_event():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # Ensure the database file exists
    conn = duckdb.connect(DB_PATH)
    conn.close()


# ---------------------------------------------------------------------------
# Background processing
# ---------------------------------------------------------------------------

def process_file(job_id: str, file_path: str):
    job_key = f"job:{job_id}"
    redis_client.hset(job_key, mapping={"status": "processing", "rows": 0})
    try:
        table = f"import_{job_id.replace('-', '_')}"
        conn = duckdb.connect(DB_PATH)
        ext = Path(file_path).suffix.lower()

        if ext == ".csv":
            conn.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{file_path}', SAMPLE_SIZE=-1)"
            )
        elif ext == ".parquet":
            conn.execute(
                f"CREATE TABLE {table} AS SELECT * FROM parquet_scan('{file_path}')"
            )
        else:
            raise ValueError("Unsupported file type")

        rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        redis_client.hset(job_key, "rows", rows)
        conn.close()
        redis_client.hset(job_key, mapping={"status": "completed", "table": table})
    except Exception as e:  # pragma: no cover - error path
        redis_client.hset(job_key, mapping={"status": "failed", "error": str(e)})


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


class Filter(BaseModel):
    column: str
    op: Literal["eq", "neq", "lt", "lte", "gt", "gte", "like", "in", "between"]
    value: Any


class OrderBy(BaseModel):
    column: str
    direction: Literal["asc", "desc"] = "asc"


class QueryRequest(BaseModel):
    filters: List[Filter] = []
    logical_operator: Literal["AND", "OR"] = "AND"
    order_by: Optional[OrderBy] = None
    limit: conint(gt=0) = 100
    offset: conint(ge=0) = 0
    fields: Optional[List[str]] = None


@lru_cache(maxsize=128)
def _run_query_cached(table_name: str, req_json: str):
    req_dict = json.loads(req_json)
    req = QueryRequest(**req_dict)
    return _run_query(table_name, req)


def _run_query(table_name: str, req: QueryRequest):
    conn = duckdb.connect(DB_PATH)
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    if table_name not in tables:
        conn.close()
        raise HTTPException(404, "Unknown table")

    rows_info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns = [r[1] for r in rows_info]
    for f in req.filters:
        if f.column not in columns:
            conn.close()
            raise HTTPException(400, f"Invalid column: {f.column}")
    if req.order_by and req.order_by.column not in columns:
        conn.close()
        raise HTTPException(400, f"Invalid column: {req.order_by.column}")
    if req.fields:
        for col in req.fields:
            if col not in columns:
                conn.close()
                raise HTTPException(400, f"Invalid column: {col}")
        select_cols = ", ".join(req.fields)
    else:
        select_cols = "*"

    clauses = []
    params: List[Any] = []
    for flt in req.filters:
        if flt.op == "in":
            if not isinstance(flt.value, list) or not flt.value:
                conn.close()
                raise HTTPException(400, "List value required")
            placeholders = ", ".join("?" for _ in flt.value)
            clauses.append(f"{flt.column} IN ({placeholders})")
            params.extend(flt.value)
        elif flt.op == "between":
            if not isinstance(flt.value, list) or len(flt.value) != 2:
                conn.close()
                raise HTTPException(400, "Two values required")
            clauses.append(f"{flt.column} BETWEEN ? AND ?")
            params.extend(flt.value)
        else:
            op_sql = {
                "eq": "=",
                "neq": "<>",
                "lt": "<",
                "lte": "<=",
                "gt": ">",
                "gte": ">=",
                "like": "LIKE",
            }[flt.op]
            value = flt.value
            if flt.op == "like":
                value = f"%{value}%"
            clauses.append(f"{flt.column} {op_sql} ?")
            params.append(value)

    where_sql = ""
    if clauses:
        joiner = f" {req.logical_operator} "
        where_sql = "WHERE " + joiner.join(clauses)

    order_sql = ""
    if req.order_by:
        order_sql = f"ORDER BY {req.order_by.column} {req.order_by.direction.upper()}"

    data_sql = (
        f"SELECT {select_cols} FROM {table_name} {where_sql} {order_sql} LIMIT ? OFFSET ?"
    )
    data_params = params + [req.limit, req.offset]
    count_sql = f"SELECT COUNT(*) FROM {table_name} {where_sql}"

    cursor = conn.execute(data_sql, data_params)
    rows = cursor.fetchall()
    col_names = [d[0] for d in cursor.description]
    total = conn.execute(count_sql, params).fetchone()[0]
    conn.close()

    result_rows = [dict(zip(col_names, row)) for row in rows]
    return result_rows, total

@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    _: None = Depends(verify_api_key),
):
    job_id = str(uuid.uuid4())
    filename = f"{job_id}_{file.filename}"
    dest_path = UPLOAD_DIR / filename

    total = request.headers.get("content-length")
    total = int(total) if total and total.isdigit() else 0

    job_key = f"job:{job_id}"
    redis_client.hset(job_key, mapping={
        "status": "uploading",
        "uploaded": 0,
        "total": total,
        "rows": 0,
        "table": "",
        "error": "",
    })

    try:
        async with aiofiles.open(dest_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):
                await out.write(chunk)
                redis_client.hincrby(job_key, "uploaded", len(chunk))
    except Exception as e:  # pragma: no cover - error path
        redis_client.hset(job_key, mapping={"status": "failed", "error": f"Upload error: {e}"})
        raise HTTPException(500, "Failed to upload file.")

    redis_client.hset(job_key, mapping={"status": "queued"})
    queue.enqueue(process_file, job_id, str(dest_path))
    return {"job_id": job_id}


@app.get("/status/{job_id}")
def status(job_id: str, _: None = Depends(verify_api_key)):
    job_key = f"job:{job_id}"
    data = redis_client.hgetall(job_key)
    if not data:
        raise HTTPException(404, "Unknown job_id")
    # Convert numeric fields
    response = {
        "status": data.get("status"),
        "uploaded": int(data.get("uploaded", 0)),
        "total": int(data.get("total", 0)) or None,
        "rows": int(data.get("rows", 0)),
        "table": data.get("table") or None,
        "error": data.get("error") or None,
    }
    return JSONResponse(response)


@app.get("/tables")
async def list_tables(_: None = Depends(verify_api_key)):
    def _list():
        conn = duckdb.connect(DB_PATH)
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        return tables

    tables = await asyncio.to_thread(_list)
    return {"tables": tables}


@app.get("/tables/{table_name}/columns")
async def table_columns(table_name: str, _: None = Depends(verify_api_key)):
    def _cols():
        conn = duckdb.connect(DB_PATH)
        try:
            rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        except Exception:
            conn.close()
            raise HTTPException(404, "Unknown table")
        if not rows:
            conn.close()
            raise HTTPException(404, "Unknown table")
        columns = [r[1] for r in rows]
        conn.close()
        return columns

    columns = await asyncio.to_thread(_cols)
    return {"columns": columns}


@app.post("/tables/{table_name}/query")
async def query_table(
    table_name: str, req: QueryRequest, _: None = Depends(verify_api_key)
):
    req_json = json.dumps(req.dict(), sort_keys=True)
    rows, total = await asyncio.to_thread(_run_query_cached, table_name, req_json)
    return {"rows": rows, "total": total}


@app.websocket("/ws/status/{job_id}")
async def ws_status(websocket: WebSocket, job_id: str):
    await websocket.accept()
    job_key = f"job:{job_id}"
    prev = None
    try:
        while True:
            data = redis_client.hgetall(job_key)
            if not data:
                await websocket.send_json({"error": "unknown job"})
                break
            if data != prev:
                response = {
                    "status": data.get("status"),
                    "uploaded": int(data.get("uploaded", 0)),
                    "total": int(data.get("total", 0)) or None,
                    "rows": int(data.get("rows", 0)),
                    "table": data.get("table") or None,
                    "error": data.get("error") or None,
                }
                await websocket.send_json(response)
                prev = data
                if data.get("status") in {"completed", "failed"}:
                    break
            await asyncio.sleep(1)
    finally:
        await websocket.close()


@app.get("/health")
def health():
    return {"status": "ok"}
