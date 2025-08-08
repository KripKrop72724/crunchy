import uuid
from pathlib import Path
import asyncio
import json
import hashlib
import re

import aiofiles
import duckdb
import redis
from rq import Queue
from typing import Any, List, Literal, Optional, Union
from datetime import date, datetime
from pydantic import BaseModel, conint, field_validator
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
from fastapi.responses import JSONResponse, StreamingResponse
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
QUERY_CACHE_TTL = 60


# Regular expression for validating SQL identifiers
IDENT_REGEX = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def quote_ident(name: str) -> str:
    """Validate and quote an SQL identifier."""
    if not IDENT_REGEX.match(name):
        raise HTTPException(400, f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _json_default(obj: Any):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError


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
        safe_table = quote_ident(table)
        conn = duckdb.connect(DB_PATH)
        ext = Path(file_path).suffix.lower()

        if ext == ".csv":
            conn.execute(
                f"CREATE TABLE {safe_table} AS SELECT * FROM read_csv_auto('{file_path}', SAMPLE_SIZE=-1, HEADER=TRUE, normalize_names=FALSE)"
            )
        elif ext == ".parquet":
            conn.execute(
                f"CREATE TABLE {safe_table} AS SELECT * FROM parquet_scan('{file_path}')"
            )
        else:
            raise ValueError("Unsupported file type")

        for col in conn.execute(f"PRAGMA table_info({safe_table})").fetchall():
            clean = col[1].lower().replace(" ", "_").replace("-", "_")
            orig = col[1].replace('"', '""')
            conn.execute(
                f'ALTER TABLE {safe_table} RENAME COLUMN "{orig}" TO "{clean}"'
            )

        rows = conn.execute(f"SELECT COUNT(*) FROM {safe_table}").fetchone()[0]
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
    op: Literal[
        "eq",
        "neq",
        "lt",
        "lte",
        "gt",
        "gte",
        "like",
        "ilike",
        "ieq",
        "in",
        "between",
        "in_range",
        "is_null",
        "is_not_null",
    ]
    value: Any = None


class LogicGroup(BaseModel):
    logic: List[Union[str, "LogicGroup", Filter]]

    @field_validator("logic")
    def check_logic(cls, v):  # pragma: no cover - validation
        if not v or not isinstance(v[0], str) or v[0] not in {"AND", "OR"}:
            raise ValueError("logic must start with 'AND' or 'OR'")
        if len(v) < 2:
            raise ValueError("logic must contain at least one condition")
        return v


LogicGroup.model_rebuild()


class OrderBy(BaseModel):
    column: str
    direction: Literal["asc", "desc"] = "asc"


class QueryRequest(BaseModel):
    filters: List[Filter] = []
    logical_operator: Literal["AND", "OR"] = "AND"
    logic: Optional[LogicGroup] = None
    order_by: Optional[OrderBy] = None
    limit: conint(gt=0) = 100
    offset: conint(ge=0) = 0
    fields: Optional[List[str]] = None

    @field_validator("logic", mode="before")
    def wrap_logic(cls, v):  # pragma: no cover - validation
        if isinstance(v, list):
            return {"logic": v}
        return v


def _prepare_query(table_name: str, req: QueryRequest):
    conn = duckdb.connect(database=DB_PATH, read_only=True)
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    if table_name not in tables:
        conn.close()
        raise HTTPException(404, "Unknown table")

    safe_table = quote_ident(table_name)

    rows_info = conn.execute(f"PRAGMA table_info({safe_table})").fetchall()
    columns = [r[1] for r in rows_info]
    if req.order_by and req.order_by.column not in columns:
        conn.close()
        raise HTTPException(400, f"Invalid column: {req.order_by.column}")
    if req.fields:
        for col in req.fields:
            if col not in columns:
                conn.close()
                raise HTTPException(400, f"Invalid column: {col}")
        select_cols = ", ".join(quote_ident(c) for c in req.fields)
    else:
        select_cols = "*"

    col_types = {r[1]: r[2] for r in rows_info}

    def parse_value(column: str, value: Any) -> Any:
        col_type = col_types.get(column, "").upper()
        if isinstance(value, str) and col_type in {"DATE", "TIMESTAMP"}:
            try:
                if col_type == "DATE":
                    return date.fromisoformat(value)
                else:
                    return datetime.fromisoformat(value)
            except Exception:
                conn.close()
                raise HTTPException(400, f"Invalid date: {value}")
        return value

    def build_filter(flt: Filter):
        if flt.column not in columns:
            conn.close()
            raise HTTPException(400, f"Invalid column: {flt.column}")
        col = quote_ident(flt.column)
        if flt.op == "in":
            if not isinstance(flt.value, list) or not flt.value:
                conn.close()
                raise HTTPException(400, "List value required")
            vals = [parse_value(flt.column, v) for v in flt.value]
            placeholders = ", ".join("?" for _ in vals)
            return f"{col} IN ({placeholders})", vals
        elif flt.op == "between":
            if not isinstance(flt.value, list) or len(flt.value) != 2:
                conn.close()
                raise HTTPException(400, "Two values required")
            vals = [parse_value(flt.column, v) for v in flt.value]
            return f"{col} BETWEEN ? AND ?", vals
        elif flt.op == "in_range":
            if not isinstance(flt.value, list) or not flt.value:
                conn.close()
                raise HTTPException(400, "List value required")
            vals = [parse_value(flt.column, v) for v in flt.value]
            lo, hi = min(vals), max(vals)
            return f"{col} BETWEEN ? AND ?", [lo, hi]
        elif flt.op == "is_null":
            return f"{col} IS NULL", []
        elif flt.op == "is_not_null":
            return f"{col} IS NOT NULL", []
        elif flt.op == "ilike":
            v = flt.value if isinstance(flt.value, str) else str(flt.value)
            return f"{col} ILIKE ?", [f"%{v}%"]
        elif flt.op == "ieq":
            v = flt.value if isinstance(flt.value, str) else str(flt.value)
            return f"LOWER({col}) = LOWER(?)", [v]
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
            value = parse_value(flt.column, flt.value)
            if flt.op == "like":
                value = f"%{value}%"
            return f"{col} {op_sql} ?", [value]

    def build_logic(group: LogicGroup):
        items = group.logic
        op = items[0]
        clauses = []
        params: List[Any] = []
        for item in items[1:]:
            if isinstance(item, LogicGroup):
                c, p = build_logic(item)
            elif isinstance(item, Filter):
                c, p = build_filter(item)
            else:
                conn.close()
                raise HTTPException(400, "Invalid logic element")
            clauses.append(c)
            params.extend(p)
        return "(" + f" {op} ".join(clauses) + ")", params

    logic_obj = req.logic
    if logic_obj is None and req.filters:
        logic_obj = LogicGroup(logic=[req.logical_operator, *req.filters])

    where_sql = ""
    params: List[Any] = []
    if logic_obj:
        clause, params = build_logic(logic_obj)
        where_sql = "WHERE " + clause

    order_sql = ""
    if req.order_by:
        col = quote_ident(req.order_by.column)
        order_sql = f"ORDER BY {col} {req.order_by.direction.upper()}"

    data_sql = (
        f"SELECT {select_cols} FROM {safe_table} {where_sql} {order_sql} LIMIT ? OFFSET ?"
    )
    data_params = params + [req.limit, req.offset]
    count_sql = f"SELECT COUNT(*) FROM {safe_table} {where_sql}"
    return conn, data_sql, data_params, count_sql, params


def _run_query(table_name: str, req: QueryRequest):
    conn, data_sql, data_params, count_sql, params = _prepare_query(table_name, req)
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
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        return tables

    tables = await asyncio.to_thread(_list)
    return {"tables": tables}


@app.get("/tables/{table_name}/columns")
async def table_columns(table_name: str, _: None = Depends(verify_api_key)):
    def _cols():
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        try:
            safe_table = quote_ident(table_name)
            rows = conn.execute(f"PRAGMA table_info({safe_table})").fetchall()
        except HTTPException:
            conn.close()
            raise
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
    key = hashlib.sha256((req_json + table_name).encode()).hexdigest()
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    rows, total = await asyncio.to_thread(_run_query, table_name, req)
    result = {"rows": rows, "total": total}
    redis_client.setex(key, QUERY_CACHE_TTL, json.dumps(result))
    return result


@app.post("/tables/{table_name}/stream")
async def stream_table(
    table_name: str, req: QueryRequest, _: None = Depends(verify_api_key)
):
    conn, data_sql, data_params, _, _ = _prepare_query(table_name, req)

    def generator():
        cursor = conn.execute(data_sql, data_params)
        cols = [d[0] for d in cursor.description]
        try:
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield json.dumps(dict(zip(cols, row)), default=_json_default) + "\n"
        finally:
            conn.close()

    return StreamingResponse(generator(), media_type="application/x-ndjson")


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
