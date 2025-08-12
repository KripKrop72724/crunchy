# app/main.py
import uuid
from pathlib import Path
import asyncio
import json
import hashlib
import re
from typing import Any, Dict, List, Optional
from datetime import date, datetime

import aiofiles
import duckdb
import redis
from redis.exceptions import RedisError
from rq import Queue
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
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# ---- project config (ENV → objects) -----------------------------------------
from .config import API_KEY, DB_PATH, REDIS_URL, UPLOAD_DIR

# -----------------------------------------------------------------------------
# App & infra
# -----------------------------------------------------------------------------
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

# cache time for /query responses
QUERY_CACHE_TTL = 60
# ingestion batch size
BATCH_SIZE = 100_000

# -----------------------------------------------------------------------------
# SQL identifier safety & helpers
# -----------------------------------------------------------------------------
IDENT_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
INTERNAL_COLS = {"row_hash", "__source_file", "__ingested_at"}

def quote_ident(name: str) -> str:
    """Strict: validate and quote a *cleaned/trusted* SQL identifier."""
    if not IDENT_REGEX.match(name):
        raise HTTPException(400, f"Invalid identifier: {name!r}")
    return f'"{name}"'

def quote_any_ident(name: str) -> str:
    """Loose: safely quote *any* identifier (raw CSV header, may have spaces, symbols)."""
    return '"' + str(name).replace('"', '""') + '"'

def record_file(con: duckdb.DuckDBPyConnection, job_id: str, filename: str, file_hash: str) -> None:
    # Insert only if (filename, file_hash) is new. This avoids choosing a conflict target
    # and works on all DuckDB versions without ON CONFLICT support.
    con.execute(
        """
        INSERT INTO files(file_id, filename, file_hash)
        SELECT ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM files WHERE filename = ? AND file_hash = ?
        );
        """,
        (job_id, filename, file_hash, filename, file_hash),
    )


def clean_col(name: str) -> str:
    # lower → replace spaces/dashes with _ → drop non [A-Za-z0-9_]
    n = name.strip().lower().replace(" ", "_").replace("-", "_")
    n = re.sub(r"[^a-z0-9_]", "", n)
    if not n or n[0].isdigit():
        n = f"col_{n}" if n else "col"
    return n

def _json_default(obj: Any):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError

# richer job status helpers
def status_set(job_id: str, **kw):
    job_key = f"job:{job_id}"
    mapping = {}
    for k, v in kw.items():
        if isinstance(v, str):
            mapping[k] = v
        elif isinstance(v, (dict, list)):
            mapping[k] = json.dumps(v)
        else:
            mapping[k] = str(v)
    redis_client.hset(job_key, mapping=mapping)

def status_get(job_id: str) -> dict:
    job_key = f"job:{job_id}"
    data = redis_client.hgetall(job_key)
    if not data:
        return {}
    def to_int(x):
        try:
            return int(x)
        except:
            return 0
    def to_float(x):
        try:
            return float(x)
        except:
            return 0.0
    out = {
        "status": data.get("status"),
        "stage": data.get("stage"),
        "uploaded": to_int(data.get("uploaded")),
        "total": to_int(data.get("total")) or None,
        "rows_total": to_int(data.get("rows_total")),
        "rows_processed": to_int(data.get("rows_processed")),
        "rows_inserted": to_int(data.get("rows_inserted")),
        "rows_skipped": to_int(data.get("rows_skipped")),
        "progress": to_float(data.get("progress")),
        "error": data.get("error") or None,
        "file": data.get("file") or None,
        "started_at": data.get("started_at"),
        "ended_at": data.get("ended_at"),
    }
    return out

# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------
def verify_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

# -----------------------------------------------------------------------------
# Startup: ensure dirs & base tables (and migrate row_hash → UBIGINT)
# -----------------------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(DB_PATH, read_only=False) as con:
        # Create tables if missing (fresh DBs get UBIGINT immediately)
        con.execute("""
            CREATE TABLE IF NOT EXISTS dataset (
                row_hash      UBIGINT,
                __source_file VARCHAR,
                __ingested_at TIMESTAMP
            );
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id     VARCHAR PRIMARY KEY,
                filename    VARCHAR,
                file_hash   VARCHAR,
                uploaded_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(filename, file_hash)
            );
        """)

        # Check current type of row_hash
        info = con.execute("PRAGMA table_info('dataset')").fetchall()
        types = {r[1]: (r[2] or "").upper() for r in info}  # col_name -> TYPE

        # If legacy BIGINT, migrate to UBIGINT:
        if types.get("row_hash", "") != "UBIGINT":
            # 1) Drop the index that depends on the column type (if it exists)
            con.execute("DROP INDEX IF EXISTS idx_dataset_rowhash;")
            # 2) Alter the column type (support both syntaxes across DuckDB versions)
            try:
                con.execute("ALTER TABLE dataset ALTER COLUMN row_hash TYPE UBIGINT;")
            except duckdb.Error:
                con.execute("ALTER TABLE dataset ALTER COLUMN row_hash SET DATA TYPE UBIGINT;")

        # 3) Ensure unique index is present (idempotent)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_rowhash ON dataset(row_hash);")

# -----------------------------------------------------------------------------
# Pydantic models (simple pre-available filters)
# -----------------------------------------------------------------------------
class SimpleQueryRequest(BaseModel):
    filters: Dict[str, List[str]] = {}
    fields: Optional[List[str]] = None
    limit: conint(gt=0) = 100
    offset: conint(ge=0) = 0

class DistinctQuery(BaseModel):
    q: Optional[str] = None
    limit: conint(gt=0) = 200

# -----------------------------------------------------------------------------
# Helpers for DB access
# -----------------------------------------------------------------------------
def list_user_columns(con: duckdb.DuckDBPyConnection) -> List[str]:
    rows = con.execute("PRAGMA table_info(dataset)").fetchall()
    cols = [r[1] for r in rows if r[1] not in INTERNAL_COLS]
    return cols

def ensure_columns(con: duckdb.DuckDBPyConnection, cols: List[str]) -> None:
    # Add missing user columns to dataset (all VARCHAR)
    current = set(list_user_columns(con))
    for c in cols:
        if c not in current:
            con.execute(f'ALTER TABLE dataset ADD COLUMN {quote_ident(c)} VARCHAR;')

def compute_hash_expression(cols: List[str], prefix: str = "") -> str:
    """Return UBIGINT hash expression over cols (optionally prefixed)."""
    if prefix:
        items = ", ".join(f"coalesce({prefix}.{quote_ident(c)}, '')" for c in cols)
    else:
        items = ", ".join(f"coalesce({quote_ident(c)}, '')" for c in cols)
    return f"CAST(hash({items}) AS UBIGINT)"

# -----------------------------------------------------------------------------
# Background ingestion job (batched + progress)
# -----------------------------------------------------------------------------
def process_file(job_id: str, file_path: str, file_hash: str, orig_filename: str):
    rkey = f"job:{job_id}"
    status_set(job_id, status="processing", stage="reading", progress=0.0)

    try:
        staging = f"staging_{job_id.replace('-', '_')}"
        safe_staging = quote_ident(staging)

        with duckdb.connect(DB_PATH, read_only=False) as con:
            # double-check file-level dedup
            exists = con.execute(
                "SELECT 1 FROM files WHERE filename=? AND file_hash=? LIMIT 1",
                (orig_filename, file_hash)
            ).fetchone()
            if exists:
                status_set(job_id, status="completed", stage="completed", ended_at=datetime.utcnow().isoformat())
                return

            ext = Path(file_path).suffix.lower()
            if ext == ".csv":
                con.execute(f"""
                    CREATE OR REPLACE TABLE {safe_staging} AS
                    SELECT * FROM read_csv_auto('{file_path}', SAMPLE_SIZE=-1, HEADER=TRUE, normalize_names=FALSE);
                """)
            elif ext == ".parquet":
                con.execute(f"""
                    CREATE OR REPLACE TABLE {safe_staging} AS
                    SELECT * FROM parquet_scan('{file_path}');
                """)
            else:
                raise ValueError("Unsupported file type (csv or parquet only)")

            # normalize columns (rename raw headers → cleaned names)
            status_set(job_id, stage="schema_evolved")
            info = con.execute(f"PRAGMA table_info({safe_staging})").fetchall()
            orig_cols = [r[1] for r in info]
            mapping = {c: clean_col(c) for c in orig_cols}

            # ensure uniqueness after cleaning (append _1, _2 … if needed)
            seen = set()
            for k, v in list(mapping.items()):
                base, i = v, 1
                while mapping[k] in seen:
                    mapping[k] = f"{base}_{i}"
                    i += 1
                seen.add(mapping[k])

            # RENAME using *loose* quoting for original headers, strict for cleaned
            for orig, new in mapping.items():
                if orig != new:
                    con.execute(
                        f'ALTER TABLE {safe_staging} RENAME COLUMN {quote_any_ident(orig)} TO {quote_ident(new)};'
                    )

            new_cols = list(mapping.values())
            # ensure columns exist in dataset
            ensure_columns(con, new_cols)

            # union of user columns in dataset AFTER evolution
            all_user_cols = list_user_columns(con)
            all_user_cols.sort()

            # count rows to drive progress
            status_set(job_id, stage="counting")
            rows_total = con.execute(f"SELECT COUNT(*) FROM {safe_staging}").fetchone()[0]
            redis_client.hset(rkey, "rows_total", rows_total)

            if rows_total == 0:
                record_file(con, job_id, orig_filename, file_hash)
                status_set(job_id, status="completed", stage="completed", progress=1.0, ended_at=datetime.utcnow().isoformat())
                con.execute(f"DROP TABLE IF EXISTS {safe_staging}")
                return

            # add a stable row_number for batching
            con.execute(f'CREATE TEMP TABLE {staging}_rn AS SELECT *, row_number() OVER () AS __rn FROM {safe_staging};')

            # build per-column expressions for projection and hashing w/ alias s
            proj_parts = []
            hash_parts = []
            for c in all_user_cols:
                if c in new_cols:
                    proj_parts.append(f'CAST(s.{quote_ident(c)} AS VARCHAR) AS {quote_ident(c)}')
                    hash_parts.append(f"coalesce(CAST(s.{quote_ident(c)} AS VARCHAR), '')")
                else:
                    proj_parts.append(f'CAST(NULL AS VARCHAR) AS {quote_ident(c)}')
                    hash_parts.append("''")
            projection = ", ".join(proj_parts)
            # >>> ensure UBIGINT hash to match column type
            row_hash_expr = f"CAST(hash({', '.join(hash_parts)}) AS UBIGINT)"

            rows_processed = 0
            rows_inserted = 0
            rows_skipped = 0

            status_set(job_id, stage="ingesting", rows_processed=0, rows_inserted=0, rows_skipped=0)
            while rows_processed < rows_total:
                lo = rows_processed + 1
                hi = min(rows_processed + BATCH_SIZE, rows_total)

                before_cnt = con.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]

                user_cols_csv = ", ".join(quote_ident(c) for c in all_user_cols)
                target_cols_csv = f"{user_cols_csv}, row_hash, __source_file, __ingested_at"

                # Insert unique rows for this batch using anti-join on row_hash
                con.execute(f"""
                    INSERT INTO dataset ({target_cols_csv})
                    SELECT {projection},
                           {row_hash_expr} AS row_hash,
                           ? AS __source_file,
                           NOW() AS __ingested_at
                    FROM {staging}_rn s
                    WHERE s.__rn BETWEEN ? AND ?
                      AND NOT EXISTS (
                          SELECT 1 FROM dataset d WHERE d.row_hash = {row_hash_expr}
                      );
                """, [orig_filename, lo, hi])

                after_cnt = con.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
                inserted = max(0, after_cnt - before_cnt)

                rows_processed = hi
                rows_inserted += inserted
                rows_skipped = rows_processed - rows_inserted

                progress = rows_processed / rows_total if rows_total else 1.0
                status_set(
                    job_id,
                    rows_processed=rows_processed,
                    rows_inserted=rows_inserted,
                    rows_skipped=rows_skipped,
                    progress=progress
                )

            # record file & cleanup
            record_file(con, job_id, orig_filename, file_hash)
            con.execute(f"DROP TABLE IF EXISTS {safe_staging}")
            con.execute(f"DROP TABLE IF EXISTS {staging}_rn")

            status_set(job_id, status="completed", stage="completed", progress=1.0, ended_at=datetime.utcnow().isoformat())

    except Exception as e:
        status_set(job_id, status="failed", stage="failed", error=str(e), ended_at=datetime.utcnow().isoformat())

# -----------------------------------------------------------------------------
# Upload / Status
# -----------------------------------------------------------------------------
@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    _: None = Depends(verify_api_key),
):
    job_id = str(uuid.uuid4())
    dest_path = UPLOAD_DIR / f"{job_id}_{file.filename}"

    total = request.headers.get("content-length")
    total = int(total) if total and total.isdigit() else 0

    status_set(
        job_id,
        status="uploading",
        stage="uploading",
        uploaded=0,
        total=total,
        rows_total=0,
        rows_processed=0,
        rows_inserted=0,
        rows_skipped=0,
        progress=0.0,
        error="",
        file=file.filename,
        started_at=datetime.utcnow().isoformat(),
        ended_at="",
    )

    # stream save and sha256 on the fly
    hasher = hashlib.sha256()
    try:
        async with aiofiles.open(dest_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):
                hasher.update(chunk)
                await out.write(chunk)
                redis_client.hincrby(f"job:{job_id}", "uploaded", len(chunk))
                if total:
                    uploaded = int(redis_client.hget(f"job:{job_id}", "uploaded") or 0)
                    prog = min(1.0, uploaded / total)
                    status_set(job_id, progress=prog)
    except Exception as e:
        status_set(job_id, status="failed", stage="uploading", error=f"Upload error: {e}", ended_at=datetime.utcnow().isoformat())
        raise HTTPException(500, "Failed to upload file.")

    file_hash = hasher.hexdigest()

    # File-level dedup check
    with duckdb.connect(DB_PATH, read_only=True) as con:
        exists = con.execute(
            "SELECT 1 FROM files WHERE filename=? AND file_hash=? LIMIT 1",
            (file.filename, file_hash)
        ).fetchone()
    if exists:
        status_set(job_id, status="completed", stage="completed", progress=1.0, ended_at=datetime.utcnow().isoformat())
        return {"job_id": job_id, "skipped": True}

    # queue ingestion
    status_set(job_id, status="queued", stage="queued")
    queue.enqueue(process_file, job_id, str(dest_path), file_hash, file.filename)
    return {"job_id": job_id}

@app.get("/status/{job_id}")
def status(job_id: str, _: None = Depends(verify_api_key)):
    data = status_get(job_id)
    if not data.get("status"):
        raise HTTPException(404, "Unknown job_id")
    return JSONResponse(data)

# Optional SSE stream of status (handy if you don't want websockets)
@app.get("/status/stream/{job_id}")
async def status_stream(job_id: str, _: None = Depends(verify_api_key)):
    async def gen():
        prev = None
        while True:
            data = status_get(job_id)
            if not data.get("status"):
                yield "event: error\ndata: {\"error\":\"unknown job\"}\n\n"
                break
            if data != prev:
                payload = json.dumps(data)
                yield f"data: {payload}\n\n"
                prev = dict(data)
                if data.get("status") in {"completed", "failed"}:
                    break
            await asyncio.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream")

# -----------------------------------------------------------------------------
# Columns & distinct values (for dropdowns)
# -----------------------------------------------------------------------------
@app.get("/columns")
def columns(_: None = Depends(verify_api_key)):
    with duckdb.connect(DB_PATH, read_only=True) as con:
        cols = list_user_columns(con)
    return {"columns": cols}

@app.get("/distinct/{column}")
def distinct_values(
    column: str,
    q: Optional[str] = None,
    limit: conint(gt=0) = 200,
    _: None = Depends(verify_api_key),
):
    if column in INTERNAL_COLS:
        raise HTTPException(400, "Invalid column")
    col = quote_ident(column)  # strict: expect cleaned names on the API
    params: List[Any] = []
    where_sql = f"WHERE {col} IS NOT NULL"
    if q:
        where_sql = f"WHERE {col} IS NOT NULL AND {col} ILIKE ?"
        params.append(f"%{q}%")
    with duckdb.connect(DB_PATH, read_only=True) as con:
        rows = con.execute(
            f"SELECT DISTINCT {col} AS v FROM dataset {where_sql} ORDER BY v LIMIT ?",
            (*params, limit),
        ).fetchall()
    return {"values": [r[0] for r in rows]}

# -----------------------------------------------------------------------------
# Simple AND filters query
# -----------------------------------------------------------------------------
class SimpleQueryRequest(BaseModel):
    filters: Dict[str, List[str]] = {}
    fields: Optional[List[str]] = None
    limit: conint(gt=0) = 100
    offset: conint(ge=0) = 0

@app.post("/query")
def simple_query(body: SimpleQueryRequest, _: None = Depends(verify_api_key)):
    # Normalize empty fields list → None (treat as “all user columns”)
    if body.fields is not None and len(body.fields) == 0:
        body.fields = None

    # cache
    cache_key = hashlib.sha256(json.dumps(body.dict(), sort_keys=True).encode()).hexdigest()
    try:
        cached = redis_client.get(cache_key)
    except RedisError:
        cached = None
    if cached:
        return json.loads(cached)

    with duckdb.connect(DB_PATH, read_only=True) as con:
        user_cols_list = list_user_columns(con)
        user_cols = set(user_cols_list)

        # If there are no user columns at all (fresh DB), return empty result gracefully
        if not user_cols_list:
            result = {"rows": [], "total": 0}
            try:
                redis_client.setex(cache_key, QUERY_CACHE_TTL, json.dumps(result))
            except RedisError:
                pass
            return result

        # projection
        if body.fields:
            for c in body.fields:
                if c not in user_cols:
                    raise HTTPException(400, f"Unknown column: {c}")
            fields = body.fields
        else:
            fields = sorted(user_cols_list)

        # Defensive: if fields resolved to empty (shouldn't happen), return empty result
        if not fields:
            result = {"rows": [], "total": 0}
            try:
                redis_client.setex(cache_key, QUERY_CACHE_TTL, json.dumps(result))
            except RedisError:
                pass
            return result

        select_cols = ", ".join(quote_ident(c) for c in fields)

        # WHERE col IN (...) AND col2 IN (...)
        where_clauses = []
        params: List[Any] = []
        for c, vals in body.filters.items():
            if c not in user_cols:
                raise HTTPException(400, f"Unknown column: {c}")
            if not vals:
                continue
            placeholders = ", ".join("?" for _ in vals)
            where_clauses.append(f"{quote_ident(c)} IN ({placeholders})")
            params.extend(vals)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        data_sql = f"""
            SELECT {select_cols}
              FROM dataset
              {where_sql}
             LIMIT ? OFFSET ?;
        """
        params.extend([body.limit, body.offset])

        cursor = con.execute(data_sql, params)
        rows = cursor.fetchall()
        col_names = [d[0] for d in cursor.description]

        # Total for pagination (same filters)
        count_sql = f"SELECT COUNT(*) FROM dataset {where_sql}"
        total = con.execute(count_sql, params[:-2]).fetchone()[0]

    result = {"rows": [dict(zip(col_names, r)) for r in rows], "total": total}
    try:
        redis_client.setex(cache_key, QUERY_CACHE_TTL, json.dumps(result))
    except RedisError:
        pass
    return result

# -----------------------------------------------------------------------------
# Streaming (optional)
# -----------------------------------------------------------------------------
@app.post("/stream")
def stream_query(body: SimpleQueryRequest, _: None = Depends(verify_api_key)):
    # Normalize empty fields list → None
    if body.fields is not None and len(body.fields) == 0:
        body.fields = None

    with duckdb.connect(DB_PATH, read_only=True) as con:
        user_cols_list = list_user_columns(con)
        user_cols = set(user_cols_list)

        # No user columns → return an empty stream immediately
        if not user_cols_list:
            def empty():
                if False:
                    yield ""
                return
            return StreamingResponse(empty(), media_type="application/x-ndjson")

        if body.fields:
            for c in body.fields:
                if c not in user_cols:
                    raise HTTPException(400, f"Unknown column: {c}")
            fields = body.fields
        else:
            fields = sorted(user_cols_list)

        if not fields:
            def empty():
                if False:
                    yield ""
                return
            return StreamingResponse(empty(), media_type="application/x-ndjson")

        select_cols = ", ".join(quote_ident(c) for c in fields)

        where_clauses = []
        params: List[Any] = []
        for c, vals in body.filters.items():
            if c not in user_cols:
                raise HTTPException(400, f"Unknown column: {c}")
            if not vals:
                continue
            placeholders = ", ".join("?" for _ in vals)
            where_clauses.append(f"{quote_ident(c)} IN ({placeholders})")
            params.extend(vals)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        data_sql = f"SELECT {select_cols} FROM dataset {where_sql};"

    def gen():
        with duckdb.connect(DB_PATH, read_only=True) as con2:
            cur = con2.execute(data_sql, params)
            names = [d[0] for d in cur.description]
            try:
                while True:
                    row = cur.fetchone()
                    if row is None:
                        break
                    yield json.dumps(dict(zip(names, row)), default=_json_default) + "\n"
            finally:
                pass

    return StreamingResponse(gen(), media_type="application/x-ndjson")

# -----------------------------------------------------------------------------
# Live job status (WebSocket)
# -----------------------------------------------------------------------------
@app.websocket("/ws/status/{job_id}")
async def ws_status(websocket: WebSocket, job_id: str):
    await websocket.accept()
    prev = None
    try:
        while True:
            data = status_get(job_id)
            if not data.get("status"):
                await websocket.send_json({"error": "unknown job"})
                break
            if data != prev:
                await websocket.send_json(data)
                prev = dict(data)
                if data.get("status") in {"completed", "failed"}:
                    break
            await asyncio.sleep(1)
    finally:
        await websocket.close()

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            con.execute("SELECT 1")
        redis_client.ping()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
