# app/main.py
import uuid
from pathlib import Path
import asyncio
import json
import hashlib
import re
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime
import os
import time
import random
from itertools import islice
from contextlib import contextmanager

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
    Response
)
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

# ---- project config (ENV → objects) -----------------------------------------
from .config import API_KEY, DB_PATH, REDIS_URL, UPLOAD_DIR

# -----------------------------------------------------------------------------
# Logging (minimal but useful)
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("crunchy")

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
    expose_headers=["*"],
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

# cache/keying helper: bump whenever dataset/files change
DATASET_VERSION_KEY = "dataset:version"

def _get_dataset_version() -> int:
    try:
        return int(redis_client.get(DATASET_VERSION_KEY) or 0)
    except RedisError:
        return 0

def _bump_dataset_version() -> None:
    try:
        redis_client.incr(DATASET_VERSION_KEY)
    except RedisError as e:
        log.warning("Failed to bump dataset version: %s", e)


def quote_ident(name: str) -> str:
    """Strict: validate and quote a *cleaned/trusted* SQL identifier."""
    if not IDENT_REGEX.match(name):
        raise HTTPException(400, f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _nonempty_predicate(cols: List[str], alias: str = "s") -> str:
    """
    TRUE when at least one of the given columns is non-empty after TRIM.
    Uses only provided `cols` (present in the new file).
    """
    if not cols:
        return "FALSE"
    pieces = []
    for c in cols:
        pieces.append(f"(TRIM(CAST({alias}.{quote_ident(c)} AS VARCHAR)) <> '')")
    return " OR ".join(pieces)


def quote_any_ident(name: str) -> str:
    """Loose: safely quote *any* identifier (raw CSV header, may have spaces/symbols)."""
    return '"' + str(name).replace('"', '""') + '"'


def _sql_literal(s: str) -> str:
    return s.replace("'", "''")


def clean_col(name: str) -> str:
    n = (name or "").strip().lower().replace(" ", "_").replace("-", "_")
    n = re.sub(r"[^a-z0-9_]", "", n)
    if not n or n[0].isdigit():
        n = f"col_{n}" if n else "col"
    return n


def _json_default(obj: Any):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError


# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------
def verify_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


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
    try:
        redis_client.hset(job_key, mapping=mapping)
    except RedisError as e:
        log.warning("Redis hset failed: %s", e)


def status_get(job_id: str) -> dict:
    job_key = f"job:{job_id}"
    try:
        data = redis_client.hgetall(job_key)
    except RedisError:
        data = {}
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


def _maybe_optimize(con: duckdb.DuckDBPyConnection) -> None:
    """Best-effort storage maintenance across DuckDB versions."""
    for stmt in ("VACUUM", "PRAGMA checkpoint"):
        try:
            con.execute(f"{stmt};")
            return
        except duckdb.Error:
            continue


# -----------------------------------------------------------------------------
# DuckDB connection with retry/backoff (handles file lock between processes)
# -----------------------------------------------------------------------------
LOCK_TEXT = "Could not set lock on file"

def _connect_duckdb_with_retry(read_only: bool, attempts: int = 20):
    delay = 0.05
    last: Optional[Exception] = None
    for _ in range(attempts):
        try:
            return duckdb.connect(DB_PATH, read_only=read_only)
        except duckdb.IOException as e:
            msg = str(e)
            if LOCK_TEXT in msg:
                time.sleep(delay + random.random() * delay * 0.5)
                delay = min(delay * 2, 1.0)
                last = e
                continue
            raise
    if last:
        raise last
    raise duckdb.IOException("Database is busy.")

@contextmanager
def db_open(read_only: bool):
    con = _connect_duckdb_with_retry(read_only=read_only)
    try:
        yield con
    finally:
        try:
            con.close()
        except Exception:
            pass

@app.exception_handler(duckdb.IOException)
async def duckdb_io_handler(request, exc: duckdb.IOException):
    if LOCK_TEXT in str(exc):
        return PlainTextResponse("Database is busy, please retry shortly.", status_code=503)
    return PlainTextResponse("Database error.", status_code=500)


# --- helpers for CSV pre-cleaning -------------------------------------------
def _detect_bom(path: str) -> Optional[str]:
    with open(path, "rb") as fh:
        head = fh.read(4)
    if head.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if head.startswith(b"\xfe\xff"):
        return "utf-16-be"
    if head.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return None


def _preclean_csv_to_temp(src_path: str) -> str:
    """
    Make a cleaned copy next to the original:
      - normalize newlines to LF
      - remove NUL/zero-width
      - coalesce physical lines until quotes balance
    """
    import io

    enc = _detect_bom(src_path) or "utf-8"
    out_path = src_path + ".clean.csv"

    with open(src_path, "rb") as fh:
        raw = fh.read()

    try:
        txt = raw.decode(enc, errors="replace")
    except Exception:
        txt = raw.decode("iso-8859-1", errors="replace")

    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    txt = txt.replace("\x00", "").replace("\u200b", "")

    out_lines: List[str] = []
    buf: List[str] = []
    open_quotes = 0

    def _add_line_part(part: str):
        nonlocal open_quotes
        buf.append(part)
        open_quotes = (open_quotes + part.count('"')) % 2

    for phys_line in io.StringIO(txt):
        phys = phys_line.rstrip("\n")
        _add_line_part(phys)
        if open_quotes == 0:
            out_lines.append("".join(buf))
            buf.clear()

    if buf:
        out_lines.append("".join(buf))

    cleaned = "\n".join(out_lines)
    with open(out_path, "w", encoding="utf-8", newline="\n") as out:
        out.write(cleaned)
    return out_path


# -----------------------------------------------------------------------------
# Robust CSV ingestion
# -----------------------------------------------------------------------------
def _guess_delimiter(sample: str) -> Optional[str]:
    candidates = [",", ";", "|", "\t"]
    lines = [ln for ln in sample.splitlines() if ln.strip()][:50]
    if not lines:
        return None
    best, best_score = None, -1
    for d in candidates:
        counts = [ln.count(d) for ln in lines]
        if max(counts) == 0:
            continue
        mean = sum(counts)/len(counts)
        var = sum((c-mean)**2 for c in counts)/len(counts)
        score = mean - var
        if score > best_score:
            best_score, best = score, d
    return best


def create_staging_from_csv(con: duckdb.DuckDBPyConnection, safe_staging: str, file_path: str) -> None:
    """
    Robust loader:
      1) tolerant read_csv_auto
      2) matrix of (enc × delim × header × quote × newline) with BOTH token and literal newline values
      3) pre-clean file and repeat
    """
    def _read_peek(path: str) -> str:
        try:
            with open(path, "rb") as fh:
                head = fh.read(128 * 1024)
            return head.decode("utf-8", errors="ignore")
        except Exception:
            return ""

    def _try_auto(path: str) -> Optional[str]:
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {safe_staging} AS
                SELECT * FROM read_csv_auto('{_sql_literal(path)}',
                    SAMPLE_SIZE=-1,
                    HEADER=TRUE,
                    NORMALIZE_NAMES=FALSE,
                    ALL_VARCHAR=TRUE,
                    IGNORE_ERRORS=TRUE,
                    maximum_line_size='10000000',
                    PARALLEL=FALSE
                );
            """)
            return None
        except duckdb.Error as e:
            return str(e)

    def _try_matrix(path: str) -> Optional[str]:
        peek = _read_peek(path)
        guessed_delim = _guess_delimiter(peek)
        delims = [d for d in [guessed_delim, ",", ";", "|", "\t"] if d]

        encodings = ["utf-8", "utf-8-sig", "utf-16", "windows-1252", "iso-8859-1"]
        header_opts = [True, False]
        quotes = ['"', "'"]
        newlines = ["auto", "LF", "CRLF", "CR", "\n", "\r\n", "\r", "lf", "crlf", "cr"]

        last_err = None
        for enc in encodings:
            for delim in delims:
                for nl in newlines:
                    for q in quotes:
                        for hdr in header_opts:
                            try:
                                con.execute(f"""
                                    CREATE OR REPLACE TABLE {safe_staging} AS
                                    SELECT * FROM read_csv('{_sql_literal(path)}',
                                        delim='{delim}',
                                        header={'TRUE' if hdr else 'FALSE'},
                                        quote='{q}',
                                        escape='{q}',
                                        comment='\0',
                                        encoding='{enc}',
                                        all_varchar=TRUE,
                                        ignore_errors=TRUE,
                                        maximum_line_size='10000000',
                                        parallel=FALSE,
                                        new_line='{nl}'
                                    );
                                """)
                                return None
                            except duckdb.Error as e2:
                                last_err = str(e2)
                                continue
                    for hdr in header_opts:
                        try:
                            con.execute(f"""
                                CREATE OR REPLACE TABLE {safe_staging} AS
                                SELECT * FROM read_csv('{_sql_literal(path)}',
                                    delim='{delim}',
                                    header={'TRUE' if hdr else 'FALSE'},
                                    quote='\0',
                                    escape='\0',
                                    comment='\0',
                                    encoding='{enc}',
                                    all_varchar=TRUE,
                                    ignore_errors=TRUE,
                                    maximum_line_size='10000000',
                                    parallel=FALSE,
                                    new_line='{nl}'
                                );
                            """)
                            return None
                        except duckdb.Error as e3:
                            last_err = str(e3)
                            continue
        return last_err

    err = _try_auto(file_path)
    if err is None:
        return
    err = _try_matrix(file_path)
    if err is None:
        return

    cleaned = _preclean_csv_to_temp(file_path)
    try:
        err2 = _try_auto(cleaned)
        if err2 is None:
            return
        err2 = _try_matrix(cleaned)
        if err2 is None:
            return
    finally:
        try:
            os.remove(cleaned)
        except FileNotFoundError:
            pass
        except Exception:
            pass

    raise RuntimeError(
        "CSV import failed after fallbacks. "
        f"Last errors: original={err!r} cleaned={err2!r}"
    )


# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------
class SimpleQueryRequest(BaseModel):
    filters: Dict[str, List[str]] = {}
    fields: Optional[List[str]] = None
    limit: conint(gt=0) = 100
    offset: conint(ge=0) = 0


class DistinctQuery(BaseModel):
    q: Optional[str] = None
    limit: conint(gt=0) = 200


class BulkDeleteRequest(BaseModel):
    row_hashes: Optional[List[int]] = None
    filters: Dict[str, List[str]] = {}
    source_files: Optional[List[str]] = None
    dry_run: bool = False
    confirm: bool = False
    expected_min: Optional[int] = None
    expected_max: Optional[int] = None
    drop_file_records: bool = False


class ClearRequest(BaseModel):
    scope: str = "all"
    confirm: bool = False
    confirm_token: Optional[str] = None


# -----------------------------------------------------------------------------
# Helpers for DB access
# -----------------------------------------------------------------------------
def list_user_columns(con: duckdb.DuckDBPyConnection) -> List[str]:
    rows = con.execute("PRAGMA table_info(dataset)").fetchall()
    cols = [r[1] for r in rows if r[1] not in INTERNAL_COLS]
    return cols


def ensure_columns(con: duckdb.DuckDBPyConnection, cols: List[str]) -> None:
    current = set(list_user_columns(con))
    for c in cols:
        if c in current:
            continue
        try:
            con.execute(f'ALTER TABLE dataset ADD COLUMN {quote_ident(c)} VARCHAR;')
        except duckdb.Error as e:
            log.debug("ADD COLUMN race tolerated for %s: %s", c, e)


# -----------------------------------------------------------------------------
# File metadata write
# -----------------------------------------------------------------------------
def record_file(con: duckdb.DuckDBPyConnection, job_id: str, filename: str, file_hash: str) -> None:
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


# -----------------------------------------------------------------------------
# Background ingestion job (batched + progress)
# -----------------------------------------------------------------------------
def process_file(job_id: str, file_path: str, file_hash: str, orig_filename: str):
    status_set(job_id, status="processing", stage="reading", progress=0.0)

    staging = f"staging_{job_id.replace('-', '_')}"
    safe_staging = quote_ident(staging)

    try:
        with db_open(read_only=False) as con:
            exists = con.execute(
                "SELECT 1 FROM files WHERE filename=? AND file_hash=? LIMIT 1",
                (orig_filename, file_hash)
            ).fetchone()
            if exists:
                status_set(job_id, status="completed", stage="completed", ended_at=datetime.utcnow().isoformat())
                return

            ext = Path(file_path).suffix.lower()
            if ext in (".csv", ".tsv", ".txt"):
                status_set(job_id, stage="parsing_csv")
                create_staging_from_csv(con, safe_staging, str(file_path))
            elif ext == ".parquet":
                con.execute(f"""
                    CREATE OR REPLACE TABLE {safe_staging} AS
                    SELECT * FROM parquet_scan('{_sql_literal(file_path)}');
                """)
            elif ext in (".xlsx", ".xlsm", ".xlsb", ".xls"):
                status_set(job_id, stage="parsing_excel")
                con.execute("INSTALL excel;")
                con.execute("LOAD excel;")
                con.execute(f"""
                    CREATE OR REPLACE TABLE {safe_staging} AS
                    SELECT * FROM read_excel('{_sql_literal(file_path)}',
                        sheet=NULL,
                        header=TRUE,
                        all_varchar=TRUE
                    );
                """)
            else:
                raise ValueError("Unsupported file type (csv/tsv/txt, parquet, or excel)")

            status_set(job_id, stage="schema_evolved")
            info = con.execute(f"PRAGMA table_info({safe_staging})").fetchall()
            orig_cols = [r[1] for r in info]
            mapping = {c: clean_col(c) for c in orig_cols}

            seen = set()
            for k, v in list(mapping.items()):
                base, i = v, 1
                while mapping[k] in seen:
                    mapping[k] = f"{base}_{i}"
                    i += 1
                seen.add(mapping[k])

            for orig, new in mapping.items():
                if orig != new:
                    con.execute(
                        f'ALTER TABLE {safe_staging} RENAME COLUMN {quote_any_ident(orig)} TO {quote_ident(new)};'
                    )

            new_cols = list(mapping.values())
            ensure_columns(con, new_cols)

            all_user_cols = list_user_columns(con)
            all_user_cols.sort()

            status_set(job_id, stage="counting")
            rows_total = con.execute(f"SELECT COUNT(*) FROM {safe_staging}").fetchone()[0]
            status_set(job_id, rows_total=rows_total)

            if rows_total == 0:
                record_file(con, job_id, orig_filename, file_hash)
                status_set(job_id, status="completed", stage="completed", progress=1.0,
                           ended_at=datetime.utcnow().isoformat())
                return

            con.execute(
                f'CREATE TEMP TABLE {staging}_rn AS SELECT *, row_number() OVER () AS __rn FROM {safe_staging};')

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
            row_hash_expr = f"CAST(hash({', '.join(hash_parts)}) AS UBIGINT)"

            rows_processed = 0
            rows_inserted = 0

            status_set(job_id, stage="ingesting", rows_processed=0, rows_inserted=0, rows_skipped=0)
            while rows_processed < rows_total:
                lo = rows_processed + 1
                hi = min(rows_processed + BATCH_SIZE, rows_total)

                before_cnt = con.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]

                user_cols_csv = ", ".join(quote_ident(c) for c in all_user_cols)
                target_cols_csv = f"{user_cols_csv}, row_hash, __source_file, __ingested_at"

                nonempty = _nonempty_predicate(new_cols, alias="s")

                sql = f"""
                WITH src AS (
                    SELECT
                        {projection},
                        {row_hash_expr} AS row_hash,
                        s.__rn
                    FROM {staging}_rn s
                    WHERE s.__rn BETWEEN ? AND ?
                      AND ({nonempty})
                ),
                dedup AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY __rn) AS _dup
                    FROM src
                )
                INSERT INTO dataset ({target_cols_csv})
                SELECT
                    {", ".join(quote_ident(c) for c in all_user_cols)},
                    row_hash,
                    ? AS __source_file,
                    NOW() AS __ingested_at
                FROM dedup
                WHERE _dup = 1
                  AND NOT EXISTS (SELECT 1 FROM dataset d WHERE d.row_hash = dedup.row_hash);
                """

                con.execute(sql, [lo, hi, orig_filename])
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

            record_file(con, job_id, orig_filename, file_hash)
            status_set(job_id, status="completed", stage="completed", progress=1.0,
                       ended_at=datetime.utcnow().isoformat())
            _bump_dataset_version()

    except Exception as e:
        log.exception("Ingestion failed for job %s", job_id)
        status_set(job_id, status="failed", stage="failed", error=str(e), ended_at=datetime.utcnow().isoformat())
    finally:
        try:
            with db_open(read_only=False) as con2:
                con2.execute(f"DROP TABLE IF EXISTS {safe_staging}")
                con2.execute(f"DROP TABLE IF EXISTS {staging}_rn")
        except Exception:
            pass
        try:
            os.remove(str(file_path) + ".clean.csv")
        except FileNotFoundError:
            pass
        except Exception:
            pass


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

    hasher = hashlib.sha256()
    try:
        async with aiofiles.open(dest_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
                await out.write(chunk)
                try:
                    redis_client.hincrby(f"job:{job_id}", "uploaded", len(chunk))
                    if total:
                        uploaded = int(redis_client.hget(f"job:{job_id}", "uploaded") or 0)
                        prog = min(1.0, uploaded / total)
                        status_set(job_id, progress=prog)
                except RedisError:
                    pass
    except Exception as e:
        status_set(job_id, status="failed", stage="uploading", error=f"Upload error: {e}",
                   ended_at=datetime.utcnow().isoformat())
        raise HTTPException(500, "Failed to upload file.")

    file_hash = hasher.hexdigest()

    with db_open(read_only=True) as con:
        exists = con.execute(
            "SELECT 1 FROM files WHERE filename=? AND file_hash=? LIMIT 1",
            (file.filename, file_hash)
        ).fetchone()
    if exists:
        status_set(job_id, status="completed", stage="completed", progress=1.0, ended_at=datetime.utcnow().isoformat())
        return {"job_id": job_id, "skipped": True}

    status_set(job_id, status="queued", stage="queued")
    queue.enqueue(process_file, job_id, str(dest_path), file_hash, file.filename)
    return {"job_id": job_id}


@app.get("/status/{job_id}")
def status(job_id: str, _: None = Depends(verify_api_key)):
    data = status_get(job_id)
    if not data.get("status"):
        raise HTTPException(404, "Unknown job_id")
    return JSONResponse(data)


@app.get("/status/stream/{job_id}")
async def status_stream(job_id: str, _: None = Depends(verify_api_key)):
    async def gen():
        prev = None
        try:
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
        except asyncio.CancelledError:
            return

    return StreamingResponse(gen(), media_type="text/event-stream")


# -----------------------------------------------------------------------------
# Columns & distinct values (for dropdowns)
# -----------------------------------------------------------------------------
@app.get("/columns")
def columns(response: Response, _: None = Depends(verify_api_key)):
    with db_open(read_only=True) as con:
        total = con.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
        cols = [] if total == 0 else list_user_columns(con)

    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["X-Dataset-Version"] = str(_get_dataset_version())
    return {"columns": cols}


@app.get("/distinct/{column}")
def distinct_values(response: Response,
    column: str,
    q: Optional[str] = None,
    limit: conint(gt=0) = 200,
    _: None = Depends(verify_api_key),
):
    if column in INTERNAL_COLS:
        raise HTTPException(400, "Invalid column")
    col = quote_ident(column)
    params: List[Any] = []
    where_sql = f"WHERE {col} IS NOT NULL"
    if q:
        where_sql = f"WHERE {col} IS NOT NULL AND {col} ILIKE ?"
        params.append(f"%{q}%")
    with db_open(read_only=True) as con:
        rows = con.execute(
            f"SELECT DISTINCT {col} AS v FROM dataset {where_sql} ORDER BY v LIMIT ?",
            (*params, limit),
        ).fetchall()

    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["X-Dataset-Version"] = str(_get_dataset_version())
    return {"values": [r[0] for r in rows]}


# -----------------------------------------------------------------------------
# Simple AND filters query
# -----------------------------------------------------------------------------
@app.post("/query")
def simple_query(body: SimpleQueryRequest, _: None = Depends(verify_api_key)):
    if body.fields is not None and len(body.fields) == 0:
        body.fields = None

    version = _get_dataset_version()
    raw_key = f"v={version}|{json.dumps(body.dict(), sort_keys=True)}"
    cache_key = hashlib.sha256(raw_key.encode()).hexdigest()

    try:
        cached = redis_client.get(cache_key)
    except RedisError:
        cached = None
    if cached:
        return json.loads(cached)

    with db_open(read_only=True) as con:
        user_cols_list = list_user_columns(con)
        user_cols = set(user_cols_list)

        if not user_cols_list:
            result = {"rows": [], "total": 0}
            try:
                redis_client.setex(cache_key, QUERY_CACHE_TTL, json.dumps(result))
            except RedisError:
                pass
            return result

        if body.fields:
            for c in body.fields:
                if c not in user_cols:
                    raise HTTPException(400, f"Unknown column: {c}")
            fields = body.fields
        else:
            fields = sorted(user_cols_list)

        if not fields:
            result = {"rows": [], "total": 0}
            try:
                redis_client.setex(cache_key, QUERY_CACHE_TTL, json.dumps(result))
            except RedisError:
                pass
            return result

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
    if body.fields is not None and len(body.fields) == 0:
        body.fields = None

    with db_open(read_only=True) as con:
        user_cols_list = list_user_columns(con)
        user_cols = set(user_cols_list)

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
        with db_open(read_only=True) as con2:
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
# Bulk delete (safe, chunked, dry-run)
# -----------------------------------------------------------------------------
def _chunks(seq: List[Any], size: int):
    it = iter(seq)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def _build_base_where_for_delete(
        con: duckdb.DuckDBPyConnection,
        filters: Dict[str, List[str]],
        source_files: Optional[List[str]]
) -> Tuple[str, List[Any]]:
    user_cols_list = list_user_columns(con)
    user_cols = set(user_cols_list)

    clauses: List[str] = []
    params: List[Any] = []

    for c, vals in filters.items():
        if c not in user_cols:
            raise HTTPException(400, f"Unknown column: {c}")
        if not vals:
            continue
        placeholders = ", ".join("?" for _ in vals)
        clauses.append(f'{quote_ident(c)} IN ({placeholders})')
        params.extend(vals)

    if source_files:
        placeholders = ", ".join("?" for _ in source_files)
        clauses.append('"__source_file" IN (' + placeholders + ')')
        params.extend(source_files)

    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params


@app.post("/delete")
def bulk_delete(body: BulkDeleteRequest, _: None = Depends(verify_api_key)):
    if not body.row_hashes and not body.filters and not body.source_files:
        raise HTTPException(400, "No delete criteria supplied (row_hashes | filters | source_files).")

    row_hashes = body.row_hashes if body.row_hashes else None

    with db_open(read_only=False) as con:
        base_where, base_params = _build_base_where_for_delete(con, body.filters, body.source_files)

        matched = 0
        if row_hashes:
            for chunk in _chunks(row_hashes, 5000):
                placeholders = ", ".join("?" for _ in chunk)
                where = base_where + ((" AND " if base_where else "WHERE ") + f"row_hash IN ({placeholders})")
                matched += con.execute(f"SELECT COUNT(*) FROM dataset {where}", (*base_params, *chunk)).fetchone()[0]
        else:
            matched = con.execute(f"SELECT COUNT(*) FROM dataset {base_where}", base_params).fetchone()[0]

        if body.dry_run:
            return {"matched": matched, "deleted": 0, "dry_run": True}

        if not body.confirm:
            raise HTTPException(400, "Set confirm=true to perform the delete (use dry_run first).")
        if body.expected_min is not None and matched < body.expected_min:
            raise HTTPException(409, f"Matched {matched} rows, below expected_min={body.expected_min}.")
        if body.expected_max is not None and matched > body.expected_max:
            raise HTTPException(409, f"Matched {matched} rows, above expected_max={body.expected_max}.")

        deleted = 0
        con.execute("BEGIN TRANSACTION;")
        try:
            if row_hashes:
                for chunk in _chunks(row_hashes, 5000):
                    placeholders = ", ".join("?" for _ in chunk)
                    where = base_where + ((" AND " if base_where else "WHERE ") + f"row_hash IN ({placeholders})")
                    n = con.execute(f"SELECT COUNT(*) FROM dataset {where}", (*base_params, *chunk)).fetchone()[0]
                    con.execute(f"DELETE FROM dataset {where}", (*base_params, *chunk))
                    deleted += n
            else:
                n = con.execute(f"SELECT COUNT(*) FROM dataset {base_where}", base_params).fetchone()[0]
                con.execute(f"DELETE FROM dataset {base_where}", base_params)
                deleted += n

            if body.source_files and body.drop_file_records:
                placeholders = ", ".join("?" for _ in body.source_files)
                con.execute(f"DELETE FROM files WHERE filename IN ({placeholders})", body.source_files)

            if deleted >= 100_000:
                _maybe_optimize(con)

            con.execute("COMMIT;")
            if deleted > 0:
                _bump_dataset_version()

        except Exception:
            con.execute("ROLLBACK;")
            raise

    return {"matched": matched, "deleted": deleted}


# -----------------------------------------------------------------------------
# Clear all data (scoped, guarded)
# -----------------------------------------------------------------------------
@app.post("/admin/clear")
def admin_clear(body: ClearRequest, _: None = Depends(verify_api_key)):
    scope = (body.scope or "all").lower()
    wipes_dataset = scope in ("dataset", "all")

    if wipes_dataset:
        if not body.confirm or body.confirm_token != "DELETE ALL":
            raise HTTPException(
                400,
                'Clearing the dataset requires confirm=true and confirm_token="DELETE ALL".'
            )
    else:
        if not body.confirm:
            raise HTTPException(400, "Set confirm=true to perform this operation.")

    cleared = []

    if scope in ("dataset", "all"):
        with db_open(read_only=False) as con:
            con.execute("BEGIN TRANSACTION;")
            try:
                # empty the table first
                con.execute("DELETE FROM dataset;")

                # drop dependent index so DROP COLUMN can proceed
                con.execute("DROP INDEX IF EXISTS idx_dataset_rowhash;")

                # drop all user columns (keep internal ones)
                user_cols = [c for c in list_user_columns(con)]
                for c in user_cols:
                    con.execute(f'ALTER TABLE dataset DROP COLUMN {quote_ident(c)};')

                if scope == "all":
                    con.execute("DELETE FROM files;")

                _maybe_optimize(con)

                # recreate the index on row_hash
                con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_rowhash ON dataset(row_hash);")

                con.execute("COMMIT;")
                cleared.append("dataset")
                if scope == "all":
                    cleared.append("files")
                _bump_dataset_version()
            except Exception:
                con.execute("ROLLBACK;")
                raise

    elif scope == "files":
        with db_open(read_only=False) as con:
            con.execute("DELETE FROM files;")
            _maybe_optimize(con)
            cleared.append("files")
            _bump_dataset_version()

    if scope in ("uploads", "all"):
        cnt = 0
        try:
            for entry in os.scandir(UPLOAD_DIR):
                try:
                    if entry.is_file():
                        os.unlink(entry.path)
                        cnt += 1
                except Exception as e:
                    log.warning("Failed to delete upload file %s: %s", entry.path, e)
        except FileNotFoundError:
            pass
        cleared.append(f"uploads:{cnt}")

    if scope in ("redis", "all"):
        removed = 0
        try:
            cur = 0
            while True:
                cur, keys = redis_client.scan(cur, match="job:*")
                if keys:
                    redis_client.delete(*keys)
                    removed += len(keys)
                if cur == 0:
                    break
        except RedisError as e:
            log.warning("Redis scan/delete failed: %s", e)
        cleared.append(f"redis:{removed}")

    return {"ok": True, "cleared": cleared}


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
# Startup: ensure dirs & base tables (and migrate row_hash → UBIGINT)
# -----------------------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    with db_open(read_only=False) as con:
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

        info = con.execute("PRAGMA table_info('dataset')").fetchall()
        types = {r[1]: (r[2] or "").upper() for r in info}

        if types.get("row_hash", "") != "UBIGINT":
            con.execute("DROP INDEX IF EXISTS idx_dataset_rowhash;")
            try:
                con.execute("ALTER TABLE dataset ALTER COLUMN row_hash TYPE UBIGINT;")
            except duckdb.Error:
                con.execute("ALTER TABLE dataset ALTER COLUMN row_hash SET DATA TYPE UBIGINT;")

        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_rowhash ON dataset(row_hash);")


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    try:
        with db_open(read_only=True) as con:
            con.execute("SELECT 1")
        redis_client.ping()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
