import uuid
from pathlib import Path

import aiofiles
import duckdb
import redis
from fastapi import BackgroundTasks, Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from .config import API_KEY, DB_PATH, REDIS_URL, UPLOAD_DIR

app = FastAPI()
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ---------------------------------------------------------------------------
# Utility & startup
# ---------------------------------------------------------------------------

def verify_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


@app.on_event("startup")
def startup_event():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DB_PATH)
    conn.execute("INSTALL excel;")
    conn.execute("LOAD excel;")
    conn.close()


# ---------------------------------------------------------------------------
# Background processing
# ---------------------------------------------------------------------------

def process_file(job_id: str, file_path: str):
    job_key = f"job:{job_id}"
    redis_client.hset(job_key, mapping={"status": "processing"})
    try:
        table = f"import_{job_id.replace('-', '_')}"
        conn = duckdb.connect(DB_PATH)
        conn.execute("LOAD excel;")
        sql = f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_excel_auto('{file_path}');
        """
        conn.execute(sql)
        conn.close()
        redis_client.hset(job_key, mapping={"status": "completed", "table": table})
    except Exception as e:  # pragma: no cover - error path
        redis_client.hset(job_key, mapping={"status": "failed", "error": str(e)})


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.post("/upload")
async def upload(
    request: Request,
    background_tasks: BackgroundTasks,
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
    background_tasks.add_task(process_file, job_id, str(dest_path))
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
        "table": data.get("table") or None,
        "error": data.get("error") or None,
    }
    return JSONResponse(response)


@app.get("/health")
def health():
    return {"status": "ok"}
