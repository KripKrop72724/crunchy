from fastapi.testclient import TestClient
import fakeredis
import sys
from pathlib import Path
import duckdb
from rq import Queue
import tempfile

sys.path.append(str(Path(__file__).resolve().parents[1]))
import app.main as main

# Replace redis client and queue with fakeredis-backed synchronous queue
main.redis_client = fakeredis.FakeRedis(decode_responses=True)
main.queue = Queue(is_async=False, connection=main.redis_client)
main.startup_event()
app = main.app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_status_unknown_job():
    response = client.get("/status/unknown", headers={"X-API-Key": "changeme"})
    assert response.status_code == 404


def test_ws_status_unknown_job():
    with client.websocket_connect("/ws/status/unknown") as websocket:
        data = websocket.receive_json()
        assert data == {"error": "unknown job"}


def test_csv_upload():
    content = "id,name\n1,Alice\n2,Bob\n"
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("test.csv", content, "text/csv")},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    assert status["status"] == "completed"
    assert status["rows"] == 2
    conn = duckdb.connect(main.DB_PATH)
    rows = conn.execute(f"SELECT COUNT(*) FROM {status['table']}").fetchone()[0]
    conn.close()
    assert rows == 2


def test_parquet_upload():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.parquet"
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t(id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        conn.execute(f"COPY t TO '{path}' (FORMAT 'parquet')")
        conn.close()
        with open(path, "rb") as f:
            response = client.post(
                "/upload",
                headers={"X-API-Key": "changeme"},
                files={"file": ("test.parquet", f.read(), "application/octet-stream")},
            )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    assert status["status"] == "completed"
    assert status["rows"] == 2


def test_unsupported_file_type():
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("test.txt", b"hello", "text/plain")},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    assert status["status"] == "failed"
    assert "Unsupported file type" in status["error"]
