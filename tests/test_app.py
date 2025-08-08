from fastapi.testclient import TestClient
import fakeredis
import sys
from pathlib import Path
import duckdb
from rq import Queue
import tempfile
import json

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
    conn = duckdb.connect(database=main.DB_PATH, read_only=True)
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


def test_query_endpoints():
    content = "id,name\n1,Alice\n2,Bob\n3,Carol\n"
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("test.csv", content, "text/csv")},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    assert status["status"] == "completed"
    table = status["table"]

    tables = client.get("/tables", headers={"X-API-Key": "changeme"}).json()["tables"]
    assert table in tables

    columns = client.get(
        f"/tables/{table}/columns", headers={"X-API-Key": "changeme"}
    ).json()["columns"]
    assert columns == ["id", "name"]

    body = {
        "filters": [{"column": "name", "op": "eq", "value": "Alice"}],
        "limit": 10,
        "offset": 0,
        "fields": ["id"],
    }
    data = client.post(
        f"/tables/{table}/query",
        headers={"X-API-Key": "changeme"},
        json=body,
    ).json()
    assert data["total"] == 1
    assert data["rows"] == [{"id": 1}]


def test_query_cache():
    content = "id,name\n1,Alice\n2,Bob\n"
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("cache.csv", content, "text/csv")},
    )
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    table = status["table"]

    main.redis_client.flushall()
    calls = {"n": 0}
    orig = main._run_query

    def wrapped(table_name, req):
        calls["n"] += 1
        return orig(table_name, req)

    main._run_query = wrapped
    body = {"filters": [], "limit": 10, "offset": 0}
    client.post(
        f"/tables/{table}/query",
        headers={"X-API-Key": "changeme"},
        json=body,
    )
    client.post(
        f"/tables/{table}/query",
        headers={"X-API-Key": "changeme"},
        json=body,
    )
    assert calls["n"] == 1
    main._run_query = orig


def test_query_order_by_normalized():
    content = "ID,Name\n1,Alice\n2,Bob\n"
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("upper.csv", content, "text/csv")},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    table = status["table"]

    columns = client.get(
        f"/tables/{table}/columns", headers={"X-API-Key": "changeme"}
    ).json()["columns"]
    assert columns == ["id", "name"]

    body = {
        "filters": [{"column": "name", "op": "eq", "value": "Alice"}],
        "order_by": {"column": "name", "direction": "asc"},
        "fields": ["id", "name"],
        "limit": 10,
        "offset": 0,
    }
    data = client.post(
        f"/tables/{table}/query",
        headers={"X-API-Key": "changeme"},
        json=body,
    )
    assert data.status_code == 200
    assert data.json()["rows"] == [{"id": 1, "name": "Alice"}]
    assert data.json()["total"] == 1


def test_column_normalization():
    content = "id,First Name,Last-Name,AGE\n1,Alice,Smith,30\n"
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("norm.csv", content, "text/csv")},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    table = status["table"]

    columns = client.get(
        f"/tables/{table}/columns", headers={"X-API-Key": "changeme"}
    ).json()["columns"]
    assert columns == ["id", "first_name", "last_name", "age"]

    body = {
        "filters": [{"column": "first_name", "op": "eq", "value": "Alice"}],
        "fields": ["first_name", "last_name"],
        "limit": 10,
        "offset": 0,
    }
    data = client.post(
        f"/tables/{table}/query",
        headers={"X-API-Key": "changeme"},
        json=body,
    ).json()
    assert data["rows"] == [{"first_name": "Alice", "last_name": "Smith"}]

    bad_body = {
        "filters": [{"column": "First Name", "op": "eq", "value": "Alice"}],
        "limit": 10,
        "offset": 0,
    }
    bad = client.post(
        f"/tables/{table}/query",
        headers={"X-API-Key": "changeme"},
        json=bad_body,
    )
    assert bad.status_code == 400
    assert "Invalid column" in bad.json()["detail"]


def test_column_normalization_duplicate_conflict():
    content = "First Name,First-Name\nAlice,Ally\n"
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("dup.csv", content, "text/csv")},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
    assert status["status"] == "failed"
    assert "already exists" in status["error"].lower()


def create_filter_table():
    content = (
        "id,name,age,joined\n"
        "1,Alice,30,2024-01-01\n"
        "2,Bob,,2024-05-15\n"
        "3,Carol,25,2025-08-01\n"
        "4,Dan,40,2023-12-31\n"
    )
    response = client.post(
        "/upload",
        headers={"X-API-Key": "changeme"},
        files={"file": ("filters.csv", content, "text/csv")},
    )
    job_id = response.json()["job_id"]
    status = client.get(
        f"/status/{job_id}", headers={"X-API-Key": "changeme"}
    ).json()
    assert status["status"] == "completed"
    return status["table"]


def test_filter_operations():
    table = create_filter_table()
    h = {"X-API-Key": "changeme"}

    def q(body):
        body.update({"limit": 100, "offset": 0})
        return client.post(f"/tables/{table}/query", headers=h, json=body).json()

    assert q({"filters": [{"column": "age", "op": "eq", "value": 30}], "fields": ["id"]})["rows"] == [{"id": 1}]
    assert q({"filters": [{"column": "age", "op": "neq", "value": 30}], "fields": ["id"]})["rows"] == [{"id": 3}, {"id": 4}]
    assert q({"filters": [{"column": "age", "op": "lt", "value": 30}], "fields": ["id"]})["rows"] == [{"id": 3}]
    assert q({"filters": [{"column": "age", "op": "lte", "value": 30}], "fields": ["id"]})["rows"] == [{"id": 1}, {"id": 3}]
    assert q({"filters": [{"column": "age", "op": "gt", "value": 30}], "fields": ["id"]})["rows"] == [{"id": 4}]
    assert q({"filters": [{"column": "age", "op": "gte", "value": 30}], "fields": ["id"]})["rows"] == [{"id": 1}, {"id": 4}]
    assert q({"filters": [{"column": "name", "op": "like", "value": "li"}], "fields": ["id"]})["rows"] == [{"id": 1}]
    assert q({"filters": [{"column": "name", "op": "ilike", "value": "ALI"}], "fields": ["id"]})["rows"] == [{"id": 1}]
    assert q({"filters": [{"column": "name", "op": "ieq", "value": "ALICE"}], "fields": ["id"]})["rows"] == [{"id": 1}]
    assert q({"filters": [{"column": "id", "op": "in", "value": [1, 3]}], "fields": ["id"]})["rows"] == [{"id": 1}, {"id": 3}]
    assert q({"filters": [{"column": "age", "op": "between", "value": [20, 35]}], "fields": ["id"]})["rows"] == [{"id": 1}, {"id": 3}]
    assert q({"filters": [{"column": "age", "op": "in_range", "value": [40, 25, 30]}], "fields": ["id"]})["rows"] == [{"id": 1}, {"id": 3}, {"id": 4}]
    assert q({"filters": [{"column": "age", "op": "is_null"}], "fields": ["id"]})["rows"] == [{"id": 2}]
    assert q({"filters": [{"column": "age", "op": "is_not_null"}], "fields": ["id"]})["rows"] == [{"id": 1}, {"id": 3}, {"id": 4}]
    assert q({"filters": [{"column": "joined", "op": "eq", "value": "2025-08-01"}], "fields": ["id"]})["rows"] == [{"id": 3}]
    assert q({"filters": [{"column": "joined", "op": "gt", "value": "2024-01-01"}], "fields": ["id"]})["rows"] == [{"id": 2}, {"id": 3}]


def test_filter_invalid_cases():
    table = create_filter_table()
    h = {"X-API-Key": "changeme"}
    body = {
        "filters": [{"column": "age", "op": "in_range", "value": []}],
        "limit": 10,
        "offset": 0,
    }
    resp = client.post(f"/tables/{table}/query", headers=h, json=body)
    assert resp.status_code == 400

    bad_date = {
        "filters": [{"column": "joined", "op": "eq", "value": "not-a-date"}],
        "limit": 10,
        "offset": 0,
    }
    resp = client.post(f"/tables/{table}/query", headers=h, json=bad_date)
    assert resp.status_code == 400


def test_nested_logic():
    table = create_filter_table()
    h = {"X-API-Key": "changeme"}
    body = {
        "logic": [
            "OR",
            {
                "logic": [
                    "AND",
                    {"column": "name", "op": "like", "value": "Al"},
                    {"column": "age", "op": "eq", "value": 30},
                ]
            },
            {
                "logic": [
                    "AND",
                    {"column": "age", "op": "is_null"},
                    {"column": "joined", "op": "lt", "value": "2025-01-01"},
                ]
            },
        ],
        "fields": ["id"],
        "limit": 10,
        "offset": 0,
    }
    data = client.post(f"/tables/{table}/query", headers=h, json=body).json()
    assert data["rows"] == [{"id": 1}, {"id": 2}]


def test_invalid_logic_operator():
    table = create_filter_table()
    h = {"X-API-Key": "changeme"}
    body = {
        "logic": ["X", {"column": "age", "op": "eq", "value": 30}],
        "limit": 10,
        "offset": 0,
    }
    resp = client.post(f"/tables/{table}/query", headers=h, json=body)
    assert resp.status_code == 422


def _parse_ndjson(resp):
    lines = resp.text.strip().splitlines()
    return [json.loads(l) for l in lines] if lines else []


def test_stream_basic():
    table = create_filter_table()
    h = {"X-API-Key": "changeme"}
    body = {"limit": 100, "offset": 0}
    resp = client.post(f"/tables/{table}/stream", headers=h, json=body)
    assert resp.status_code == 200
    rows = _parse_ndjson(resp)
    assert len(rows) == 4
    assert rows[0]["name"] == "Alice"


def test_stream_empty_result():
    table = create_filter_table()
    h = {"X-API-Key": "changeme"}
    body = {
        "filters": [{"column": "name", "op": "eq", "value": "ZZZ"}],
        "limit": 100,
        "offset": 0,
    }
    resp = client.post(f"/tables/{table}/stream", headers=h, json=body)
    assert resp.status_code == 200
    assert resp.text == ""


def test_stream_unknown_table():
    resp = client.post(
        "/tables/unknown/stream",
        headers={"X-API-Key": "changeme"},
        json={"limit": 1, "offset": 0},
    )
    assert resp.status_code == 404
