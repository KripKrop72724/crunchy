from fastapi.testclient import TestClient
from rq import Queue
import fakeredis
import tempfile
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))
import app.main as main


def create_client():
    main.redis_client = fakeredis.FakeRedis(decode_responses=True)
    main.queue = Queue(is_async=False, connection=main.redis_client)
    tmpdir = tempfile.TemporaryDirectory()
    main.DB_PATH = Path(tmpdir.name) / "test.duckdb"
    main.startup_event()
    client = TestClient(main.app)
    return client, tmpdir


def test_facets_basic():
    client, tmpdir = create_client()
    try:
        csv = "country,status\nUSA,Open\nUSA,Closed\nCanada,Open\nGermany,Open\n"
        r = client.post(
            "/upload",
            headers={"X-API-Key": "changeme"},
            files={"file": ("t.csv", csv, "text/csv")},
        )
        assert r.status_code == 200
        job_id = r.json()["job_id"]
        st = client.get(f"/status/{job_id}", headers={"X-API-Key": "changeme"}).json()
        assert st["status"] == "completed"

        resp = client.post(
            "/facets",
            headers={"X-API-Key": "changeme"},
            json={"filters": {"country": ["USA"]}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert data["columns"] == ["country", "status"]
        assert data["facets"]["country"][0] == {"value": "USA", "count": 2}
        assert {"value": "Canada", "count": 1} in data["facets"]["country"]
        assert {"value": "Germany", "count": 1} in data["facets"]["country"]
        assert data["facets"]["status"] == [
            {"value": "Closed", "count": 1},
            {"value": "Open", "count": 1},
        ]

        resp2 = client.post(
            "/facets",
            headers={"X-API-Key": "changeme"},
            json={"filters": {"country": ["USA"]}, "fields": ["country"], "exclude_self": False},
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["facets"]["country"] == [{"value": "USA", "count": 2}]
    finally:
        tmpdir.cleanup()
