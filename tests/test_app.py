from fastapi.testclient import TestClient
import fakeredis

import app.main as main

# Replace redis client with fakeredis
main.redis_client = fakeredis.FakeRedis(decode_responses=True)
app = main.app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_status_unknown_job():
    response = client.get("/status/unknown", headers={"X-API-Key": "changeme"})
    assert response.status_code == 404
