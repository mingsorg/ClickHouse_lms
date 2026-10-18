"""Check interruption and process restart, not host/disk loss or HA."""
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

root = Path(__file__).resolve().parents[1]
config = {key: value.strip("'") for key, value in (line.split("=", 1) for line in (root / ".env").read_text().splitlines())}
token = next(token for token, grant in json.loads(config["LMS_TOKENS"]).items() if grant["tenant"] == "demo")
marker = "restart-" + uuid.uuid4().hex


def call(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request("http://127.0.0.1:18080" + path, data=data, headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=root, check=True, timeout=90)


def require(condition, label):
    if not condition:
        raise AssertionError(label)
    print("PASS", label, flush=True)


status, _ = call("/v1/logs", {"dataset": "logs", "events": [{"message": marker, "service": marker}]})
require(status == 200, "seed row confirmed before interruption")
try:
    compose("stop", "clickhouse")
    status, _ = call("/readyz")
    require(status == 503, "readiness fails while database is stopped")
    status, _ = call("/v1/logs", {"dataset": "logs", "events": [{"message": "unconfirmed", "service": marker}]})
    require(status == 503, "downstream outage does not receive a success acknowledgment")
finally:
    compose("start", "clickhouse")

deadline = time.monotonic() + 90
while True:
    status, _ = call("/readyz")
    if status == 200:
        break
    if time.monotonic() > deadline:
        raise AssertionError("database did not recover before deadline")
    time.sleep(0.2)
now = datetime.now(timezone.utc)
status, body = call("/v1/logs/search", {"dataset": "logs", "from": (now - timedelta(hours=1)).isoformat(), "to": (now + timedelta(minutes=1)).isoformat(), "service": marker, "limit": 10})
require(status == 200 and len(body["events"]) == 1 and body["events"][0]["message"] == marker, "confirmed row remains queryable after database process restart")
print("PASS process interruption/restart; no host-loss, disk-loss or replication claim")
