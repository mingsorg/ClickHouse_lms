"""Bounded integration checks against real logd and ClickHouse; no cleanup deletes."""
import gzip
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

root = Path(__file__).resolve().parents[1]
config = {}
for line in (root / ".env").read_text().splitlines():
    key, value = line.split("=", 1)
    config[key] = value.strip("'")
grants = json.loads(config["LMS_TOKENS"])
tokens = {grant["tenant"]: token for token, grant in grants.items()}
base = os.environ.get("LMS_TEST_URL", "http://127.0.0.1:18080")
run_id = "live-" + uuid.uuid4().hex


def call(path, body=None, scheme="Bearer", tenant="demo", compressed=False):
    headers = {"Authorization": scheme + " " + tokens[tenant]}
    data = None
    if body is not None:
        data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if compressed:
        data = gzip.compress(data)
        headers["Content-Encoding"] = "gzip"
    req = urllib.request.Request(base + path, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def check(condition, label):
    if not condition:
        raise AssertionError(label)
    print("PASS", label, flush=True)


deadline = time.monotonic() + 90
while True:
    try:
        status, _ = call("/readyz")
        if status == 200:
            break
    except (urllib.error.URLError, TimeoutError):
        pass
    if time.monotonic() > deadline:
        raise RuntimeError("readiness deadline exceeded")
    time.sleep(0.2)
check(True, "real ClickHouse readiness")

now = datetime.now(timezone.utc)
# Nanosecond timestamps in the same microsecond exercise cursor precision.
prefix = now.strftime("%Y-%m-%dT%H:%M:%S")
events = [
    {"@timestamp": prefix + f".12345678{i}Z", "message": f"{run_id} native-{i}", "service": run_id}
    for i in range(2)
]
status, body = call("/v1/logs", {"dataset": "logs", "events": events})
check(status == 200 and body["committed"] == 2, "native batch confirmed")
status, body = call("/services/collector/event", {"event": {"message": run_id + " hec", "service": run_id}, "index": "logs"}, "Splunk", compressed=True)
check(status == 200 and body["code"] == 0, "gzip HEC object confirmed")
bulk = '\n'.join([
    json.dumps({"index": {"_index": "logs"}}),
    json.dumps({"message": run_id + " bulk", "service": run_id}),
    json.dumps({"index": {"_index": "logs", "_id": "unsupported"}}),
    json.dumps({"message": run_id + " must-not-appear", "service": run_id}),
]) + '\n'
status, body = call("/_bulk", bulk, "ApiKey")
check(status == 200 and body["errors"] and body["items"][0]["index"]["status"] == 201 and body["items"][1]["index"]["status"] == 400, "Bulk partial success and explicit ID rejection")
status, _ = call("/services/collector/event", '{"event":"valid-prefix"}{"event":false}', "Splunk")
check(status == 400, "malformed HEC batch rejected")
search = {"dataset": "logs", "from": (now - timedelta(minutes=2)).isoformat(), "to": (now + timedelta(minutes=2)).isoformat(), "service": run_id, "limit": 1}
seen = []
while True:
    status, body = call("/v1/logs/search", search)
    if status != 200:
        raise AssertionError(f"search failed: HTTP {status}, {body}")
    check(True, "search page succeeds")
    seen.extend(body["events"])
    if not body["next_cursor"]:
        break
    search["cursor"] = body["next_cursor"]
    if len(seen) > 10:
        raise AssertionError("pagination failed to terminate")
check(len(seen) == 4 and len({e["event_id"] for e in seen}) == 4, "all three protocols queryable with no pagination duplicates")
check({e["event_time"] for e in seen if "native-" in e["message"]} == {e["@timestamp"] for e in events}, "nanosecond timestamps preserved")
search.pop("cursor", None)
status, body = call("/v1/logs/search", search, tenant="other")
check(status == 200 and body["events"] == [], "tenant isolation on real storage")
search["contains"] = "'; DROP TABLE lms.logs;--"
status, body = call("/v1/logs/search", search)
check(status == 200 and body["events"] == [], "text predicates remain parameterized")
status, _ = call("/services/collector/ack", {"acks": [0]}, "Splunk")
check(status == 501, "HEC ACK is explicitly unsupported")
print("PASS live slice; no capacity or HA claim; test data retained under service", run_id)
