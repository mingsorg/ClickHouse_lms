"""Verify real read-only diagnostics and intentionally incomplete HA scope."""
import json
import subprocess
import base64
import urllib.error
import urllib.request
from pathlib import Path

run = subprocess.run([
    "docker", "compose", "exec", "-T", "logd", "/usr/local/bin/lmsctl", "doctor", "--config", "/etc/lms/doctor.json"
], capture_output=True, text=True, timeout=90)
if run.returncode != 0:
    raise AssertionError("doctor failed: " + run.stdout + run.stderr)
report = json.loads(run.stdout)
assert report["status"] == "WARN", report
assert report["production_acceptance"] == "NOT RUN", report
assert len(report["nodes"]) == 1, report
checks = {check["name"]: check for check in report["nodes"][0]["checks"]}
assert set(checks) == {"server", "table", "disks", "parts", "replicas", "merges", "mutations"}, report
assert checks["table"]["status"] == "WARN" and checks["replicas"]["status"] == "WARN", report
for name in ["server", "disks", "parts", "merges", "mutations"]:
    assert checks[name]["status"] == "PASS", report
assert checks["server"]["evidence"][0]["version"] == "26.9.21.9", report
print("PASS read-only live doctor: actual pinned version, disk, parts, merges and mutations")
print("PASS single-node MergeTree and absent replication yield WARN; production acceptance NOT RUN")

root = Path(__file__).resolve().parents[1]
config = {key: value.strip("'") for key, value in (line.split("=", 1) for line in (root / ".env").read_text().splitlines())}
for sql in ["SELECT count() FROM lms.logs", "INSERT INTO lms.logs SELECT * FROM lms.logs LIMIT 0"]:
    request = urllib.request.Request("http://127.0.0.1:18123", data=sql.encode())
    request.add_header("Authorization", "Basic " + base64.b64encode(("lms_observer:" + config["CLICKHOUSE_PASSWORD"]).encode()).decode())
    try:
        with urllib.request.urlopen(request, timeout=10):
            raise AssertionError("metadata observer gained log-data or write access")
    except urllib.error.HTTPError as error:
        message = error.read().decode()
        assert "ACCESS_DENIED" in message or "READONLY" in message, "unexpected observer rejection"
print("PASS observer cannot read business log rows or perform even a zero-row insert")
