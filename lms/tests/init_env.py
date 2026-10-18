"""Create local development secrets without printing or overwriting them."""
import json
import os
import secrets
from pathlib import Path

target = Path(__file__).resolve().parents[1] / ".env"
grants = {
    secrets.token_urlsafe(32): {"tenant": "demo", "datasets": ["logs"]},
    secrets.token_urlsafe(32): {"tenant": "other", "datasets": ["logs"]},
}
descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w") as handle:
    handle.write("CLICKHOUSE_PASSWORD=" + secrets.token_urlsafe(32) + "\n")
    handle.write("LMS_CURSOR_KEY=" + secrets.token_urlsafe(48) + "\n")
    handle.write("LMS_TOKENS='" + json.dumps(grants, separators=(",", ":")) + "'\n")
print("Created ignored .env with two isolated development tenants; values not displayed.")
