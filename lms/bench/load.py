"""Bounded synthetic HTTP load; no capacity inference from small-data runs."""
import argparse
import concurrent.futures
import gzip
import http.client
import json
import math
import os
import platform
import time
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

connections = threading.local()


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


def credentials(path, tenant, dataset):
    # Parse the deliberately limited generated environment format, never execute it.
    config = {}
    for line in Path(path).read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        config[key] = value.strip("'")
    tokens = json.loads(config["LMS_TOKENS"])
    matches = [token for token, grant in tokens.items() if grant["tenant"] == tenant and dataset in grant["datasets"]]
    if len(matches) != 1:
        raise ValueError("select exactly one authorized benchmark credential")
    return matches[0]


def send(base, token, path, payload, scheme, compress=False, keepalive=False):
    wire = gzip.compress(payload) if compress else payload
    headers = {"Authorization": scheme + " " + token, "Content-Type": "application/json"}
    if compress:
        headers["Content-Encoding"] = "gzip"
    started = time.perf_counter()
    connection = None
    try:
        if keepalive:
            endpoint = urllib.parse.urlsplit(base)
            connection = getattr(connections, "connection", None)
            if connection is None:
                connection_type = http.client.HTTPSConnection if endpoint.scheme == "https" else http.client.HTTPConnection
                connection = connection_type(endpoint.hostname, endpoint.port, timeout=25)
                connections.connection = connection
            connection.request("POST", endpoint.path.rstrip("/") + path, body=wire, headers=headers)
            response = connection.getresponse()
            raw = response.read(16 * 1024 * 1024 + 1)
            if len(raw) > 16 * 1024 * 1024:
                raise ValueError("response limit")
            status, result = response.status, json.loads(raw)
            if response.will_close:
                connection.close()
                connections.connection = None
        else:
            request = urllib.request.Request(base + path, data=wire, headers=headers)
            with urllib.request.urlopen(request, timeout=25) as response:
                raw = response.read(16 * 1024 * 1024 + 1)
                if len(raw) > 16 * 1024 * 1024:
                    raise ValueError("response limit")
                status, result = response.status, json.loads(raw)
    except urllib.error.HTTPError as error:
        # Do not retain payloads or server errors that could disclose customer data.
        status, result = error.code, {}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, http.client.HTTPException):
        if connection is not None:
            connection.close()
            connections.connection = None
        status, result = 0, {}
    return status, result, (time.perf_counter() - started) * 1000, len(wire)


def ingestion(protocol, dataset, service, events):
    if protocol == "native":
        return "/v1/logs", json.dumps({"dataset": dataset, "events": events}).encode(), "Bearer"
    if protocol == "hec":
        return "/services/collector/event", "".join(json.dumps({"index": dataset, "event": event}) for event in events).encode(), "Splunk"
    lines = []
    for event in events:
        lines.extend([json.dumps({"index": {"_index": dataset, "_id": "ignored-benchmark-id"}}), json.dumps(event)])
    return "/_bulk", ("\n".join(lines) + "\n").encode(), "ApiKey"


def confirmed(protocol, count, status, result):
    if status != 200 or not isinstance(result, dict):
        return False
    if protocol == "native":
        return result.get("committed") == count
    if protocol == "hec":
        return result.get("code") == 0
    items = result.get("items")
    return result.get("errors") is False and isinstance(items, list) and len(items) == count and all(isinstance(item, dict) and isinstance(item.get("index"), dict) and item["index"].get("status") == 201 for item in items)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18080")
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--tenant", default="demo")
    parser.add_argument("--dataset", default="logs")
    parser.add_argument("--mode", choices=["ingest", "query", "mixed"], default="mixed")
    parser.add_argument("--protocol", choices=["native", "hec", "bulk"], default="native")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--qps", type=float, default=100)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--message-bytes", type=int, default=256)
    parser.add_argument("--gzip", action="store_true")
    parser.add_argument("--keepalive", action="store_true", help="reuse one HTTP connection per generator worker")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    endpoint = urllib.parse.urlsplit(args.url)
    if endpoint.scheme not in ("http", "https") or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
        parser.error("use a credential-free HTTP(S) base URL")
    if not (1 <= args.requests <= 20000 and 0 < args.qps <= 1000 and 1 <= args.workers <= 64 and 1 <= args.batch <= 1000 and 64 <= args.message_bytes <= 4096):
        parser.error("requests 1..20000, QPS (0,1000], workers 1..64, batch 1..1000, message bytes 64..4096")
    output = Path(args.output)
    # Reserve before sending any load. Never overwrite a previous result.
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        token = credentials(args.env_file, args.tenant, args.dataset)
        run_id = "synthetic-bench-" + uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        event = {"@timestamp": now.isoformat(), "service": run_id, "message": "synthetic " + "x" * (args.message_bytes - 10)}
        events = [event.copy() for _ in range(args.batch)]
        path, payload, scheme = ingestion(args.protocol, args.dataset, run_id, events)
        if len(payload) > 4 * 1024 * 1024:
            raise ValueError("batch exceeds the gateway decoded body limit")
        event_bytes = sum(len(json.dumps(e, separators=(",", ":")).encode()) for e in events)
        search = json.dumps({"dataset": args.dataset, "from": (now - timedelta(minutes=1)).isoformat(), "to": (now + timedelta(minutes=1)).isoformat(), "service": run_id, "contains": "synthetic", "limit": 20}).encode()
        seed_events = 0
        if args.mode != "ingest":
            seed_path, seed, seed_scheme = ingestion("native", args.dataset, run_id, [event])
            status, result, _, _ = send(args.url, token, seed_path, seed, seed_scheme, keepalive=args.keepalive)
            if not confirmed("native", 1, status, result):
                raise RuntimeError("synthetic query seed was not confirmed")
            seed_events = 1

        def work(index, scheduled):
            write = args.mode == "ingest" or (args.mode == "mixed" and index % 5 == 0)
            started = time.perf_counter()
            if write:
                status, result, latency, wire_bytes = send(args.url, token, path, payload, scheme, args.gzip, args.keepalive)
                ok = confirmed(args.protocol, len(events), status, result)
            else:
                status, result, latency, wire_bytes = send(args.url, token, "/v1/logs/search", search, "Bearer", args.gzip, args.keepalive)
                ok = status == 200 and isinstance(result, dict) and isinstance(result.get("events"), list) and bool(result["events"]) and all(row.get("service") == run_id for row in result["events"])
            return {"kind": "write" if write else "query", "status": status, "ok": ok, "latency_ms": latency, "start_lag_ms": max(0, (started - scheduled) * 1000), "wire_bytes": wire_bytes}

        started = time.perf_counter()
        finished, pending = [], set()
        shed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            for index in range(args.requests):
                scheduled = started + index / args.qps
                delay = scheduled - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                completed = {future for future in pending if future.done()}
                for future in completed:
                    finished.append(future.result())
                pending -= completed
                if len(pending) >= args.workers:
                    shed += 1
                    continue
                pending.add(executor.submit(work, index, scheduled))
            offered_seconds = time.perf_counter() - started
            for future in concurrent.futures.as_completed(pending):
                finished.append(future.result())
        elapsed = time.perf_counter() - started
        successful = [r for r in finished if r["ok"]]
        failures = [r for r in finished if not r["ok"]]
        writes = [r for r in successful if r["kind"] == "write"]
        kinds = {}
        for kind in ("write", "query"):
            rows = [r for r in successful if r["kind"] == kind]
            values = [r["latency_ms"] for r in rows]
            kinds[kind] = {"successful": len(rows), "failed_or_unconfirmed": sum(r["kind"] == kind for r in failures), "p50_ms": percentile(values, 50), "p95_ms": percentile(values, 95), "p99_ms": percentile(values, 99), "successful_over_100ms": sum(value > 100 for value in values)}
        statuses = {}
        for r in finished:
            key = str(r["status"])
            statuses[key] = statuses.get(key, 0) + 1
        report = {
            "scope": "SYNTHETIC small-data HTTP smoke benchmark; not production capacity or 100TB/day acceptance",
            "production_acceptance": "NOT RUN", "run_id": run_id, "recorded_at": datetime.now(timezone.utc).isoformat(),
            "generator_platform": platform.platform(), "mode": args.mode, "protocol": args.protocol,
            "client_transport": "persistent_http_per_worker" if args.keepalive else "urllib_connection_per_request",
            "offered_requests": args.requests, "target_qps": args.qps, "workers": args.workers, "batch": args.batch,
            "offered_window_seconds": offered_seconds, "elapsed_including_drain_seconds": elapsed,
            "completed_requests": len(finished), "successful_requests": len(successful), "failed_or_unconfirmed_requests": len(failures), "client_shed_requests": shed,
            "successful_requests_per_second_including_drain": len(successful) / elapsed,
            "status_counts": statuses, "successful_latency_by_kind": kinds,
            "p99_start_lag_ms": percentile([r["start_lag_ms"] for r in finished], 99),
            "confirmed_events": len(writes) * args.batch, "seed_events_excluded_from_rates": seed_events,
            "confirmed_synthetic_event_json_bytes": len(writes) * event_bytes,
            "confirmed_synthetic_event_json_bytes_per_second": len(writes) * event_bytes / elapsed,
            "attempted_wire_request_bytes": sum(r["wire_bytes"] for r in finished), "gzip": args.gzip,
            "limits": ["latencies describe successful requests only; inspect failures and shedding", "one selective service/time/text query over this run's tiny synthetic dataset", "no sustained merge/TTL equilibrium, failure recovery, compression ratio or production disk cost measured", "timeouts/partial Bulk failures may have committed events not counted as confirmed"],
        }
        if len(finished) + shed != args.requests:
            raise AssertionError("request accounting mismatch")
        json.dump(report, handle, indent=2)
        handle.write("\n")
    print("Saved bounded synthetic benchmark report; no production claim.")
    return 1 if failures or shed else 0


if __name__ == "__main__":
    raise SystemExit(main())
