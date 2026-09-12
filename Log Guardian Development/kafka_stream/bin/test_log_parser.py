#!/usr/bin/env python
"""
test_log_parser.py

Offline unit-test for log_parser.py.
No Kafka connection required.

Tests:
  1. Built-in sample lines (bundled in log_parser.__main__)
  2. First 200 lines of each data/*.log file, reporting parse rate
  3. JSON serialisability of every parsed record
  4. Key field validation (event_id, timestamp, log_level, service, message)

Run from the project root with the venv active:
    python bin/test_log_parser.py
"""

import json
import os
import sys
import uuid

# Make sure log_parser is importable when running from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from log_parser import parse_line, parse_file

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}OK{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}WARN{RESET}  {msg}")
def fail(msg): print(f"  {RED}FAIL{RESET}  {msg}")
def info(msg): print(f"  {CYAN}INFO{RESET}  {msg}")

# ---------------------------------------------------------------------------
# TEST 1 - hand-crafted sample lines
# ---------------------------------------------------------------------------
SAMPLE_LINES = [
    (
        "HTTP INFO line (nova-api)",
        'nova-api.log.2017-05-14_21:27:04 2017-05-14 19:39:01.445 25746 INFO '
        'nova.osapi_compute.wsgi.server [req-5a2050e7-b381-4ae9-92d2-8b08e9f9f4c0 '
        '113d3a99c3da401fbd62cc2caa5b96d2 54fadb412c4e40cdbaed9335e4c35a9e - - -] '
        '10.11.10.1 "GET /v2/54fadb412c4e40cdbaed9335e4c35a9e/servers/detail HTTP/1.1" '
        'status: 200 len: 1583 time: 0.1919448',
        {
            "log_file":    "nova-api.log",
            "log_level":   "INFO",
            "http_method": "GET",
            "status_code": 200,
            "http_path":   "/v2/54fadb412c4e40cdbaed9335e4c35a9e/servers/detail",
        },
    ),
    (
        "Instance line (nova-compute)",
        'nova-compute.log.2017-05-14_21:27:09 2017-05-14 19:39:03.166 2931 INFO '
        'nova.compute.manager [-] [instance: 2b590f10-49fd-4ec9-ae41-19596c2f4b25] '
        'VM Stopped (Lifecycle Event)',
        {
            "log_file":    "nova-compute.log",
            "log_level":   "INFO",
            "instance_id": "2b590f10-49fd-4ec9-ae41-19596c2f4b25",
            "http_method": None,
        },
    ),
    (
        "ERROR traceback line (no context bracket)",
        'nova-compute.log.1.2017-05-16_13:55:31 2017-05-16 03:19:45.356 2931 ERROR '
        'oslo_service.periodic_task Traceback (most recent call last):',
        {
            "log_file":  "nova-compute.log",
            "log_level": "ERROR",
            "request_id": None,
        },
    ),
    (
        "CRITICAL line (auth_token)",
        'nova-api.log.1.2017-05-16_13:53:08 2017-05-16 06:25:02.869 25746 CRITICAL '
        'keystonemiddleware.auth_token [req-1cc7d50c-25a2-46b0-a668-9c00f589160c '
        '113d3a99c3da401fbd62cc2caa5b96d2 54fadb412c4e40cdbaed9335e4c35a9e - - -] '
        'Unable to validate token: Failed to fetch token data from identity server',
        {
            "log_level": "CRITICAL",
            "log_file":  "nova-api.log",
        },
    ),
    (
        "Blank line should return None",
        "",
        None,
    ),
]


def test_sample_lines():
    print(f"\n{'='*60}")
    print("TEST 1 - Sample Lines")
    print(f"{'='*60}")
    passed = failed = 0
    for label, raw, expected in SAMPLE_LINES:
        result = parse_line(raw)
        if expected is None:
            if result is None:
                ok(f"{label} -> correctly returned None")
                passed += 1
            else:
                fail(f"{label} -> expected None, got {result}")
                failed += 1
            continue

        if result is None:
            fail(f"{label} -> parse returned None (no match)")
            failed += 1
            continue

        # JSON round-trip
        try:
            dumped = json.dumps(result)
            reloaded = json.loads(dumped)
        except (TypeError, ValueError) as e:
            fail(f"{label} -> JSON serialisation failed: {e}")
            failed += 1
            continue

        # Field checks
        errors = []
        for key, want in expected.items():
            got = result.get(key)
            if got != want:
                errors.append(f"{key}: expected {want!r}, got {got!r}")

        # Mandatory fields always present
        for mandatory in ("event_id", "timestamp", "log_level", "service", "message"):
            if result.get(mandatory) is None:
                errors.append(f"mandatory field '{mandatory}' is None/missing")

        # event_id should be a valid UUID
        try:
            uuid.UUID(result["event_id"])
        except (ValueError, KeyError):
            errors.append("event_id is not a valid UUID")

        if errors:
            fail(f"{label}")
            for e in errors:
                print(f"       -> {e}")
            failed += 1
        else:
            ok(f"{label}")
            passed += 1

    print(f"\n  Sample lines: {passed} passed, {failed} failed")
    return failed == 0


# ---------------------------------------------------------------------------
# TEST 2 - parse rate on real log files (first N lines only)
# ---------------------------------------------------------------------------
DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
LOG_FILES   = ["openstack_normal1.log", "openstack_normal2.log", "openstack_abnormal.log"]
SAMPLE_ROWS = 200   # how many raw lines to read per file


def test_real_files():
    print(f"\n{'='*60}")
    print(f"TEST 2 - Real Log File Parse Rate (first {SAMPLE_ROWS} lines)")
    print(f"{'='*60}")
    all_ok = True
    for fname in LOG_FILES:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            warn(f"{fname} - file not found, skipping")
            continue

        total = parsed = json_ok = 0
        first_record = None
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            for raw in fh:
                if total >= SAMPLE_ROWS:
                    break
                total += 1
                rec = parse_line(raw)
                if rec is not None:
                    parsed += 1
                    try:
                        json.dumps(rec)
                        json_ok += 1
                        if first_record is None:
                            first_record = rec
                    except (TypeError, ValueError):
                        pass

        rate = (parsed / total * 100) if total else 0
        if rate >= 90:
            ok(f"{fname}  ->  {parsed}/{total} parsed ({rate:.1f}%)  |  {json_ok} JSON-serialisable")
        elif rate >= 50:
            warn(f"{fname}  ->  {parsed}/{total} parsed ({rate:.1f}%)  |  {json_ok} JSON-serialisable")
        else:
            fail(f"{fname}  ->  {parsed}/{total} parsed ({rate:.1f}%)  |  {json_ok} JSON-serialisable")
            all_ok = False

        # Pretty-print first parsed record as a JSON sample
        if first_record:
            info("First parsed record (JSON):")
            print(json.dumps(first_record, indent=4))

    return all_ok


# ---------------------------------------------------------------------------
# TEST 3 - JSON schema completeness check on a larger batch
# ---------------------------------------------------------------------------
EXPECTED_KEYS = {
    "event_id", "timestamp", "dataset_source", "log_file",
    "service", "process_id", "log_level",
    "request_id", "user_id", "project_id", "instance_id",
    "client_ip", "http_method", "http_path",
    "message", "status_code", "response_time",
}
SCHEMA_CHECK_ROWS = 500


def test_json_schema():
    print(f"\n{'='*60}")
    print(f"TEST 3 - JSON Schema Completeness ({SCHEMA_CHECK_ROWS} records)")
    print(f"{'='*60}")

    # Use the first available file
    for fname in LOG_FILES:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            break
    else:
        warn("No log files found - skipping schema test")
        return True

    missing_keys_count = 0
    extra_keys_count   = 0
    records_checked    = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for raw in fh:
            if records_checked >= SCHEMA_CHECK_ROWS:
                break
            rec = parse_line(raw)
            if rec is None:
                continue
            records_checked += 1
            rec_keys = set(rec.keys())
            missing = EXPECTED_KEYS - rec_keys
            extra   = rec_keys - EXPECTED_KEYS
            if missing:
                missing_keys_count += 1
            if extra:
                extra_keys_count += 1

    if missing_keys_count == 0 and extra_keys_count == 0:
        ok(f"All {records_checked} records have exactly the expected {len(EXPECTED_KEYS)} keys")
        return True
    else:
        if missing_keys_count:
            fail(f"{missing_keys_count} records had missing keys")
        if extra_keys_count:
            warn(f"{extra_keys_count} records had unexpected extra keys")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    results = [
        test_sample_lines(),
        test_real_files(),
        test_json_schema(),
    ]
    print(f"\n{'='*60}")
    if all(results):
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED - see details above")
    print(f"{'='*60}\n")
    sys.exit(0 if all(results) else 1)
