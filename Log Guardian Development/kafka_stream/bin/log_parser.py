#!/usr/bin/env python
"""
log_parser.py

Parses raw OpenStack log lines (nova-api.log / nova-compute.log /
nova-scheduler.log, as delivered in the LogHub OpenStack dataset) into
the unified JSON schema used across the LogGuardian AI pipeline.

Real line shape (confirmed against the actual dataset, not just docs):

    <rotated-filename> <date> <time.micros> <pid> <LEVEL> <service> \
        [<oslo-context>] <rest-of-message>

Examples:
  nova-api.log.2017-05-14_21:27:04 2017-05-14 19:39:01.445 25746 INFO \
      nova.osapi_compute.wsgi.server \
      [req-5a2050e7-b381-4ae9-92d2-8b08e9f9f4c0 113d3a99... 54fadb41... - - -] \
      10.11.10.1 "GET /v2/54fadb.../servers/detail HTTP/1.1" status: 200 len: 1583 time: 0.1919448

  nova-compute.log.2017-05-14_21:27:09 2017-05-14 19:39:03.166 2931 INFO \
      nova.compute.manager [-] [instance: 2b590f10-49fd-4ec9-ae41-19596c2f4b25] \
      VM Stopped (Lifecycle Event)

  nova-compute.log.1.2017-05-16_13:55:31 2017-05-16 03:19:45.356 2931 ERROR \
      oslo_service.periodic_task Traceback (most recent call last):

Key design points:
- The "rotated filename" prefix (e.g. "nova-api.log.2017-05-14_21:27:04" or
  "nova-compute.log.1.2017-05-16_13:55:31") is normalized down to a clean
  base name ("nova-api.log", "nova-compute.log") — the rotation index/date
  is noise for our purposes and would otherwise fragment the same log
  source into dozens of distinct string values.
- The oslo context bracket "[req-... user proj - - -]" or "[-]" gives us
  request_id / user_id / project_id when present.
- Two different "shapes" of message follow the context bracket:
    1. HTTP access-log style (osapi/metadata wsgi.server lines):
       ip(s) "METHOD path HTTP/1.1" status: N len: N time: N
    2. Free-text message, optionally prefixed with "[instance: <uuid>]"
       (compute manager / libvirt / scheduler / claims / tracebacks).
  We try (1) first; if it doesn't match we fall back to (2). This means
  every line in the dataset gets parsed -- including WARNING/ERROR/CRITICAL
  lines and traceback lines, since this dataset repeats the full header on
  every physical line (no multi-line stitching needed).
- instance_id is captured whenever present, since the anomaly label file
  ships instance UUIDs (not request IDs) -- this is what a later Silver-layer
  join uses to tag events as normal/abnormal.
"""

import re
import uuid

# ---------------------------------------------------------------------------
# Stage 1: header, common to every line regardless of which nova log it's from
# ---------------------------------------------------------------------------
HEADER_PATTERN = re.compile(
    r"""
    ^(?P<log_file_raw>\S+)\s+                                     # nova-api.log.2017-05-14_21:27:04
    (?P<timestamp>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+)\s+    # 2017-05-14 19:39:01.445
    (?P<process_id>\d+)\s+                                        # 25746
    (?P<log_level>[A-Z]+)\s+                                      # INFO / WARNING / ERROR / CRITICAL
    (?P<service>\S+)\s*                                           # nova.osapi_compute.wsgi.server
                                                                    # (some ERROR lines end right here, nothing follows)
    (?:\[(?P<context>[^\]]*)\]\s*)?                                # [req-... user proj - - -] or [-] -- OPTIONAL
                                                                    # (traceback continuation lines have no bracket)
    (?P<rest>.*)$                                                  # everything else
    """,
    re.VERBOSE,
)

# Normalizes "nova-api.log.2017-05-14_21:27:04" / "nova-compute.log.1.2017-05-16_13:55:31"
# down to "nova-api.log" / "nova-compute.log".
LOG_FILE_BASE_PATTERN = re.compile(r"^(?P<base>[\w\-]+?\.log)")

# ---------------------------------------------------------------------------
# Stage 2a: HTTP access-log style rest (osapi_compute / metadata wsgi.server)
# ---------------------------------------------------------------------------
HTTP_REST_PATTERN = re.compile(
    r"""
    ^(?P<client_ip>[\d.,]+)\s+                       # 10.11.10.1  (or "10.11.21.122,10.11.10.1" via proxy)
    "(?P<method>[A-Z]+)\s+(?P<path>\S+)\s+HTTP/[\d.]+"\s*
    (?:status:\s*(?P<status_code>\d+))?\s*
    (?:len:\s*\d+)?\s*
    (?:time:\s*(?P<response_time>[\d.]+))?
    """,
    re.VERBOSE,
)

# ---------------------------------------------------------------------------
# Stage 2b: optional leading "[instance: <uuid>]" on free-text message lines
# ---------------------------------------------------------------------------
INSTANCE_PATTERN = re.compile(r"\[instance:\s*([0-9a-f\-]{36})\]")


def _normalize_log_file(raw_prefix: str) -> str:
    match = LOG_FILE_BASE_PATTERN.match(raw_prefix)
    return match.group("base") if match else raw_prefix


def _parse_context(context: str):
    """Parse the oslo request-context bracket into request_id/user_id/project_id."""
    if context is None or context.strip() == "-":
        return None, None, None
    parts = context.split()
    request_id = parts[0].replace("req-", "", 1) if parts and parts[0].startswith("req-") else None
    user_id = parts[1] if len(parts) > 1 and parts[1] != "-" else None
    project_id = parts[2] if len(parts) > 2 and parts[2] != "-" else None
    return request_id, user_id, project_id


def parse_line(raw_line: str, log_file: str = None) -> dict | None:
    """
    Convert a single raw OpenStack log line into the unified schema.
    Returns None only for lines that don't even match the common header
    (e.g. truly blank lines) -- every real log line in this dataset has
    the header, so match rate should be effectively 100%.
    """
    if not raw_line or not raw_line.strip():
        return None

    header_match = HEADER_PATTERN.match(raw_line.strip())
    if not header_match:
        return None

    h = header_match.groupdict()
    resolved_log_file = log_file or _normalize_log_file(h["log_file_raw"])

    request_id, user_id, project_id = _parse_context(h["context"])

    rest = h["rest"].strip()
    client_ip = http_method = http_path = status_code = response_time = None
    instance_id = None

    http_match = HTTP_REST_PATTERN.match(rest)
    if http_match:
        g = http_match.groupdict()
        client_ip = g["client_ip"]
        http_method = g["method"]
        http_path = g["path"]
        status_code = int(g["status_code"]) if g["status_code"] else None
        response_time = float(g["response_time"]) if g["response_time"] else None
        message = f"{http_method} {http_path}"
    else:
        instance_match = INSTANCE_PATTERN.search(rest)
        if instance_match:
            instance_id = instance_match.group(1)
            message = INSTANCE_PATTERN.sub("", rest, count=1).strip()
        else:
            message = rest

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": h["timestamp"],
        "dataset_source": "OpenStack",
        "log_file": resolved_log_file,
        "service": h["service"],
        "process_id": h["process_id"],
        "log_level": h["log_level"],
        "request_id": request_id,
        "user_id": user_id,
        "project_id": project_id,
        "instance_id": instance_id,
        "client_ip": client_ip,
        "http_method": http_method,
        "http_path": http_path,
        "message": message,
        "status_code": status_code,
        "response_time": response_time,
    }


def parse_file(path: str, log_file: str = None):
    """
    Generator that yields parsed JSON dicts from a raw log file, one at a time.
    `log_file` overrides the per-line detected filename (useful when you're
    streaming a single-source file and want a fixed value); leave it None to
    let each line resolve its own normalized log_file from its own prefix
    (needed for these OpenStack files, since api/compute/scheduler lines are
    interleaved within the same file).
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            parsed = parse_line(raw_line, log_file=log_file)
            if parsed:
                yield parsed


if __name__ == "__main__":
    sample_lines = [
        'nova-api.log.2017-05-14_21:27:04 2017-05-14 19:39:01.445 25746 INFO '
        'nova.osapi_compute.wsgi.server [req-5a2050e7-b381-4ae9-92d2-8b08e9f9f4c0 '
        '113d3a99c3da401fbd62cc2caa5b96d2 54fadb412c4e40cdbaed9335e4c35a9e - - -] '
        '10.11.10.1 "GET /v2/54fadb412c4e40cdbaed9335e4c35a9e/servers/detail HTTP/1.1" '
        'status: 200 len: 1583 time: 0.1919448',

        'nova-compute.log.2017-05-14_21:27:09 2017-05-14 19:39:03.166 2931 INFO '
        'nova.compute.manager [-] [instance: 2b590f10-49fd-4ec9-ae41-19596c2f4b25] '
        'VM Stopped (Lifecycle Event)',

        'nova-compute.log.1.2017-05-16_13:55:31 2017-05-16 03:19:45.356 2931 ERROR '
        'oslo_service.periodic_task Traceback (most recent call last):',

        'nova-api.log.1.2017-05-16_13:53:08 2017-05-16 06:25:02.869 25746 CRITICAL '
        'keystonemiddleware.auth_token [req-1cc7d50c-25a2-46b0-a668-9c00f589160c '
        '113d3a99c3da401fbd62cc2caa5b96d2 54fadb412c4e40cdbaed9335e4c35a9e - - -] '
        'Unable to validate token: Failed to fetch token data from identity server',
    ]
    for line in sample_lines:
        print(parse_line(line))
