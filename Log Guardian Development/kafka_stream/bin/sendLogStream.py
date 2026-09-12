#!/usr/bin/env python
"""
sendLogStream.py

Drop-in replacement for the original repo's bin/sendStream.py.
Instead of reading a timestamped CSV, it reads a raw OpenStack log file,
parses each line into the unified JSON schema (via log_parser.py), and
replays it to Kafka preserving the original inter-event timing
(optionally sped up with --speed).

Usage:
    python bin/sendLogStream.py data/nova-api.log my-stream --speed 100

Everything below the argparse block mirrors the control flow of the
original sendStream.py (same firstline bootstrap, same diff/sleep timing,
same producer.flush()/acked callback) — only the "what is a record"
part has changed.
"""

import argparse
import json
import sys
import time
import socket
from datetime import datetime

from confluent_kafka import Producer

# log_parser.py should live alongside this script (or on PYTHONPATH)
from log_parser import parse_line


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def event_stream(filename):
    """
    Generator that yields parsed event dicts, skipping any line that
    the regex parser can't match (there shouldn't be any -- see log_parser.py).

    Note: we do NOT pass a fixed log_file override here. Your OpenStack
    dataset files interleave nova-api.log / nova-compute.log /
    nova-scheduler.log lines all within the same physical file, so each
    line must resolve its own real source from its own header prefix.
    """
    with open(filename, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            event = parse_line(raw_line)
            if event is not None:
                yield event


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                         help='Raw OpenStack log file (e.g. nova-api.log).')
    parser.add_argument('topic', type=str,
                         help='Name of the Kafka topic to stream to.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                         help='Speed up replay by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    # ----------------------------------------------------------------
    # Aiven Free Trial config  (set these env vars before running)
    #
    #   $env:AIVEN_BOOTSTRAP  = "kafka-xxxxx-yourproject.aivencloud.com:12345"
    #   $env:AIVEN_USERNAME   = "avnadmin"          # shown in Aiven console
    #   $env:AIVEN_PASSWORD   = "YOUR_AIVEN_PASSWORD"
    #   $env:AIVEN_CA_CERT    = "certs/ca.pem"      # download from Aiven console
    #
    # For local Kafka (docker-compose), leave AIVEN_BOOTSTRAP unset
    # (falls back to localhost:9092 with no SSL)
    # ----------------------------------------------------------------
    import os
    bootstrap = os.environ.get("AIVEN_BOOTSTRAP", "localhost:9092")
    username  = os.environ.get("AIVEN_USERNAME",  "")
    password  = os.environ.get("AIVEN_PASSWORD",  "")
    ca_cert   = os.environ.get("AIVEN_CA_CERT",   "certs/ca.pem")

    conf = {
        'bootstrap.servers': bootstrap,
        'client.id': socket.gethostname(),
    }
    if username:
        conf.update({
            'security.protocol':        'SASL_SSL',
            'sasl.mechanisms':          'PLAIN',
            'sasl.username':            username,
            'sasl.password':            password,
            'ssl.ca.location':          ca_cert,   # Aiven requires the CA cert
        })

    producer = Producer(conf)

    stream = event_stream(args.filename)

    prev_timestamp = None
    event_count = 0

    for event in stream:
        try:
            curr_timestamp = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        except (ValueError, TypeError):
            # Malformed timestamp on an otherwise-parsed line -> skip timing, send immediately
            curr_timestamp = None

        if prev_timestamp is not None and curr_timestamp is not None:
            diff_seconds = (curr_timestamp - prev_timestamp).total_seconds()
            # Guard against negative diffs from out-of-order/rotated log files
            if diff_seconds > 0:
                time.sleep(diff_seconds / args.speed)

        jresult = json.dumps(event)
        producer.produce(topic, key=p_key, value=jresult, callback=acked)
        producer.poll(0)  # serve delivery callbacks without blocking

        if curr_timestamp is not None:
            prev_timestamp = curr_timestamp

        event_count += 1
        if event_count % 500 == 0:
            producer.flush()

    producer.flush()
    print(f"Finished streaming {event_count} events from {args.filename} to topic '{topic}'.")


if __name__ == "__main__":
    main()
