#!/usr/bin/env python

"""Consumes stream for printing all messages to the console.
"""

import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException


def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = msg.value().decode('utf-8')
    dval = json.loads(val)
    print(time_start, dval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    # ----------------------------------------------------------------
    # Aiven Free Trial config  (set these env vars before running)
    #
    #   $env:AIVEN_BOOTSTRAP  = "kafka-xxxxx-yourproject.aivencloud.com:12345"
    #   $env:AIVEN_USERNAME   = "avnadmin"
    #   $env:AIVEN_PASSWORD   = "YOUR_AIVEN_PASSWORD"
    #   $env:AIVEN_CA_CERT    = "certs/ca.pem"   # download from Aiven console
    #
    # For local Kafka (docker-compose), leave AIVEN_BOOTSTRAP unset.
    # ----------------------------------------------------------------
    import os
    bootstrap = os.environ.get("AIVEN_BOOTSTRAP", "localhost:9092")
    username  = os.environ.get("AIVEN_USERNAME",  "")
    password  = os.environ.get("AIVEN_PASSWORD",  "")
    ca_cert   = os.environ.get("AIVEN_CA_CERT",   "certs/ca.pem")

    print(f"Connecting to Kafka at {bootstrap}...")
    if username:
        print(f"Using Aiven credentials (user: {username})")
    else:
        print("WARNING: AIVEN_BOOTSTRAP not set — using localhost:9092 (Docker must be running)")

    conf = {
        'bootstrap.servers': bootstrap,
        'auto.offset.reset': 'earliest',
        'group.id': socket.gethostname(),
    }
    if username:
        conf.update({
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms':   'PLAIN',
            'sasl.username':     username,
            'sasl.password':     password,
            'ssl.ca.location':   ca_cert,
        })

    consumer = Consumer(conf)
    consumer.subscribe([args.topic])
    print(f"Subscribed to topic '{args.topic}'")
    print("Waiting for messages... (Ctrl+C to stop)\n")

    running = True

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write(f"Topic '{args.topic}' not found yet — retrying...\n")
                    time.sleep(2)
                elif msg.error():
                    sys.stderr.write('%% Kafka Error: %s\n' % (msg.error()))
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
