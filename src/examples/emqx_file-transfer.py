#!/bin/env python3

"""
EMQX File Transfer Client Example

This script demonstrates how to use the MQTT client library to send files to
an EMQX broker using the EMQX File Transfer feature.

The EMQX File Transfer documentation is available here:
https://www.emqx.io/docs/en/v5/file-transfer/introduction.html

This example reads a file from the file system and publishes it to the
broker (the file transfer feature has to be enabled in the EMQX
configuration).

Prerequisites:
- The file transfer feature must be enabled in the EMQX broker configuration
- Python 3.10+ (for match-case syntax)
- paho-mqtt library

Usage:
    python file_transfer.py --file-id <unique_id> --file <path_to_file> [options]

Run with --help to see all available options.

Limitations:
This example does not handle PUBACK Reason Codes the broker sends in response
to the messages, due to limitations of the client library.
Hence, there's no proper error handling as it's not possible to tell if
some file transfer operation was successful or not.

The script is based on:
https://github.com/emqx/MQTT-Client-Examples/blob/master/mqtt-client-Python3/file_transfer.py - #cd442c9 (2023-08-09)
"""

import argparse
from collections import namedtuple
import logging
import os
import json
import hashlib
import paho.mqtt.client as mqtt

# Default connection parameters
BROKER = 'broker.emqx.io'
PORT = 1883
CLIENTID = 'python-mqtt-file-transfer-1'

# UserData structure to store file transfer state
UserData = namedtuple(
    "UserData",
    ["file", "file_id", "meta", "segment_size", "stages", "state"])


def process_transfer(client, userdata, stage):
    """
    Process a stage of the file transfer protocol.

    The EMQX file transfer protocol has multiple stages:
    - init: Send file metadata
    - segments: Send file content in chunks
    - fin: Send final message with total size and checksum
    - close: Clean up resources

    Args:
        client: The MQTT client instance
        userdata: The UserData namedtuple containing transfer state
        stage: Current stage of the transfer protocol
    """
    logging.info(f"Processing transfer stage '{stage}' ...")
    file_id = userdata.file_id
    match stage:
        case "init":
            # Publish the metadata
            meta = json.dumps(userdata.meta)
            pub = client.publish(f"$file/{file_id}/init", payload=meta, qos=1)
            userdata.stages[pub.mid] = "segments"

        case "segments":
            # Publish the file segments in chunks of segment_size
            sent = 0
            ssize = userdata.segment_size
            hasher = hashlib.new('sha256')  # Create checksum calculator
            segment = userdata.file.read(ssize)
            while segment:
                hasher.update(segment)
                # Topic format: $file/{file_id}/{offset}
                pub = client.publish(f"$file/{file_id}/{sent}", payload=segment, qos=1)
                sent += len(segment)
                segment = userdata.file.read(ssize)
            userdata.state["sent"] = sent
            userdata.state["hasher"] = hasher
            userdata.stages[pub.mid] = "fin"

        case "fin":
            # Publish the fin packet with total size and SHA-256 checksum
            sent = userdata.state["sent"]
            hasher = userdata.state["hasher"]
            # Topic format: $file/{file_id}/fin/{total_size}/{sha256_checksum}
            pub = client.publish(f"$file/{file_id}/fin/{sent}/{hasher.hexdigest()}", qos=1)
            userdata.stages[pub.mid] = "close"

        case "close":
            # Clean up resources and disconnect
            userdata.file.close()
            client.disconnect()


def on_connect(client, userdata, flags, rc, properties=None):
    """
    Callback for when the client connects to the broker.

    Args:
        client: The MQTT client instance
        userdata: The UserData namedtuple containing transfer state
        flags: Connection flags
        rc: Result code of the connection
        properties: MQTT v5 properties (optional)
    """
    if rc == 0 and client.is_connected():
        logging.info("Connected to the MQTT Broker")
        # If the initial connection is successful, start the file transfer.
        # Otherwise, let the client retransmit outstanding messages and complete
        # the transfer.
        if not userdata.stages:
            process_transfer(client, userdata, "init")
    else:
        logging.error(f'Failed to connect, return code {rc}')


def on_publish(client, userdata, mid):
    """
    Callback for when a message is published.

    This function drives the file transfer state machine by triggering
    the next stage when a message is successfully published.

    Args:
        client: The MQTT client instance
        userdata: The UserData namedtuple containing transfer state
        mid: Message ID of the published message
    """
    logging.info(f"Message {mid} published")
    if mid in userdata.stages:
        process_transfer(client, userdata, userdata.stages[mid])


def run():
    """
    Main function to parse arguments and start the file transfer.

    This function:
    1. Sets up logging
    2. Parses command line arguments
    3. Prepares file metadata
    4. Creates and configures the MQTT client
    5. Initiates the connection and transfer process
    """
    # Setup logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='File transfer utility')

    # MQTT connection options
    parser.add_argument("--client-id", default=CLIENTID, type=str, help="MQTT Client ID")
    parser.add_argument("--host", default=BROKER, type=str, help="MQTT Broker host")
    parser.add_argument("--port", default=PORT, type=int, help="MQTT Broker port")

    # File transfer options
    parser.add_argument("--file-id", required=True, type=str, help="Unique ID for this file transfer")
    parser.add_argument("--file", required=True, type=argparse.FileType("rb"), help="Path to the file to transfer")
    parser.add_argument("--file-name", required=False, type=str, help="Name of the file to tell the Broker, base name of --file if not specified")
    parser.add_argument("--segment-size", default=1024, type=int, help="Size of each segment to send (in bytes)")

    # Parse arguments and exit if there are any errors
    args = parser.parse_args()

    print(args)

    # Populate the metadata for the file transfer
    meta = {
        "name": args.file_name if args.file_name else os.path.basename(args.file.name),
        "size": os.stat(args.file.fileno()).st_size
    }

    # Create the MQTT client with MQTTv5 protocol
    client = mqtt.Client(
        client_id=args.client_id,
        protocol=mqtt.MQTTv5,
        transport="tcp",
        userdata=UserData(
            args.file,            # File handle
            args.file_id,         # Unique file transfer ID
            meta,                 # File metadata
            args.segment_size,    # Size of each segment
            stages={},            # Maps message IDs to next stages
            state={}              # Stores transfer state
        ))

    # Configure client logging and callbacks
    client.enable_logger(logging.getLogger())
    client.on_connect = on_connect
    client.on_publish = on_publish

    # Connect to the broker
    client.connect(args.host, args.port, keepalive=120)

    # Start the network loop and wait for completion
    client.loop_forever()


if __name__ == '__main__':
    run()
