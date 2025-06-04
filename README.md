# Watchdog2MQTT
Watchdog2MQTT watches a directory for new files and publishes them via MQTT oriented on
the EMQX file transfer protocol (incomplete).


## Features
  - Lightweight CLI tool
  - Monitors a directory for new files
  - Publishes files via MQTT with metadata
  - Supports TLS and authentication
  - Robust error handling with retries and quarantine
  - Configurable via command line arguments or environment variables
  - Useful logging for debugging and monitoring


## Implementation Details
Watchdog2MQTT is built using [Watchdog](https://pythonhosted.org/watchdog/) to monitor file system events and
[Eclipse Paho MQTT Python Client](https://eclipse.dev/paho/files/paho.mqtt.python/html/index.html) to transmit the data.

The file transfer is based on [EMQX File Transfer over MQTT](https://docs.emqx.com/en/emqx/latest/file-transfer/introduction.html).
In particular based on [EMQX File Transfer Clients Development](https://docs.emqx.com/en/emqx/latest/file-transfer/client.html) and
[EMQX Client Code Example ``Python3 - Paho``](https://github.com/emqx/MQTT-Client-Examples/blob/master/mqtt-client-Python3/file_transfer.py).
![EMQX File Transfer Process](./docs/)

Though full implementation of File Transfer over MQTT is an EMQX Enterprise edition feature (including the missing parts mentioned above).

incomplete list of specifications implemented and missing:
  - ✅ Topic structure follows the protocol (``$file/<file_id>/meta and $file/<file_id>/0``)
  - ✅ Metadata includes required fields (name, size, segments)

Specifications Missing/Limited:
  - ❌ File transfer does not follow .
  - ❌ No file chunking: We always set ``segments = 1`` and send the entire file as one segment — for simplicity on the broker-side.
  - ❌ No size checking: We don't check if the file exceeds MQTT message size limits
