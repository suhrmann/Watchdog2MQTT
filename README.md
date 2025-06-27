# Watchdog2MQTT
Watchdog2MQTT watches a directory for new files and publishes them via MQTT oriented on
the EMQX file transfer protocol (incomplete).


## Features
  - Lightweight CLI tool
  - Monitors a directory for new files
  - Publishes files via MQTT with metadata
  - Supports TLS and authentication
  - Configurable via command line arguments or environment variables
  - Logging for debugging and monitoring
  - 🚧 TODO: Robust error handling with retries and quarantine

## Requirements
 - Python v3.9 or higher

## Usage
**tl;dr:** Download the project, install Python requirements and run ``src/watchdog2mqtt.py``

1. Download or clone the Watchdog2MQTT: ``git clone https://github.com/suhrmann/Watchdog2MQTT``
2. [recommended] Create a virtual environment for the Python requirements: ``cd Watchdog2MQTT && python3 -m venv venv``
  2.1 Activate the virtual environment: ``source venv/bin/activate`` for MacOS and Linux  or ``venv\Scripts\activate`` on Windows
5. Install Python requirements: ``pip install -r requirements.txt``
6. Start the application: ``python src/watchdog2mqtt.py``


## Implementation Details
Watchdog2MQTT is built using [Watchdog](https://pythonhosted.org/watchdog/) to monitor file system events and
[Eclipse Paho MQTT Python Client](https://eclipse.dev/paho/files/paho.mqtt.python/html/index.html) to transmit the data.

### File Watcher: Watchdog

MQTT File Transmission
The file transfer functionality is provided by the ``MqttFileTransfer`` class.
This class implements a simple protocol that encodes the file as base64 and sends it in a single JSON message, 
which is then published to the specified MQTT topic.

To use a different file transmission protocol simply create a new class with the same constructor params, 
and implement method ``publish_file(self, filepath)``, like this:
```python
class MqttFileTransfer:
    def __init__(self,
                 topic: str,
                 sensor_id: str | None = None,
                 qos: int = 1,
                 hostname: str = "localhost",
                 port: int = 1883,
                 auth: dict | None = None,
                 tls: dict | None = None,
                 ) -> None:
        # implement me

    def publish_file(self, filepath):
        # implement me
```

## Limitations / Missing:
  - ❌ No file chunking: the entire file is sent as one segment — for simplicity; may lead to next problem:
  - ❌ No size checking: We don't check if the file exceeds MQTT message size limits
    - => Have a look at [EMQX File transfer over MQTT](https://www.emqx.com/en/blog/file-transfer-over-mqtt)
  - ❌ Unit Tests
