#!/usr/bin/env python3
"""
Watchdog2MQTT
================

Watches a directory for new files (Watchdog) and publishes them via MQTT (Eclipse Paho) based on
the EMQX file transfer protocol (incomplete).

Features:
- Monitors a directory for new files
- Publishes files via MQTT with metadata
- Supports TLS and authentication
- Robust error handling with retries and quarantine
- Configurable via command line arguments or environment variables

Usage:
    python mqtt_file_watcher.py [options]

Example:
    python mqtt_file_watcher.py --watch-dir /data --broker-host mqtt.example.com --use-tls

Environment Variables:
    All command line options can be set via environment variables:
    WATCH_DIR, MQTT_BROKER, MQTT_PORT, MQTT_USE_TLS, MQTT_AUTH_USERNAME, MQTT_AUTH_PASSWORD,
    MQTT_TOPIC, MQTT_QOS, RETRY_ATTEMPTS, RETRY_BACKOFF, QUARANTINE_DIR,
    LOG_LEVEL, LOG_FILE
"""
# "Watchdog2MQTT" in font "Standard",
# created with Text to ASCII Art Generator (https://patorjk.com/software/taag/)
WATCHDOG2MQTT_LOGO = r"""
 __        __    _       _         _             ____  __  __  ___ _____ _____ 
 \ \      / /_ _| |_ ___| |__   __| | ___   __ _|___ \|  \/  |/ _ \_   _|_   _|
  \ \ /\ / / _` | __/ __| '_ \ / _` |/ _ \ / _` | __) | |\/| | | | || |   | |  
   \ V  V / (_| | || (__| | | | (_| | (_) | (_| |/ __/| |  | | |_| || |   | |  
    \_/\_/ \__,_|\__\___|_| |_|\__,_|\___/ \__, |_____|_|  |_|\__\_\|_|   |_|  
                                           |___/                               
      Watchdog2MQTT
"""
print(WATCHDOG2MQTT_LOGO)

import argparse
import logging
import os
import time

import paho.mqtt.client as mqtt
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from mqtt_file_transfer_simple import MqttFileTransfer


class Watchdog2MqttHandler(FileSystemEventHandler):
    """
    Watchdog event handler that publishes new files via MQTT.

    This handler detects new files in the watched directory and publishes them
    via MQTT using the EMQX file transfer protocol (metadata + file content).
    It includes robust error handling with retries and quarantine for failed files.

    Attributes:
        args (argparse.Namespace): Command line arguments
        client (mqtt.Client): MQTT client instance
    """

    def __init__(self, args: argparse.Namespace):
        """
        Initialize the MQTT publish handler.

        Args:
            args (argparse.Namespace): Command line arguments
        """
        super().__init__()
        self.args = args

        self.mqtt_auth = {
            "username": self.args.username,  # username: Required[str]
            "password": self.args.password,  # password: NotRequired[str]
        } if self.args.username else None

        self.mqtt_tls = {
            "ca_certs": "",  # Required[str]
            "certfile": None,  # NotRequired[str]
            "keyfile": None,  # NotRequired[str]
            "tls_version": None,  # NotRequired[int]
            "ciphers": None,  # NotRequired[str]
            "insecure": None,  # NotRequired[bool]
        } if self.args.use_tls or self.args.ca_certs else None

        self.mqtt_file_transfer = MqttFileTransfer(
            topic=self.args.topic,  # str
            qos=self.args.qos,  # int = 1
            hostname=self.args.broker_host,  # str = "localhost"
            port=self.args.broker_port,  # int = 1883
            sensor_id=self.args.sensor_id,  # str = ""
            auth=self.mqtt_auth,  # todo  # AuthParameter [dict] | None = None
            tls=self.mqtt_tls,  # todo # TLSParameter [dict] | None = None
        )

    def on_created(self, event) -> None:
        """
        Handle file creation events.

        This method is called by Watchdog Observer when a new file is created
        in the watched directory. It reads the file and publishes it via MQTT.

        Args:
            event: Watchdog file system event
        """
        if event.is_directory:
            logging.debug(f"Detected new directory (ignored): {event.src_path}", extra={"event": event})
            return
        logging.debug(f"Detected new file: {event.src_path}", extra={"event": event})

        filepath = event.src_path
        filename = os.path.basename(filepath)
        logging.info(f"Detected new file: {filename}")

        self.mqtt_file_transfer.publish_file(filepath)


def setup_logging(args: argparse.Namespace) -> None:
    """
    Set up logging based on command line arguments.

    Args:
        args (argparse.Namespace): Command line arguments
    """
    log_level = getattr(logging, args.log_level)
    log_format = "%(asctime)s %(levelname)s %(message)s"

    if args.log_file:
        logging.basicConfig(
            level=log_level,
            format=log_format,
            filename=args.log_file,
            filemode='a'
        )
    else:
        logging.basicConfig(
            level=log_level,
            format=log_format
        )


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments and environment variables.

    Command line arguments take precedence over environment variables.
    Default values are used if neither is provided.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Watch a directory and publish new files via MQTT")

    # Required arguments
    parser.add_argument("--watch-dir", required=False, default="./watched", help="Directory to watch for new files")
    parser.add_argument("--sensor-id", required=False, default="generic-sensor", help="Name to identify the sensor that generates the file.")

    # MQTT connection arguments
    mqtt_group = parser.add_argument_group("MQTT Connection")
    mqtt_group.add_argument("--topic", default="sensors/test-sensor/file", help="MQTT topic for file transfer")
    mqtt_group.add_argument("--broker-host", required=False, default="localhost", help="MQTT broker hostname")
    mqtt_group.add_argument("--broker-port", type=int, required=False, default=1883, help="MQTT broker port")
    mqtt_group.add_argument("--qos", type=int, choices=[0, 1, 2], default=1, help="MQTT QoS level (0, 1, or 2)")
    mqtt_group.add_argument("--use-tls", action="store_true", help="Use TLS for MQTT connection")

    mqtt_auth_group = parser.add_argument_group("MQTT Auth")
    mqtt_auth_group.add_argument("--username", help="MQTT auth username")
    mqtt_auth_group.add_argument("--password", help="MQTT auth password")

    mqtt_tls_group = parser.add_argument_group("MQTT TLS")
    mqtt_tls_group.add_argument("--ca_certs", help="")  # : "",  # Required[str]
    mqtt_tls_group.add_argument("--certfile", help="")  # : None,  # NotRequired[str]
    mqtt_tls_group.add_argument("--keyfile", help="")  # : None,  # NotRequired[str]
    mqtt_tls_group.add_argument("--tls_version", help="")  # : None,  # NotRequired[int]
    mqtt_tls_group.add_argument("--ciphers", help="")  # : None,  # NotRequired[str]
    mqtt_tls_group.add_argument("--insecure", help="")  # : None,  # NotRequired[bool]

    # Error handling arguments
    # TODO implement Error handling
    error_group = parser.add_argument_group("Error Handling")
    error_group.add_argument("--retry-attempts", type=int, default=5, help="Number of retry attempts for operations")
    error_group.add_argument("--retry-backoff", type=float, default=2.0, help="Exponential backoff factor for retries (seconds)")
    error_group.add_argument("--quarantine-dir", default="./quarantine", help="Directory for files that fail to transmit")

    # Logging arguments
    log_group = parser.add_argument_group("Logging")
    log_group.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Logging level")
    log_group.add_argument("--log-file", help="Log to file instead of console")

    # Parse environment variables first, then command line arguments
    args_from_env = {
        "watch_dir": os.environ.get("WATCH_DIR"),
        "sensor_id": os.environ.get("SENSOR_ID"),
        # mqtt
        "topic": os.environ.get("MQTT_TOPIC"),
        "broker_host": os.environ.get("MQTT_BROKER"),
        "broker_port": os.environ.get("MQTT_PORT"),
        "qos": os.environ.get("MQTT_QOS"),
        # auth
        "username": os.environ.get("MQTT_AUTH_USERNAME"),
        "password": os.environ.get("MQTT_AUTH_PASSWORD"),
        # tls
        "use_tls": os.environ.get("MQTT_USE_TLS", "").lower() == "true",
        "ca_certs": os.environ.get("MQTT_TLS_CA_CERTS"),
        "certfile": os.environ.get("MQTT_TLS_CERTFILE"),
        "keyfile": os.environ.get("MQTT_TLS_KEYFILE"),
        "tls_version": os.environ.get("MQTT_TLS_TLS_VERSION"),
        "ciphers": os.environ.get("MQTT_TLS_ciphers"),
        "insecure": os.environ.get("MQTT_TLS_insecure"),
        # retry -> # todo implement retry & error handling
        "retry_attempts": os.environ.get("RETRY_ATTEMPTS"),
        "retry_backoff": os.environ.get("RETRY_BACKOFF"),
        "quarantine_dir": os.environ.get("QUARANTINE_DIR"),
        # logging
        "log_level": os.environ.get("LOG_LEVEL"),
        "log_file": os.environ.get("LOG_FILE"),
    }

    # Remove None values
    args_from_env = {k: v for k, v in args_from_env.items() if v is not None}

    # Parse command line arguments (these override environment variables)
    args = parser.parse_args()

    # Convert namespace to dictionary
    args_dict = vars(args)

    # Update with environment variables (command line takes precedence)
    for k, v in args_from_env.items():
        if args_dict[k] == parser.get_default(k):
            # Only use env var if arg is still at default value
            if k == "broker_port" or k == "qos" or k == "retry_attempts":
                args_dict[k] = int(v)
            elif k == "retry_backoff":
                args_dict[k] = float(v)
            else:
                args_dict[k] = v

    return args


def main() -> None:
    """
    Main entry point for the MQTT file watcher.

    Parses arguments, sets up logging, and starts the file watcher.
    """
    args = parse_arguments()
    setup_logging(args)

    # Create directories if they don't exist
    watch_dir = args.watch_dir
    if not os.path.exists(watch_dir):
        logging.warning(f"The watch directory '{watch_dir}' does not exist! Creating it...")
    os.makedirs(watch_dir, exist_ok=True)

    # Set up Watchdog: observer (Watchdog watch dir) and event handler (mqtt file transmission)
    event_handler = Watchdog2MqttHandler(args)
    observer = Observer()
    observer.schedule(event_handler, args.watch_dir, recursive=False)
    observer.start()

    logging.info("🚀 Watchdog2MQTT is running:")
    logging.info(f"├─ Watching {args.watch_dir} for new files...")
    logging.info(f"├─ MQTT broker: {args.broker_host}:{args.broker_port}")
    logging.info(f"└─ MQTT topic: {args.topic}")

    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping file watcher (keyboard interrupt)...")
        observer.stop()
        # event_handler.stop()
    observer.join()


if __name__ == "__main__":
    main()
