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
    WATCH_DIR, MQTT_BROKER, MQTT_PORT, MQTT_USE_TLS, MQTT_USERNAME, MQTT_PASSWORD,
    MQTT_TOPIC_PREFIX, MQTT_QOS, RETRY_ATTEMPTS, RETRY_BACKOFF, QUARANTINE_DIR,
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

import os
import time
import json
import logging
import shutil
import argparse
from typing import Optional, Dict, Any, Union
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import paho.mqtt.client as mqtt


class MQTTPublishHandler(FileSystemEventHandler):
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
        self.client = None
        self._connect_mqtt_with_retries()

    def _connect_mqtt_with_retries(self) -> None:
        """
        Connect to the MQTT broker with retry logic.

        Attempts to connect to the MQTT broker, retrying with exponential backoff
        if the connection fails. Raises an exception if all retries fail.

        Raises:
            Exception: If connection fails after all retry attempts
        """
        attempt = 0
        while attempt < self.args.retry_attempts:
            try:
                self.client = mqtt.Client()
                if self.args.username and self.args.password:
                    self.client.username_pw_set(self.args.username, self.args.password)
                if self.args.use_tls:
                    self.client.tls_set()
                self.client.connect(self.args.broker_host, self.args.broker_port)
                self.client.loop_start()
                logging.info(f"Connected to MQTT broker at {self.args.broker_host}:{self.args.broker_port}")
                return
            except Exception as e:
                attempt += 1
                wait = self.args.retry_backoff ** attempt
                logging.error(f"MQTT connection failed (attempt {attempt}/{self.args.retry_attempts}): {e}")
                if attempt < self.args.retry_attempts:
                    logging.info(f"Retrying MQTT connection in {wait:.1f} seconds...")
                    time.sleep(wait)
                else:
                    logging.critical("Could not connect to MQTT broker after retries. Exiting.")
                    raise

    def _publish_with_retries(self, topic: str, payload: Union[str, bytes], qos: Optional[int] = None) -> bool:
        """
        Publish a message to MQTT with retry logic.

        Args:
            topic (str): MQTT topic to publish to
            payload (Union[str, bytes]): Message payload
            qos (Optional[int]): MQTT QoS level (0, 1, or 2). Defaults to args.qos.

        Returns:
            bool: True if publish succeeded, False if all retries failed
        """
        if qos is None:
            qos = self.args.qos

        attempt = 0
        while attempt < self.args.retry_attempts:
            try:
                result = self.client.publish(topic, payload, qos=qos)
                result.wait_for_publish()
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    return True
                else:
                    raise Exception(f"MQTT publish error code: {result.rc}")
            except Exception as e:
                attempt += 1
                wait = self.args.retry_backoff ** attempt
                logging.error(f"MQTT publish failed (attempt {attempt}/{self.args.retry_attempts}): {e}")
                if attempt < self.args.retry_attempts:
                    logging.info(f"Retrying publish in {wait:.1f} seconds...")
                    time.sleep(wait)
                    # -> retry
                else:
                    logging.error("Giving up on publishing after too many retries.")
                    return False

    def _read_file_with_retries(self, filepath: str) -> Optional[bytes]:
        """
        Read a file with retry logic.

        Args:
            filepath (str): Path to the file to read

        Returns:
            Optional[bytes]: File contents as bytes, or None if all retries failed
        """
        attempt = 0
        while attempt < self.args.retry_attempts:
            try:
                with open(filepath, "rb") as f:
                    return f.read()
            except Exception as e:
                attempt += 1
                wait = self.args.retry_backoff ** attempt
                logging.warning(f"File read failed (attempt {attempt}/{self.args.retry_attempts}): {e}")
                if attempt < self.args.retry_attempts:
                    logging.info(f"Retrying file read in {wait:.1f} seconds...")
                    time.sleep(wait)
                    # -> retry
                else:
                    logging.error("Giving up on file read after retries.")
                    return None

    def _quarantine_file(self, filepath: str) -> None:
        """
        Move a file to the quarantine directory.

        Args:
            filepath (str): Path to the file to quarantine
        """
        try:
            os.makedirs(self.args.quarantine_dir, exist_ok=True)
            dest = os.path.join(self.args.quarantine_dir, os.path.basename(filepath))
            shutil.move(filepath, dest)
            logging.warning(f"File moved to quarantine: {dest}")
        except Exception as e:
            logging.critical(f"Failed to move file to quarantine: {e}")

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

        # Read the file with retries
        file_bytes = self._read_file_with_retries(filepath)
        if file_bytes is None:
            self._quarantine_file(filepath)
            return

        # Prepare metadata for EMQX file transfer protocol
        meta = {
            "name": filename,
            "size": len(file_bytes),
            "segments": 1
        }
        file_id = str(int(time.time() * 1000))

        # Publish metadata
        meta_topic = f"{self.args.topic_prefix}{file_id}/meta"
        if not self._publish_with_retries(meta_topic, json.dumps(meta)):
            self._quarantine_file(filepath)
            return

        # Publish file content
        data_topic = f"{self.args.topic_prefix}{file_id}/0"
        if not self._publish_with_retries(data_topic, file_bytes):
            self._quarantine_file(filepath)
            return

        logging.info(f"Successfully published {filename} via MQTT.")
        logging.debug(f"Successfully published {json.dumps(meta)} via MQTT to '{meta_topic}' and {data_topic}'")

    def stop(self) -> None:
        """
        Stop the MQTT client.

        This method should be called when shutting down to properly disconnect
        from the MQTT broker.
        """
        logging.debug("Stopping watchdog client...")
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logging.info("MQTT client disconnected.")


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

    # MQTT connection arguments
    mqtt_group = parser.add_argument_group("MQTT Connection")
    mqtt_group.add_argument("--broker-host", required=False, default="localhost", help="MQTT broker hostname")
    mqtt_group.add_argument("--broker-port", type=int, required=False, default=1883, help="MQTT broker port")
    mqtt_group.add_argument("--use-tls", action="store_true", help="Use TLS for MQTT connection")
    mqtt_group.add_argument("--username", help="MQTT username")
    mqtt_group.add_argument("--password", help="MQTT password")
    mqtt_group.add_argument("--topic-prefix", default="$file/", help="MQTT topic prefix for file transfer")
    mqtt_group.add_argument("--qos", type=int, choices=[0, 1, 2], default=1, help="MQTT QoS level (0, 1, or 2)")

    # Error handling arguments
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
        "broker_host": os.environ.get("MQTT_BROKER"),
        "broker_port": os.environ.get("MQTT_PORT"),
        "use_tls": os.environ.get("MQTT_USE_TLS", "").lower() == "true",
        "username": os.environ.get("MQTT_USERNAME"),
        "password": os.environ.get("MQTT_PASSWORD"),
        "topic_prefix": os.environ.get("MQTT_TOPIC_PREFIX"),
        "qos": os.environ.get("MQTT_QOS"),
        "retry_attempts": os.environ.get("RETRY_ATTEMPTS"),
        "retry_backoff": os.environ.get("RETRY_BACKOFF"),
        "quarantine_dir": os.environ.get("QUARANTINE_DIR"),
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
    event_handler = MQTTPublishHandler(args)
    observer = Observer()
    observer.schedule(event_handler, args.watch_dir, recursive=False)
    observer.start()

    logging.info(f"Watching {args.watch_dir} for new files...")
    logging.info(f"MQTT broker: {args.broker_host}:{args.broker_port}")
    logging.info(f"MQTT topic prefix: {args.topic_prefix}")

    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping file watcher (keyboard interrupt)...")
        observer.stop()
        event_handler.stop()
    observer.join()


if __name__ == "__main__":
    main()
