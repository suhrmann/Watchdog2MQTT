## THIS FILE IS BROKEN
# This file contains a summary of robust and demo transmission stuff!

# Based on Paho MQTT Python Client
# Protocol and message format from https://mygit.th-deg.de/dmine/dmine-backend/-/tree/main/development/sensor-mock

import base64
import logging
import os
import shutil
import time
from typing import Optional

import paho.mqtt.client as mqtt

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


################################################################################################################################################################################################################################################


def publish_file(filepath, mqtt_host, mqtt_port, mqtt_topic):
    # todo
    pass

def _encode_image(self, image):
    """Encode TIFF Image to base64 string."""
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()
    return base64.b64encode(img_byte_arr).decode('utf-8')

def _create_mqtt_message(self, image_data) -> str:
    """Create a message containing image data and metadata."""
    return json.dumps({
        'timestamp': ImageLoopSensorMock._time_iso_str(),
        'image_data': image_data,
        'sensor_id': f'mock_sensor_{random.randint(0, 1000)}',
        'image_format': 'PNG',
        'encoding': 'base64'
    })
