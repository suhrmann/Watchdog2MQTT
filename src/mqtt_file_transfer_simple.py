# Based on Paho MQTT Python Client
# Protocol and message format from https://mygit.th-deg.de/dmine/dmine-backend/-/tree/main/development/sensor-mock

import base64
import json
import logging
import os
import random
from datetime import datetime

import paho
import paho.mqtt.client as mqtt
from paho.mqtt import publish

logger = logging.getLogger(__name__)


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
        """
        Configure MQTT client parameters for file transfer.

        :param topic:
        :param hostname:
        :param port:
        :param qos:
        :param sensor_id:
        :param auth: See paho.mqtt.publish.AuthParameter
        :param tls: See paho.mqtt.publish.TLSParameter
        """
        self.topic = topic
        self.host = hostname
        self.port = port
        self.qos = qos

        self.client_id = f'watchdog2mqtt-{random.randint(1000, 9999)}'
        self.sensor_id = sensor_id if sensor_id else 'generic-sensor'

        self.auth = auth  # See paho.mqtt.publish.AuthParameter
        self.tls = tls  # See paho.mqtt.publish.TLSParameter

        self.protocol = paho.mqtt.client.MQTTv5  # use MQTT v5 (over default v3.1.1)

    def publish_file(self, filepath):
        """
        Publish a file with MQTT.

        :param filepath: The path of the file to publish.
        """
        # meta data
        file_name = os.path.basename(filepath)  # full filename from path
        file_type = file_name.split('.')[-1] if file_name.count('.') > 0 else None  # get type of file - e.g. ".pdf", ".jpg", or ".docx"

        # read file
        f = open(filepath, 'rb')
        file_data: bytes = f.read()
        logger.debug(f'Read file {file_name}')

        # create message payload
        msg = MqttFileTransfer._create_mqtt_message(file_name, file_type, file_data, self.sensor_id)
        logger.debug(f'Created mqtt message')

        # publish message with mqtt / Paho
        try:
            publish.single(topic=self.topic, payload=msg, qos=self.qos,
                           hostname=self.host, port=self.port,
                           client_id=self.client_id,
                           protocol=self.protocol,
                           auth=self.auth,
                           tls=self.tls)
            logger.info(f"Published file successfully: {file_name}")
        except Exception as e:
            logger.error(f"Failed publish of file {file_name} - Exception: {e}")

    @staticmethod
    def _create_mqtt_message(file_name: str, file_type: str | None, file_data: bytes, sensor_id: str) -> str:
        """Create a message containing image data and metadata."""
        # encode file data as bas64
        file_base64 = base64.b64encode(file_data).decode('utf-8')  # todo decode necessary?

        return json.dumps({
            'timestamp': MqttFileTransfer._time_iso_str(),
            'file_data': file_base64,
            'sensor_id': sensor_id,
            'file_name': file_name,
            'file_type': file_type,
            'encoding': 'base64'
        })

    @staticmethod
    def _time_iso_str():
        """ Create timestamp as ISO string.
        The full format looks like 'YYYY-MM-DD HH:MM:SS.mmmmmm'. See `datetime.isoformat()`
        """
        return datetime.now().isoformat()
