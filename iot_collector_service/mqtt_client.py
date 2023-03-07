"""
    File name: mqtt_client.py
    Date: 10.01.2023
"""

from paho.mqtt import client as mqttclient
from abc import ABC, abstractmethod
import random
from queue import Queue

class IMqttClient(ABC):
    @abstractmethod
    def mqtt_client_connect(self, usr: str, password: str, broker: str, port: int) -> int:
        pass

    @abstractmethod
    def mqtt_client_start(self):
        pass

    @abstractmethod
    def mqtt_client_disconnect(self):
        pass

    @abstractmethod
    def mqtt_client_subscribe(self, topic):
        pass

    @abstractmethod
    def mqtt_client_unsubscribe(self, topic):
        pass

    @abstractmethod
    def mqtt_get_data(self):
        pass


class MqttClientPaho(IMqttClient, mqttclient.Client):
    """
    Mqtt Client for reading and writing to mqtt broker

    Args:

    Attributes:

    """
    def __init__(self):
        self.client_id = f'python-mqtt-{random.randint(0, 1000)}'
        super().__init__(self.client_id)

        self._usr = ""
        self._password = ""
        self.broker = ""
        self.port = 0
        self.data_queue = Queue(maxsize=10)

    def mqtt_client_connect(self, usr: str, password: str, broker: str, port: int):
        """
        Connect client to MQTT broker
        args: /
        return: /
        """
        self._usr = usr
        self._password = password
        self.broker = broker
        self.port = port
        self.username_pw_set(usr, password)

        # Event handles
        self.on_connect = self._on_connect_handle
        self.on_message = self._on_message_handle
        self.on_log = self._on_log_handle

        # Connect to broker
        try:
            self.connect(broker, port)
            return 1
        except Exception as sh:
            print(sh)
            return -1

    def mqtt_client_start(self):
        self.loop_start()

    def mqtt_client_disconnect(self):
        self.disconnect()

    def mqtt_client_subscribe(self, topic):
        self.subscribe(topic)

    def mqtt_client_unsubscribe(self, topic):
        self.unsubscribe(topic)

    def mqtt_get_data(self):
        return self.data_queue.get()

    def _on_connect_handle(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    def _on_message_handle(self, client, userdata, msg):
        data_packet = {"topic": msg.topic, "data": msg.payload.decode()}
        self.data_queue.put(data_packet)

    def _on_log_handle(self, userdata, level, buf):
        print(buf)