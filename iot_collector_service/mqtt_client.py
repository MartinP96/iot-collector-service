"""
    File name: mqtt_client.py
    Date: 10.01.2023
"""

from paho.mqtt import client as mqttclient
import time
import random
from queue import Queue

class MqttClient(mqttclient.Client):
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
        self.current_data = ""
        self.current_data_dict = {}

        self.data_queue = Queue(maxsize=10)

    def mqtt_client_connect(self, usr: str, password: str, broker: str, port: int):
        """
        Connect client to MQTT broker
        args: /
        return: /
        """
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)

        self._usr = usr
        self._password = password
        self.broker = broker
        self.port = port
        self.username_pw_set(usr, password)

        # Event handles
        self.on_connect = self._on_connect_handle
        self.on_message = self._on_message_handle

        # Connect to broker
        self.connect(broker, port)

    def mqtt_client_subscribe(self, topic):
        self.current_data_dict[topic] = ""
        self.subscribe(topic)

    def _on_connect_handle(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    def _on_message_handle(self, client, userdata, msg):
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        self.current_data = msg.payload.decode()
        self.current_data_dict[msg.topic] = msg.payload.decode()
        data_packet = {"topic": msg.topic, "data": msg.payload.decode()}
        self.data_queue.put(data_packet)
