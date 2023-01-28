"""
    File name: mqtt_client.py
    Date: 10.01.2023
"""

from paho.mqtt import client as mqttclient
import time
import random

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
        self.loop_start()

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

        response = 1

        self.username_pw_set(usr, password)
        self.on_connect = self.on_connect_handle
        self.connect(broker, port)

        return response

    def on_connect_handle(handle_client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)