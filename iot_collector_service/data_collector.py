from .mqtt_client import MqttClientPaho, IMqttClient
import json
from abc import ABC, abstractmethod

data_topic = "porenta/martin_room/air_quality/data/measurements"
beat_topic = "porenta/martin_room/air_quality/sys/beat"

class IDataCollectorService(ABC):
    pass

class DataCollectorService(IDataCollectorService):
    pass

class IDataCollector(ABC):

    @abstractmethod
    def run_collector(self):
        pass

    @abstractmethod
    def connect_collector(self, usr: str, password: str, ip_addr: str, port: int):
        pass

    @abstractmethod
    def disconnect_collector(self):
        pass

class MqttDataCollector(IDataCollector):

    def __init__(self, client: IMqttClient):
        self.client = client
        self.client.mqtt_client_connect(
            usr="admin",
            password="admin",
            broker="192.168.0.101",
            port=1883
        )
        self.client.mqtt_client_start()

    def connect_collector(self, usr: str, password: str, ip_addr: str, port: int):
        self.client.mqtt_client_connect(
            usr="admin",
            password="admin",
            broker="192.168.0.101",
            port=1883
        )

    def disconnect_collector(self):
        self.client.mqtt_client_disconnect()

    def subscribe_topic(self, topic):
        self.client.mqtt_client_subscribe(topic=topic)

    def unsubscribe_topic(self, topic):
        self.client.mqtt_client_unsubscribe(topic)

    def run_collector(self):
        #while 1:
        data = self.client.mqtt_get_data()
        # Parse data
        if data["topic"] == data_topic:
            # Parse string to dict
            dict_data = json.loads(data["data"])
            print(f"Timestamp: {dict_data['timestamp']}\nTemp: {dict_data['temperature']}\n"
                  f"Hum: {dict_data['humidity']}\nCO2: {dict_data['co2']}\n")