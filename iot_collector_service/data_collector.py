from .mqtt_client import IMqttClient

from abc import ABC, abstractmethod
import json
from threading import Thread
from queue import Queue

class CollectorConfiguration:

    def __init__(self, collector_name: str,
                 usr: str,
                 password: str,
                 ip_addr: str,
                 port: int,
                 topic: dict):
        self.collector_name = collector_name
        self.usr = usr
        self.password = password
        self.ip_addr = ip_addr
        self.port = port
        self.topic = topic

class IDataCollector(ABC):

    @abstractmethod
    def run_collector(self):
        pass

    @abstractmethod
    def stop_collector(self):
        pass

    @abstractmethod
    #def connect_collector(self, usr: str, password: str, ip_addr: str, port: int):
    def connect_collector(self):
        pass

    @abstractmethod
    def disconnect_collector(self):
        pass

    @abstractmethod
    def set_configuration(self, conf: CollectorConfiguration):
        pass

    @abstractmethod
    def get_data(self):
        pass

class MqttDataCollector(IDataCollector):

    def __init__(self, client: IMqttClient):
        self.client = client
        self.collector_configuration = None

        self._collection_thread = Thread(target=self._colection_thread)
        self._thread_stop = False
        self.data_queue = Queue(maxsize=10)

    def connect_collector(self):
        self.client.mqtt_client_connect(
            usr=self.collector_configuration.usr,
            password=self.collector_configuration.password,
            broker=self.collector_configuration.ip_addr,
            port=self.collector_configuration.port
        )
        self.client.mqtt_client_start()
        self.subscribe_topic()

    def disconnect_collector(self):
        self.client.mqtt_client_disconnect()

    def subscribe_topic(self):
        for t in self.collector_configuration.topic:
            self.client.mqtt_client_subscribe(topic=self.collector_configuration.topic[t])

    def unsubscribe_topic(self, topic):
        self.client.mqtt_client_unsubscribe(topic)

    def run_collector(self):
        self._collection_thread.start()

    def stop_collector(self):
        self._thread_stop = True
        self._collection_thread.join()
        self._thread_stop = False

    def set_configuration(self, configuration: CollectorConfiguration):
        self.collector_configuration = configuration

    def get_data(self):
        data = self.data_queue.get()
        #dict_data = json.loads(data["data"])
        return data

    def _colection_thread(self):
        while not self._thread_stop:
            data = self.client.mqtt_get_data()
            self.data_queue.put(data)
