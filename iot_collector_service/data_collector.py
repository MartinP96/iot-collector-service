from .mqtt_client import IMqttClient
from .collector_configuration import CollectorConfiguration
from .device_configuration import  DeviceConfiguration

from abc import ABC, abstractmethod
from threading import Thread
from queue import Queue

class IDataCollector(ABC):

    @abstractmethod
    def run_collector(self):
        pass

    @abstractmethod
    def stop_collector(self):
        pass

    @abstractmethod
    def connect_collector(self) -> int:
        pass

    @abstractmethod
    def disconnect_collector(self):
        pass

    @abstractmethod
    def set_configuration(self, collector_conf: CollectorConfiguration, device_configuration: list):
        pass

    @abstractmethod
    def get_data(self):
        pass

class MqttDataCollector(IDataCollector):

    def __init__(self, client: IMqttClient):
        self.client = client
        self.collector_configuration = None
        self.device_configuration = None

        self._collection_thread = Thread(target=self._colection_thread)
        self._thread_stop = False
        self.data_queue = Queue(maxsize=10)

    def connect_collector(self) -> int:
        status = self.client.mqtt_client_connect(
            usr=self.collector_configuration.usr,
            password=self.collector_configuration.password,
            broker=self.collector_configuration.ip_addr,
            port=self.collector_configuration.port
        )

        if status == 1:
            self.client.mqtt_client_start()
            self.subscribe_topic()
            return 1
        else:
            return -1

    def disconnect_collector(self):
        self.client.mqtt_client_disconnect()

    def subscribe_topic(self):
        for device in self.device_configuration:
            topic = device.topic

            for t in topic:
                conn_string = t["topic"]
                self.client.mqtt_client_subscribe(topic=conn_string)

    def unsubscribe_topic(self, topic):
        self.client.mqtt_client_unsubscribe(topic)

    def run_collector(self):
        self._collection_thread.start()

    def stop_collector(self):
        self._thread_stop = True
        self._collection_thread.join()
        self._thread_stop = False

    def set_configuration(self, collector_conf: CollectorConfiguration, device_configuration: list):
        self.collector_configuration = collector_conf
        self.device_configuration = device_configuration

    def get_data(self):
        data = self.data_queue.get()
        return data

    def _colection_thread(self):
        while not self._thread_stop:
            data = self.client.mqtt_get_data()
            self.data_queue.put(data)
