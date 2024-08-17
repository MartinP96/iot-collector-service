from .mqtt_client import IMqttClient
from .collector_configuration import CollectorConfiguration
from .device_configuration import  DeviceConfiguration

from abc import ABC, abstractmethod
from threading import Thread, Event
from queue import Queue
import json

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
    def set_configuration(self, collector_conf: CollectorConfiguration, device_topic_configuration: list,
                          device_configuration: DeviceConfiguration):
        pass

    @abstractmethod
    def get_data(self):
        pass

class MqttDataCollector(IDataCollector):

    def __init__(self, client: IMqttClient):
        self.client = client
        self.collector_configuration = None
        self.device_configuration = None
        self.device_settings = None

        self._thread_stop = False
        self.data_queue = Queue(maxsize=10)
        self.publish_queue = Queue(maxsize=5)

        self._stop_event = Event()
        self._stop_event2 = Event()
        self._collection_thread = Thread(target=self._colection_thread_fun)
        self._publish_thread = Thread(target=self._publish_thread_fun)

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
            topic = device["topic"]
            self.client.mqtt_client_subscribe(topic=topic)

    def unsubscribe_topic(self, topic):
        self.client.mqtt_client_unsubscribe(topic)

    def unsubscribe_all_topic(self):
        for device in self.device_configuration:
            topic = device["topic"]
            self.client.mqtt_client_unsubscribe(topic=topic)

    def run_collector(self):
        self._collection_thread.start()
        self._publish_thread.start()

    def stop_collector(self):
        self._stop_event.set()
        self._stop_event2.set()

    def set_configuration(self, collector_conf: CollectorConfiguration, device_topic_configuration: list,
                          device_configuration: DeviceConfiguration):
        self.collector_configuration = collector_conf
        self.device_configuration = device_topic_configuration
        self.device_settings = device_configuration

    def get_data(self):
        data = {}
        try:
            data = self.data_queue.get(timeout=0.1)
        except:
            # print("No data received")
            pass
        return data

    def publish_data(self, data):
        self.publish_queue.put(data)

    def _colection_thread_fun(self):
        #while not self._thread_stop:
        while not self._stop_event.is_set():
            data = self.client.mqtt_get_data()
            self.data_queue.put(data)
        return

    def _publish_thread_fun(self):
        #while not self._thread_stop:
        while not self._stop_event2.is_set():
            packet = self.publish_queue.get()
            mqtt_msg = json.dumps(packet["data"])
            self.client.mqtt_publish_data(topic=packet["topic"],
                                          data=mqtt_msg)
        return
