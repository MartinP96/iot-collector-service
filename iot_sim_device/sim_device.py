from abc import ABC, abstractmethod
from iot_collector_service import mqtt_client

sim_topic_configuration = {
    "data_publish_topic": "",
    "data_subscribe_topic": ""
}


class ISimDevice(ABC):
    @abstractmethod
    def run_device(self):
        pass

    @abstractmethod
    def publish_data(self):
        pass


class SimDevice(ISimDevice):

    def __init__(self, client: mqtt_client.IMqttClient, device_configuration: dict):
        self.mqtt_client = mqtt_client
        self.configuration = device_configuration

        # Connect client

    def run_device(self):
        pass

    def publish_data(self):
        pass
