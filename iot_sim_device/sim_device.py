from abc import ABC, abstractmethod
from iot_collector_service import mqtt_client
from time import sleep
import json
from threading import Thread
import random


class SimDeviceConfiguration:
    def __init__(self, broker_usr: str, broker_password: str, broker_ip: str, broker_port: int, publish_topic: str,
                 subscribe_topic: str, publish_interval: float, device_measurements: list):

        self.measurements = device_measurements
        self.configuration = {
            "broker": {
                "usr": broker_usr,
                "password": broker_password,
                "ip_addr":  broker_ip,
                "port": broker_port
            },
            "data_publish_topic": publish_topic,
            "data_subscribe_topic": subscribe_topic,
            "publish_interval": publish_interval
        }


class ISimDevice(ABC):
    @abstractmethod
    def run_device(self):
        pass

    @abstractmethod
    def _publish_data_fun(self):
        pass


class SimDevice(ISimDevice):

    def __init__(self, client: mqtt_client.IMqttClient, device_configuration: SimDeviceConfiguration):
        self.mqtt_client = client
        self.sim_device_configuration = device_configuration

        # Connect client
        self.measurement_conf = device_configuration.measurements
        status = self.mqtt_client.mqtt_client_connect(
            usr=self.sim_device_configuration.configuration["broker"]["usr"],
            password=self.sim_device_configuration.configuration["broker"]["password"],
            broker=self.sim_device_configuration.configuration["broker"]["ip_addr"],
            port=self.sim_device_configuration.configuration["broker"]["port"]
        )

        self.publish_interval = self.sim_device_configuration.configuration["publish_interval"]

        if status == 1:
            print(f"Device connected to the broker {self.sim_device_configuration.configuration['broker']['usr']}")
        else:
            print("Connection to broker failed.")

        self.mqtt_client.mqtt_client_start()

        # Define sim device thread
        self._sim_device_thread = Thread(target=self._publish_data_fun)
        self._sim_device_receive_thread = Thread(target=self._receive_data_fun)

        # Subrscribe to topic
        if self.sim_device_configuration.configuration["data_subscribe_topic"] != "":
            self.mqtt_client.mqtt_client_subscribe(self.sim_device_configuration.configuration["data_subscribe_topic"])

    def run_device(self):
        self._sim_device_thread.start()
        self._sim_device_receive_thread.start()

    def _publish_data_fun(self):

        # Generate random measurements
        while 1:
            data = {}
            for mindex in self.measurement_conf:
                if mindex["floating_point"] == '1':
                    value = random.uniform(int(mindex["min_value"]), int(mindex["max_value"]))
                else:
                    value = random.randint(int(mindex["min_value"]), int(mindex["max_value"]))
                data[mindex["measurement_type"]] = value

            print(data)
            mqtt_msg = json.dumps(data)
            self.mqtt_client.mqtt_publish_data(topic=self.sim_device_configuration.configuration["data_publish_topic"],
                                               data=mqtt_msg)
            sleep(self.publish_interval)

    def _receive_data_fun(self):
        while 1:
            param_packet = self.mqtt_client.mqtt_get_data()
            data = param_packet["data"]
            dict_data = json.loads(data)
            self.publish_interval = dict_data["Sampling"]
            print(dict_data)
