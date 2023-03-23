from abc import ABC, abstractmethod
from .data_collector_service import DataCollectorService
from .data_collector import MqttDataCollector, CollectorConfiguration
from .mqtt_client import MqttClientPaho
from .sql_client import MySqlClient
from .sql_service import SQLService
import json

from threading import Thread
from queue import Queue

import time
import datetime

class iIOTService(ABC):

    @abstractmethod
    def service_start(self):
        pass

    @abstractmethod
    def service_stop(self):
        pass

    @abstractmethod
    def service_run(self):
        pass

class IOTService(iIOTService):

    def __init__(self):
        # Define SQL client
        self.sql_client = MySqlClient()

        # Define SQL service
        self.sql_service = SQLService(sql_client=self.sql_client)
        self.sql_service.connect_service()

        self.collector_configuration = self.sql_service.read_iot_configuration()
        self.device_configuration = self.sql_service.read_device_configuration()
        self.topic_configuration = self.sql_service.read_topic_configuration()

        self.collector_service = DataCollectorService()
        for collector_conf in self.collector_configuration:
            current_collector_topic_configuration = []
            for i in self.topic_configuration:
                if i["iot_configuration"] == collector_conf.configuration_id:
                    current_collector_topic_configuration.append(i)
            collector = MqttDataCollector(MqttClientPaho())
            collector.set_configuration(collector_conf, current_collector_topic_configuration)
            self.collector_service.add_collector(collector)

        self._data_publish_thread = Thread(target=self._data_publish_thread_fun)
        self._measurement_collection_thread = Thread(target=self._measurement_collection_thread_fun)

    def service_start(self):
        self.collector_service.start_collection()
        self._data_publish_thread.start()
        self._measurement_collection_thread.start()

    def service_stop(self):
        pass

    def service_run(self):
        try:
            while 1:
                response = self.collector_service.get_data()
                topic = response["topic"]
                try:  # TMP: Začasna rešitev, v prihodnje bodo vse naprave pošiljale podatke v JSON formatu
                    data = json.loads(response["data"])
                except:
                    data = response["data"]

                # Assign measurement to device
                for i in self.topic_configuration:
                    if i["topic"] == topic:
                        if i["topic_type"] == 1:  # Measurement
                            measurement = {"device_id": i["device_id"], "topic_id": i["topic_id"]}
                            # Parse measurement packet
                            for m in data:
                                if m != "timestamp":  # TMP: Začasna rešitev, dodelati naprave da pošljejo zraven timestmp
                                    measurement["measurement_type_id"] = m
                                    measurement["value"] = data[m]
                                    print(measurement)
                                    self.sql_service.write_measurement_to_sql(measurement)

        except KeyboardInterrupt:
            self.collector_service.stop_collection()
            self.sql_client.disconnect_sql()
            print('Service interrupted')

    def _measurement_collection_thread_fun(self):
        while 1:
            response = self.collector_service.get_data()
            topic = response["topic"]
            try:  # TMP: Začasna rešitev, v prihodnje bodo vse naprave pošiljale podatke v JSON formatu
                data = json.loads(response["data"])
            except:
                data = response["data"]

            # Assign measurement to device
            for i in self.topic_configuration:
                if i["topic"] == topic:
                    if i["topic_type"] == 1:  # Measurement
                        measurement = {"device_id": i["device_id"], "topic_id": i["topic_id"]}
                        # Parse measurement packet
                        for m in data:
                            if m != "timestamp":  # TMP: Začasna rešitev, dodelati naprave da pošljejo zraven timestmp
                                measurement["measurement_type_id"] = m
                                measurement["value"] = data[m]
                                print(measurement)
                                self.sql_service.write_measurement_to_sql(measurement)

    def _data_publish_thread_fun(self):
        tmp = 0
        while 1:
            data = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "val": tmp
            }
            data_packet = {
                "topic": "IOT_System/service/beat",
                "data": data
            }
            self.collector_service.publish_data(data_packet)
            tmp += 1
            time.sleep(1)
