from abc import ABC, abstractmethod
from .data_collector_service import DataCollectorService
from .data_collector import MqttDataCollector, CollectorConfiguration
from .mqtt_client import MqttClientPaho
from .sql_client import MySqlClient
from .sql_service import SQLService
import json

from threading import Thread, Lock, Event
from queue import Queue

import time

class iIOTService(ABC):

    @abstractmethod
    def service_run(self):
        pass

    @abstractmethod
    def service_stop(self):
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

        self._stop_event = Event()
        self._mutex = Lock()
        #self._data_publish_thread = Thread(target=self._data_publish_thread_fun) # Začasno zakomentirano ker se ne rabi
        self._measurement_collection_thread = Thread(target=self._measurement_collection_thread_fun)
        self._service_main_thread = Thread(target=self._service_main_thread_fun)

    def service_run(self):
        self._service_main_thread.start()

    def service_stop(self):
        pass

    def _measurement_collection_thread_fun(self):

        self.collector_service.start_collection()
        stop_flag = False
        while 1:
            if not self._stop_event.is_set():
                stop_flag = False
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

                                    self._mutex.acquire()
                                    try:
                                        self.sql_service.write_measurement_to_sql(measurement)
                                    finally:
                                        self._mutex.release()
            else:
                if not stop_flag:
                    stop_flag = True
                    self.collector_service.hold_collection()
                    print("Collection thread stopped!")

    '''
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
            # self.collector_service.publish_data(data_packet)
            tmp += 1
            time.sleep(1)
    '''

    def _service_main_thread_fun(self):

        # Start secondary threads
        self._measurement_collection_thread.start()

        try:
            while 1:
                self._mutex.acquire()
                try:
                    # Get service commands
                    cmds = self.sql_service.read_cmd_from_sql()
                    # Service all recevied commands
                    for cmd in cmds:
                        if cmd["flag"] == 1:
                            if cmd["cmd_type"] == 100:  # CMD: Write parameters
                                self._cmd_write_parameters(cmd)
                                #self.sql_service.reset_cmd_flag(cmd["id"])
                            elif cmd["cmd_type"] == 0:  # CMD: Start service
                                self.collector_service.resume_collection()
                                self._stop_event.clear()
                                #self.sql_service.reset_cmd_flag(cmd["id"])
                            elif cmd["cmd_type"] == 1:  # CMD: Stop service
                                self._stop_event.set()
                                self.sql_service.reset_cmd_flag(cmd["id"])
                                #self._measurement_collection_thread.join()
                            elif cmd["cmd_type"] == 5:  # CMD: Get new configuration
                                # Transfer new configuraiont
                                self._cmd_get_new_configuration()

                            self.sql_service.reset_cmd_flag(cmd["id"])
                finally:
                    self._mutex.release()
                time.sleep(1)

        except KeyboardInterrupt:
            print('Service interrupted')
            self.collector_service.stop_collection()
            self.sql_client.disconnect_sql()
            self._measurement_collection_thread.join()
            self._service_main_thread.join()

    def _cmd_write_parameters(self, cmd):

        # Get parameters from SQL
        parameters = self.sql_service.read_parameters_from_sql(cmd["device_id"])

        # Get list of all topics in parameters
        topic_list = list(set([d["topic"] for d in parameters if "topic" in d]))

        # Build data packets for each topic
        for t in topic_list:
            parameter_for_topic = list(filter(lambda par: par["topic"] == t, parameters))
            data = {}
            for param in parameter_for_topic:
                data[param["param_name"]] = param["value"]
            data_packet = {
                "topic": t,
                "data": data
            }
            self.collector_service.publish_data(data_packet)

    def _cmd_get_new_configuration(self):
        print("Getting new configuration")
        self.collector_configuration = self.sql_service.read_iot_configuration()
        self.device_configuration = self.sql_service.read_device_configuration()
        self.topic_configuration = self.sql_service.read_topic_configuration()

        self.collector_service.stop_collection()

        # Create new data collectors
        for collector_conf in self.collector_configuration:
            current_collector_topic_configuration = []
            for i in self.topic_configuration:
                if i["iot_configuration"] == collector_conf.configuration_id:
                    current_collector_topic_configuration.append(i)
            collector = MqttDataCollector(MqttClientPaho())
            collector.set_configuration(collector_conf, current_collector_topic_configuration)
            self.collector_service.add_collector(collector)

        self.collector_service.start_collection()
