"""
===============================================================================
Module: iot_service.py
Description:
    This module implements the `IOTService` class, which serves as the core 
    runtime of an IoT data collection system. It connects to a MySQL database, 
    reads device and topic configurations, manages MQTT data collectors, 
    handles service commands (e.g., start/stop collection, write parameters), 
    and writes measurements to SQL.

    Key Responsibilities:
    - Initialize and run the service loop in background threads.
    - Manage collector configuration and MQTT connections.
    - Process incoming MQTT data and convert to structured measurements.
    - Execute control commands from SQL (like start, stop, reconfigure).

Dependencies:
    - SQL Client (`MySqlClient`) and SQL Service (`SQLService`)
    - MQTT Client (`MqttClientPaho`)
    - DataCollectorService, MqttDataCollector
    - Logging (via `setup_logger`)
    - Standard libraries: `threading`, `json`, `sys`, `os`, `time`

Author: [Martin P]
===============================================================================
"""

from abc import ABC, abstractmethod
from .data_collector_service import DataCollectorService
from .data_collector import MqttDataCollector, CollectorConfiguration
from .mqtt_client import MqttClientPaho
from .sql_client import MySqlClient
from .sql_service import SQLService
from .event_logging import setup_logger
import json

import sys
from threading import Thread, Lock, Event
import time
import os

class iIOTService(ABC):
    """
    Abstract base class for IoT services.
    Defines the interface for service execution and setup routines.
    """
    @abstractmethod
    def service_run(self):
        """Starts the IoT service runtime."""
        pass

    @abstractmethod
    def _create_folder_structure(self):
        """Initializes required folder structures."""
        pass

class IOTService(iIOTService):
    """
    Implements the core IoT data collection service logic.
    Manages MQTT collectors, SQL interactions, and threaded execution of service and data collection.
    """
    def __init__(self):
        """
        Constructor that initializes the IoT service.
        Sets up logger, database connection, reads configuration,
        creates collectors, and prepares background threads.
        """
        # Create folder structure
        self._create_folder_structure()

        # Create datalog
        self.logger = setup_logger("IOT Service Log", "iot_service_logs/log")

        # Define SQL client
        self.sql_client = MySqlClient()

        # Define SQL service
        self.sql_service = SQLService(sql_client=self.sql_client)

        try:
            self.sql_service.connect_service()
            self.logger.info(f"Connecting to SQL server!")
        except:
            self.logger.info(f"Connecting to SQL failed! Stopping program execution")
            sys.exit(1)

        self.logger.info(f"Connected to sql!")

        self.collector_configuration = self.sql_service.read_iot_configuration()
        self.device_configuration = self.sql_service.read_device_configuration()
        self.topic_configuration = self.sql_service.read_topic_configuration()

        self.collector_service = DataCollectorService()
        for collector_conf in self.collector_configuration:
            for device_conf in self.device_configuration:
                current_collector_topic_configuration = []
                for i in self.topic_configuration:
                    if i["iot_configuration"] == collector_conf.configuration_id and i["device_id"] == device_conf.device_id:
                        current_collector_topic_configuration.append(i)
                collector = MqttDataCollector(MqttClientPaho())
                collector.set_configuration(collector_conf, current_collector_topic_configuration, device_conf)
                self.collector_service.add_collector(collector)

        self._stop_event = Event()
        self._mutex = Lock()
        #self._data_publish_thread = Thread(target=self._data_publish_thread_fun) # Začasno zakomentirano ker se ne rabi
        self._measurement_collection_thread = Thread(target=self._measurement_collection_thread_fun)
        self._service_main_thread = Thread(target=self._service_main_thread_fun)

    def service_run(self):
        """
        Starts the main service thread.
        """
        self._service_main_thread.start()

    def _measurement_collection_thread_fun(self):
        """
        Thread function that continuously collects and stores measurements from MQTT.
        Parses data and writes measurements to the SQL database.
        Suspends collection on stop signal.
        """
        self.collector_service.start_collection()
        stop_flag = False
        while 1:
            if not self._stop_event.is_set():
                stop_flag = False
                data_packet = self.collector_service.get_data()
                if data_packet:
                    for response in data_packet:
                        topic = response["topic"]
                        data = json.loads(response["data"])
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
                time.sleep(1)

    def _service_main_thread_fun(self):
        """
        Thread function that continuously reads and processes control commands from SQL.
        Supports commands like start, stop, write parameters, and reload configuration.
        """
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
        """
        Handles the command to write device parameters.
        Publishes parameter packets via MQTT to respective topics.
        """
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
        """
        Handles the command to reload the IoT configuration from SQL.
        Stops current collection, creates new collectors, and resumes collection.
        """
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

    def _create_folder_structure(self):
        """
        Creates the log folder structure required by the service.
        """
        # Create collector log folder
        if not os.path.exists("iot_service_logs/"):
            os.makedirs("iot_service_logs/")
