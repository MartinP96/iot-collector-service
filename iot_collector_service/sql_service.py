"""
===============================================================================
Module: sql_service.py
Description:
    This module defines an abstract service interface `ISQLService` and its 
    concrete implementation `SQLService`, which interacts with a SQL database 
    using a provided `ISqlClient` instance. It handles reading configurations 
    for IoT devices, writing measurements, and fetching commands.

    It depends on:
    - A SQL client interface (`ISqlClient`)
    - Data models for device and collector configurations
    - Stored procedures defined in the database

Usage:
    The `SQLService` class is designed to abstract SQL operations related to 
    an IoT system, including:
    - Establishing SQL connections
    - Reading device and collector configurations
    - Writing measurement data
    - Fetching service commands

Dependencies:
    - sql_client.py
    - device_configuration.py
    - collector_configuration.py
    - csv
    - abc (abstract base class)

Author: [Martin P]

===============================================================================
"""

from .sql_client import ISqlClient
from .device_configuration import DeviceConfiguration
from .collector_configuration import CollectorConfiguration

from abc import ABC, abstractmethod
import csv

configuration_path = "./configuration/"

class ISQLService(ABC):
    """
    Abstract base class defining an interface for SQL service operations.
    """

    @abstractmethod
    def connect_service(self):
        """Connect to the SQL database using configuration settings."""
        pass

    @abstractmethod
    def disconnect_service(self):
        """Disconnect from the SQL database."""
        pass

    @abstractmethod
    def read_iot_configuration(self):
        """Read collector (IoT broker) configuration from the database."""
        pass

    @abstractmethod
    def read_device_configuration(self):
        """Read device configurations and associated topics from the database."""
        pass

    @abstractmethod
    def write_measurement_to_sql(self, measurement):
        """Write a single measurement record to the SQL database."""
        pass

    @abstractmethod
    def read_data_from_sql(self):
        """(Placeholder) Read measurement data from the SQL database."""
        pass

    @abstractmethod
    def read_cmd_from_sql(self):
        """Read service commands from the SQL database."""
        pass

class SQLService(ISQLService):
    """
    Concrete implementation of ISQLService that uses a provided ISqlClient to
    manage database operations related to an IoT system.
    """

    def __init__(self, sql_client: ISqlClient):
        """
        Initialize SQLService with a SQL client and load configuration.
        """
        self.sql_client = sql_client

        # Read configuration
        with open(f"{configuration_path}sql_configuration.csv") as f:
            reader = csv.DictReader(f)
            sql_configuration_dict = next(reader)
        self.sql_configuration = sql_configuration_dict

    def connect_service(self):
        """
        Connect to the SQL server using configuration loaded during initialization.
        """
        try:
            status = self.sql_client.connect_sql(
                    host=self.sql_configuration["host"],
                    database=self.sql_configuration["database"],
                    user=self.sql_configuration["user"],
                    password=self.sql_configuration["password"]
                    )
        except:
            raise

    def disconnect_service(self):
        """
        Disconnect from the SQL server.
        """
        self.sql_client.disconnect_sql()

    def read_iot_configuration(self):
        """
        Fetch and return IoT broker configurations using stored procedure.
        """

        response = self.sql_client.execute_stored_procedure("GetIotConfiguration")

        configuration = []
        for conf in response:
            tmp = CollectorConfiguration(
                configuration_id=conf["id"],
                usr=conf["user"],
                password=conf["password"],
                ip_addr=conf["broker"],
                port=conf["port"],
            )

            configuration.append(tmp)
        return configuration

    def read_device_configuration(self):
        """
        Fetch and return a list of device configurations, including their topics.
        """

        # Read device list
        device_list = self.sql_client.execute_stored_procedure("GetDeviceList")

        # Get topics for each device
        configuration = []
        for dev in device_list:
            topic = self.sql_client.execute_stored_procedure("GetTopicsForDevice", (dev["device_id"],))
            dev_configuration = DeviceConfiguration(
                device_name=dev["device_name"],
                device_id=dev["device_id"],
                topic=topic,
                iot_configuration=dev["iot_configuration"])
            configuration.append(dev_configuration)
        return configuration

    def read_topic_configuration(self):
        """
        Fetch and return a list of topics from the database.
        """
        # Read device list
        topic_list = self.sql_client.execute_stored_procedure("GetTopics")
        return topic_list

    def write_measurement_to_sql(self, measurement):
        """
        Insert a measurement into the SQL database using a stored procedure.
        """
        m = (measurement["topic_id"], measurement["measurement_type_id"], float(measurement["value"]))
        self.sql_client.execute_stored_procedure("InsertMeasurement", m)

    def read_data_from_sql(self):
        """
        Placeholder for reading measurement data from the database.
        """
        pass

    def read_cmd_from_sql(self):
        """
        Fetch and return service commands using a stored procedure.
        """
        cmds = self.sql_client.execute_stored_procedure("GetServiceCommands")
        return cmds

    def read_parameters_from_sql(self, dev_id):
        """
        Fetch parameters for a specific device ID using a stored procedure.
        """
        parameters = self.sql_client.execute_stored_procedure("GetParametersForDevice", (dev_id,))
        return parameters

    def reset_cmd_flag(self, cmd_id):
        """
        Reset the command flag for a given command ID using a stored procedure.
        """
        self.sql_client.execute_stored_procedure("ResetCmdFlagForDevice", (cmd_id,))
