from .sql_client import ISqlClient
from .device_configuration import DeviceConfiguration
from .collector_configuration import CollectorConfiguration

from abc import ABC, abstractmethod
import csv

configuration_path = "./configuration/"

class ISQLService(ABC):

    @abstractmethod
    def connect_service(self):
        pass

    @abstractmethod
    def disconnect_service(self):
        pass

    @abstractmethod
    def read_iot_configuration(self):
        pass

    @abstractmethod
    def read_device_configuration(self):
        pass

    @abstractmethod
    def write_measurement_to_sql(self, measurement):
        pass

    @abstractmethod
    def read_data_from_sql(self):
        pass

    @abstractmethod
    def read_cmd_from_sql(self):
        pass

class SQLService(ISQLService):

    def __init__(self, sql_client: ISqlClient):
        self.sql_client = sql_client

        # Read configuration
        with open(f"{configuration_path}sql_configuration.csv") as f:
            reader = csv.DictReader(f)
            sql_configuration_dict = next(reader)
        self.sql_configuration = sql_configuration_dict

    def connect_service(self):
        status = self.sql_client.connect_sql(
                host=self.sql_configuration["host"],
                database=self.sql_configuration["database"],
                user=self.sql_configuration["user"],
                password=self.sql_configuration["password"]
                )

    def disconnect_service(self):
        self.sql_client.disconnect_sql()

    def read_iot_configuration(self):
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
        # Read device list
        topic_list = self.sql_client.execute_stored_procedure("GetTopics")
        return topic_list

    def write_measurement_to_sql(self, measurement):
        m = (measurement["topic_id"], measurement["measurement_type_id"], float(measurement["value"]))
        self.sql_client.execute_stored_procedure("InsertMeasurement", m)

    def read_data_from_sql(self):
        pass

    def read_cmd_from_sql(self):
        cmds = self.sql_client.execute_stored_procedure("GetServiceCommands")
        return cmds

    def read_parameters_from_sql(self, dev_id):
        parameters = self.sql_client.execute_stored_procedure("GetParametersForDevice", (dev_id,))
        return parameters

    def reset_cmd_flag(self, cmd_id):
        self.sql_client.execute_stored_procedure("ResetCmdFlagForDevice", (cmd_id,))
