from .sql_client import ISqlClient
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
    def write_data_to_sql(self):
        pass

    @abstractmethod
    def read_data_from_sql(self):
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
        pass

    def write_data_to_sql(self):
        pass

    def read_data_from_sql(self):
        pass
