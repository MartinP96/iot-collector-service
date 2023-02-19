from abc import ABC, abstractmethod
import mysql.connector
from mysql.connector import Error

class SqlClient(ABC):

    @abstractmethod
    def connect_sql(self, host: str, database: str, user: str, password: str):
        pass

    @abstractmethod
    def disconnect_sql(self):
        pass

    @abstractmethod
    def insert_sql(self, table_name: str, column_names: list, values: list):
        pass

    @abstractmethod
    def select_sql(self):
        pass

    @abstractmethod
    def execute_stored_procedure(self):
        pass

class MySqlClient(SqlClient):

    def __init__(self):
        self.connection = None

    def connect_sql(self, host: str, database: str, user: str, password: str):
        try:
            self.connection = mysql.connector.connect(host=host,
                                                      database=database,
                                                      user=user,
                                                      password=password)
            print("Connected to SQL server")
            return 1

        except mysql.connector.Error as error:
            print("Failed to connect to server: {}".format(error))
            return -1

    def disconnect_sql(self):
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")

    def insert_sql(self, table_name: str, column_names: list, values: list):
        cursor = self.connection.cursor()
        sql_insert_str = f"INSERT INTO {table_name} ({','.join(str(x) for x in column_names)}) " \
                  f"VALUES ({','.join(str(x) for x in values)})"

        cursor.execute(sql_insert_str)
        self.connection.commit()
        cursor.close()

    def select_sql(self):
        pass

    def execute_stored_procedure(self):
        pass
