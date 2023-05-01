from abc import ABC, abstractmethod
import mysql.connector
from mysql.connector.locales.eng import client_error
from mysql.connector import Error

class ISqlClient(ABC):

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
    def select_sql(self, table_name: str):
        pass

    @abstractmethod
    def execute_stored_procedure(self, stored_procedure: str, input_args=()):
        pass

class MySqlClient(ISqlClient):

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

    def select_sql(self, table_name: str):
        cursor = self.connection.cursor()
        sql_select_str = f"SELECT * FROM {table_name}"
        cursor.execute(sql_select_str)
        myresult = cursor.fetchall()
        return myresult

    def execute_stored_procedure(self, stored_procedure: str, input_args=()):
        data = []
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.callproc(stored_procedure, input_args)
            self.connection.commit()
            for result in cursor.stored_results():
                data = result.fetchall()

        except mysql.connector.Error as error:
            print(f"Failed to execute stored procedure: {error}")
        return data
