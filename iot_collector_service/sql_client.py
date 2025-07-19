"""
===============================================================================
Module: mysql_client.py
Description:
    This module defines an interface `ISqlClient` for SQL client operations 
    and a concrete implementation `MySqlClient` that uses the MySQL Connector 
    to interact with a MySQL database.

    The implementation includes:
    - Connecting and disconnecting from a MySQL database.
    - Inserting and selecting data from tables.
    - Executing stored procedures with optional input arguments.

Dependencies:
    - mysql-connector-python
    - Python's abc (Abstract Base Class) module

Author: [Martin P.]

===============================================================================
"""


from abc import ABC, abstractmethod
import mysql.connector
from mysql.connector.locales.eng import client_error
from mysql.connector import Error

class ISqlClient(ABC):
    """
    Abstract base class that defines the interface for an SQL client.
    Subclasses must implement all abstract methods to interact with a database.
    """

    @abstractmethod
    def connect_sql(self, host: str, database: str, user: str, password: str):
        """Connect to the SQL database."""
        pass

    @abstractmethod
    def disconnect_sql(self):
        """Disconnect from the SQL database."""
        pass

    @abstractmethod
    def insert_sql(self, table_name: str, column_names: list, values: list):
        """Insert a record into the specified table."""
        pass

    @abstractmethod
    def select_sql(self, table_name: str):
        """Select and return all records from the specified table."""
        pass

    @abstractmethod
    def execute_stored_procedure(self, stored_procedure: str, input_args=()):
        """Execute a stored procedure with optional input arguments."""
        pass

class MySqlClient(ISqlClient):
    """
    Concrete implementation of ISqlClient for MySQL databases using mysql.connector.
    """

    def __init__(self):
        self.connection = None

    def connect_sql(self, host: str, database: str, user: str, password: str):
        """
        Connect to the MySQL database using the provided credentials.
        Returns 1 if successful, raises an exception on failure.
        """
        try:
            self.connection = mysql.connector.connect(host=host,
                                                      database=database,
                                                      user=user,
                                                      password=password)
            print("Connected to SQL server")
            return 1

        except mysql.connector.Error as error:
            print("Failed to connect to server: {}".format(error))
            raise

    def disconnect_sql(self):
        """
        Close the MySQL database connection if it is open.
        """
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")

    def insert_sql(self, table_name: str, column_names: list, values: list):
        """
        Insert a new record into the specified table using the given column names and values.
        """
        cursor = self.connection.cursor()
        sql_insert_str = f"INSERT INTO {table_name} ({','.join(str(x) for x in column_names)}) " \
                  f"VALUES ({','.join(str(x) for x in values)})"

        cursor.execute(sql_insert_str)
        self.connection.commit()
        cursor.close()

    def select_sql(self, table_name: str):
        """
        Fetch and return all records from the specified table.
        """
        cursor = self.connection.cursor()
        sql_select_str = f"SELECT * FROM {table_name}"
        cursor.execute(sql_select_str)
        myresult = cursor.fetchall()
        return myresult

    def execute_stored_procedure(self, stored_procedure: str, input_args=()):
        """
        Execute the specified stored procedure with optional input arguments.
        Returns any data returned by the procedure.
        """
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
