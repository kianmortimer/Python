"""
Database Context Manager

08/06/25
Python 3.10.0
"""

import mysql.connector
import pandas

class DBContext(object):
    """
    Defines a connection to a mysql database as a Context Manager
    """
    def __init__(self, database: str, host: str = "localhost", 
                 user: str = "root", password: str = "") -> None:
        self._con: mysql.connector.MySQLConnection
        self.database: str = database
        self.host: str = host
        self.user: str = user
        self._password: str = password

    def __enter__(self):  # -> Self:
        """ Context Manager START
        Creates the connection to the database
        
        @return the current instance of DBContext
        """
        self._con = mysql.connector.connect(
            database=self.database,
            host=self.host,
            user=self.user,
            password=self._password
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """ Context Manager END 
        Closes the connection automatically
        """
        self._con.close()

    def __str__(self) -> str:
        """ String method for DBContext instance """
        return (f"Database:  {self._con.database}\n"
        f"Host:      {self._con.server_host}:{self._con.server_port}\n"
        f"User:      {self._con.user}\n"
        f"Connected: {self._con.is_connected()}")
    
    def is_connected(self) -> bool:
        """ Check if the database is connected """
        return self._con.is_connected()
    
    def select(self, query: str) -> list[tuple]:
        """ Handle SQL read query process """
        if query is None: 
            raise ValueError("Query string cannot be null")
        # Cursor object manages the SQL interaction
        cursor: mysql.connector.MySQLCursor = self._con.cursor()
        # Execute the query
        cursor.execute(query)
        results: list[tuple] = cursor.fetchall()
        # Close the cursor connection
        cursor.close()
        return results
    
    def insert(self, query: str, values: list[tuple] = None) -> int:
        """ Handle SQL write query process """
        if query is None: 
            raise ValueError("Query string cannot be null")
        elif values is not None and not (isinstance(values, list)):
            raise TypeError(f"Invalid format for values: "
                             f"Expected {type(list)}, "
                             f"Received {type(values)}")
        # Cursor object manages the SQL interaction
        cursor: mysql.connector.MySQLCursor = self._con.cursor()
        if values is None:
            # Execute the query as a basic single line
            cursor.execute(query)
        elif len(values) > 0:
            # Execute the query as a complex multi input
            cursor.executemany(query, values)
        row_count: int = cursor.rowcount
        cursor.fetchall()  # Read results in case of non insert query
        # Close the cursor connection and commit the write
        cursor.close()
        self._con.commit()
        return row_count

    @staticmethod
    def export_csv(records: list[tuple], file_name: str = "results", 
                header: list[str] | bool = True) -> None:
        """ Function for basic CSV exporting """
        if records is None or records == []:
            raise ValueError("Records list cannot be null")
        elif not isinstance(records, list):
            raise TypeError("Invalid format for records: "
                            f"Expected {type(list)}, "
                            f"Received {type(records)}")
        # Use pandas to export data
        dataframe: pandas.DataFrame = pandas.DataFrame(records)
        dataframe.to_csv(
            f"{file_name}{'' if file_name.endswith('.csv') else '.csv'}", 
            index=False, header=header
        )

    @staticmethod
    def import_csv(file_name: str) -> list:
        """ Function for reading CSVs to python """
        # Use pandas to import data
        df: pandas.DataFrame = pandas.read_csv(
            f"{file_name}{'' if file_name.endswith('.csv') else '.csv'}")
        records: list[tuple] = list(df.itertuples(index=False, name=None))
        return records