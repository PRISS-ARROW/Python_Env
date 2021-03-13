import numpy as py
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3 as sql


# Create a class there I can create a connection to a specific table of a specific db.
class conn_sql:
    """
    This class connect to a specific database and with the method "select" it is
    possible to select a specific row of a specific table of the previous selected data
    base.
    """
    def __init__(self, db_name, columns_name, table_name):
        self.db_name = db_name
        self.columns_name = columns_name
        self.table_name = table_name
        self.conn = sql.connect(self.db_name)
        self.select_columns = pd.read_sql("SELECT " + self.columns_name + " FROM " + self.table_name, self.conn)
        self.cursor = self.conn.cursor()


# Unit test: eine Datendank/table auswählen und testen lassen.

# This is a class which take one row of a specific table of a database and subtract all values of this one row with
# all values of all rows of another table of a database.

class sql_calc(conn_sql):
    """
    Doc string: to insert!
    """
    def __init__(self, db_name, columns_name, table_name, table2_name):
        super().__init__(db_name, columns_name, table_name)
        self.table2_name = table2_name


    def calc(self):
        # Count the numbers of the of one table.
        columnsQuery1 = "PRAGMA table_info(ideal)"
        self.cursor.execute(columnsQuery1)
        numberOfColumns1 = len(self.cursor.fetchall())-2

        # Count the numbers of the of the other table.
        columnsQuery2 = "PRAGMA table_info(ideal)"
        self.cursor.execute(columnsQuery2)
        numberOfColumns2 = len(self.cursor.fetchall())-2
        # For Schleife für die berechnung
        for i in range(numberOfColumns1-1):
            a = pd.read_sql("SELECT " + self.columns_name + " FROM " + self.table_name, self.conn) - \
                pd.read_sql("SELECT " + self.columns_name + " FROM " + self.table2_name, self.conn)
            for j in range()

        #for i in numberOfColumns:
        # Speichern der ergebnisse in einer anderen Table



a = sql_calc("find_ideal_function.db", "y1", "train", "test")

a.calc()











