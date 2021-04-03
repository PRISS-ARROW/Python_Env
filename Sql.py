import sqlite3 as sql
from select_ideal_func import sql_connect
import pandas as pd


# Create dataframes of 3 local csv files.
train_data = pd.read_csv("/Users/maximkiesel/Desktop/Master/Vorbereitungssemester/Python\
/Written Assignment/Data_Sets/train.csv")
ideal_data = pd.read_csv("/Users/maximkiesel/Desktop/Master/Vorbereitungssemester/Python\
/Written Assignment/Data_Sets/ideal.csv")
test_data = pd.read_csv("/Users/maximkiesel/Desktop/Master/Vorbereitungssemester\
/Python/Written Assignment/Data_Sets/test.csv")


class sql_data(sql_connect):
    """
    This class create tables.

    Inheritance
    ----------
    sql_connect
      Parent class is used to get the get existing database, tables, connection parameter execute parameter and the
      length of the used tables.

    Attributes
    ----------
    table1_name : str
        The name of existing table which is used to calculate trough all columns of another table.
    table2_name : str
        The name of existing table which is used for calculation with single columns of another table.
    db_name : str
        The name of existing data base.

    Methods
    -------
    create_table(name_table):
        Create tables with a define name and adding indexes on the name in the amount of the inherited table columns.
    """
    def __init__(self, db_name, table1_name, table2_name):
        super().__init__(db_name, table1_name, table2_name)

    def create_table(self, name_table, number):
        """
        Creating tables in a existing database.

        Parameters
        ----------
        name_table : str
            The name for the new tables.

        number : str
            How much tables should be create.

        Returns
        -------
        None

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        """
        # Create the tables.
        try:
            for i in range(1, number + 1):
                create_table = "CREATE TABLE " + name_table + str(i) + "(id int)"  # Add the index to the new table
                # name.
                self.cursor.execute(create_table)  # Execute the creating of a new table
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.


if __name__ == "__main__":
    conn = sql.connect("find_ideal_function.db")  # Create a database in sql.
    train_data.to_sql("train", conn)  # Create a sql table with the train data.
    ideal_data.to_sql("ideal", conn)  # Create a sql table with the ideal functions.
    test_data.to_sql("test", conn)  # Create a sql table with the test data.
    new_tables = sql_data("find_ideal_function.db", "train", "ideal")  # Create instance of the class sql_data.
    new_tables.create_table("y_deviation", 4)  # Create 4 new tables for all deviations between the train data and the
    # ideal functions.
