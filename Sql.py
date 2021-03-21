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

    def create_table(self, name_table):
        """
        Creating tables in a database which use another the number of columns of another table to define the index
        number of the new tables.

        Parameters
        ----------
        name_table : str
            The name for the new tables.

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
            for i in range(1, self.number_columns1 + 1):
                create_table = "CREATE TABLE " + name_table + str(i) + "(id int)"  # Add the index to the new table
                # name.
                self.cursor.execute(create_table)
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.


#if __name__ == "__main__":
    #conn = sql.connect("find_ideal_function.db")
    #train_data.to_sql("train", conn)
    #ideal_data.to_sql("ideal", conn)
    #test_data.to_sql("test", conn)
    #c = sql_data("find_ideal_function.db", "train", "ideal")
    #c.create_table("y_deviation")
