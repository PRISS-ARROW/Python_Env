import numpy as np
import pandas as pd
import sqlite3 as sql
from exceptions import Wrong_Input


class sql_connect:
    """
    This class connect to a existing database and perform calculation with the tables in this database.

    Attributes
    ----------
    table1_name : str
        The name of existing table which is used to calculate trough all columns of another table.
    table2_name : str
        The name of existing table which is used for calculation with single columns of another table.
    db_name : str
        The name of existing data base.
    conn :
        Create a connection to a database.
    cursor:
        Execute sql statements.
    number_columns1:
        Column length of the first table.
    number_columns2:
        Column length of the second table.
    """
    def __init__(self, db_name, table1_name, table2_name):
        """
        Constructs all the necessary attributes for the sql_connect object.

        Parameters
        ----------
        table1_name : str
            The name of existing table which is used to calculate trough all columns of another table.
        table2_name : str
            The name of existing table which is used for calculation with single columns of another table.
        db_name : str
            The name of existing data base.
        conn :
            Create a connection to a database.
        cursor:
            Execute sql statements.
        number_columns1:
            Column length of the first table.
        number_columns2:
            Column length of the second table.
        """
        self.db_name = db_name
        self.table1_name = table1_name
        self.table2_name = table2_name
        self.conn = sql.connect(self.db_name)
        self.cursor = self.conn.cursor()
        # Determine the number of columns.
        columns_query1 = "PRAGMA table_info(" + self.table1_name + ")"
        self.cursor.execute(columns_query1)
        self.number_columns1 = len(self.cursor.fetchall()) - 2
        # Determine the number of columns.
        columns_query2 = "PRAGMA table_info(" + self.table2_name + ")"
        self.cursor.execute(columns_query2)
        self.number_columns2 = len(self.cursor.fetchall()) - 2


class sql_calc(sql_connect):
    """
    This class is used to calculate columns in a specific form, transfer the results in other tables.

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
    calc_delta_sq(column1_name, table_name):
        Calculate the delta deviation and square them.
    read_table(table_name):
        Read the sql data from a table.
    min_dev(table_name):
        Sum every row of each column separately and determine the minimum value.
    max_dev(table_name_ideal, table_name_train, ideal_func, train_data):
        Determine the maximum deviation of two single columns of the corresponding tables.
    create_dev_func(max_dev, name_min_dev, table_name):
        Create a table with two columns (content: 2 functions). This functions are the chosen ideal functions added
        with a maximum deviation between this ideal function and the corresponding training data multiplied with
        the squared root of 2.
    insert_value_table(x_value, ideal1, ideal2, ideal3, ideal4, table_name):
        Create a new table with data from 4 columns of a existing table.
    mapping(self, test_data, ideal_function_collect, dev_func):
        Select y-data from a column of a table and determine if this data is in a specific range of a ideal function.
        Then save this data in a new table with his x-value, the smallest delta to the ideal function and the column
        name of the ideal function.
    """

    def __init__(self, db_name, table1_name, table2_name):
        """
        Constructs all the necessary attributes for the sql_calc object.
        Use the parameter of the parent class sql_connect to communicate and work with a database and get information
        about used the tables in the database.

        Parameters
        ----------
        table1_name : str
            he name of existing table which is used to calculate trough all columns of another table.
        table2_name : str
            The name of existing table which is used for calculation with single columns of another table.
        db_name : str
            The name of existing data base.
        """
        super().__init__(db_name, table1_name, table2_name)

    def calc_delta_sq(self, column1_name, table_name):
        """
        Method to calculate the deviation and then squares it between a single columns of a table and all columns
        of another table.

        Parameters
        ----------
        column1_name : str
            The column name with out the index of table1_name.
        table_name : str
            The input are existing table names, but with out their index.

        Returns
        -------
        None

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        Wrong_Input
            Check if there is a number in the input data.
        """
        try:
            if any(map(str.isdigit, column1_name)) or any(map(str.isdigit, table_name)):  # Check if there is a number
                # in the input data.
                raise Wrong_Input  # Raise a user defined exception.
            else:
                string = ""
                for i in range(1, self.number_columns2 + 1):  # Collect all column names of the table to one variable.
                    string += column1_name + str(i) + ", "
                string = string[0:len(string) - 2]
                for i in range(1, self.number_columns1 + 1):  # Delta calculation with each column of table1_name.
                    a = pd.read_sql("SELECT " + column1_name + str(i) + " FROM " + self.table1_name, self.conn)
                    d = np.array(a)  # Transform the dataframe in a array for calculation.
                    b = pd.read_sql("SELECT " + string + " FROM " + self.table2_name, self.conn)  # Then select all
                    # columns of a table which are need for a delta calculation.
                    c = b.sub(d, axis="columns") ** 2  # This calculates the delta and then squares it.
                    c.to_sql(table_name + str(i), con=self.conn, if_exists='replace')  # Import the delta to the
                    # suitable table.
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.

    def read_table(self, table_name):
        """
        Read a table from a sql database.

        Parameters
        ----------
        table_name : str
            The full table name.

        Returns
        -------
        The selected data in a DataFrame format.

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        """
        try:
            select_data = pd.read_sql("SELECT * FROM " + table_name, self.conn)  # Convert a specific sql table in a
            # data frame
            return select_data
        except TypeError:
            print("Please insert for the variable only string data.")  # Exception if the user fill the input data
            # with the wrong type.

    def min_dev(self, table_name, column_name):
        """
        Determine the column name of smallest sum of a column in a table.

        Parameters
        ----------
        table_name : str
            The name of the table for which the minimum should be determined.
        column_name : str
            The general name of the columns without the index.
        Returns
        -------
        The name of the column with the minimum sum of all columns.

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        Wrong_Input
            Check if there is a number in the input data.
        """
        try:
            if any(map(str.isdigit, column_name)):  # Check if there is a number in the input data.
                raise Wrong_Input  # Raise a user defined exception.
            else:
                # Determine the column length of "table_name".
                columns_query = "PRAGMA table_info(" + table_name + ")"
                self.cursor.execute(columns_query)
                number_columns = len(self.cursor.fetchall()) - 1
                # Collect all sums.
                sum_all = []
                for i in range(1, number_columns + 1):
                    # Select the specific column.
                    select_data = pd.read_sql("SELECT " + column_name + str(i) + " FROM " + table_name, self.conn)
                    sum_column = float(select_data.sum())  # Sum all values of the specific column.
                    sum_all.append(sum_column)  # Insert the sum in a list.
                min_sum = min(sum_all)  # Determine the smallest value of the sums.
                for j in range(0, len(sum_all)):  # find the position/index of the ideal function
                    if sum_all[j] == min_sum:
                        index = j + 1  # Add 1 because the iterator starts with 0.
                return column_name + str(index)  # Return the full column name of the ideal function.
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.

    def max_dev(self, ideal_func, train_data):
        """
        Determine the maximum deviation between to columns.

        Parameters
        ----------
        ideal_func : str
            The column name of the chosen ideal function.
        train_data : str
            The column name of the corresponding training data.
        Returns
        -------
        The Value of the maximum deviation.

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        """
        try:
            ideal_data = pd.read_sql("SELECT " + ideal_func + " FROM " + self.table2_name, self.conn)  # Select the
            # specific column.
            train_data = pd.read_sql("SELECT " + train_data + " FROM " + self.table1_name, self.conn)  # Select the
            # specific column.
            train_data_np = np.array(train_data)  # Transform the dataframe in a array for calculation.
            delta1 = ideal_data.sub(train_data_np, axis="columns") ** 2  # This calculates the delta and squares it.
            # The squaring is important to consider also negative values.
            max_delta = (delta1[ideal_func].max())  # Determine the maximum delta.
            return max_delta ** 0.5  # Return the square root of the maximum delta to get the real maximum value.
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.

    def create_dev_func(self, max_dev, name_min_dev, table_name):
        """
        Create a table with two columns (content: 2 functions). This functions are the chosen ideal functions added
        with a maximum deviation between this ideal function and the corresponding training data multiplied with
        the squared root of 2.

        Parameters
        ----------
         max_dev : str
            Maximum deviation between to columns.
        name_min_dev : str
            Column name of chosen ideal function of a specific training data set.
        table_name : str
            The name of the new table.

        Returns
        -------
        None

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        """
        try:
            min_dev_data = pd.read_sql("SELECT " + name_min_dev + " FROM " + self.table2_name, self.conn)  # Select the
            # column data from the data base.
            calc1 = min_dev_data + (max_dev * (2 ** 0.5))  # Adding to the ideal function the positive maximum deviation
            # multiplied with the squared root of 2.
            calc2 = min_dev_data + (max_dev * (2 ** 0.5) * (-1))  # Adding to the ideal function the negative maximum
            # deviation multiplied with the squared root of 2.
            calc1.insert(1, name_min_dev + "_2", calc2, True)  # Connect the two dataframes to one for further
            # processing.
            calc1.to_sql(table_name, con=self.conn, if_exists='replace')  # Add the data to the new table.
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.

    def insert_value_table(self, x_value, ideal1, ideal2, ideal3, ideal4, table_name):
        """
        Create a new table with data from 4 columns of a existing table.

        Parameters
        ----------
        ideal1-4 : str
            Name of columns of a existing table.
        table_name : str
            The name of the new table.
         x_value : str
            The column name of a x-value in a table.


        Returns
        -------
        None

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        """
        try:
            # Create dataframes from the sql data for each column.
            x_value_column = pd.read_sql("SELECT " + x_value + " FROM " + self.table2_name, self.conn)
            ideal1_column = pd.read_sql("SELECT " + ideal1 + " FROM " + self.table2_name, self.conn)
            ideal2_column = pd.read_sql("SELECT " + ideal2 + " FROM " + self.table2_name, self.conn)
            ideal3_column = pd.read_sql("SELECT " + ideal3 + " FROM " + self.table2_name, self.conn)
            ideal4_column = pd.read_sql("SELECT " + ideal4 + " FROM " + self.table2_name, self.conn)
            # Integrated all other dataframes in one.
            ideal1_column.insert(1, x_value, x_value_column, True)
            ideal1_column.insert(1, ideal2, ideal2_column, True)
            ideal1_column.insert(1, ideal3, ideal3_column, True)
            ideal1_column.insert(1, ideal4, ideal4_column, True)
            # Change the order to put the x value in the position 1 and the other columns in the order of their
            # corresponding train data.
            cols = ideal1_column.columns.tolist()
            cols = [cols[4]] + [cols[0]] + [cols[3]] + [cols[2]] + [cols[1]]
            # create a sql table with the new data.
            ideal1_column[cols].to_sql(table_name, con=self.conn, if_exists='replace')
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.

    def mapping(self, test_data, ideal_function_collect, dev_func):
        """
        Select y-data from a column of a table and determine if this data is in a specific range of a ideal function.
        Then save this data in a new table with his x-value, the smallest delta to the ideal function and the column
        name of the ideal function.

        Parameters
        ----------
        test_data : str
            Name of the test data table.
        ideal_function_collect : str
            Name of the ideal function data table.
        dev_func: str
            This functions are the chosen ideal functions added
            with a maximum deviation between this ideal function and the corresponding training data multiplied with
            the squared root of 2.

        Returns
        -------
        None

        Exception
        ---------
        try/except
            Handle the exception then the user of the Methode use the wrong data type.
        """
        try:
            test = pd.read_sql("SELECT * FROM " + test_data, self.conn)  # Get the test data from the data base.
            ideal_function_collect = pd.read_sql("SELECT * FROM " + ideal_function_collect, self.conn)  # Get the ideal
            # functions from the data base.
            test.insert(3, "delta_ideal_func", np.NaN, True)  # Add a column with the delta value between the y train
            # data and the corresponding ideal function. Fill all rows wit empty values (NaN).
            test.insert(4, "no_ideal_func", np.NaN, True)  # Add a column with the matching ideal function. Fill all
            # rows wit empty values (NaN).
            for i in range(0, test.count().iloc[1]):  # Iterate trough all x values of test data.
                u = 0  # Variable to count the numbers of the dev_func tables.
                m = {}  # Empty list to collect all deltas between a ideal function the matching training point.
                for j in range(0, ideal_function_collect.count().iloc[1]):  # Iterate through all x values of the ideal
                    # functions.
                    if test.iloc[i, 1] == ideal_function_collect.iloc[j, 1]:  # Match the the test data and ideal
                        # function x values to find the position of the y values of the ideal functions.
                        for k in ideal_function_collect.columns.tolist()[2:]:  # Iterate through all selected ideal
                            # function column names.
                            u += 1  # Count the numbers of the dev_func tables.
                            # Get the maximum deviations (positive an negative) of the ideal functions.
                            ideal_func_dev = pd.read_sql("SELECT * FROM " + dev_func + str(u), self.conn)
                            max_dev = ideal_func_dev.iloc[j, 1]
                            min_dev = ideal_func_dev.iloc[j, 2]
                            if min_dev <= test.iloc[i, 2] <= max_dev:  # Check if the test data is between the max_dev
                                # and min_dev.
                                delta = (ideal_function_collect.iloc[j, u + 1] - test.iloc[i, 2]) ** 2  # Calculate
                                # delta to ideal and square it (to be sure that the programme don´t choose the wrong
                                # delta value because it is negative).
                                m[k] = delta  # Append the delta value in the list.
                        # Find the minimum delta value in the dictionary (important then one train point can fit in
                        # two ideal functions.
                        if m != {}:
                            g = min(m, key=m.get)  # Find the smallest value.
                            test.loc[i, "delta_ideal_func"] = m[g] ** 0.5  # Import the choose value in the data frame
                            # of "test".
                            test.loc[i, "no_ideal_func"] = g  # Import the ideal function column name of the choose
                            # value in the data frame of "test".
            test.to_sql("new_train", con=self.conn, if_exists='replace')  # Import the "test" data frame to sql in
            # a new table.
        except TypeError:
            print("Please insert for the two variables only string data.")  # Exception if the user fill the input data
            # with the wrong type.


if __name__ == '__main__':
    start = sql_calc("find_ideal_function.db", "train", "ideal")  # Create instance of the class sql_calc.
    start.calc_delta_sq("y", "y_deviation")  # Calculate the deviation between the train data and all ideal functions.
    # Create for the 4 choose ideal functions a table each with two columns (content: 2 functions). This functions are
    # the chosen ideal functions added with a maximum deviation between this ideal function and the corresponding
    # training data multiplied with the squared root of 2.
    start.create_dev_func(start.max_dev(start.min_dev("y_deviation1", "y"), "y1"), start.min_dev("y_deviation1", "y"),
                          "dev_func1")  #
    start.create_dev_func(start.max_dev(start.min_dev("y_deviation2", "y"), "y2"), start.min_dev("y_deviation2", "y"),
                          "dev_func2")
    start.create_dev_func(start.max_dev(start.min_dev("y_deviation3", "y"), "y3"), start.min_dev("y_deviation3", "y"),
                          "dev_func3")
    start.create_dev_func(start.max_dev(start.min_dev("y_deviation4", "y"), "y4"), start.min_dev("y_deviation4", "y"),
                          "dev_func4")
    # Create a table with the 4 ideal functions and the corresponding x value.
    start.insert_value_table("x", start.min_dev("y_deviation1", "y"), start.min_dev("y_deviation2", "y"),
                             start.min_dev("y_deviation3", "y"), start.min_dev("y_deviation4", "y"),
                             "ideal_function_collect")
    # Map the test points to the ideal function.
    start.mapping("test", "ideal_function_collect", "dev_func")
