from bokeh.plotting import figure
from bokeh.io import show
import pandas as pd
from select_ideal_func import sql_calc

if __name__ == '__main__':
    #  Get the column names of the ideal functions.
    a = sql_calc("find_ideal_function.db", "train", "ideal")  # Create a instance of the class sql_calc
    min_column1 = a.min_dev("y_deviation1", "y")
    min_column2 = a.min_dev("y_deviation2", "y")
    min_column3 = a.min_dev("y_deviation3", "y")
    min_column4 = a.min_dev("y_deviation4", "y")

    #  Get all ideal functions from the database.
    ideal_func1 = pd.read_sql("SELECT " + min_column1 + " FROM ideal", a.conn)
    ideal_func2 = pd.read_sql("SELECT " + min_column2 + " FROM ideal", a.conn)
    ideal_func3 = pd.read_sql("SELECT " + min_column3 + " FROM ideal", a.conn)
    ideal_func4 = pd.read_sql("SELECT " + min_column4 + " FROM ideal", a.conn)

    # Get all training data
    train_data1 = pd.read_sql("SELECT y1 FROM train", a.conn)
    train_data2 = pd.read_sql("SELECT y2 FROM train", a.conn)
    train_data3 = pd.read_sql("SELECT y3 FROM train", a.conn)
    train_data4 = pd.read_sql("SELECT y4 FROM train", a.conn)

    # Get the x values from test and ideal/train.
    x_values = pd.read_sql("SELECT x FROM ideal", a.conn)
    x_values_test = pd.read_sql("SELECT x FROM test", a.conn)

    # Get the test data.
    test_data = pd.read_sql("SELECT y FROM test", a.conn)

    # Get the two maximum (positive and negative)(multiplied with the squared root of 2) deviation functions of the
    # chosen ideal functions.
    a1 = pd.read_sql("SELECT y38 FROM dev_func1", a.conn)
    a2 = pd.read_sql("SELECT y38_2 FROM dev_func1", a.conn)
    a3 = pd.read_sql("SELECT y46 FROM dev_func2", a.conn)
    a4 = pd.read_sql("SELECT y46_2 FROM dev_func2", a.conn)
    a5 = pd.read_sql("SELECT y49 FROM dev_func3", a.conn)
    a6 = pd.read_sql("SELECT y49_2 FROM dev_func3", a.conn)
    a7 = pd.read_sql("SELECT y5 FROM dev_func4", a.conn)
    a8 = pd.read_sql("SELECT y5_2 FROM dev_func4", a.conn)

    # Creating a figure
    p1 = figure(plot_width=1450, plot_height=800, title='All Ideal function', x_axis_label='X', y_axis_label='Y')

    p1.background_fill_color = "black"  # Create a transparent background.
    p1.background_fill_alpha = 0.2  # Define the transparency of the background
    # Create a white grid for the y and x axis.
    p1.ygrid.minor_grid_line_color = 'white'
    p1.xgrid.minor_grid_line_color = 'white'

    # Add line glyph for the chosen ideal functions and the corresponding maximum deviation functions.
    p1.line(x_values["x"].tolist(), ideal_func1[min_column1].tolist(), line_color="navy", legend_label="ideal "
                                                                                                       "function 1")
    p1.line(x_values["x"].tolist(), a1["y38"].tolist(), line_color="tomato", legend_label="ideal function 1 delta")
    p1.line(x_values["x"].tolist(), a2["y38_2"].tolist(), line_color="tomato", legend_label="ideal function 1 delta")

    p1.line(x_values["x"].tolist(), ideal_func2[min_column2].tolist(), line_color="green", legend_label="ideal "
                                                                                                       "function 2")
    p1.line(x_values["x"].tolist(), a3["y46"].tolist(), line_color="khaki", legend_label="ideal function 2 delta")
    p1.line(x_values["x"].tolist(), a4["y46_2"].tolist(), line_color="khaki", legend_label="ideal function 2 delta")

    p1.line(x_values["x"].tolist(), ideal_func3[min_column3].tolist(), line_color="purple", legend_label="ideal "
                                                                                                       "function 3")
    p1.line(x_values["x"].tolist(), a5["y49"].tolist(), line_color="hotpink", legend_label="ideal function 3 delta")
    p1.line(x_values["x"].tolist(), a6["y49_2"].tolist(), line_color="hotpink", legend_label="ideal function 3 delta")

    p1.line(x_values["x"].tolist(), ideal_func4[min_column4].tolist(), line_color="grey", legend_label="ideal "
                                                                                                       "function 4")
    p1.line(x_values["x"].tolist(), a7["y5"].tolist(), line_color="peru", legend_label="ideal function 4 delta")
    p1.line(x_values["x"].tolist(), a8["y5_2"].tolist(), line_color="peru", legend_label="ideal function 4 delta")

    # Add circle glyph for the train data.
    p1.circle(x_values_test["x"].tolist(), test_data["y"].tolist(), size=2, color='crimson', legend_label="test data")

    p1.legend.location = "bottom_left"  # Place the legend location in the bottom left area where are no points or
    # graphs

    # Show the plot
    show(p1)
