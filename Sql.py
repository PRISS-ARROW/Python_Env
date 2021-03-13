import sqlite3 as sql
import pandas as pd

train_data = pd.read_csv("/Users/maximkiesel/Desktop/Master/Vorbereitungssemester/Python\
/Written Assignment/Data_Sets/train.csv")
ideal_data = pd.read_csv("/Users/maximkiesel/Desktop/Master/Vorbereitungssemester/Python\
/Written Assignment/Data_Sets/ideal.csv")
test_data = pd.read_csv("/Users/maximkiesel/Desktop/Master/Vorbereitungssemester\
/Python/Written Assignment/Data_Sets/test.csv")

if __name__ = "Sql":
    conn = sql.connect("find_ideal_function.db")
    train_data.to_sql("train", conn)
    ideal_data.to_sql("ideal", conn)
    test_data.to_sql("test", conn)



