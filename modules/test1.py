import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk-19.jdk/Contents/Home"
# os.environ["JAVA_HOME"] = JAVA_HOME


def etl_function():
    spark = SparkSession.builder\
        .appName("ETL")\
        .config("spark.jars", "/Users/asadullah/Desktop/student/postgresql-42.2.6.jar") \
        .getOrCreate()

    # extract data
    df = spark.read.format("csv").option("header", "true").load('file:///Users/asadullah/Desktop/student1.csv')
    df2 = spark.read.format("csv").option("header", "true").load('file:///Users/asadullah/Desktop/student2.csv')

    # transform data 

    # transformations on Dataframe1
    df = df.filter((df.Age != '') & (df.Age > 0))
    df = df.filter(df.marks != '')
    df = df.filter(df.marks >=60 )
    df = df.withColumn("date", to_date(df["date"], "dd/mm/yyyy"))
    df = df.withColumn("Id",df.Id.cast('bigint'))
    df = df.withColumn("Age",df.Age.cast('int'))
    df = df.withColumn("marks",df.marks.cast('int'))

    # transformations on Dataframe2
    df2 = df2.withColumn("date", to_date(df2["date"], "dd/mm/yyyy"))
    df2 = df2.withColumn("Id",df2.Id.cast('bigint'))
    df2 = df2.withColumn("Age",df2.Age.cast('int'))
    df2 = df2.withColumn("marks",df2.marks.cast('int'))
    df2.createOrReplaceTempView("table2")
    sql_query = "SELECT * FROM table2 WHERE marks > 60"
    df2 = spark.sql(sql_query)

    # combining the results of the dataframe
    result_df = df.union(df2)
    # result_df.show()


    # load data 

    result_df.write\
    .format("jdbc")\
    .mode("append")\
    .option("url", "jdbc:postgresql://localhost:5434/student")\
    .option("dbtable", "student_test")\
    .option("user", "postgres")\
    .option("password", "password")\
    .option("driver", "org.postgresql.Driver")\
    .save()
    result_df.show()

    spark.stop()

if __name__ == "__main__":
    etl_function()