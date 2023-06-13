# ETL-POSTGRES-

Search Medium
Write


The provided code represents an Extract, Transform, and Load (ETL) pipeline implemented using PySpark. The pipeline performs the following steps:

Importing necessary libraries:
findspark: Initializes the Spark environment.
pyspark: Provides the PySpark library for Spark operations.
SparkSession and functions from pyspark.sql: Required for Spark SQL operations.
os: Required for setting environment variables.
2. Defining the ETL function:

The etl_function is the main function that performs the ETL operations.
It starts by creating a SparkSession with the name “ETL” and configuring it with the required parameters.
The spark.jars configuration is set to include the PostgreSQL JDBC driver.
Two data sources are read using the spark.read.format("csv") method, which loads CSV files from local paths.
Data extraction is completed by reading the “student1.csv” and “student2.csv” files and creating DataFrames (df and df2).
3. Transformations on DataFrame 1 (df):

Multiple transformations are applied to clean and process the data:
Filtering out rows where the “Age” column is empty or less than or equal to 0.
Filtering out rows where the “marks” column is empty or less than 60.
Converting the “date” column to a DateType using the to_date function.
Casting the “Id” column to a BigInt type.
Casting the “Age” and “marks” columns to Integer types.
4. Transformations on DataFrame 2 (df2):

Multiple transformations are applied to clean and process the data:
Converting the “date” column to a DateType using the to_date function.
Casting the “Id” column to a BigInt type.
Casting the “Age” and “marks” columns to Integer types.
Creating a temporary view named “table2” to enable executing SQL queries on df2.
Executing an SQL query to select rows from “table2” where the “marks” column is greater than 60.
5. Combining the results:

The transformed DataFrames (df and df2) are combined using the union method to create a new DataFrame named result_df.
The result_df DataFrame is written to a PostgreSQL database using the JDBC format.
The database connection details are provided in the options, including the URL, table name, username, password, and driver.
The save method is used to append the data to the existing "student_test" table in the database.
The show method is called on the result_df DataFrame to display the contents of the DataFrame.
The spark.stop() method is called to stop the SparkSession and release the resources.
To run the ETL pipeline, execute the etl_function() when the script is directly invoked (i.e., __name__ == "__main__").

Note: The provided code assumes that the necessary dependencies, such as Spark and the PostgreSQL JDBC driver, are properly installed and configured.

The code for the etl_function is below.

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
For orchestration, I run the airflow locally on my machine, and the steps involved in the installation and running of airflow locally are given in this link [3].

The provided code snippet defines a DAG (Directed Acyclic Graph) in Apache Airflow [4]. Here is the documentation for the code and its components:

PythonOperator — Airflow Documentation [5]
The code imports the PythonOperator from the airflow.operators.python module.
The PythonOperator is used to execute a Python callable as a task in an Airflow DAG.
The PythonOperator executes the etl_function from the test1 module as a task in the DAG.
The code creates a new DAG object named “my_dag” using the DAG constructor.
The DAG has a start date of June 7, 2023 (datetime(2023, 6, 7)) and is scheduled to run daily (schedule_interval="@daily").
The DAG has one task named “etl_task” defined using the PythonOperator. The task executes the etl_function from the test1 module.
The etl_task is added to the DAG using the dag parameter of the PythonOperator.
The complete code for the DAG “my_dag” is given below.


from airflow import DAG
from modules import test1
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG("my_dag", start_date=datetime(2023,6,7),
          schedule_interval="@daily", catchup = False) as dag:

        etl_task= PythonOperator(
        
                task_id="etl_task",
                python_callable = test1.etl_function,
                dag=dag
        )      
        
        etl_task
After running the airflow locally visit localhost:8080 in your browser to access its UI and it will match the screen below containing the “my_dag” DAG.


After you click the “my_dag“ and run it and in case of success it will match the below screen.


After the DAG has run successfully, you can open the Postgres SQL to check if the data has landed in the student_test table. In case of success, you can see the below records in the student_test table if you have used my provided CSV files.


The complete code along with the CSV files used for this demo can be downloaded from

https://github.com/AsadUllah3/ETL-POSTGRES-/tree/master

Conclusion:

In this post, we learned

to write an ETL pipeline that reads multiple CSV files, transforms them, and writes the results to a Postgres database
use airflow with Python operator for orchestration of the workflow.
References:

Apache Spark Documentation: Pipeline — PySpark 3.4.0 documentation [1]
Towards Data Science: Create your first ETL Pipeline in Apache Spark and Python [2]
https://airflow.apache.org/docs/apache-airflow/stable/start.html [3]
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html [4]
https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#pythonoperator-airflow-documentation [5]
