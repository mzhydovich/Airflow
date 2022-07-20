
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import timedelta, datetime
import pandas as pd
import json


# read variables from json
json_file = open("/home/magzim/innowise/py/Airflow_Introduction/variables2.json")
variables = json.load(json_file)
json_file.close()

DIR = variables['dir']
FILE_NAME = variables['file_name']


def etl():
	"""Get data from .csv and load them to db"""
	data = pd.read_csv(DIR + FILE_NAME + ".csv")

	hook = SnowflakeHook(snowflake_conn_id=variables['conn_id'], database=variables['db'])
	engine = hook.get_sqlalchemy_engine()
	connection = engine.connect()

	#data.loc[:100].to_sql(variables['raw_table'], if_exists="append", con=engine, index=False)
	for i in range(len(data) // 16000):
		data.loc[16000 * i:16000 * (i + 1)].to_sql(variables['raw_table'], if_exists="append", con=engine, index=False)
	data.loc[16000 * i:].to_sql(variables['raw_table'], if_exists="append", con=engine, index=False)

	connection.close()
	engine.dispose()

	
with DAG("csv_to_snowflake", schedule_interval=None, catchup=False, start_date=datetime(2022, 7, 19, 0, 0)) as dag:

	task_create_db = SnowflakeOperator(task_id="task_create_db", snowflake_conn_id=variables['conn_id'], sql="CREATE OR REPLACE DATABASE " + variables['db'])
	
	task_create_raw_table = SnowflakeOperator(task_id="task_create_raw_table", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; " \
	+ "CREATE OR REPLACE TABLE " + variables['raw_table'] + """(
	"_ID" VARCHAR(16777216),
	"IOS_App_Id" NUMBER(38,0),
	"Title" VARCHAR(16777216),
	"Developer_Name" VARCHAR(16777216),
	"Developer_IOS_Id" FLOAT,
	"IOS_Store_Url" VARCHAR(16777216),
	"Seller_Official_Website" VARCHAR(16777216),
	"Age_Rating" VARCHAR(16777216),
	"Total_Average_Rating" FLOAT,
	"Total_Number_of_Ratings" FLOAT,
	"Average_Rating_For_Version" FLOAT,
	"Number_of_Ratings_For_Version" NUMBER(38,0),
	"Original_Release_Date" VARCHAR(16777216),
	"Current_Version_Release_Date" VARCHAR(16777216),
	"Price_USD" FLOAT,
	"Primary_Genre" VARCHAR(16777216),
	"All_Genres" VARCHAR(16777216),
	"Languages" VARCHAR(16777216),
	"Description" VARCHAR(16777216)
	);""")

	task_create_stage_table = SnowflakeOperator(task_id="task_create_stage_table", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; CREATE OR REPLACE TABLE " + variables['stage_table'] + " LIKE " + variables['raw_table'])
	task_create_master_table = SnowflakeOperator(task_id="task_create_master_table", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; CREATE OR REPLACE TABLE " + variables['master_table'] + " LIKE " + variables['raw_table'])
	
	task_create_raw_stream = SnowflakeOperator(task_id="task_create_raw_stream", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; CREATE OR REPLACE STREAM " + variables['raw_stream'] + " ON TABLE " + variables['raw_table'])
	task_create_stage_stream = SnowflakeOperator(task_id="task_create_stage_stream", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; CREATE OR REPLACE STREAM " + variables['stage_stream'] + " ON TABLE " + variables['stage_table'])

	task_etl = PythonOperator(task_id="task_etl", python_callable=etl)

	task_insert_from_raw_to_stage = SnowflakeOperator(task_id="task_insert_from_raw_to_stage", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; INSERT INTO " + variables['stage_table'] + """ SELECT 
		    "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description" FROM """ + variables['raw_stream'])

	task_insert_from_stage_to_master = SnowflakeOperator(task_id="task_insert_from_stage_to_master", snowflake_conn_id=variables['conn_id'], sql="USE " + variables['db'] + "; INSERT INTO " + variables['master_table'] + """ SELECT 
		    "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description" FROM """ + variables['stage_stream'])

	task_create_db >> task_create_raw_table >> task_create_stage_table >> [task_create_raw_stream, task_create_stage_stream] >> task_etl >> task_insert_from_raw_to_stage >> task_create_master_table >> task_insert_from_stage_to_master
