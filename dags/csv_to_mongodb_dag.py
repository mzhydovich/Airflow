
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

import pandas as pd

import json

from pymongo import MongoClient


default_args = {
    "owner": "Maksim Zhydovich",
    "schedule_interval": "@once",
    "start_date": datetime(2021, 7, 7, 23, 21)
}

# read variables from json
json_file = open("variables.json")
variables = json.load(json_file)
json_file.close()

DIR = variables['dir']
FILE_NAME = variables['file_name']
LOCAL_HOST = variables['localhost']
DB = variables['db']
COLLECTION = variables['collection']


def extract(ti):
    """Get all data from .csv"""
    print("---------------Extract---------------")

    data = pd.read_csv(DIR + FILE_NAME + ".csv")

    data.to_csv(DIR + FILE_NAME + "_raw.csv")

    ti.xcom_push(FILE_NAME + "_raw", DIR + FILE_NAME + "_raw.csv")


def transform(ti):
    """Prepare data for loading"""
    print("---------------Transform---------------")

    data = pd.read_csv(ti.xcom_pull(key=FILE_NAME + "_raw"), index_col=0)
    
    # clean all unset rows
    data.dropna(how="all", inplace=True)
    # replace 'null' values with '-'
    data.fillna("-", inplace=True)
    # sort data by created data
    data.sort_values(by='at', inplace=True)
    # remove all unnecessary symbols from the content column
    data['content'].replace(r'[^\w\s.,?!]', '', regex=True, inplace=True)

    data.to_csv(DIR + FILE_NAME + "_proc.csv")

    ti.xcom_push(FILE_NAME + "_proc", DIR + FILE_NAME + "_proc.csv")


def load(ti):
    """Push data to MongoDB"""
    print("---------------Load---------------")

    data = pd.read_csv(ti.xcom_pull(key=FILE_NAME + "_proc"), index_col=0)

    # connect to mongodb
    client = MongoClient("mongodb://localhost:" + LOCAL_HOST)
    # connect to database
    db = client[DB]
    # connect to collection
    collection = db[COLLECTION]

    collection.insert_many(data.to_dict("records"))


with DAG("csv_to_mongodb", default_args=default_args) as dag:

    task_extract = PythonOperator(task_id="extract", python_callable=extract)
    task_transfrom = PythonOperator(task_id="transform", python_callable=transform)
    task_load = PythonOperator(task_id="load", python_callable=load)

    task_extract >> task_transfrom >> task_load
