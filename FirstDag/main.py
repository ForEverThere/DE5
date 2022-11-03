from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
import pymongo as pm
import json

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag1 = DAG(
    'Dag1',
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule='@once'
)


def FuncDone():
    df = pd.read_csv('/home/vlad/Desktop/DE5/FirstDag/tiktok_google_play_reviews.csv', delimiter=',')
    df = df.drop_duplicates()
    df = df.replace('null', "-").bfill()
    df = df.replace(['\+', r'(?u)[^\w\s\?\.\,\-\:\d]+', '\s*$'], ['', '', ''], regex=True)
    df = df.sort_values(by='at')
    client = pm.MongoClient('localhost', 27017)
    db = client['DE5']
    collection = db['DE5']
    record = json.loads(df.T.to_json()).values()
    collection.insert_many(record)


t1 = PythonOperator(
    task_id='task_1',
    python_callable=FuncDone,
    dag=dag1)
