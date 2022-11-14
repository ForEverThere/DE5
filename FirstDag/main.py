from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import pymongo as pm
import json

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag_config = Variable.get("dag1_config", deserialize_json=True)
host = dag_config["host"]
port = dag_config["port"]
db = dag_config["mongo_db"]
collection = dag_config["mongo_collection"]


def Extract_Data(**kwargs):
    ti = kwargs['ti']
    df = pd.read_csv('/home/vlad/Desktop/DE5/FirstDag/tiktok_google_play_reviews.csv', delimiter=',')
    ti.xcom_push(key='reviews_csv', value=df)


def Transform_Data(**kwargs):
    ti = kwargs['ti']
    res_df = ti.xcom_pull(key='reviews_csv', task_ids=['Extract_Data'])[0]
    res_df = res_df.drop_duplicates()
    res_df = res_df.replace('null', "-").bfill()
    res_df = res_df.replace(['\+', r'(?u)[^\w\s\?\.\,\-\:\d]+', '\s*$'], ['', '', ''], regex=True)
    res_df = res_df.sort_values(by='at')
    ti.xcom_push(key='reviews_df', value=res_df)


def Load_Data(**kwargs):
    ti = kwargs['ti']
    res_df = ti.xcom_pull(key='reviews_df', task_ids=['Transform_Data'])[0]
    connection = pm.MongoClient(host, port)[db][collection]
    record = json.loads(res_df.T.to_json()).values()
    connection.insert_many(record)


with DAG('Dag1', default_args=default_args, description="A simple tutorial DAG", schedule='@once') as dag:
    extract_data = PythonOperator(task_id='Extract_Data', python_callable=Extract_Data)
    transform_data = PythonOperator(task_id='Transform_Data', python_callable=Transform_Data)
    load_data = PythonOperator(task_id='Load_Data', python_callable=Load_Data)

    extract_data >> transform_data >> load_data
