
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.cloud import storage
import json
import pandas as pd
from google.cloud import bigquery
import os
from io import StringIO

def fetch_data():
    print("-------fetching_data--------")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('compressed-coviddata-bucket')
    blob = bucket.blob('covid_data_compressed.csv')
    new_data = pd.read_csv(StringIO(blob.download_as_string().decode('utf-8')),sep=",")
    data = new_data.to_json(orient="columns")
    return data


def analyze_data(**kwargs):
    print("-----analysing data-------")
    print('arguments',str(kwargs))
    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='extract_data')
    print("new_data : ",data)
    new_data = pd.read_json(data)
    geo_data = task_instance.xcom_pull(task_ids='state_geo_data')
    print("geo_data : ",geo_data)
    geo_data = pd.DataFrame(geo_data,columns=['state','area']).set_index('state')
    # print("geo_data : ", json.dumps(geo_data.to_json()))
    past_data = task_instance.xcom_pull(task_ids='state_case_data')
    print("past_data : ", past_data)
    past_data = pd.DataFrame(past_data,columns=['date','state','population','confirmed','deaths','confirmed_daily','deaths_daily'])
    date_data = list(past_data['date'].drop_duplicates().sort_values())[-13:]
    past_data = past_data[pd.DataFrame(past_data.date.tolist()).isin(date_data).any(1).values]

    concated_data = past_data.append(new_data)
    population_data = concated_data[['state','population']].drop_duplicates()
    concated_data = concated_data.drop(columns=["date","population","confirmed","deaths"])
    sum_data = concated_data.groupby(["state"],as_index=False).sum()
    sum_data['date'] = new_data['date']
    sum_data = sum_data.set_index('state').join(population_data.set_index('state'))
    sum_data = sum_data.join(geo_data)
    sum_data["active_idx"] = (sum_data["confirmed_daily"] * 1000) / sum_data['population']
    sum_data["density_idx"] = (sum_data["confirmed_daily"]*1000)/sum_data['area']
    # sum_data = sum_data.drop_duplicates()

    print("sum_data : ",sum_data.to_string())


    transformed_data = {}
    transformed_data['new_data'] = new_data.to_json(orient="columns")
    transformed_data['analyzed_data'] = sum_data.reset_index().to_json(orient="columns")

    transformed_data = json.loads(json.dumps(transformed_data))
    print("-----Transformed data------")

    return transformed_data


def load_data(**kwargs):
    datasetID = "covid_data"
    task_instance = kwargs["task_instance"]
    data = task_instance.xcom_pull(task_ids="transform_data")
    print("Received dataframes")
    print(json.dumps(data))

    analyzed_data = pd.read_json(data['analyzed_data'])
    new_data = pd.read_json(data['new_data'])

    bqclient = bigquery.client.Client(project="covid-data-project-328321")
    try:
        bqclient.create_table("state_case_data")
    except:
        pass
    try:
        bqclient.create_table("state_analyzed_data")
    except:
        pass
    bqclient.load_table_from_dataframe(new_data, "covid-data-project-328321.covid_data.state_case_data")
    bqclient.load_table_from_dataframe(analyzed_data, "covid-data-project-328321.covid_data.state_analyzed_data")

default_args={
    "depends on past" : False,
    "email" : [],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "owner" : "airflow",
    "retries" : 3,
    "retry_delay" : timedelta(minutes=1)
}
dag = DAG(dag_id='etl_dag',start_date=days_ago(1),default_args=default_args,schedule_interval=None)

start=DummyOperator(task_id="start",dag=dag)
end=DummyOperator(task_id="end",dag=dag)

extract_step = PythonOperator(
    task_id='extract_data',
    python_callable=fetch_data,
    dag=dag
)

bigquery_data_fetch1 = BigQueryGetDataOperator(
    task_id="state_geo_data",
    dataset_id="covid_data",
    table_id="state_geo_data",
    location="US",
)

bigquery_data_fetch2 = BigQueryGetDataOperator(
    task_id="state_case_data",
    dataset_id="covid_data",
    table_id="state_case_data",
    location="US",
)

transform_step = PythonOperator(
task_id='transform_data',
python_callable=analyze_data,
provide_context=True,
dag=dag
)



load_step = PythonOperator(
task_id='load_data',
python_callable=load_data,
provide_context=True,
dag=dag
)

start >> extract_step >> bigquery_data_fetch1 >> bigquery_data_fetch2 >> transform_step >> load_step >> end