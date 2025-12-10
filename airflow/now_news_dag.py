from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
import requests

# Load credentials
key_path = "/opt/airflow/keys/nownews-479616-3a04ce3ddb21.json"
credentials = service_account.IDTokenCredentials.from_service_account_file(
    key_path,
    target_audience="https://extract-newspaper-titles-561628890420.europe-west1.run.app"
)

def call_cloud_function():
    credentials.refresh(Request())
    id_token = credentials.token
    url = "https://extract-newspaper-titles-561628890420.europe-west1.run.app"
    headers = {"Authorization": f"Bearer {id_token}"}
    response = requests.post(url, json={"param": "value"}, headers=headers)
    response.raise_for_status()
    print(response.text)  # opcional: loguea la respuesta
    return response.text


def load_sql_from_gcs(bucket_name, file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    sql = blob.download_as_text()
    return sql

sql_insert_newspapers = load_sql_from_gcs("now-news-data-lake", "scripts/sql/insert_dim_newspapers.sql")
sql_insert_topics = load_sql_from_gcs("now-news-data-lake", "scripts/sql/insert_dim_topics.sql")
sql_insert_fact_newspapers_topic = load_sql_from_gcs("now-news-data-lake", "scripts/sql/insert_fact_newspaper_topic.sql")



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="now_news_dag",
    default_args=default_args,
    description="A DAG to fetch news and load them to a data warehouse",
    schedule="0 7 * * *", 
    catchup=False,
) as dag:

    cloud_function_task = PythonOperator(
        task_id="call_cloud_function",
        python_callable=call_cloud_function,
    )
    
    Bronze_to_Silver_NowNews = DataprocCreateBatchOperator(
    task_id="Bronze_to_Silver_NowNews",
    project_id="nownews-479616",
    region="us-central1",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://now-news-data-lake/scripts/spark/nowNews-transform-bronze-silver.py"
        },
        "runtime_config": {
            "properties": {
                "spark.executor.instances": "2",
                "spark.executor.cores": "4",
                "spark.executor.memory": "8g",
                "spark.driver.memory": "4g",
            }
        }
    },

    )
    
    Silver_to_Gold_NowNews = DataprocCreateBatchOperator(
    task_id="Silver_to_Gold_NowNews",
    project_id="nownews-479616",
    region="us-central1",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://now-news-data-lake/scripts/spark/newNews-transform-silver-gold.py"
        },
        "runtime_config": {
            "properties": {
                "spark.executor.instances": "2",
                "spark.executor.cores": "4",
                "spark.executor.memory": "8g",
                "spark.driver.memory": "4g",
            }
        }
    },

    )
    
    sql_insert_newspapers = BigQueryInsertJobOperator(
        task_id="sql_insert_newspapers",
        configuration={
            "query": {
                "query": sql_insert_newspapers,
                "useLegacySql": False
            }
        },
    )

    sql_insert_topics = BigQueryInsertJobOperator(
        task_id="sql_insert_topics",
        configuration={
            "query": {
                "query": sql_insert_topics,
                "useLegacySql": False
            }
        },
    )

    sql_insert_fact_newspapers_topic = BigQueryInsertJobOperator(
        task_id="sql_insert_fact_newspapers_topic",
        configuration={
            "query": {
                "query": sql_insert_fact_newspapers_topic,
                "useLegacySql": False
            }
        },
    )

cloud_function_task >> Bronze_to_Silver_NowNews >> Silver_to_Gold_NowNews >> sql_insert_newspapers >> sql_insert_topics >> sql_insert_fact_newspapers_topic
