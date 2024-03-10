from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
from config import BUCKET_NAME, FILE_NAME, DATASET_ID, TABLE_ID, K_DATASET, KAGGLE2BQ_DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 10)
}

dag = DAG(
    KAGGLE2BQ_DAG,
    default_args=default_args,
    description='A DAG to load data from Kaggle to BigQuery',
    schedule_interval=None,
)

def download_from_kaggle():
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset=K_DATASET, path='/tmp', unzip=True)

def upload_to_gcs():
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    blob.upload_from_filename('/tmp/' + FILE_NAME)

# if dataset doesn't exist
#def create_bigquery_dataset():
#    client = bigquery.Client()
#    dataset_ref = client.dataset(DATASET_ID)
#    dataset = bigquery.Dataset(dataset_ref)
#    dataset.location = 'EU' 
#    dataset = client.create_dataset(dataset)

def load_to_bigquery():
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("entity", "STRING"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("terrorist_attacks", "INTEGER"),
        ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition='WRITE_TRUNCATE'
    )
    
    uri = f"gs://{BUCKET_NAME}/{FILE_NAME}"
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )
    load_job.result()

with dag:
    download_task = PythonOperator(
        task_id='download_from_kaggle',
        python_callable=download_from_kaggle,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

#    create_dataset_task = PythonOperator(
#        task_id='create_bigquery_dataset',
#        python_callable=create_bigquery_dataset,
#    )

    load_to_bigquery_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
    )

    # download_task >> upload_task >> create_dataset_task >> load_to_bigquery_task
    download_task >> upload_task >> load_to_bigquery_task
