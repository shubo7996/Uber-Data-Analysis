from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage,bigquery
from google.oauth2 import service_account
import sys
sys.path.insert(0, '/opt/airflow/local_dir')
sys.path.insert(0,'/opt/airflow/config')
from uber_data_transformer import transform

default_args = {
    'owner': 'airflow',
    
    'depends_on_past': False,
    'start_date': datetime.now(),  # Set to today's date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'uber_pipeline_dag',
    default_args=default_args,
    description='A simple Example pipeline',
    schedule_interval=None, #'*/10 * * * *'
)

def get_gcp_credentials(service_json, project_id,string):

    credentials = service_account.Credentials.from_service_account_file(service_json)
    storage_client = storage.Client(credentials= credentials,project=project_id)
    bigquery_client = bigquery.Client(credentials= credentials,project=project_id)
    
    if string=='storage':
        return storage_client
    else: 
        return bigquery_client

def download_blob():
    client=get_gcp_credentials(service_json='/opt/airflow/local_dir/uberanalytics-425005-5c578ebdd20a.json',\
                    project_id='uberanalytics-425005',string='storage')
    bucket_id=client.get_bucket(bucket_or_name='uber-analytics-bucket')
    blob=bucket_id.get_blob('uber_data.csv')
    blob.download_to_filename('/opt/airflow/local_dir/uber_data.csv')
    
def transform_data(ti):
    uber_data='/opt/airflow/local_dir/uber_data.csv'
    result=transform(uber=pd.read_csv(uber_data))
    
    df=pd.DataFrame(result['datetime_dim'])
    datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in datetime_columns:
        df[col] = df[col].apply(lambda x: x.isoformat())
    
    result['datetime_dim']=df.to_dict(orient="dict")
    ti.xcom_push(key='uber_dataframe',value=result)

def upload_data_to_bigquery(ti):
    bq_client=get_gcp_credentials(service_json='/opt/airflow/local_dir/uberanalytics-425005-5c578ebdd20a.json',\
                    project_id='uberanalytics-425005',string='bigquery')    
    dict_dataset=ti.xcom_pull(key='uber_dataframe',task_ids='transform_data')
    
    df=pd.DataFrame(dict_dataset['datetime_dim'])
    datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
    dict_dataset['datetime_dim']=df.to_dict(orient="dict")    
    
    for key,value in dict_dataset.items():
        table_id=f"uberanalytics-425005.uber_analytics_dataset.{key}"
        job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
        )
        job=bq_client.load_table_from_dataframe(pd.DataFrame(value),table_id,job_config=job_config)
        job.result()
        bq_client.get_table(table_id)
    



download_blob_task = PythonOperator(
    task_id='download_blob',
    python_callable=download_blob,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

upload_data_to_bigquery_task = PythonOperator(
    task_id='upload_data_to_bigquery',
    python_callable=upload_data_to_bigquery,
    dag=dag
)    


download_blob_task >> transform_data_task >> upload_data_to_bigquery_task