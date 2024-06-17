from google.cloud import storage,bigquery
from google.oauth2 import service_account
from uber_data_transformer import transform
import pandas as pd

def get_gcp_credentials(service_json, project_id):

    credentials = service_account.Credentials.from_service_account_file(service_json)
    client = storage.Client(credentials= credentials,project=project_id)

    return client

def download_blob():

    client=get_gcp_credentials(service_json='opt/airflow/local_dir/uberanalytics-425005-5c578ebdd20a.json',\
                    project_id='uberanalytics-425005')
    bucket_id=client.get_bucket(bucket_or_name='uber-analytics-bucket')
    blob=bucket_id.get_blob('uber_data.csv')
    print(blob)

def transform_data():
    uber_data='uber_data.csv'
    result=transform(uber=pd.read_csv(uber_data))
    return result

def upload_data_to_bigquery(res):
    credentials = service_account.Credentials.from_service_account_file('uberanalytics-425005-5c578ebdd20a.json')
    bq_client=bigquery.Client(credentials=credentials,project='uberanalytics-425005')
    for key,value in res.items():
        table_id=f"uberanalytics-425005.uber_analytics_dataset.{key}"
        job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
        )
        job=bq_client.load_table_from_dataframe(pd.DataFrame(value),table_id,job_config=job_config)
        job.result()
        bq_client.get_table(table_id)
    
if __name__ == '__main__':
    res=transform_data()
    upload_data_to_bigquery(res)
    

# volumes:
#   #   - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
#   #   - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
#   #   - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
#   #   - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    