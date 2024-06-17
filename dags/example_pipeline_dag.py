from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from datetime import datetime
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),  # Set to today's date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_pipeline_dag',
    default_args=default_args,
    description='A simple Example pipeline',
    schedule_interval='*/10 * * * *',
)


def print_welcome():
    print('Welcome to Airflow!')


def print_date():
    print('Today is {}'.format(datetime.today().date()))



def print_random_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the day: "{}"'.format(quote))



print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)



print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag

)


print_random_quote = PythonOperator(
    task_id='print_random_quote',
    python_callable=print_random_quote,
    dag=dag

)



# Set the dependencies between the tasks

print_welcome_task >> print_date_task >> print_random_quote