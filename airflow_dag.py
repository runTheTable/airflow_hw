import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from operators.airflow_operator import SendEmailOperator


ARGS_CONFIG = Variable.get('ARGS_CONFIG', deserialize_json=True)
PROJECT = str(ARGS_CONFIG['project'])
DAG_CONFIG = Variable.get('DAG_CONFIG', deserialize_json=True)
DAG_ARGS = {
    'owner': str(ARGS_CONFIG['owner']),
    'depends_on_past': False,
    'start_date': datetime(
        int(ARGS_CONFIG['start_year']),
        int(ARGS_CONFIG['start_month']),
        int(ARGS_CONFIG['start_day']),
        int(ARGS_CONFIG['start_hour']),
        int(ARGS_CONFIG['start_minute'])),
    'py_options': [],
    'dataflow_default_options': {
        'project': str(ARGS_CONFIG['project']),
        'region': str(ARGS_CONFIG['region'])
    },
    }

BQ_DATASET_ID = Variable.get('BQ_DATASET_ID') 
BQ_TABLE_ID = Variable.get('BQ_TABLE_ID') 
ST_BUCKET_ID = Variable.get('ST_BUCKET_ID')
CURR_DATE = str(dt.datetime.now().date())
EMAIL = Variable.get('EMAIL')


with DAG(dag_id=str(DAG_CONFIG['dag_id']),
         default_args=DAG_ARGS,
         max_active_runs=int(DAG_CONFIG['max_active_runs']),
         concurrency=int(DAG_CONFIG['concurrency']),
         description=str(DAG_CONFIG['description']),
         schedule_interval=dt.timedelta(days=int(DAG_CONFIG['schedule_interval'])),
         catchup=False,
         ) as dag:

    start_pipeline = DummyOperator(task_id='start-pipeline', dag=dag)

    save_csv_report_to_gcs = BigQueryToCloudStorageOperator(
        task_id='save_csv_report_to_gcs',
        source_project_dataset_table=f'{BQ_DATASET_ID}.{BQ_TABLE_ID}',
        destination_cloud_storage_uris=f'gs://{ST_BUCKET_ID}/report_{CURR_DATE}.csv',
        export_format='text/csv',
        field_delimiter=',',
        print_header=True,
        bigquery_conn_id='gcp_connection',
        dag=dag
    )

    #  here your homework code
    
    check_csv_report_exist = GCSObjectExistenceSensor(
        task_id='check_csv_exists',
        bucket=ST_BUCKET_ID,
        object=f'report_{CURR_DATE}.csv',
        google_cloud_conn_id='gcp_connection',
        dag=dag
    )
    

    send_email = SendEmailOperator(my_operator_param='SendEmailOperator is ready to work', task_id='send-email', dag=dag)

    finish_pipeline = DummyOperator(task_id='finish-pipeline', dag=dag)

    # airflow dag graph
    start_pipeline >> save_csv_report_to_gcs >> check_csv_report_exist >> send_email >> finish_pipeline
