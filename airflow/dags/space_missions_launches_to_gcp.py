import logging
from datetime import datetime, date
from io import BytesIO
from tempfile import NamedTemporaryFile

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from utils.missions_launches_transformation_script import transform_spase_mission_file

EXECUTION_DATE = date.today()

dag = DAG(
    'space_missions_launches_to_gcp_dag',
    start_date=datetime(2023, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'viktoria.k',
    }
)


def mission_launches_to_gcp():
    gcp_hook = GCSHook()
    mission_launches = pd.read_csv(
        BytesIO(gcp_hook.download(bucket_name='dataset_source_bucket', object_name='archive.zip')),
        compression='zip'
    )
    list_existed = gcp_hook.list(bucket_name='de-camp-v1_dl', prefix='mission_launches_')
    existed_rows = []
    for file in list_existed:
        existed_rows.append(pd.read_csv(BytesIO(gcp_hook.download(bucket_name='de-camp-v1_dl', object_name=file))))
    mission_launches = transform_spase_mission_file(mission_launches)

    if existed_rows:
        existed_rows = pd.concat(existed_rows)
        existed_rows['date'] = pd.to_datetime(existed_rows['date'])
        mission_launches = pd.concat([mission_launches, existed_rows]).drop_duplicates(keep=False)

    if not mission_launches.empty:
        with NamedTemporaryFile() as temp:
            mission_launches.to_csv(temp.name, index=False)
            gcp_hook.upload(
                bucket_name='de-camp-v1_dl',
                object_name=f'mission_launches_{EXECUTION_DATE}.csv',
                filename=temp.name
            )
        return 'continue'
    else:
        logging.warning('Nothing to update')
        return 'skip'


@task.branch(task_id="branch_task")
def branch_func(**kwargs):
    xcom_value = kwargs['ti'].xcom_pull(task_ids="space_missions_launches_to_gcp_task")
    if xcom_value == 'continue':
        return 'space_missions_launches_from_gcs_to_bq_task'
    else:
        return 'stop_task'


space_missions_launches_to_gcp = PythonOperator(
    dag=dag,
    task_id='space_missions_launches_to_gcp_task',
    python_callable=mission_launches_to_gcp,
    do_xcom_push=True
)

space_missions_launches_from_gcs_to_bq = GCSToBigQueryOperator(
    dag=dag,
    task_id='space_missions_launches_from_gcs_to_bq_task',
    bucket='de-camp-v1_dl',
    source_objects=f'mission_launches_{EXECUTION_DATE}.csv',
    destination_project_dataset_table='de-camp-v1.space_mission_launches.mission_launches',
    schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
    write_disposition='WRITE_APPEND'
)
branch_op = branch_func()
stop_op = EmptyOperator(task_id="stop_task", dag=dag)

space_missions_launches_to_gcp >> branch_op >> [space_missions_launches_from_gcs_to_bq, stop_op]
