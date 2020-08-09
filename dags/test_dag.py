import os
import airflow
import logging
import json
import tempfile
import time

from io import BytesIO
from gzip import GzipFile
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from datetime import timedelta, datetime

S3_BUCKET_IN = 'aiqua-prd-qgraph-backup-singapore'
S3_BUCKET_OUT = 'aiqua-stg-project'
S3_PATH_IN = 'offline-event-data'
S3_PATH_OUT = 'offline-event'

LOGGER = logging.getLogger('aiqua-project-campaign-user')

# CHANNEL_LIST = ['', '_web', '_common', '_ios']
CHANNEL_LIST = ['']
APP_ID_LIST = ['8bef3e3e0190f65726e7']

args = {
    'owner': 'AIQUA Project Team',
    'start_date': airflow.utils.dates.days_ago(2)
}

def read_done_record(app_id, channel, **context):
    s3 = AwsHook().get_client_type('s3')

    # Test
    file_path = None
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as fp:
        file_path = fp.name

        fp.write('2020/08/03')
    s3.upload_file(Filename=file_path, Bucket=S3_BUCKET_OUT,
                   Key='campaign-user/{}/done-record{}'.format(app_id, channel))
    # Test

    # Get the latest timestamp from event export S3
    latest_object = s3.get_object(Bucket=S3_BUCKET_IN,
                                  Key='event-export/{}{}/mongo-to-s3-timestamp.txt'.format(app_id, channel))
    latest_timestamp = latest_object['Body'].read().decode('utf-8')
    latest_date = datetime.fromtimestamp(int(latest_timestamp)).date()

    # Get the last done date
    done_object = s3.get_object(Bucket=S3_BUCKET_OUT, Key='campaign-user/{}/done-record{}'.format(app_id, channel))
    done_date_str = done_object['Body'].read().decode('utf-8')
    done_date = datetime.strptime(done_date_str, '%Y/%m/%d').date()

    date_list = []
    for date in date_range(done_date, latest_date):
        date_str = date.strftime("%Y/%m/%d")
        files = s3.list_objects(Bucket=S3_BUCKET_IN, Prefix='event-export/{}/{}/'.format(app_id, date_str))
        file_list = {
            'date': date_str,
            'files': files['Contents']
        }
        date_list.append(file_list)

    context['task_instance'].xcom_push(key='date_list_{}{}'.format(app_id, channel), value=date_list)

# Get date range, exclude start_date, end_date and a day before end_date
def date_range(start_date, end_date):
    for n in range(int ((end_date - start_date).days)-2):
        yield start_date + timedelta(n+1)


def process_event_file(app_id, channel, **context):
    s3 = AwsHook().get_client_type('s3')
    date_list = context['task_instance'].xcom_pull(key='date_list_{}{}'.format(app_id, channel))

    campaign_list = []
    for date in date_list:
        for file in date['files']:
            event_object = s3.get_object(Bucket=S3_BUCKET_IN, Key=file['Key'])
            bytestream = BytesIO(event_object['Body'].read())
            events = GzipFile(None, 'rb', fileobj=bytestream).read().splitlines()

            for event in events:
                event_object = json.loads(event)

                LOGGER.info('!!!!!!')
                LOGGER.info(event_object['eventName'])

                if event_object['eventName'] == 'notification_clicked':

                    campaign_list.append(event_object)

    context['task_instance'].xcom_push(key='campaign_list_{}{}'.format(app_id, channel),
                                       value=campaign_list)

with DAG(
    dag_id='aiqua-project-campaign-user',
    default_args=args,
    schedule_interval='@daily') as dag:

    start_task = DummyOperator(task_id='start', dag=dag)
    end_task = DummyOperator(task_id='end', dag=dag)

    channel_tasks = []
    for app_id in APP_ID_LIST:
        for channel in CHANNEL_LIST:
            read_done_record_task = PythonOperator(
                task_id='read_done_record_{}{}'.format(app_id, channel),
                python_callable=read_done_record,
                op_kwargs={'app_id': app_id, 'channel': channel},
                provide_context=True,
                dag=dag
            )
            process_event_file_task = PythonOperator(
                task_id='process_event_file_{}{}'.format(app_id, channel),
                python_callable=process_event_file,
                op_kwargs={'app_id': app_id, 'channel': channel},
                provide_context=True,
                dag=dag
            )
            read_done_record_task >> process_event_file_task >> end_task
            channel_tasks.append(read_done_record_task)

    start_task >> channel_tasks
