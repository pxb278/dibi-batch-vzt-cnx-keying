import subprocess
import json
import airflow
import pytz
from airflow.models import DAG, Variable
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor, GoogleCloudStorageObjectSensor
from datetime import datetime, timedelta, date
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook
import logging
import time

# Define Global Variables
appName = "monthly-cnx-batch-sales"

dagId = "ENV-dibi-batch-" + appName
dagConfigName = "ENV-dibi-batch-config-" + appName

dagConfig = Variable.get(dagConfigName, deserialize_json=True)
dbConfig = Variable.get(dagConfig['sqlDbConfig'], deserialize_json=True)
commonsConfig = dagConfig['commons']

# Get Database Password
dbPassword = Variable.get(dagConfig['sqlDbPassword'])

# Logger
logger = logging.getLogger("airflow.task")

date=datetime.now().strftime("%Y%m%d")

# Re-Usable Function To Move Files
def gsutil_mv(params):
    try:
        subprocess.check_output(["bash gsutil -m mv " + params], universal_newlines=True, shell=True)
    except subprocess.CalledProcessError as err:
        print("Error in gsutil_mv()")


# Define Default Args for DAG
default_args = {
    "owner": "DIBI-Batch-BAU-Team",
    "email": ['et.atl.data@equifax.com'],
    "email_on_failure": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "dataflow_default_options": {
        'project': dagConfig['project'],
        'region': dagConfig['region'],
        'tempLocation': dagConfig['tempLocation']
    }
}

# Define DAG parameters
dag = DAG(
    dag_id=dagId,
    catchup=False,
   # schedule_interval='30 5 * * *',
    schedule_interval=None,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60))

# Task 1 - Run MonthlyCnxBatchSalesTrack
t1 = DataflowTemplateOperator(
    gcp_conn_id=dagConfig['gcpConnId'],
    task_id=appName + '-load-task',
    template=dagConfig['MonthlyCnxBatchSalesTrack']['templateLocation'],
    job_name='vzt-datashare-monthly-cnx-batch-sales',
    parameters={
        # Datashare DB Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': dbPassword,
        # Job Related Parameters
        'reportGenPath': dagConfig['MonthlyCnxBatchSalesTrack']['reportGenPath'],
        'writeToXlsFileName': dagConfig['MonthlyCnxBatchSalesTrack']['writeToXlsFileName']+ dagConfig['MonthlyCnxBatchSalesTrack']['suffix'],
        'date':date,
        'summaryUpdateFileName':dagConfig['MonthlyCnxBatchSalesTrack']['summaryUpdateFileName'],
         # Encryption Related Default Parameters
        'pgpCryptionEnabled': commonsConfig['pgp']['pgpCryptionEnabled'],
        'pgpSigned': commonsConfig['pgp']['pgpSigned'],
        'hcvKeystoreGcsBucketName': commonsConfig['vault']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': commonsConfig['vault']['hcvKeystoreGcsFilePath'],
        'hcvUrl': commonsConfig['vault']['hcvUrl'],
        'hcvNamespace': commonsConfig['vault']['hcvNamespace'],
        'hcvSecretPath': commonsConfig['vault']['hcvSecretPath'],
        'hcvEngineVersion': commonsConfig['vault']['hcvEngineVersion'],
        'hcvGcpRole': commonsConfig['vault']['hcvGcpRole'],
        'hcvGcpProjectId': commonsConfig['vault']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': commonsConfig['vault']['hcvGcpServiceAccount']
    },
    dataflow_default_options=default_args['dataflow_default_options'],
    dag=dag)

# Task 2 - Archive
def archive_files():
    dateFolder = date
    gsutil_mv(dagConfig['rootFolder'] + "/reports/* " + dagConfig['rootFolder'] + "/archive/" + dateFolder + "/reports/")

t2= PythonOperator(
    task_id='archive-files',
    python_callable=archive_files,
    trigger_rule='all_done',
    dag=dag)

t1 >> t2