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

# Schedule - 00 02 * * *

# Define Global Variables
appName = "vz-cnx-keying"

dagId = "ENV-dibi-batch-" + appName + "-request-monitor"
dagConfigName = "ENV-dibi-batch-config-" + appName + "-monitor"

dagConfig = Variable.get(dagConfigName, deserialize_json=True)
dbConfig = Variable.get("ENV-dibi-batch-verizonds-sql-db-config", deserialize_json=True)
commonsConfig = dagConfig['commons']

# Get Database Password
sqlDbPassword = Variable.get("ENV-dibi-batch-verizonds-sql-db-password")
# Get Date YYYYMMDD format
dateNow = datetime.now().strftime("%Y%m%d")
# Get Time HHMMSS format
timeNow = datetime.now().strftime("%H%M%S")

# Logger
logger = logging.getLogger("airflow.task")

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
    schedule_interval=None,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
)


# Task1 - Run Request File Monitor
t1= DataflowTemplateOperator(
    gcp_conn_id=dagConfig['gcpConnId'],
    task_id=appName + '-request-file-monitor',
    template=dagConfig['RequestFileMonitor']['templateLocation'],
    job_name=appName + '-request-file-monitor',
    parameters={
        # DB Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,
        # Job Parameters
        'outputPath': dagConfig['RequestFileMonitor']['outputPath'],
        'emailFileName': dagConfig['RequestFileMonitor']['emailFileName'],
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
        'hcvGcpServiceAccount': commonsConfig['vault']['hcvGcpServiceAccount'],

    },
    dataflow_default_options=default_args['dataflow_default_options'],
    dag=dag)

t1