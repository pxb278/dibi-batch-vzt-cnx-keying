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
appName = "load-and-maintain-vzt-cnx-keying"

dagConfigName = "ENV-dibi-batch-config-" + appName
dagXComName = "ENV-dibi-batch-xcom-" + appName
dbConfigName="ENV-dibi-batch-verizonds-sql-db-config"
dbPasswordConfigName = "ENV-dibi-batch-verizonds-sql-db-password"
barricadeConfigName = "ENV-dibi-batch-barricading-" + appName


dagConfig = Variable.get(dagConfigName, deserialize_json=True)
dbConfig = Variable.get(dbConfigName, deserialize_json=True)
dagXCom = Variable.get(dagXComName, deserialize_json=True)
barricade = Variable.get(barricadeConfigName, deserialize_json=True)

# Get Database Password
sqlDbPassword = Variable.get(dbPasswordConfigName)

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

    dag_id="ENV-dibi-batch-"+ "load-maintain-vzt-cnx-keying",
    catchup=False,
    schedule_interval=None,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
)

# Re-Usable Function To Move Files
def gsutil_mv(params):
    try:
        subprocess.check_output(["bash gsutil -m mv " + params], universal_newlines=True, shell=True)
    except subprocess.CalledProcessError as err:
        print("Error in gsutil_mv()")

# Re-Usable Function To Copy Files
def gsutil_cp(params):
    try:
        subprocess.check_output(["bash gsutil -m cp " + params], universal_newlines=True, shell=True)
    except subprocess.CalledProcessError as err:
        print("Error in gsutil_cp()")
        
        
        
 # Task - Scan for VZT_DS_PROD.KREQ* input files
def scan_for_input_files():
    print("Scanning for NCTUE :: input = " + dagConfig['RequestLoadProcess']
    ['input'])

    files = subprocess.check_output(
        ["bash gsutil ls " + dagConfig['RequestLoadProcess']['input']],
        universal_newlines=True, shell=True).splitlines()

    dagXCom['picked-LoadAndMaintainViasatRequestPipeline-input'] = files[0]
    Variable.set(dagXComName, json.dumps(dagXCom))

    print("Picked the top, NCTUE :: input = " + dagXCom['picked-LoadAndMaintainVztRequestPipeline-input'])

# Task0 - Pick The File
t0 = PythonOperator(
    task_id='scan_for_request_input_files',
    python_callable=scan_for_input_files,
    dag=dag)

# Get Date YYYYMMDD format
dateNow = datetime.now().strftime("%Y%m%d")
# Get Time HHMMSS format
timeNow = datetime.now().strftime("%H%M%S")

# Task1 - Get Batch Id
def get_batchid():
    batchId = dagXCom['batchId'] + 1
    dagXCom['batchId'] = batchId
    Variable.set(dagXComName, json.dumps(dagXCom))
    dagXCom['archiveSubDirName'] = str(dagXCom['batchId'])+"/"
    Variable.set(dagXComName, json.dumps(dagXCom))
    return str(batchId)

t1 = PythonOperator(
    task_id='Load-VztDataShare-Table_get-batchid',
    python_callable=get_batchid,
    dag=dag
)

# Task1 - Get Batch Id
def get_deltaseqno():
    deltaseq = dagXCom['deltaseq'] + 1
    dagXCom['deltaseq'] = deltaseq
    Variable.set(dagXComName, json.dumps(dagXCom))
    #dagXCom['archiveSubDirName'] = str(dagXCom['batchId'])+"/"
    #Variable.set(dagXComName, json.dumps(dagXCom))
    return str(deltaseq)

t01 = PythonOperator(
    task_id='load-delta-seq-no',
    python_callable=get_deltaseqno,
    dag=dag
)

# Task2 - Load Vzt Data Share Table
t2 = DataflowTemplateOperator(

    task_id='load-vzt-datashare-table',
    #job_name='load-Vzt-DataShare-table',
    template=dagConfig['ResponseLoad']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,
        # Machine Type
        'numWorkers': dagConfig['ResponseLoad']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoad']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoad']['workerMachineType'],
        # Encryption Related Default Parameters
        'encryptionEnabled': barricade['encryptionEnabled'],
        'kmsKeyReference': barricade['kmsKeyReference'],
        'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek'],
        'hmacWrappedDekBucketName': barricade['hmacWrappedDekBucketName'],
        'hmacWrappedDekFilePath': barricade['hmacWrappedDekFilePath'],
        'metadataBucketName': barricade['metadataBucketName'],
        'metadataFilePath': barricade['metadataFilePath'],
        'hmacMetadataBucketName': barricade['hmacMetadataBucketName'],
        'hmacMetadataFilePath': barricade['hmacMetadataFilePath'],
        # PGP Encryption
        'mftPgpCryptionEnabled': barricade['mftPgpCryptionEnabled'],
        'mftPgpSigned': barricade['mftPgpSigned'],
        'mftPgpArmored': barricade['mftPgpArmored'],
        'mftPgpObjectFactory': barricade['mftPgpObjectFactory'],

        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'batchId': str(dagXCom['batchId']),
        'processName': dagXCom['processName'],
        'input': dagXCom['picked-LoadAndMaintainVztResponsePipeline-input']

    },
    gcp_conn_id=dagConfig['gcpConnId'],
   # dataflow_default_options=default_args['dataflow_default_options'],
    dag=dag)

# Task3 - Extract DICNX0000002_Efx_Vzw FilePipeLine
t3 = DataflowTemplateOperator(

    task_id='extract-dicnx0000002efxvzw-file-pipeline',
    template=dagConfig['ResponseLoadForExtractDsCnxFile']['templateLocation'],
    parameters={

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        'pgpObjectFactory': barricade['pgpObjectFactory'],
        # Machine Type
        'numWorkers': dagConfig['ResponseLoadForExtractDsCnxFile']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadForExtractDsCnxFile']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadForExtractDsCnxFile']['workerMachineType'],
        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainvztResponsePipeline-stDsCnxinput'],
        'outputPath': dagConfig['ResponseLoadForExtractDsCnxFile']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadForExtractDsCnxFile']['DsCnxFileName'] + dateNow + timeNow + ".snd"
        
    },

    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task3 - Extract DICNX0000002_Efx_Vzw FilePipeLine
t03 = DataflowTemplateOperator(

    task_id='extract-dicnx0000002efxvzw-ctl-file-pipeline',
    template=dagConfig['ResponseLoadForExtractDsCnxFile']['templateLocation'],
    parameters={

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        'pgpObjectFactory': barricade['pgpObjectFactory'],
        # Machine Type
        'numWorkers': dagConfig['ResponseLoadForExtractDsCnxFile']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadForExtractDsCnxFile']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadForExtractDsCnxFile']['workerMachineType'],
        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainvztResponsePipeline-stDsCnxinput'],
        'outputPath': dagConfig['ResponseLoadForExtractDsCnxFile']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadForExtractDsCnxFile']['DsCnxFileName'] + dateNow + timeNow + ".ctl"

    },

    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task4 - load Vzt-Batch Vzt-Summary VZT-Stat table
t4 = DataflowTemplateOperator(

    task_id='load-vzt-batch-summary-stat',
    template=dagConfig['ResponseLoadForLoadVZTBatchSummarryStats']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,
        # Encryption Related Default Parameters
        #'encryptionEnabled': barricade['encryptionEnabled'],
        #'kmsKeyReference': barricade['kmsKeyReference'],
        #'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        #'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        #'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek']
        # Machine Type
        'numWorkers': dagConfig['ResponseLoadForLoadVZTBatchSummarryStats']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadForLoadVZTBatchSummarryStats']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadForLoadVZTBatchSummarryStats']['workerMachineType'],
        # Job Parameters
        'batchId': str(dagXCom['batchId']),
        'ultimateBatchId': dagXCom['ultimateBatchId'],
        'CNXExtractCount': dagXCom['CNXExtractCount'],
        'srcFileName': dagXCom['srcFileName'],
        'sourceVbbRecordCount': dagXCom['sourceVbbRecordCount'],
        'sourceVzbRecordCount': dagXCom['sourceVzbRecordCount'],
        'sourceVztRecordCount': dagXCom['sourceVztRecordCount'],
        'sourceVzwRecordCount': dagXCom['sourceVzwRecordCount'],
        'inputfileFinalCount': dagXCom['inputfileFinalCount'],
        'inputfileLiveCount': dagXCom['inputfileLiveCount'],
        'inputFileRecCount': dagXCom['inputFileRecCount'],
        'keyingFlagValueF': dagXCom['keyingFlagValueF'],
        'keyingFlagValueT': dagXCom['keyingFlagValueT'],
        'vztInsertCnt': dagXCom['vztInsertCnt'],
        'vztCnxReqFile': dagXCom['vztCnxReqFile']


    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task5 - Extract From StDsOut File To Load VztDataShare table
t5 = DataflowTemplateOperator (

    task_id='extract-from-stdsout-to-load-vzt-datashare',
    template=dagConfig['ResponseLoadToExtractFromStDsOut']['templateLocation'],
     parameters = {
       # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,
      # Encryption Related Default Parameters
        #'encryptionEnabled': barricade['encryptionEnabled'],
        #'kmsKeyReference': barricade['kmsKeyReference'],
        #'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        #'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        #'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek'],
        # Machine Type
        'numWorkers': dagConfig['ResponseLoadToExtractFromStDsOut']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadToExtractFromStDsOut']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadToExtractFromStDsOut']['workerMachineType'],
        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],
      # DIBI Vaut Parameters
       'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
       'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
       'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
       'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
       'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
       'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
       'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
       'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
       'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
      # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainvztResponsePipeline-StDsOutinput'],
         
        'batchId': str(dagXCom['batchId']),
        'processName': dagXCom['processName']
     },
     gcp_conn_id = dagConfig['gcpConnId'],
     dag = dag)

# Task6 - Extract VzDsFuzzyReqExtract  File
t6 = DataflowTemplateOperator(

    task_id='extract-vzdsfuzzyreq-extract',
    template=dagConfig['ResponseLoadVzDsFuzzyReq']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,
        # Machine Type
        'numWorkers': dagConfig['ResponseLoadVzDsFuzzyReq']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadVzDsFuzzyReq']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadVzDsFuzzyReq']['workerMachineType'],
        # Encryption Related Default Parameters
        'encryptionEnabled': barricade['encryptionEnabled'],
        'kmsKeyReference': barricade['kmsKeyReference'],
        'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek'],
        'hmacWrappedDekBucketName': barricade['hmacWrappedDekBucketName'],
        'hmacWrappedDekFilePath': barricade['hmacWrappedDekFilePath'],
        'metadataBucketName': barricade['metadataBucketName'],
        'metadataFilePath': barricade['metadataFilePath'],
        'hmacMetadataBucketName': barricade['hmacMetadataBucketName'],
        'hmacMetadataFilePath': barricade['hmacMetadataFilePath'],
        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'outputPath': dagConfig['ResponseLoadVzDsFuzzyReq']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadVzDsFuzzyReq']['VzDsFuzzyReqFileName'] + "_" + str(
            dagXCom['batchId']) + ".txt",
        'batchId': str(dagXCom['batchId'])

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task7 - Extract StDsFuzzy Resp File To Load Vzt DataShare Table
t7 = DataflowTemplateOperator(

    task_id='stdsfuzzyresp-extract-load-table',
    template=dagConfig['ResponseLoadStDsFuzzyResp']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadStDsFuzzyResp']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadStDsFuzzyResp']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadStDsFuzzyResp']['workerMachineType'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        'pgpObjectFactory': barricade['pgpObjectFactory'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainVztResponsePipeline-StDsFuzzyinput'],
        'batchId': str(dagXCom['batchId'])

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task 8- load Vzt-Batch Vzt-Summary table
t8 = DataflowTemplateOperator(

    task_id='load-vzt-batch-and-vzt-summary',
    template=dagConfig['ResponseLoadForLoadVztBatchAndVztSummary']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadForLoadVztBatchAndVztSummary']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadForLoadVztBatchAndVztSummary']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadForLoadVztBatchAndVztSummary']['workerMachineType'],
        # Encryption Related Default Parameters
        #'encryptionEnabled': barricade['encryptionEnabled'],
        #'kmsKeyReference': barricade['kmsKeyReference'],
        #'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        #'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        #'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek']
        # Job Parameters
        'batchId': str(dagXCom['batchId']),
        'ultimateBatchId': dagXCom['ultimateBatchId'],
        'vztCnxRespFile': dagXCom['vztCnxRespFile'],
        'vztCnxRespFileCnt': dagXCom['vztCnxRespFileCnt']

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task9 - Extract Inactivated Override  File
t9 = DataflowTemplateOperator(

    task_id='extract-inactivated-override',
    template=dagConfig['ResponseLoadextractInactivatedOverride']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadextractInactivatedOverride']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadextractInactivatedOverride']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadextractInactivatedOverride']['workerMachineType'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'batchId': str(dagXCom['batchId']),
        'outputPath': dagConfig['ResponseLoadextractInactivatedOverride']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadextractInactivatedOverride']['InactivatedOverrideFileName'] + ".txt"

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task10 - Load From InactiveOvrerride Records  File
t10 = DataflowTemplateOperator(

    task_id='load-from-inactive-ovrerride-records',
    template=dagConfig['ResponseLoadFromInactiveOvrerrideRecords']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadFromInactiveOvrerrideRecords']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadFromInactiveOvrerrideRecords']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadFromInactiveOvrerrideRecords']['workerMachineType'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # VZT Vaut Parameters
        'hcvVZKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvVZKeystoreGcsBucketName'],
        'hcvVZKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvVZKeystoreGcsFilePath'],
        'hcvVZUrl': dagConfig['vault-dibi']['hcvVZUrl'],
        'hcvVZNamespace': dagConfig['vault-dibi']['hcvVZNamespace'],
        'hcvVZSecretPath': dagConfig['vault-dibi']['hcvVZSecretPath'],
        'hcvVZEngineVersion': dagConfig['vault-dibi']['hcvVZEngineVersion'],
        'hcvVZGcpRole': dagConfig['vault-dibi']['hcvVZGcpRole'],
        'hcvVZGcpProjectId': dagConfig['vault-dibi']['hcvVZGcpProjectId'],
        'hcvVZGcpServiceAccount': dagConfig['vault-dibi']['hcvVZGcpServiceAccount'],

        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainVztResponsePipeline-Overrideinput'],
        'outputPath': dagConfig['ResponseLoadFromInactiveOvrerrideRecords']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadFromInactiveOvrerrideRecords']['DeltaFileName'] + ".txt",
        'currentTimestamp': dagXCom['currentTimestamp']

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task11 - Extract CnxOverride File
t11 = DataflowTemplateOperator(

    task_id='extract-cnx-overridefile',
    template=dagConfig['ResponseLoadForExtractCnxOverrideFile']['templateLocation'],
    parameters={
        # DB Related Default Parameters
       # 'driverClass': dbConfig['driverClass'],
       # 'connectionString': dbConfig['connectionString'],
       # 'username': dbConfig['username'],
       # 'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadForExtractCnxOverrideFile']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadForExtractCnxOverrideFile']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadForExtractCnxOverrideFile']['workerMachineType'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # IC Vaut Parameters
        'hcvMFTKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvMFTKeystoreGcsBucketName'],
        'hcvMFTKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvMFTKeystoreGcsFilePath'],
        'hcvMFTUrl': dagConfig['vault-dibi']['hcvMFTUrl'],
        'hcvMFTNamespace': dagConfig['vault-dibi']['hcvMFTNamespace'],
        'hcvMFTSecretPath': dagConfig['vault-dibi']['hcvMFTSecretPath'],
        'hcvMFTEngineVersion': dagConfig['vault-dibi']['hcvMFTEngineVersion'],
        'hcvMFTGcpRole': dagConfig['vault-dibi']['hcvMFTGcpRole'],
        'hcvMFTGcpProjectId': dagConfig['vault-dibi']['hcvMFTGcpProjectId'],
        'hcvMFTGcpServiceAccount': dagConfig['vault-dibi']['hcvMFTGcpServiceAccount'],

        # DIBI Vaut Parameters
        'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount'],
        # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainVztResponsePipeline-Overrideinput'],
        'outputPath': dagConfig['ResponseLoadForExtractCnxOverrideFile']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadForExtractCnxOverrideFile']['CnxFileName'] + ".txt",
        'currentTimestamp': dagXCom['currentTimestamp']

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task12 - Extract dextractDivzcomms File
t12 = DataflowTemplateOperator(

    task_id='extract-divzcomms-file-from-vw',
    template=dagConfig['ResponseLoadextractDivzcomms']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadextractDivzcomms']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadextractDivzcomms']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadextractDivzcomms']['workerMachineType'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # VZT Vaut Parameters
        'hcvVZKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvVZKeystoreGcsBucketName'],
        'hcvVZKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvVZKeystoreGcsFilePath'],
        'hcvVZUrl': dagConfig['vault-dibi']['hcvVZUrl'],
        'hcvVZNamespace': dagConfig['vault-dibi']['hcvVZNamespace'],
        'hcvVZSecretPath': dagConfig['vault-dibi']['hcvVZSecretPath'],
        'hcvVZEngineVersion': dagConfig['vault-dibi']['hcvVZEngineVersion'],
        'hcvVZGcpRole': dagConfig['vault-dibi']['hcvVZGcpRole'],
        'hcvVZGcpProjectId': dagConfig['vault-dibi']['hcvVZGcpProjectId'],
        'hcvVZGcpServiceAccount': dagConfig['vault-dibi']['hcvVZGcpServiceAccount'],

        # Job Parameters
        'batchId': str(dagXCom['batchId']),
        'outputPath': dagConfig['ResponseLoadextractDivzcomms']['OutputPath'],
        'outputFileName': dagConfig['ResponseLoadextractDivzcomms']['DivzcommsFileName'] + "." + str(
            dagXCom['deltaseq']) + "." + dateNow + timeNow

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task13 -Response Load From RowGenerator
t13 = DataflowTemplateOperator(

    task_id='response-load-from-rowgenerator',
    template=dagConfig['ResponseLoadRowGenerator']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadRowGenerator']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadRowGenerator']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadRowGenerator']['workerMachineType'],

        # PGP Encryption
        #'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        #'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # DIBI Vaut Parameters
        #'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        #'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        #'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        #'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        #'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        #'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        #'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        #'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        #'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount']
        # Job Parameters
       'batchId': str(dagXCom['batchId']),
       'ultimateBatchId': dagXCom['ultimateBatchId'],
       'vztDSRespFileName': dagXCom['ultimateBatchId'],
       'vztDSRespFileCount': dagXCom['vztDSRespFileCount'],
       'vztHitCount': dagXCom['vztHitCount'],
       'vztNoHitCount': dagXCom['vztNoHitCount']

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task14 -Load-Vzt-DataShare-Cnx-Repo
t14 = DataflowTemplateOperator(

    task_id='load-vzt-datashare-cnx-repo',
    template=dagConfig['ResponseLoadVztDataShareCnxRepo']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadVztDataShareCnxRepo']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadVztDataShareCnxRepo']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadVztDataShareCnxRepo']['workerMachineType'],

        # Encryption Related Default Parameters
        'encryptionEnabled': barricade['encryptionEnabled'],
        'kmsKeyReference': barricade['kmsKeyReference'],
        'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek'],
        'hmacWrappedDekBucketName': barricade['hmacWrappedDekBucketName'],
        'hmacWrappedDekFilePath': barricade['hmacWrappedDekFilePath'],
        'metadataBucketName': barricade['metadataBucketName'],
        'metadataFilePath': barricade['metadataFilePath'],
        'hmacMetadataBucketName': barricade['hmacMetadataBucketName'],
        'hmacMetadataFilePath': barricade['hmacMetadataFilePath'],
        # PGP Encryption
        #'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        #'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # DIBI Vaut Parameters
        #'hcvKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvKeystoreGcsBucketName'],
        #'hcvKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvKeystoreGcsFilePath'],
        #'hcvUrl': dagConfig['vault-dibi']['hcvUrl'],
        #'hcvNamespace': dagConfig['vault-dibi']['hcvNamespace'],
        #'hcvSecretPath': dagConfig['vault-dibi']['hcvSecretPath'],
        #'hcvEngineVersion': dagConfig['vault-dibi']['hcvEngineVersion'],
        #'hcvGcpRole': dagConfig['vault-dibi']['hcvGcpRole'],
        #'hcvGcpProjectId': dagConfig['vault-dibi']['hcvGcpProjectId'],
        #'hcvGcpServiceAccount': dagConfig['vault-dibi']['hcvGcpServiceAccount']
        # Job Parameters
        'batchId': str(dagXCom['batchId'])

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

# Task15 - Extract DataShareSsaCon File
t15 = DataflowTemplateOperator(

    task_id='response-datashare-ssa-con',
    template=dagConfig['ResponseDataShareSsaCon']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseDataShareSsaCon']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseDataShareSsaCon']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseDataShareSsaCon']['workerMachineType'],

        # Encryption Related Default Parameters
        'encryptionEnabled': barricade['encryptionEnabled'],
        'kmsKeyReference': barricade['kmsKeyReference'],
        'wrappedDekBucketName': barricade['wrappedDekBucketName'],
        'wrappedDekFilePath': barricade['wrappedDekFilePath'],
        'validateAndProvisionWrappedDek': barricade['validateAndProvisionWrappedDek'],
        'hmacWrappedDekBucketName': barricade['hmacWrappedDekBucketName'],
        'hmacWrappedDekFilePath': barricade['hmacWrappedDekFilePath'],
        'metadataBucketName': barricade['metadataBucketName'],
        'metadataFilePath': barricade['metadataFilePath'],
        'hmacMetadataBucketName': barricade['hmacMetadataBucketName'],
        'hmacMetadataFilePath': barricade['hmacMetadataFilePath'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        #'pgpArmored': barricade['pgpArmored'],
        #'pgpObjectFactory': barricade['pgpObjectFactory'],

        # DIBI Vaut Parameters
        'hcvSSAKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvSSAKeystoreGcsBucketName'],
        'hcvSSAKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvSSAKeystoreGcsFilePath'],
        'hcvSSAUrl': dagConfig['vault-dibi']['hcvSSAUrl'],
        'hcvSSANamespace': dagConfig['vault-dibi']['hcvSSANamespace'],
        'hcvSSASecretPath': dagConfig['vault-dibi']['hcvSSASecretPath'],
        'hcvSSAEngineVersion': dagConfig['vault-dibi']['hcvSSAEngineVersion'],
        'hcvSSAGcpRole': dagConfig['vault-dibi']['hcvSSAGcpRole'],
        'hcvSSAGcpProjectId': dagConfig['vault-dibi']['hcvSSAGcpProjectId'],
        'hcvSSAGcpServiceAccount': dagConfig['vault-dibi']['hcvSSAGcpServiceAccount'],
        # Job Parameters
        'batchId': str(dagXCom['batchId']),
        'outputPath': dagConfig['ResponseDataShareSsaCon']['OutputPath'],
        'outputFileName': dagConfig['ResponseDataShareSsaCon']['SsaConFileName'] + "_" + str(dagXCom['batchId']) + ".txt"

},
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)
# Task16 - Delete VzDataShareRepo Table
t16 = DataflowTemplateOperator(

    task_id='delete-vz-data-share-repo-table',
    template=dagConfig['ResponseLoadDeleteTable']['templateLocation'],
    parameters={
        # DB Related Default Parameters
        'driverClass': dbConfig['driverClass'],
        'connectionString': dbConfig['connectionString'],
        'username': dbConfig['username'],
        'password': sqlDbPassword,

        # Machine Type
        'numWorkers': dagConfig['ResponseLoadDeleteTable']['numWorkers'],
        'maxNumWorkers': dagConfig['ResponseLoadDeleteTable']['maxNumWorkers'],
        'workerMachineType': dagConfig['ResponseLoadDeleteTable']['workerMachineType'],

        # PGP Encryption
        'pgpCryptionEnabled': barricade['pgpCryptionEnabled'],
        'pgpSigned': barricade['pgpSigned'],
        # 'pgpObjectFactory': barricade['pgpObjectFactory'],
        # DIBI Vaut Parameters
        'hcvSSAKeystoreGcsBucketName': dagConfig['vault-dibi']['hcvSSAKeystoreGcsBucketName'],
        'hcvSSAKeystoreGcsFilePath': dagConfig['vault-dibi']['hcvSSAKeystoreGcsFilePath'],
        'hcvSSAUrl': dagConfig['vault-dibi']['hcvSSAUrl'],
        'hcvSSANamespace': dagConfig['vault-dibi']['hcvSSANamespace'],
        'hcvSSASecretPath': dagConfig['vault-dibi']['hcvSSASecretPath'],
        'hcvSSAEngineVersion': dagConfig['vault-dibi']['hcvSSAEngineVersion'],
        'hcvSSAGcpRole': dagConfig['vault-dibi']['hcvSSAGcpRole'],
        'hcvSSAGcpProjectId': dagConfig['vault-dibi']['hcvSSAGcpProjectId'],
        'hcvSSAGcpServiceAccount': dagConfig['vault-dibi']['hcvSSAGcpServiceAccount'],
        # Job Parameters
        'input': dagXCom['picked-LoadAndMaintainVztResponsePipeline-SsaConinput']

    },
    gcp_conn_id=dagConfig['gcpConnId'],
    dag=dag)

def archive_input_files():
    #Output File For SSACONINPut
    gsutil_mv(dagXCom['picked-LoadAndMaintainVztResponsePipeline-SsaConinput'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])
    #output File For cnx
    gsutil_mv(dagXCom['picked-LoadAndMaintainVztResponsePipeline-CnxOutput'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])

    # output File For delta
    gsutil_mv(dagXCom['picked-LoadAndMaintainVztResponsePipeline-deltaOutput'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])

    # output File For OverrideFile
    gsutil_mv(dagXCom['picked-LoadAndMaintainVztResponsePipeline-Overrideinput'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])

    # output File For VZ_DS_Fuzzy_Req_Extract File
    gsutil_mv(dagXCom['picked-LoadAndMaintainVztRequestPipeline-input'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])

    # output File For DICNX0000002-efx_vzw.snd File
    gsutil_mv(dagXCom['picked-LoadAndMaintainDICNX-efx-snd-file-output']  + " " + dagConfig['archivePath'] +
             dagXCom['archiveSubDirName'])

    # output File For DICNX0000002-efx_vzw.ctl File
    gsutil_mv(dagXCom['picked-LoadAndMaintainDICNX-efx-ctl-file-output'] + " " + dagConfig['archivePath'] +
        dagXCom['archiveSubDirName'])

    # output File For DIVZ-Com-SR File
    gsutil_mv(dagXCom['picked-LoadAndMaintainDIVZ-Com-SR-file-output'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])

    # output File For VZ-Ds-Fuzzy-Req File
    gsutil_mv(dagXCom['picked-LoadAndMaintainVZ-Ds-Fuzzy-Req-file-output'] + " " + dagConfig['archivePath'] +
              dagXCom['archiveSubDirName'])

t17 = PythonOperator(
        task_id='vzt-cnx-keying_archive_files',
        python_callable=archive_input_files,
        dag=dag)

t0 >> t1 >> t01 >> t2 >> t3 >> t03 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12 >> t13 >> t14 >> t15 >> t16 >>t17