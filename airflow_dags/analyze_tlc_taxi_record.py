from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


bucket = Variable.get("AWS_S3_BUCKET_TLC_TAXI")
src = Variable.get("AWS_S3_SOURCE")
output = Variable.get("AWS_S3_OUTPUT")
script = Variable.get("AWS_S3_SCRIPT")


def get_latest_year_partition():
    s3 = S3Hook('aws_default')
    prefix = src + '/'

    prefixes = s3.list_prefixes(
        bucket_name=bucket, prefix=prefix, delimiter='/')
    latest_year = max([int(prefix.split('/')[-2]) for prefix in prefixes])

    return latest_year


def make_preprocess_data_definition(**context):
    latest_year = context["ti"].xcom_pull(task_ids='get_latest_year_partition')

    STEP = [
        {
            "Name": "Preprocess TLC Taxi Record",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/preprocess_data.py",
                    "--src",
                    f"s3://{bucket}/{src}/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                ]
            }
        }
    ]

    return STEP


def make_analyze_elapsed_time_definition(**context):
    latest_year = context["ti"].xcom_pull(task_ids='get_latest_year_partition')

    STEP = [
        {
            "Name": "Analyze Elapsed Time",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/analyze_elapsed_time.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]

    return STEP


def make_prepare_eta_prediction_definition(**context):
    latest_year = context["ti"].xcom_pull(task_ids='get_latest_year_partition')

    STEP = [
        {
            "Name": "Prepare Data for ETA Prediction",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/prepare_eta_prediction.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]

    return STEP


def make_analyze_market_share_definition(**context):
    latest_year = context["ti"].xcom_pull(task_ids='get_latest_year_partition')

    STEP = [
        {
            "Name": "Analyze Market Share",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/analyze_market_share.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]

    return STEP


def make_analyze_popular_location_definition(**context):
    latest_year = context["ti"].xcom_pull(task_ids='get_latest_year_partition')

    STEP = [
        {
            "Name": "Analyze Popular Location",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/analyze_popular_location.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]

    return STEP


JOB_FLOW_OVERRIDES = {
    "Name": "PySpark Cluster",
    "LogUri": "s3://emr--log/",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "EmrManagedMasterSecurityGroup": "sg-0a8997b0ae4e90d07",
        "EmrManagedSlaveSecurityGroup": "sg-055cef9cc6cc12658",
        "Ec2KeyName": "airflow",
        "Ec2SubnetId": "subnet-8cf1eee4",
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "Configurations": [
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.resourcemanager.am.max-attempts": "1"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ],
    "StepConcurrencyLevel": 3
}


with DAG(
    'analyze_tlc_taxi_record',
    start_date=datetime(2023, 2, 28),
    schedule=None,
    tags=['tlc_taxi_record']
) as dag:

    get_latest_year_partition = PythonOperator(
        task_id="get_latest_year_partition",
        python_callable=get_latest_year_partition
    )

    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    with TaskGroup('preprocess', tooltip="Task for Preprocess Data") as preprocess:
        make_preprocess_data_definition = PythonOperator(
            task_id="make_preprocess_data_definition",
            python_callable=make_preprocess_data_definition
        )

        preprocess_data = EmrAddStepsOperator(
            task_id="preprocess_data",
            job_flow_id=create_job_flow.output,
            steps=make_preprocess_data_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool'
        )

    with TaskGroup('analyze_1', tooltip="Task for Elapsed Time") as analyze_1:
        make_analyze_elapsed_time_definition = PythonOperator(
            task_id="make_analyze_elapsed_time_definition",
            python_callable=make_analyze_elapsed_time_definition
        )

        analyze_elapsed_time = EmrAddStepsOperator(
            task_id="analyze_elapsed_time",
            job_flow_id=create_job_flow.output,
            steps=make_analyze_elapsed_time_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=3
        )

    with TaskGroup('prepare_eta_prediction', tooltip="Task for ETA Prediction") as prepare_eta_prediction:
        make_prepare_eta_prediction_definition = PythonOperator(
            task_id="make_prepare_eta_data_definition",
            python_callable=make_prepare_eta_prediction_definition
        )

        prepare_eta_prediction = EmrAddStepsOperator(
            task_id="prepare_eta_prediction",
            job_flow_id=create_job_flow.output,
            steps=make_prepare_eta_prediction_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=3
        )

    with TaskGroup('analyze_2', tooltip="Task for Market Share") as analyze_2:
        make_analyze_market_share_definition = PythonOperator(
            task_id="make_analyze_market_share_definition",
            python_callable=make_analyze_market_share_definition
        )

        analyze_market_share = EmrAddStepsOperator(
            task_id="analyze_market_share",
            job_flow_id=create_job_flow.output,
            steps=make_analyze_market_share_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=2
        )

    with TaskGroup('analyze_3', tooltip="Task for Popular Location") as analyze_3:
        make_analyze_popular_location_definition = PythonOperator(
            task_id="make_analyze_popular_location_definition",
            python_callable=make_analyze_popular_location_definition
        )

        analyze_popular_location = EmrAddStepsOperator(
            task_id="analyze_popular_location",
            job_flow_id=create_job_flow.output,
            steps=make_analyze_popular_location_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=1
        )

    check_job_flow = EmrJobFlowSensor(
        task_id="check_job_flow",
        job_flow_id=create_job_flow.output,
        target_states='WAITING'
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_job_flow.output
    )

chain(
    get_latest_year_partition,
    create_job_flow,
    preprocess,
    [analyze_1, prepare_eta_prediction, analyze_2, analyze_3],
    check_job_flow,
    remove_cluster
)
