from datetime import datetime

from airflow import DAG
from airflow.models import Variable
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


def make_dynamic_step_definition(**context):
    latest_year = context["task_instance"].xcom_pull(
        task_ids='get_latest_year_partition')

    SPARK_STEPS = [
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
        },
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
        },
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
        },
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

    return SPARK_STEPS


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
    ]
}


with DAG(
    'analyze_tlc_taxi_record',
    start_date=datetime(2023, 3, 28),
    tags=['tlc_taxi_record']
) as dag:

    get_latest_year_partition = PythonOperator(
        task_id="get_latest_year_partition",
        python_callable=get_latest_year_partition
    )

    make_dynamic_step_definition = PythonOperator(
        task_id="make_dynamic_step_definition",
        python_callable=make_dynamic_step_definition
    )

    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    preprocess_data = EmrAddStepsOperator(
        task_id="preprocess_data",
        job_flow_id=create_job_flow.output,
        steps=make_dynamic_step_definition.output[0],
        wait_for_completion=True,
    )

    analyze_elapsed_time = EmrAddStepsOperator(
        task_id="analyze_elapsed_time",
        job_flow_id=create_job_flow.output,
        steps=make_dynamic_step_definition.output[1],
        wait_for_completion=True,
    )

    analyze_market_share = EmrAddStepsOperator(
        task_id="analyze_market_share",
        job_flow_id=create_job_flow.output,
        steps=make_dynamic_step_definition.output[2],
        wait_for_completion=True,
    )

    analyze_popular_location = EmrAddStepsOperator(
        task_id="analyze_popular_location",
        job_flow_id=create_job_flow.output,
        steps=make_dynamic_step_definition.output[3],
        wait_for_completion=True,
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

get_latest_year_partition >> make_dynamic_step_definition >> create_job_flow >> preprocess_data

preprocess_data >> [analyze_elapsed_time, analyze_market_share,
                    analyze_popular_location] >> check_job_flow >> remove_cluster
