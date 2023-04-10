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


def get_latest_year_partition():
    bucket = Variable.get("AWS_S3_BUCKET_TLC_TAXI")
    prefix = "source/"

    s3 = S3Hook('aws_default')
    r2 = s3.list_keys(bucket_name=bucket)
    print(f"r2 : {r2}")
    r3 = s3.list_prefixes(bucket_name=bucket)
    print(f"r3 : {r3}")
    result = s3.list_prefixes(bucket_name=bucket, prefix=prefix)
    print(f"result : {result}")


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
                "s3://tlc-taxi/scripts/preprocess_data.py",
                "--src",
                "s3://tlc-taxi/source/2019/",
                "--output",
                "s3://tlc-taxi/output/preprocess/",
            ]
        }
    },
    {
        "Name": "Analyze preprocessed TLC Taxi Record",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://tlc-taxi/scripts/analyze_data.py",
                "--src",
                "s3://tlc-taxi/output/preprocess/",
                "--output",
                "s3://tlc-taxi/output/analyze/",
            ]
        }
    },
]


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
'''
    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_job_flow.output,
        steps=SPARK_STEPS,
        wait_for_completion=True,
    )

    check_job_flow = EmrJobFlowSensor(
        task_id="check_job_flow",
        job_flow_id=create_job_flow.output
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_job_flow.output
    )

get_latest_year_partition >> create_job_flow >> add_steps >> check_job_flow >> remove_cluster
'''
