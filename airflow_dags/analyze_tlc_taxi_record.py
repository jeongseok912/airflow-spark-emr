from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

SPARK_STEPS = [
    {
        "Name": "Analyze TLC Taxi Record",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://tlc-taxi/scripts/spark_etl.py",
                "--src",
                "s3://tlc-taxi/source/2019/",
                "--output",
                "s3://tlc-taxi/output/2019/"
            ]
        }
    }
]


JOB_FLOW_OVERRIDES = {
    "Name": "PySpark Cluster",
    "LogUri": "s3://airflow--log/emr-log/",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
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
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "c4.large",
                "InstanceCount": 2
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}


with DAG(
    'analyze_tlc_taxi_record',
    start_date=datetime(2023, 3, 28),
    tags=['tlc_taxi_record']
) as dag:

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

create_job_flow >> add_steps >> check_job_flow >> remove_cluster
