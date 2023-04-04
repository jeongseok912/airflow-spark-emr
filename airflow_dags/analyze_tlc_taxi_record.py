from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
    EmrStartNotebookExecutionOperator,
    EmrStopNotebookExecutionOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrNotebookExecutionSensor

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

    '''
    start_execution = EmrStartNotebookExecutionOperator(
        task_id="start_execution",
        editor_id="e-1SFM5AFSW2P6SVEUKZAHMLZXU",
        cluster_id="j-2VW9BK2ID9RMI",
        relative_path="tlc-taxi-record.ipynb",
        service_role="EMR_DefaultRole"
    )

    wait_for_execution_start = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_start",
        notebook_execution_id=start_execution.output,
        target_states={"RUNNING"},
        poke_interval=5
    )
    '''

# create_job_flow >> add_steps
# create_job_flow >> start_execution >> wait_for_execution_start
