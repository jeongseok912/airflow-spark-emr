from datetime import datetime
import requests
import boto3
import logging
import time
from json import dumps

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable


class DBHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__()
        self.hook = MySqlHook.get_hook(conn_id="TLC_TAXI")
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()
        self.default_log = {}

    def set_default_log(self, default_log):
        self.default_log = default_log
        print(self.default_log)

    def emit(self, record):
        if record:
            # msg = dict(record.msg)
            self.cursor.execute(
                f"INSERT INTO dataset_log VALUES ('{self.default_log['dataset_id']}', '{self.default_log['dag']}', '{self.default_log['run_id']}', '{self.default_log['ti']}', '{self.default_log['logical_date']}', '{record.msg}', SYSDATE());")

    def select(self, sql):
        return self.hook.get_records(sql)

    def close(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


def set_system_log(default_log, msg):
    default_log['msg'] = msg
    return default_log


################### Task functions ###################

@task
def get_latest_dataset_id(db):
    id = db.select("""
        SELECT
            MAX(dataset_id)
        FROM dataset_log
        WHERE message = 'S3 upload completed.';
        """
                   )[0][0]

    if id is None:
        id = 1

    db.close()

    return id


@task
def make_dynamic_url(db, num, **context):
    id = context['ti'].xcom_pull(task_ids='get_latest_dataset_id')
    start = 0
    end = 0
    urls = []

    if id == 1:
        start = 1
        end = num
    else:
        start = id + 1
        end = start + (num - 1)

    result = db.select(
        f"SELECT dataset_link FROM dataset_meta WHERE id BETWEEN {start} AND {end};")

    for row in result:
        urls.append(row[0])

    db.close()

    return urls


@task
def fetch(url, **context):
    # set logger
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    logger = logging.getLogger("dataset")
    logger.setLevel(logging.INFO)

    dbhandler = DBHandler()
    dbhandler.setFormatter(formatter)
    logger.addHandler(dbhandler)

    file_name = url.split("/")[-1]
    year = file_name.split("-")[0].split("_")[-1]

    # get id
    id = "NA"
    result = dbhandler.select(
        f"SELECT id FROM dataset_meta WHERE dataset_link = '{url}';")

    for row in result:
        id = str(row[0])

    # additional system parameter for logging
    system_args = {}
    system_args['dataset_id'] = id

    for key, value in context.items():
        if key in ('dag', 'run_id', 'ti', 'logical_date'):
            system_args[key] = value

    dbhandler.set_default_log(system_args)

    # download dataset
    logger.info(set_system_log(system_args, f"{url}"))
    logger.info("#########################################################")
    logger.info(set_system_log(system_args, "Download & S3 upload started."))
    downup_start = time.time()

    MB = 1024 * 1024
    chunk = 100 * MB
    with requests.get(url, stream=True) as r:
        if r.ok:
            aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

            s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

            bucket = Variable.get("AWS_S3_BUCKET_TLC_TAXI")
            dir = f"source/{year}"
            key = f"{dir}/{file_name}"

            mpu_parts = []

            mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
            mpu_id = mpu['UploadId']
            logger.info(set_system_log(system_args, f"mpu id : {mpu_id}"))

            for i, chunk in enumerate(r.iter_content(chunk_size=chunk), start=1):
                logger.info(set_system_log(
                    system_args, f"Uploading Chunk {i} to S3 started."))
                upload_start = time.time()

                part = s3.upload_part(
                    Body=chunk, Bucket=bucket, Key=key, UploadId=mpu_id, PartNumber=i)
                part_dict = {"PartNumber": i, "ETag": part["ETag"]}
                mpu_parts.append(part_dict)
                logger.info(set_system_log(system_args, f"{dumps(part_dict)}"))

                logger.info(set_system_log(
                    system_args, f"Uploading Chunk {i} to S3 completed."))
                upload_end = time.time()
                upload_elapsed = int(upload_end - upload_start)
                logger.info(set_system_log(
                    system_args, f"Upload {upload_elapsed}s elapsed."))

            logger.info(set_system_log(
                system_args, "Assembling chunks started."))
            result = s3.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=mpu_id, MultipartUpload={"Parts": mpu_parts})
            logger.info(set_system_log(
                system_args, "S3 upload completed."))
            logger.info(set_system_log(system_args, f"{dumps(result)}"))

    logger.info(set_system_log(system_args, "Download & S3 upload completed."))
    downup_end = time.time()
    downup_elapsed = int(downup_end - downup_start)
    logger.info(set_system_log(
        system_args, f"total {downup_elapsed}s elapsed."))

    dbhandler.close()


with DAG(
    'download_tlc_taxi_record',
    start_date=datetime(2022, 3, 25),
    schedule="@daily",
    catchup=True,
    tags=["tlc taxi record"]
) as dag:
    db = DBHandler()

    get_latest_dataset_id = get_latest_dataset_id(db)
    get_urls = make_dynamic_url(db, num=2)

    get_latest_dataset_id >> get_urls

    fetch.expand(url=get_urls)
