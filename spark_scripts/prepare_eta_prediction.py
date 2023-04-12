import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def get_elapsed_data_for_eta_prediction(spark, src, output):
    df = spark.read.parquet(src)
    elapsed_df = df.na.drop(subset=['request_datetime', 'on_scene_datetime'])\
        .withColumn('elapsed(m)', round((unix_timestamp('on_scene_datetime') - unix_timestamp('request_datetime')) / 60))

    for_eta_df = elapsed_df.select(
        'Company',
        'Date',
        'PULocationID',
        'DOLocationID',
        'elapsed(m)'
    )
    for_eta_df.coalesce(10).write.option('header', 'True').mode(
        'overwrite').csv(f'{output}/elapsed_for_eta_prediction')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', type=str,
                        help='S3 source', default='s3://tlc-taxi/output/preprocess')
    parser.add_argument('--output', type=str,
                        help='S3 output', default='s3://tlc-taxi/output')
    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        "Prepare Data for ETA Prediction").getOrCreate()

    # 도착예정시간(ETA) 예측을 위한 ML 학습 데이터
    get_elapsed_data_for_eta_prediction(
        spark, src=args.src, output=args.output)


if __name__ == "__main__":
    main()
