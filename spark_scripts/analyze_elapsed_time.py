import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def get_elapsed_data(spark, src, output):
    df = spark.read.parquet(src)
    elapsed_df = df.na.drop(subset=['request_datetime', 'on_scene_datetime'])\
        .withColumn('elapsed(m)', round((unix_timestamp('on_scene_datetime') - unix_timestamp('request_datetime')) / 60))

    avg_elapsed_by_month_df = elapsed_df.groupby('Company', 'year_month')\
        .avg('elapsed(m)')\
        .select('Company', 'year_month', round('avg(elapsed(m))').alias('elapsed(m)'))
    avg_elapsed_by_month_df.coalesce(1).write.option('header', 'True').mode('overwrite').csv(
        f'{output}/avg_elapsed_by_month')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', type=str,
                        help='S3 source', default='s3://tlc-taxi/output/preprocess')
    parser.add_argument('--output', type=str,
                        help='S3 output', default='s3://tlc-taxi/output')
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Analyze Elapsed Time").getOrCreate()

    # 소요시간 분석
    get_elapsed_data(spark, src=args.src, output=args.output)


if __name__ == "__main__":
    main()
