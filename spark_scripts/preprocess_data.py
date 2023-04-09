import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', type=str,
                        help='S3 source', default='s3://tlc-taxi/source')
    parser.add_argument('--output', type=str,
                        help='S3 output', default='s3://tlc-taxi/output/preprocess')
    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        "PySpark - Preprocess TLC Taxi Record").getOrCreate()

    '''
    year_months = []
    for file_name in list:
        year_months = file_name.split('_')[-1].replace('-', '')[:6]
    '''
    # year_month = ['201902', '201903']

    df = spark.read.parquet(args.src)

    # ML 학습용 데이터 및 평균 소요시간 추이 분석을 위한 기초 데이터
    cols = ('hvfhs_license_num', 'dispatching_base_num', 'originating_base_num', 'shared_request_flag',
            'shared_match_flag', 'access_a_ride_flag', 'wav_request_flag', 'wav_match_flag')
    base_df = df.withColumn('Company',
                            when(col('hvfhs_license_num') == 'HV0002', 'Juno')
                            .when(col('hvfhs_license_num') == 'HV0003', 'Uber')
                            .when(col('hvfhs_license_num') == 'HV0004', 'Via')
                            .when(col('hvfhs_license_num') == 'HV0005',  'Lyft'))\
        .withColumn('Date', to_date(split(col('request_datetime'), ' ')[0]))\
        .withColumn('year_month', date_format(col('request_datetime'), 'yyyyMM'))\
        .drop(*cols)\
        .select(
            'Company',
            'Date',
            'year_month',
            'PULocationID',
            'DOLocationID',
            'request_datetime',
            'on_scene_datetime'
    )
    # .filter(col('year_month').isin(year_month))\

    base_df.write.mode(
        'overwrite').partitionBy('year_month').parquet(args.output)


if __name__ == "__main__":
    main()
