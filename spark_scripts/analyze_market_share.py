import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def get_market_share_data(spark, src, output):
    sumWindowSpec = Window.partitionBy('year_month')

    df = spark.read.parquet(src)
    share_df = df.groupby('Company', 'year_month').count()\
        .withColumn('share', round(col('count') / sum('count').over(sumWindowSpec) * 100, 2))\
        .sort('year_month', 'Company')

    share_df.coalesce(1).write.option('header', 'True').mode(
        'overwrite').csv(f'{output}/market_share')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', type=str,
                        help='S3 source', default='s3://tlc-taxi/output/preprocess')
    parser.add_argument('--output', type=str,
                        help='S3 output', default='s3://tlc-taxi/output')
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Analyze Market Share").getOrCreate()

    # 시장 점유율 분석
    get_market_share_data(spark, src=args.src, output=args.output)


if __name__ == "__main__":
    main()
