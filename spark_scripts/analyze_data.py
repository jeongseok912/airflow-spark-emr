import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def get_elapsed_data(spark, src, output):
    df = spark.read.parquet(src)
    elapsed_df = df.na.drop(subset=['request_datetime', 'on_scene_datetime'])\
        .withColumn('elapsed(m)', round((unix_timestamp('on_scene_datetime') - unix_timestamp('request_datetime')) / 60))

    # 도착예정시간(ETA) 예측을 위한 ML 학습 데이터
    elapsed_for_export_df = elapsed_df.select(
        'Company',
        'Date',
        'PULocationID',
        'DOLocationID',
        'elapsed(m)'
    )
    elapsed_for_export_df.coalesce(1).write.option('header', 'True').mode(
        'overwrite').csv(f'{output}/elapsed')

    # 경쟁사 간 월별 평균 소요시간 추이 분석을 위한 데이터
    avg_elapsed_by_month_df = elapsed_df.groupby('Company', 'year_month')\
        .avg('elapsed(m)')\
        .select('Company', 'year_month', round('avg(elapsed(m))').alias('elapsed(m)'))
    avg_elapsed_by_month_df.coalesce(1).write.option('header', 'True').mode('overwrite').csv(
        f'{output}/avg_elapsed_by_month')


def get_market_share_data(spark, src, output):
    sumWindowSpec = Window.partitionBy('year_month')

    df = spark.read.parquet(src)
    share_df = df.groupby('Company', 'year_month').count()\
        .withColumn('share', round(col('count') / sum('count').over(sumWindowSpec) * 100, 2))\
        .sort('year_month', 'Company')

    share_df.coalesce(1).write.option('header', 'True').mode(
        'overwrite').csv(f'{output}/market_share')


def get_popular_location_data(spark, src, output):
    sumWindowSpec = Window.partitionBy('year_month')
    rankWindowSpec = Window.partitionBy('year_month').orderBy(desc('count'))

    df = spark.read.parquet(src)

    pu_location_df = df.groupby('year_month', 'PULocationID').count()\
        .withColumn('share', round(col('count') / sum('count').over(sumWindowSpec) * 100, 2))\
        .withColumn('rank', dense_rank().over(rankWindowSpec))\
        .sort('year_month', desc(col('count')))

    with_prev_df = pu_location_df\
        .withColumn('prev_month', date_format(date_sub(to_date(concat(col('year_month'), lit('01')), 'yyyyMMdd'), 1), 'yyyyMM'))
    join_df = pu_location_df\
        .withColumnRenamed('year_month', 'prev_month')\
        .withColumnRenamed('share', 'prev_month_share')\
        .withColumnRenamed('rank', 'prev_month_rank')

    cond = ['PULocationID', 'prev_month']
    popular_location_df = with_prev_df\
        .join(join_df, cond, 'left')\
        .drop(join_df['count'])\
        .select(
            'year_month',
            'PULocationID',
            'count',
            'share',
            'rank',
            'prev_month_share',
            'prev_month_rank',
            round(col('share') - col('prev_month_share')).alias('share_var(%p)'),
            (col('rank') - col('prev_month_rank')).alias('rank_var')
        )\
        .sort('year_month', desc('rank_var'))

    popular_location_df.coalesce(1).write.option('header', 'True').mode('overwrite').csv(
        f'{output}/popular_location')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', type=str,
                        help='S3 source', default='s3://tlc-taxi/output/preprocess')
    parser.add_argument('--output', type=str,
                        help='S3 output', default='s3://tlc-taxi/output')
    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        "PySpark - Analyze preprocessed TLC Taxi Record").getOrCreate()

    # 소요시간 분석
    get_elapsed_data(spark, src=args.src, output=args.output)

    # 시장 점유율 분석
    get_market_share_data(spark, src=args.src, output=args.output)

    # 인기 지역 분석
    get_popular_location_data(spark, src=args.src, output=args.output)


if __name__ == "__main__":
    main()
