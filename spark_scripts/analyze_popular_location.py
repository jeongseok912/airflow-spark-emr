import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


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
        "Analyze Popular Location").getOrCreate()

    # 인기 지역 분석
    get_popular_location_data(spark, src=args.src, output=args.output)


if __name__ == "__main__":
    main()
