import sys
import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.window import Window

def get_geo_city_df(spark):
    city = spark.read.csv('/user/lovalovan/data/geo/geo.csv', sep=';', header=True)\
        .withColumn('lat', F.translate('lat', ',', '.'))\
        .withColumn('lng', F.translate('lng', ',', '.'))\
        .withColumn('lat', F.radians('lat'))\
        .withColumn('lng', F.radians('lng'))\
        .withColumnRenamed('lat', 'city_lat')\
        .withColumnRenamed('lng', 'city_lon')
    return city

def get_event_city_df(events, city):
    events_city = events\
        .withColumn('lat', F.radians('lat'))\
        .withColumn('lon', F.radians('lon'))\
        .withColumn('event_id', F.col('event.message_id'))\
        .crossJoin(city)\
        .withColumn('distance', F.acos(F.sin(F.col('city_lat')) * F.sin(F.col('lat')) + 
                                       F.cos(F.col('city_lat')) * F.cos(F.col('lat')) * 
                                       F.cos(F.col('city_lon')-F.col('lon'))) * F.lit(6371))
    return events_city


def get_city_df(events_city):
    window = Window.partitionBy('event.message_from').orderBy(F.col('distance'))
    city_df = events_city\
        .withColumn('row_num', F.row_number().over(window))\
        .filter(F.col('row_num') == 1)\
        .drop('row_num')\
        .withColumnRenamed('id', 'city_id').persist()
    return city_df

def get_city_act_df(city_df):
        window = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())

        city_act_df = city_df.where('event_type == "message"') \
            .withColumn('row_num', F.row_number().over(window)) \
            .filter(F.col('row_num') == 1) \
            .selectExpr('event.message_from as user_id', 'city_id as act_city')
        return city_act_df

def get_travel_city_df(city_df):
    window = Window().partitionBy('event.message_from', 'event_id').orderBy(F.col('date'))

    travel_city_df = city_df.where('event_type == "message"') \
        .withColumn('dense_rank', F.dense_rank().over(window)) \
        .withColumn("date_diff", F.datediff(F.col('date'), F.to_date(F.col("dense_rank").cast("string"), 'dd')))\
        .selectExpr('date_diff', 'event.message_from as user_id', 'date', 'event_id' ) \
        .groupBy("user_id", "date_diff", "event_id") \
        .agg(F.countDistinct(F.col('date')).alias('cnt_city'))
    return travel_city_df

def get_home_df(travel_city_df):
    home_city = travel_city_df.filter(F.col('cnt_city') > 27)
    home_df = home_city \
        .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user_id'))) \
        .filter(F.col('date_diff') == F.col('max_dt'))
    return home_df

def get_travel_array_df(travel_city_df, city_df):
    travel_array_df = travel_city_df \
        .join(city_df, travel_city_df.event_id == city_df.event_id, 'left') \
        .orderBy('date').groupBy('user_id') \
        .agg(F.collect_list('city_id').alias('travel_array')) \
        .selectExpr('user_id', 'size(travel_array) as travel_count', 'travel_array')

    return travel_array_df

def get_local_time_df(events_city):
    city_zones = ['ACT', 'Adelaide', 'Brisbane', 'Broken_Hill', 
                  'Canberra	', 'Currie', 'Darwin', 'Eucla', 'Hobart', 
                  'LHI', 'Lindeman', 'Lord_Howe', 'Melbourne', 'North', 
                  'NSW', 'Perth', 'Queensland', 'South', 'Sydney', 
                  'Tasmania', 'Victoria', 'West', 'Yancowinna']
    window = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())

    local_time_df = events_city \
        .filter(F.col('city').isin(city_zones))\
        .withColumn('row_num', F.row_number().over(window)) \
        .filter(F.col('row_num') == 1) \
        .withColumn('last_ts', F.when(F.col('event.datetime').isNull(), F.col('event.message_ts')).otherwise(
        F.col('event.datetime'))) \
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city')))\
        .withColumn('local_time', F.from_utc_timestamp(F.col('last_ts'), F.col('timezone'))) \
        .selectExpr('event.message_from as user_id', 'city', 'local_time')

    return local_time_df

def get_period_stat_df(city_df, period):
    message = city_df \
        .where('event.message_id is not Null') \
        .groupBy(['city_id', F.trunc(F.col('date'), period).alias(period)]) \
        .count() \
        .withColumnRenamed('count', f'{period}_message')

    reaction = city_df \
        .where('event.reaction_from is not Null') \
        .groupBy(['city_id', F.trunc(F.col('date'), period).alias(period)]) \
        .count() \
        .withColumnRenamed('count', f'{period}_reaction')

    subscription = city_df \
        .where('event.subscription_channel is not Null') \
        .groupBy(['city_id', F.trunc(F.col('date'), period).alias(period)]) \
        .count() \
        .withColumnRenamed('count', f'{period}_subscription')

    window = Window().partitionBy('event.message_from').orderBy(F.col('last_ts'))
    user = city_df \
        .withColumn('last_ts', F.when(F.col('event.datetime').isNull(), F.col('event.message_ts')).otherwise(
        F.col('event.datetime'))) \
        .withColumn('row_num', F.row_number().over(window)) \
        .filter(F.col('row_num') == 1) \
        .groupBy(['city_id', F.trunc(F.col('date'), period).alias(period)]) \
        .count() \
        .withColumnRenamed('count', f'{period}_user')

    stat_df = message \
        .join(reaction, ['city_id', period], 'left') \
        .join(subscription, ['city_id', period], 'left') \
        .join(user, ['city_id', period], 'left')

    return stat_df

def get_friends_df(city_df, local_time_df):
    window = Window().partitionBy('user_id').orderBy('date')
    city_df_from = city_df.selectExpr('event.message_from as user_id', 'city_lat', 'city_lon', 'date')
    city_df_to = city_df.selectExpr('event.message_to as user_id', 'city_lat', 'city_lon', 'date')

    window_rn = Window().partitionBy('user_id').orderBy(F.col('date').desc())
    df = city_df_from \
        .union(city_df_to) \
        .select(F.col('user_id'), F.col('date'),
                F.last(F.col('city_lat'), ignorenulls=True).over(window).alias('lat'),
                F.last(F.col('city_lon'), ignorenulls=True).over(window).alias('lng')) \
        .withColumn('rn', F.row_number().over(window_rn)) \
        .filter(F.col('rn') == 1) \
        .drop('rn') \
        .where('user_id is not null') \
        .distinct()

    df_city_r = df.selectExpr('user_id as r_user', 'date', 'lat as r_lat', 'lng as r_lng')
    distance_users = df \
        .join(df_city_r, 'date', 'left') \
        .withColumn('distance', F.acos(
        F.sin(F.col('r_lat')) * F.sin(F.col('lat')) + F.cos(F.col('r_lat')) * F.cos(F.col('lat')) * F.cos(
            F.col('r_lng') - F.col('lng'))) * F.lit(6371)) \
        .filter(F.col('distance') <= 1) \
        .selectExpr('user_id as l_user', 'r_user')

    sub_right = city_df.selectExpr('event.subscription_user as r_user', 'event.subscription_channel as channel')
    sub_df = city_df \
        .join(sub_right, city_df.event.subscription_channel == sub_right.channel) \
        .selectExpr('event.subscription_user as user_left', 'r_user as user_right', 'city_id') \
        .distinct()

    dist_sub_df = distance_users \
        .join(sub_df, (distance_users.l_user == sub_df.user_left) & (distance_users.r_user == sub_df.user_right),
              'inner') \
        .selectExpr('user_left', 'user_right', 'city_id')

    msg_from = city_df.selectExpr('event.message_from as from_user', 'event.message_to as to_user')
    msg_to = city_df.selectExpr('event.message_from as to_user', 'event.message_to as from_user')
    all_msg = msg_from.unionByName(msg_to).distinct()

    friends_df = dist_sub_df \
        .join(all_msg, (all_msg.from_user == dist_sub_df.user_left) & (all_msg.to_user == dist_sub_df.user_right),
              'left_anti') \
        .join(local_time_df, local_time_df.user_id == dist_sub_df.user_left, 'left') \
        .selectExpr('user_left', 'user_right', 'current_date() as processed_dttm', 'city_id as zone_id', 'local_time')

    return friends_df

def main():
    base_input_path = sys.argv[1]
    base_output_path = sys.argv[2]

    conf = SparkConf().setAppName('Project 6')
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    events = spark.read.parquet(base_input_path)
    city = get_geo_city_df(spark)
    events_city = get_event_city_df(events, city)
    city_df = get_city_df(events_city)
    city_act_df = get_city_act_df(city_df)
    travel_city_df = get_travel_city_df(city_df)
    home_df = get_home_df(travel_city_df)
    travel_array_df = get_travel_array_df(travel_city_df, city_df)
    local_time_df = get_local_time_df(events_city)

    home_city_df = home_df \
        .join(city_df, (home_df.user_id == city_df.event.message_from) & (home_df.event_id == city_df.event_id), 'left')\
        .selectExpr('user_id', 'city_id as home_city')

    travel_df = city_df \
        .selectExpr('event.message_from as user_id') \
        .distinct() \
        .join(city_act_df, 'user_id', 'left') \
        .join(home_city_df, 'user_id', 'left') \
        .join(travel_array_df, 'user_id', 'left') \
        .join(local_time_df, 'user_id', 'left') \
        .drop('city')
    travel_df.write.format('parquet').mode("overwrite").save(f'{base_output_path}/dm_users')

    month_stat_df = get_period_stat_df(city_df, 'month')
    week_stat_df = get_period_stat_df(city_df, 'week')
    all_stat_df = month_stat_df.join(week_stat_df, 'city_id', 'left')\
        .selectExpr('month', 
                    'week', 
                    'city_id as zone_id', 
                    'week_message', 
                    'week_reaction', 
                    'week_subscription', 
                    'week_user', 
                    'month_message', 
                    'month_reaction', 
                    'month_subscription', 
                    'month_user')
    all_stat_df.write.format('parquet').mode("overwrite").save(f'{base_output_path}/dm_stats')
    
    friends_df = get_friends_df(city_df, local_time_df)
    friends_df.write.format('parquet').mode("overwrite").save(f'{base_output_path}/dm_friends')

if __name__ == "__main__":
    main()
