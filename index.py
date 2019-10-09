from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.foreach_writer import ForeachWriter

if __name__ == '__main__':
    TOPICS = ['Financial', 'Trend', 'Tweet.AAPL', 'Tweet.MSFT', 'Tweet.GOOGL', 'Tweet.GS', 'Tweet.WMT']
    BOOTSTRAP_SERVERS = "localhost:9092"

    tweet_schema = StructType([
        StructField("created_at", StringType(), nullable=False),
        StructField("user_name", StringType(), nullable=False),
        StructField("user_screen_name", StringType(), nullable=False),
        StructField("user_favourites_count", IntegerType(), nullable=False),
        StructField("user_followers_count", IntegerType(), nullable=False),
        StructField("user_friends_count", IntegerType(), nullable=False),
        StructField("user_statuses_count", IntegerType(), nullable=False),
        StructField("favorite_count", IntegerType(), nullable=False),
        StructField("quote_count", IntegerType(), nullable=False),
        StructField("reply_count", IntegerType(), nullable=False),
        StructField("retweet_count", IntegerType(), nullable=False),
        StructField("text", StringType(), nullable=False),
    ])

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName('dynamas') \
        .getOrCreate()

    for topic in TOPICS:
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS timestamp)")

        df_tweets = df_stream.select(from_json(col=df_stream.value, schema=tweet_schema).alias("tweets"),
                                     df_stream.timestamp) \
            .select("tweets.*", "timestamp") \
            .writeStream \
            .foreach(ForeachWriter(collection=topic)) \
            .start()

    spark.streams.awaitAnyTermination()
