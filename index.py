import json

from src.logger import Logger
from src.text_processing import TextProcessing
from src.utils import flatten

import findspark

findspark.init('C:\opt\spark-2.4.3-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def save_to_db(rdd):
    if not rdd.isEmpty():
        df = rdd.map(flatten).map(lambda x: Row(**x)).toDF()
        df.write \
            .format('com.mongodb.spark.sql.DefaultSource') \
            .mode('append') \
            .option('database', 'dynamas') \
            .option('collection', 'tweets') \
            .save()
    return rdd


if __name__ == '__main__':
    TOPICS = ['Financial', 'Trend', 'Stock.AAPL', 'Stock.MSFT', 'Stock.GOOGL']

    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 1)
    spark = SparkSession.builder \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
        .getOrCreate()
    Logger.get_instance().info('Contexts initialized!')

    kafka_stream = KafkaUtils.createDirectStream(ssc, TOPICS, {"metadata.broker.list": 'localhost:9092'})
    tweets = kafka_stream.map(lambda x: (x[0], json.loads(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_url(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_html_tag(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_user_tag(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_hash_tag(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_emojis(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_emoticons(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_punctuation(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.expand_abbreviation(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.expand_contract_word(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.remove_double_space(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.lower_case(x[1]))) \
        .map(lambda x: (x[0], TextProcessing.lemmatize(x[1])))

    # Display computation time {For now, the latency is one second maximum}
    # from datetime import datetime
    # tweets.map(lambda x: f'{datetime.now().time()} - {x[1]["created_at"]}').pprint()

    # Display the number of tweets by topic
    nbr_tweets = tweets.map(lambda x: (x[0], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .pprint()

    tweets_to_db = tweets.foreachRDD(save_to_db)

    ssc.start()
    ssc.awaitTermination()
