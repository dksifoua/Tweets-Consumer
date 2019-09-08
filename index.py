import json
from datetime import datetime

from src.logger import Logger

import findspark

findspark.init('C:\opt\spark-2.4.3-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def flatten(x):
    res = x[1]
    res['category'] = x[0]
    return res


def process_rdd(rdd):
    if not rdd.isEmpty():
        df = rdd.map(flatten).map(lambda x: Row(**x)).toDF()
        # df.write \
        #     .format('com.mongodb.spark.sql.DefaultSource') \
        #     .mode('append') \
        #     .option('database', 'dynamas') \
        #     .option('collection', 'tweets') \
        #     .save()
        Logger.get_instance().debug(f'{df.count()} Tweets saved in MongoDB')
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

    kafkaStream = KafkaUtils.createDirectStream(ssc, TOPICS, {"metadata.broker.list": 'localhost:9092'})
    kafkaStream.map(lambda x: (x[0], json.loads(x[1]))).foreachRDD(process_rdd)

    ssc.start()
    ssc.awaitTermination()
