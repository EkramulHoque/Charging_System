from pyspark import SparkConf, SparkContext
import sys
import math

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


def process_row(row):
    print ("Row", row)

def save_batch(df, epoch_id):
    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append") \
            .option("database","billing") \
            .option("collection", "custEventSource") \
            .save()
    pass

def main(topic):
    spark = SparkSession.builder.appName('Read_Stream') \
            .config('spark.cassandra.connection.host','localhost') \
            .config('spark.mongodb.input.uri','mongodb://Mirza_Tauqeer:<pwd>@ds031968.mlab.com:31968/billing') \
            .config('spark.mongodb.output.uri', 'mongodb://Mirza_Tauqeer:<pwd>@ds031968.mlab.com:31968/billing') \
            .getOrCreate()

    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', topic).load()

    spark.sparkContext.setLogLevel('WARN')

    values = messages.select(messages['value'].cast('string'))
    split_val = functions.split(values['value'], ',')
    values = values.withColumn('custId', split_val.getItem(0))
    values = values.withColumn('startDate', split_val.getItem(1))
    #values = values.withColumn('endDate', split_val.getItem(2))

    stream = values.writeStream.foreachBatch(save_batch).start()

    stream.awaitTermination(600)

if __name__ == "__main__":
    topic = "events"
    main(topic)
