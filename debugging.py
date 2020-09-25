from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def built_in_sink_exercise():
    """
    Submit this code using spark-submit command rather than running on jupyter notebook
    We'll be using "console" sink
    :return:
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("spark streaming example") \
        .getOrCreate()

    df = spark.read.json('data.txt')

    # create a struct for this schema - you might want to load this file in jupyter notebook then figure out the schema
    jsonSchema = schema = StructType([
        StructField('timestamp',StringType(),True),
        StructField('status',StringType(),True)])

    # note - this will only print the progress report once because we ONLY have one file!
    # if you want to see multiple batches in the console, try to duplicate this data.txt file within the directory
    ## Treat a sequence of files as a stream by picking one file at a time
    streaming_input_df = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger","1").json('data.txt')
    streaming_count_df = streaming_input_df.groupBy(streaming_input_df.status, window(streaming_input_df.timestamp, "1 hour")).count()
    streaming_count_df.printSchema()

    # memory = store in-memory table (for testing only in Spark 2.0)
    # counts = name of the in-memory table
    # complete = all the counts should be in the table
    #More About update :https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-OutputMode.html#Complete
    query = streaming_count_df.writeStream.format('memory').queryName('counts').outputMode('complete').start()

if __name__ == "__main__":
    built_in_sink_exercise()
