"""
RatesStreamingSource and Joins is a streaming source that generates consecutive numbers with timestamp that can be useful for testing and PoCs.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr


def join_exercise():

    spark = SparkSession.builder \
            .master("local[2]") \
            .appName("join and watermark exercise") \
            .getOrCreate()
    #setting shuffle partition to 1
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    
    # select expressions value and timestamp
    left = spark.readStream\
        .format("rate") \
        .option("rowsPerSecond", "5") \
        .option("numPartitions", "1").load() \
        .selectExpr("value AS row_id", "timestamp AS left_timestamp")
#        .writeStream.outputMode("append").format("console").start().awaitTermination()
    # enable writeStream line above to just run it on console and test

    # create a streaming dataframe that we'll join on
    right = spark.readStream \
        .format("rate")\
        .option("rowsPerSecond", "5")\
        .option("numPartitions", "1").load()\
        .where((rand() * 100).cast("integer") < 10) \
        .selectExpr("(value - 50) AS row_id ", "timestamp AS right_timestamp") \
        .where("row_id > 0")

    # join using row_id
    join_query = left.join(right, "row_id")

    #join_query.writeStream.outputMode("append").format("console").start().awaitTermination()

    print(query)

if __name__ == "__main__":
    join_exercise()
