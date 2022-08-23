from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName("SparkStructredStream")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.sql.streaming.schemaInference","true")\
        .getOrCreate()

    # Read a Streaming Source
    input_df = spark.readStream\
        .format("json")\
        .option("path", "data")\
        .load()

    input_df.printSchema()