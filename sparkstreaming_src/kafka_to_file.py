from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import expr,from_json,col

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName("SparkStructredStream")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
        .getOrCreate()

    schema = StructType([
        StructField("res_id", DoubleType()),
        StructField("name", StringType()),
        StructField("has_table_booking", IntegerType()),
        StructField("is_delivering_now", IntegerType()),
        StructField("deeplink", StringType()),
        StructField("menu_url", StringType()),
        StructField("cuisines", StringType()),
        StructField("location", StructType([
            StructField("latitude", StringType()),
            StructField("address", StringType()),
            StructField("city", StringType()),
            StructField("country_id", DoubleType()),
            StructField("locality_verbose", StringType()),
            StructField("city_id", DoubleType()),
            StructField("zipcode", StringType()),
            StructField("longitude", StringType()),
            StructField("locality", StringType())
        ])),
        StructField("user_ratings", ArrayType(StructType([
            StructField("rating_text", StringType()),
            StructField("user_id", StringType()),
            StructField("rating", StringType())
        ]))),
    ])

    # Read a Streaming Source
    input_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "quickstart-events")\
        .option("startingOffsets", "earliest")\
        .load()

    # Transform to Output DataFrame
    value_df = input_df.select(from_json(col("value").cast("string"),schema).alias("value"))
    # value_df.printSchema()

    exploded_df = value_df.selectExpr("value.res_id","value.name","value.menu_url","value.cuisines",
                                      "value.location.address as address",
                                      "value.location.country_id as country_id",
                                      "value.location.city_id as city_id","value.location.zipcode as zipcode",
                                      "explode(value.user_ratings) as usr_ratings")

    # exploded_df.printSchema()
    flattened_df = exploded_df\
        .withColumn('rating_text',expr('usr_ratings.rating_text'))\
        .withColumn('user_id',expr('usr_ratings.user_id'))\
        .withColumn('rating',expr('usr_ratings.rating'))\
        .drop('usr_ratings')

    # flattened_df.printSchema()

    # Write to Sink
    output_query = flattened_df.writeStream\
        .format("json")\
        .option("path","output")\
        .option("checkpointLocation", "chck-pnt-dir")\
        .outputMode("append")\
        .queryName("Flattened Invoice Writter")\
        .start()

    output_query.awaitTermination()