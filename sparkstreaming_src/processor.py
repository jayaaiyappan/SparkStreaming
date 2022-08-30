from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import explode,col,from_json

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName("Spark_Processor")\
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
            StructField("rating", DoubleType())
        ]))),
    ])

    # Read a Streaming Source
    input_df = spark.read.json('data\\input.json')
    input_df.printSchema()

    # Transform to Output DataFrame
    # value_df = input_df.select(from_json(col('value').cast("string"),schema).alias("value"))

    exploded_df = input_df.selectExpr('res_id','name','has_table_booking','is_delivering_now',
                                      'deeplink','menu_url','cuisines','location.latitude as latitude',
                                      'location.address as address','location.city as city',
                                      'location.country_id as country_id',
                                      'location.locality_verbose as locality_verbose',
                                      'location.city_id as city_id','location.zipcode as zipcode',
                                      'location.longitude as longitude', 'location.locality as locality',
                                      'explode(user_ratings) as user_ratings')

    flattened_df = exploded_df\
        .withColumn('rating_text',col('user_ratings.rating_text'))\
        .withColumn('user_id',col('user_ratings.user_id'))\
        .withColumn('rating',col('user_ratings.rating'))\
        .drop('user_ratings')

    # flattened_df.printSchema()
    flattened_df.show()
