from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder \
        .appName("SparkApp") \
        .getOrCreate()
 
current_date = datetime.now().strftime("%Y-%m-%d")
 
route = "gs://now-news-data-lake/now-news-silver/" + current_date + "_newspaper_titles_silver.json"
        
df_silver = spark.read.json(route)

df_exploded_tokens = df_silver.withColumn("token", F.explode("tokens_clean"))
df_grouped_tokens = (df_exploded_tokens.groupBy("newspaper","token").count())
df_ordered_tokens = df_grouped_tokens.orderBy(F.desc("count"))

df_exploded_bigrams = df_silver.withColumn("bigram", F.explode("bigrams"))
df_grouped_bigams = df_exploded_bigrams.groupBy("newspaper","bigram").count()
df_ordered_bigrams = df_grouped_bigams.orderBy(F.desc("count"))

df_ordered = df_ordered_tokens.union(df_ordered_bigrams).orderBy(F.desc("count")).limit(200)

df_dim_newspapers = df_silver.select("newspaper").distinct()

df_dim_topics = df_ordered.select("token").distinct()


df_dim_newspapers = df_dim_newspapers.withColumn('creation_date', F.current_timestamp())

df_dim_newspapers.write.format("bigquery") \
    .option("table", "nownews-479616.NowNewsStaging.staging_dim_newspapers") \
    .option("temporaryGcsBucket", "nownews-temp-bucket") \
    .mode("overwrite") \
    .save()
    
    
df_dim_topics = df_dim_topics.withColumn('creation_date', F.current_timestamp()).withColumnRenamed('token', 'topic')

df_dim_topics.write.format("bigquery") \
    .option("table", "nownews-479616.NowNewsStaging.staging_dim_topics") \
    .option("temporaryGcsBucket", "nownews-temp-bucket") \
    .mode("overwrite") \
    .save()
    
df_ordered = df_ordered.withColumnRenamed('token','topic').withColumnRenamed('count', 'topic_count').withColumn('creation_date', F.current_timestamp())

df_ordered.write.format("bigquery") \
    .option("table", "nownews-479616.NowNewsStaging.staging_fact_topics_count") \
    .option("temporaryGcsBucket", "nownews-temp-bucket") \
    .mode("overwrite") \
    .save()
    
print("Transformation process completed successfully.")    

