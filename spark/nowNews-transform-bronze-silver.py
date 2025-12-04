from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
from datetime import datetime

spark = SparkSession.builder \
        .appName("SparkApp") \
        .getOrCreate()

current_date = datetime.now().strftime("%Y-%m-%d")

route_origin = "gs://now-news-data-lake/now-news-bronze/" + current_date + "_newspaper_titles.csv"
        
df_bronze = spark.read.csv(route_origin, header=True, sep=";")

#Convert to Lowercase
df = df_bronze.withColumn(
    'title_clean',
    F.lower(
        F.regexp_replace('title', r"[^a-zA-Z0-9áéíóúñÁÉÍÓÚÑ ]", "")
    )
)

#Substitute multiple spaces by single ones
df = df.withColumn(
        'title_clean',
        F.regexp_replace("title_clean", r"\s+", " ")
    )

#Tokenization:
tokenizer = Tokenizer(
    inputCol = 'title_clean',
    outputCol = 'tokens'
)

df = tokenizer.transform(df)

#Elimination of stopwords

route_custom_stopwords = "gs://now-news-data-lake/scripts/spark/stopwords.csv"
df_stopwords = spark.read.csv(route_stopwords, header=True, sep=";")

custom_stopwords = []
for row in df_stopwords.collect():
    custom_stopwords.append(row['stopword'])

default_stopwords = StopWordsRemover.loadDefaultStopWords('spanish')

all_stopwords = list(set(default_stopwords + custom_stopwords))

remover = StopWordsRemover(
    inputCol = 'tokens',
    outputCol = 'tokens_clean',
    stopWords = all_stopwords
)

df = remover.transform(df)

#n-grams generation
ngram = NGram(
    n=2, 
    inputCol = 'tokens_clean',
    outputCol = 'bigrams'
)

df = ngram.transform(df)

route_silver = "gs://now-news-data-lake/now-news-silver/" + current_date + "_newspaper_titles_silver.json"

df.write.json(route_silver, mode = "overwrite")

print("Transformation process completed successfully.")