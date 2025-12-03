# Dataproc serverless silver-gold
gcloud dataproc batches submit pyspark \
  gs://now-news-data-lake/scripts/newNews-transform-silver-gold.py \
  --region=us-central1 \
  --batch=nownews-transform-silver-gold-$(date +%s) \
  --properties spark.executor.instances=4 \
  --properties spark.executor.cores=2 \
  --properties spark.executor.memory=8g \
  --properties spark.driver.memory=4g
