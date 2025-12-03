INSERT INTO `nownews-479616.NowNewsGold.fact_newspaper_topic` (topic_count, id_newspaper, id_topic, creation_date)

SELECT a.topic_count, b.id_newspaper, c.id_topic, a.creation_date  FROM `nownews-479616.NowNewsStaging.staging_fact_topics_count` a
LEFT JOIN `nownews-479616.NowNewsGold.dim_newspapers` b
ON a.newspaper = b.newspaper
LEFT JOIN `nownews-479616.NowNewsGold.dim_topics` c
ON a.topic = c.topic