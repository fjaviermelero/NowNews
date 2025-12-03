-- Insert into dim_topics with incremental ID 
INSERT INTO `nownews-479616.NowNewsGold.dim_topics` (id_topic, topic, creation_date)

--New topics not created yet
WITH new_topics AS (
  SELECT 
    a.topic, 
    a.creation_date
  FROM 
    `nownews-479616.NowNewsStaging.staging_dim_topics` AS a
  LEFT JOIN 
    `nownews-479616.NowNewsGold.dim_topics` AS b
  ON a.topic = b.topic
  WHERE b.topic IS NULL
)

--Asign new IDs
SELECT
  COALESCE((SELECT MAX(id_topic) FROM `nownews-479616.NowNewsGold.dim_topics`), 0)
  + ROW_NUMBER() OVER(ORDER BY topic) AS id_topic,
  topic,
  creation_date
FROM new_topics;