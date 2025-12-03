-- Insert into dim_newspapers with incremental ID 
INSERT INTO `nownews-479616.NowNewsGold.dim_newspapers` (id_newspaper, newspaper, creation_date)

--New newspapers not created yet
WITH new_newspapers AS (
  SELECT 
    a.newspaper, 
    a.creation_date
  FROM 
    `nownews-479616.NowNewsStaging.staging_dim_newspapers` AS a
  LEFT JOIN 
    `nownews-479616.NowNewsGold.dim_newspapers` AS b
  ON a.newspaper = b.newspaper
  WHERE b.newspaper IS NULL
)

--Asign new IDs
SELECT
  COALESCE((SELECT MAX(id_newspaper) FROM `nownews-479616.NowNewsGold.dim_newspapers`), 0)
  + ROW_NUMBER() OVER(ORDER BY newspaper) AS id_newspaper,
  newspaper,
  creation_date
FROM new_newspapers;