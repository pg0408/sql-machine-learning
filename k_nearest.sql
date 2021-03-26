CREATE TABLE IF NOT EXISTS
   k_nearest (id SERIAL, x_loc NUMERIC, y_loc NUMERIC, category VARCHAR) ;

TRUNCATE TABLE k_nearest ;

COPY k_nearest FROM 'k_nearest.csv' CSV HEADER ;

-- CTE to get labelled training data
WITH training AS
  (SELECT
      id,
      POINT(x_loc, y_loc) as xy,
      category
  FROM
      k_nearest
  WHERE
      category IS NOT NULL
  ),

-- CTE to get the unlabelled points
test AS
  (SELECT
      id,
      POINT(x_loc, y_loc) as xy,
      category
  FROM
      k_nearest
  WHERE
      category IS NULL
  ),

-- calculate distances between unlabelled & labelled points
distances AS
  (SELECT
      test.id,
      training.category,
      test.xy<->training.xy AS dist,
      ROW_NUMBER() OVER (
         PARTITION BY test.id
         ORDER BY test.xy<->training.xy 
         ) AS row_no
  FROM
      test
  CROSS JOIN training
  ORDER BY 1, 4 ASC
  ),

-- count the 'votes' per label for each unlabelled point
votes AS
  (SELECT
      id,
      category,
      count(*) AS votes
  FROM distances
  WHERE row_no <= {{K}}
  GROUP BY 1,2
  ORDER BY 1)

-- query for the label with the most votes
SELECT
  v.id,
  v.category
FROM
  votes v
JOIN
  (SELECT
      id,
      max(votes) AS max_votes
  FROM
      votes
  GROUP BY 1
  ) mv 
ON v.id = mv.id
AND v.votes = mv.max_votes
ORDER BY 1 ASC ;
