CREATE TABLE IF NOT EXISTS
   k_nearest (id SERIAL, x_loc NUMERIC, y_loc NUMERIC, category VARCHAR) ;

TRUNCATE TABLE k_nearest ;

COPY k_nearest FROM 'k_nearest.csv' CSV HEADER ;

-- CTE to get labelled training data
WITH training AS
  (SELECT
      *
   FROM
      k_nearest
   WHERE
      category IS NOT NULL
  ),

-- CTE to get the unlabelled points
test AS
  (SELECT
      id,
      x_loc,
      y_loc
   FROM
      k_nearest
   WHERE
      category IS NULL
  ),

-- calculate distances between unlabelled & labelled points
distances AS
  (SELECT
      test.id,
      test.x_loc,
      test.y_loc,
      category,
      ((test.x_loc - training.x_loc)^2 + 
         (test.y_loc - training.y_loc)^2)^0.5 AS dist,
      ROW_NUMBER() OVER (
         PARTITION BY test.id
         ORDER BY (
            (test.x_loc - training.x_loc)^2 + 
               (test.y_loc - training.y_loc)^2)^0.5
         ) AS row_no
   FROM
      test
   CROSS JOIN training
   ORDER BY 1, 5 ASC
  ),

-- count the 'votes' per label for each unlabelled point
votes AS
  (SELECT
      id,
      x_loc,
      y_loc,
      category,
      count(*) AS votes
   FROM distances
   WHERE row_no <= {{K}}
   GROUP BY 1,2,3,4
   ORDER BY 1)

-- query for the label with the most votes
SELECT
   v.id,
   v.x_loc,
   v.y_loc,
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