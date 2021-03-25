CREATE TABLE IF NOT EXISTS
   naive_bayes (id SERIAL, record TEXT, category VARCHAR) ;

TRUNCATE TABLE naive_bayes ;

COPY naive_bayes FROM 'naive_bayes.csv' CSV HEADER ;

-- CTE to create one row per word
WITH staging AS
  (SELECT 
      REGEXP_SPLIT_TO_TABLE(
         LOWER(record), '[^a-z]+') AS word,
      category
   FROM
      naive_bayes
   WHERE
      category IS NOT NULL
  ),

-- testing data
test AS
  (SELECT
      id,
      record
   FROM
      naive_bayes
   WHERE
      category is NULL
  ),
          
-- one row per word + category
cartesian AS
  (SELECT
      *
   FROM
     (SELECT
         DISTINCT word
      FROM
         staging) w
      CROSS JOIN
     (SELECT
         DISTINCT category
      FROM
         staging) c
      WHERE
         length(word) > 0
   ),

-- CTE of smoothed frequencies of each word by category
frequencies AS
  (SELECT
      c.word,
      c.category,
      -- numerator plus one
      (SELECT
          count(*)+1
       FROM
          staging s
       WHERE
          s.word = c.word
       AND
          s.category = c.category) /
      -- denominator plus two
      (SELECT
          count(*)+2
       FROM
          staging s1
       WHERE
          s1.category = c.category) ::DECIMAL AS freq
   FROM
      cartesian c
   ),

-- for each row in testing, get the probabilities   
probabilities AS
  (SELECT
      t.id,
      f.category,
      SUM(LN(f.freq)) AS probability
   FROM
     (SELECT
         id,
         REGEXP_SPLIT_TO_TABLE(
            LOWER(record), '[^a-z]+') AS word
      FROM
         test) t
   JOIN
     (SELECT
         word,
         category,
         freq
      FROM
         frequencies) f 
   ON t.word = f.word
   GROUP BY 1, 2
  )

-- keep only the highest estimate            
SELECT
   record,
   probabilities.category
FROM
   probabilities
JOIN
  (SELECT
      id,
      max(probability) AS max_probability
   FROM
      probabilities
   GROUP BY 1) p
ON probabilities.id = p.id
AND probabilities.probability = p.max_probability
JOIN
   test
ON probabilities.id = test.id
ORDER BY 1 ;