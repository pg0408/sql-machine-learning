CREATE TABLE IF NOT EXISTS
   linear_regression (x NUMERIC, y NUMERIC) ;

TRUNCATE TABLE linear_regression ;

COPY linear_regression FROM 'linear_regression.csv' CSV HEADER ;

WITH regression AS
  (SELECT 
      regr_slope(y, x) AS gradient,
      regr_intercept(y, x) AS intercept
   FROM
      linear_regression
   WHERE
      y IS NOT NULL
   )

SELECT
   x,
   (x * gradient) + intercept AS prediction
FROM
   linear_regression
CROSS JOIN
   regression
WHERE
   y IS NULL ;