/* LICENSE INFORMATION

Copyright (c) 2015, Jim Nasby
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

CREATE TABLE IF NOT EXISTS
   k_means_clustering (id SERIAL, record TEXT, category VARCHAR) ;

TRUNCATE TABLE k_means_clustering ;

COPY k_means_clustering FROM 'k_means.csv' CSV HEADER ;

WITH points AS
   (SELECT
       id,
       POINT(x_loc, y_loc) AS xy
    FROM
       k_means_clustering
    ),

initial AS
   (SELECT 
       RANK() OVER (
          ORDER BY random() 
       ) AS cluster,
       xy
    FROM points 
    LIMIT {{K}}
    ),

iteration AS
   (WITH RECURSIVE kmeans(iter, id, cluster, avg_point) AS (
       SELECT
          1,
          NULL::INTEGER,
          *
        FROM 
           initial
        UNION ALL
        SELECT
           iter + 1,
           id,
           cluster,
           midpoint
        FROM (
           SELECT DISTINCT ON(iter, id)
              *
           FROM (
              SELECT
                 iter,
                 cluster,
                 p.id, 
                 p.xy<->k.avg_point AS distance,
                 @@ LSEG(p.xy, k.avg_point) AS midpoint,
                 p.xy,
                 k.avg_point
               FROM points p
               CROSS JOIN kmeans k
               ) d
            ORDER BY 1, 3, 4
            ) r
       WHERE iter < {{max_iter}}
   )
   SELECT
      *
   FROM
      kmeans
   )

SELECT
   k.*,
   cluster
FROM
   iteration i
JOIN
   k_means_clustering k
USING(id)
WHERE
   iter = {{max_iter}}
ORDER BY 4,1 ASC ;
