WITH left_outer AS (
{{ sub_queries[0] }}
EXCEPT DISTINCT
{{ sub_queries[1] }}
),

right_outer AS (
{{ sub_queries[1] }}
EXCEPT DISTINCT
{{ sub_queries[0] }}
)

SELECT * FROM left_outer
UNION ALL
SELECT * FROM right_outer
