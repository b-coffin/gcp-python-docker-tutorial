WITH compare1 AS (
{{ sub_queries[0] }}
),

compare2 AS (
{{ sub_queries[1] }}
)

SELECT *
FROM compare1
FULL OUTER JOIN compare2
    {%- for condition in join_conditions %}
    {{ condition -}}
    {%- endfor %}
{%- if where_condition != "" %}
WHERE {{ where_condition }}
{% endif -%}
