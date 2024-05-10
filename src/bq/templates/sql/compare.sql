WITH
{%- for i in [0, 1] %}
compare{{- i + 1 }} AS (
    {{ sub_queries[i] }}
){% if i == 0 %},{% endif %}
{%- endfor %}
SELECT *
FROM compare1
FULL OUTER JOIN compare2
    {%- for condition in join_conditions %}
    {{ condition -}}
    {%- endfor %}
{%- if where_condition != "" %}
WHERE {{ where_condition }}
{% endif -%}
