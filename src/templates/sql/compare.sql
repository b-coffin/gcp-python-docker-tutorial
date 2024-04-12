WITH
{%- for i in [0, 1] %}
compare{{- i + 1 }} AS (
    SELECT
        main.* {% if except_columns[i] %}EXCEPT({{ ", ".join(except_columns[i]) }}){% endif %},
        {%- if uunest_select_queries[i] %}
        {%- for query in uunest_select_queries[i] %}
        {{ query }},
        {%- endfor %}
        {%- endif %}
    FROM {{ "`{}`".format(compare_tables[i]['full_tableid']) }} AS main
    {%- if except_columns[i] %}
    {%- for column in except_columns[i] %}
    LEFT OUTER JOIN UNNEST(main.{{ column }}) AS {{ column -}}
    {%- endfor %}
    {%- endif %}
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
;
