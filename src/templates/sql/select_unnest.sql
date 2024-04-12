SELECT
    {%- for column in unnestcolumns %}
    {{ column }},
    {%- endfor %}
FROM {{ "`{}`".format(full_tableid) }} AS main
{%- for j in unnestjoins %}
LEFT OUTER JOIN UNNEST({{ j["column_name"] }}) AS {{ j["alias"] }}
{%- endfor %}
;