SELECT
    {%- for col in columns %}
    {{ col["name"] }} AS {{ col["alias"] }},
    {%- endfor %}
FROM {{ "`{}`".format(full_tableid) }} AS main
{%- for j in joins %}
LEFT OUTER JOIN UNNEST({{ j["name"] }}) AS {{ j["alias"] }}
{%- endfor %}
;