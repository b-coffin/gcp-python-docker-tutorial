import os
import polars

from bq.select import get_unnestcolumns
from classes.bigquery import Bigquery
from classes.config import Config
from classes.util import (
    COLOR_BLUE,
    COLOR_YELLOW,
    get_text_used_jinja2template,
    print_with_color,
    write_df_to_csv,
    write_used_jinja2template,
)


def bq_compare(bq: Bigquery, config: Config, result_dir: str) -> None:
    compare_tables = []
    for tbl in config.tables:
        full_tableid = f"{config.project}.{tbl['dataset']}.{tbl['table']}"

        print_with_color(f"\n### {full_tableid}", COLOR_BLUE)

        unnestcolumns: list[dict] = get_unnestcolumns(bq.get_schemafields(full_tableid))

        columns: list[dict] = [
            {
                "name": col["name"],
                "alias": f"{"__".join(col["parents"])}__{col["name"]}",
                "type": col["type"],
            }
            for col in unnestcolumns
        ]
            
        compare_tables.append({
            "full_tableid": full_tableid,
            "table": tbl['table'],
            "columns": columns,
            "unnest_query": get_text_used_jinja2template(
                template_path=os.path.join(os.path.dirname(__file__), "templates", "sql", "select_unnest.sql"),
                render_content={
                    "full_tableid": full_tableid,
                    "columns": [{"name": f"{"__".join(col["parents"])}.{col["name"]}", "alias": f"{"__".join(col["parents"])}__{col["name"]}"} for col in unnestcolumns if col["type"] != "RECORD"],
                    "joins": [{"name": f"{"__".join(col["parents"])}.{col["name"]}", "alias": f"{"__".join(col["parents"])}__{col["name"]}"} for col in unnestcolumns if col["type"] == "RECORD"]
                }
            )
        })

    if len(compare_tables[0]["columns"]) != len(compare_tables[1]["columns"]):
        print_with_color("WARNING: Number of schemas in compare tables does not match", COLOR_YELLOW)
    
    left = polars.DataFrame(compare_tables[0]["columns"])
    right = polars.DataFrame(compare_tables[1]["columns"])

    # 比較表作成
    outer_merged_df = left.join(right, on=["name", "alias", "type"], how="outer")
    result_basefilename = f"{'__and__'.join([tbl['table'] for tbl in compare_tables])}"
    write_df_to_csv(os.path.join(result_dir, f"compare-{result_basefilename}.csv"), outer_merged_df)

    # 値を比較するsql作成

    inner_merged_df = left.join(right, on=["name", "alias", "type"], how="inner")

    join_conditions = []
    where_condition = ""
    for i, row in enumerate(inner_merged_df.rows(named=True)):

        if row["type"] == "RECORD":
            continue

        alias1 = f"compare1.{row['alias']}"
        alias2 = f"compare2.{row['alias']}"

        if i == 0:
            conjunction = "ON"
            where_condition = f"{alias1} IS NULL OR {alias2} IS NULL"
        else:
            conjunction = "AND"

        join_conditions.append(f"{conjunction} {Bigquery.get_ifnull_sql(alias1, row['type'])} = {Bigquery.get_ifnull_sql(alias2, row['type'])}")

    write_used_jinja2template(
        template_path=os.path.join(os.path.dirname(__file__), "templates", "sql", "compare.sql"),
        write_target_path=os.path.join(result_dir, f"compare-{result_basefilename}.sql"),
        render_content={
            "sub_queries": [tbl["unnest_query"] for tbl in compare_tables],
            "join_conditions": join_conditions,
            "where_condition": where_condition,
        }
    )

    write_used_jinja2template(
        template_path=os.path.join(os.path.dirname(__file__), "templates", "sql", "compare2.sql"),
        write_target_path=os.path.join(result_dir, f"compare2-{result_basefilename}.sql"),
        render_content={
            "sub_queries": [tbl["unnest_query"] for tbl in compare_tables],
        }
    )

    return
