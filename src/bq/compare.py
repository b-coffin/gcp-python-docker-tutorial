import jinja2
import os
import polars

from classes.bigquery import Bigquery
from classes.config import Config
from classes.util import (
    COLOR_YELLOW,
    print_with_color,
    write_df_to_csv
)


def bq_compare(bq: Bigquery, config: Config, result_dir: str) -> None:
    compare_tables = []
    for tbl in config.tables:
        full_tableid = f"{config.project}.{tbl['dataset']}.{tbl['table']}"
        schemafields = bq.get_schemafields(full_tableid)

        columns = []
        except_columns = []
        uunest_select_queries = []
        for schemafield in schemafields:
            if schemafield.field_type == "RECORD":
                for f in schemafield.fields:
                    alias = f"{schemafield.name}_{f.name}"
                    columns.append({
                        "name": f.name,
                        "alias": alias,
                        "type": f.field_type
                    })
                    uunest_select_queries.append(f"{schemafield.name}.{f.name} AS {alias}")
                except_columns.append(schemafield.name)
            else:
                columns.append({
                    "name": schemafield.name,
                    "alias": schemafield.name,
                    "type": schemafield.field_type
                })

        compare_tables.append({
            "full_tableid": full_tableid,
            "table": tbl['table'],
            "columns": columns,
            "except_columns": except_columns,
            "uunest_select_queries": uunest_select_queries
        })

    if len(compare_tables[0]["columns"]) != len(compare_tables[1]["columns"]):
        print_with_color("WARNING: Number of schemas in compare tables does not match", COLOR_YELLOW)
    
    left = polars.DataFrame(compare_tables[0]["columns"])
    right = polars.DataFrame(compare_tables[1]["columns"])

    # 比較表作成
    outer_merged_df = left.join(right, on=["name", "alias", "type"], how="outer")
    result_basefilename = f"compare-{'__and__'.join([tbl['table'] for tbl in compare_tables])}"
    write_df_to_csv(os.path.join(result_dir, f"{result_basefilename}.csv"), outer_merged_df)

    # 値を比較するsql作成

    inner_merged_df = left.join(right, on=["name", "alias", "type"], how="inner")

    join_conditions = []
    where_condition = ""
    for i, row in enumerate(inner_merged_df.rows(named=True)):
        alias1 = f"compare1.{row['alias']}"
        alias2 = f"compare2.{row['alias']}"

        if i == 0:
            conjunction = "ON"
            where_condition = f"{alias1} IS NULL OR {alias2} IS NULL"
        else:
            conjunction = "AND"

        join_conditions.append(f"{conjunction} {Bigquery.get_ifnull_sql(alias1, row['type'])} = {Bigquery.get_ifnull_sql(alias2, row['type'])}")

    # jinja2のテンプレートを読み込む
    # 参考: https://qiita.com/simonritchie/items/cc2021ac6860e92de25d
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath="./templates/sql", encoding="utf8"))
    template = env.get_template("compare.sql")

    # 結果を出力
    with open(os.path.join(result_dir, f"{result_basefilename}.sql"), "w") as f:
        f.write(template.render({
            "except_columns": [v["except_columns"] for v in compare_tables],
            "uunest_select_queries": [v["uunest_select_queries"] for v in compare_tables],
            "compare_tables": compare_tables,
            "join_conditions": join_conditions,
            "where_condition": where_condition
        }))

    return
