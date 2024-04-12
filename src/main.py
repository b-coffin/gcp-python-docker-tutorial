import csv
import datetime
import jinja2
import json
import os
import pathlib
import polars
import traceback
from zoneinfo import ZoneInfo

from classes.bigquery import Bigquery
from classes.config import Config
from classes.util import *

def main():

    # configファイルをキーボード入力から取得
    default_config_file = "sample_bq_getcolumn.json"
    config_filepath = os.path.join("config", input(f"Input config file name 1 (default: {default_config_file}) : ") or default_config_file)
    config_json = json.load(open(config_filepath, "r"))

    # configのバリデーションチェック
    try:
        config = Config(config_json)
    except Exception:
        print(f"Stacktrace: {traceback.format_exc()}")

    # 結果を格納するフォルダ
    result_dir = os.path.join("result", datetime.datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y%m%d%H%M%S") + "_" + config.service)

    if config.service == Config.SERVICES["BIGQUERY"]:
        bq = Bigquery(config.project)

        if config.mode == "compare":

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
            result_basefilename = os.path.join(result_dir, f"compare-{'__and__'.join([tbl['table'] for tbl in compare_tables])}")
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

        elif config.mode == "select":
            for tbl in config.tables:
                full_tableid = f"{config.project}.{tbl['dataset']}.{tbl['table']}"
                print_with_color(f"\n### {full_tableid}", COLOR_BLUE)

                result_dir_bytable = os.path.join(result_dir, f"{tbl['dataset']}.{tbl['table']}")

                column_names = bq.get_columnslist(full_tableid)

                # カラムがなかったらスキップ
                if len(column_names) == 0:
                    continue

                # sql出力
                write_text_file(os.path.join(result_dir_bytable, "select.sql"), 
f"""
SELECT
    {',\n\t'.join(column_names)}
FROM `{full_tableid}`
"""
                )

                # csv出力
                with open(os.path.join(result_dir_bytable, "columns.csv"), "w") as f:
                    writer = csv.writer(f)
                    writer.writerow(column_names)

                # uploadコマンドのサンプル
                upload_command: str = f"bq load --source_format=CSV --replace=false --skip_leading_rows 1 {config.project}:{tbl['dataset']}.{tbl['table']} path/to/csv\n"

                # jsonl出力

                result_jsons: list[dict] = []

                # inputがあれば、その情報をもとにjsonlを作成
                # inputがなければテンプレートを作成
                input_files: list[str] = [p for p in pathlib.Path().glob("input/**/*.csv") if is_contain_allwords(str(p), [tbl['dataset'], rf"{tbl['table']}\."])]
                if len(input_files) == 0:
                    result_jsons.append({
                        "path": os.path.join(result_dir_bytable, "template.jsonl"),
                        "json": [bq.get_columnsjson(bq.get_schemafields(full_tableid), data=None)]
                    })
                else:
                    for input_file in input_files:
                        print(input_file)

                        temp_jsons: list[dict] = []
                        with open(input_file, mode="r", encoding="shift_jis") as f:
                            for row in list(csv.DictReader(f)):
                                temp_jsons.append(bq.get_columnsjson(bq.get_schemafields(full_tableid), data=row))

                        result_jsons.append({
                            "path": os.path.join(result_dir_bytable, f"{get_filename_withoutextension(input_file)}.jsonl"),
                            "json": temp_jsons
                        })

                for result_json in result_jsons:
                    write_jsonl_file(result_json["path"], result_json["json"])
                    upload_command += f"bq load --source_format=NEWLINE_DELIMITED_JSON --replace=false {config.project}:{tbl['dataset']}.{tbl['table']} {os.path.join("src", get_escapedtext_forcommand(result_json["path"]))}\n"

                write_text_file(os.path.join(result_dir_bytable, "upload.sh"), upload_command)

                print_with_color("...Done", COLOR_GREEN)

    return

# メイン
if __name__ == "__main__":
    main()
