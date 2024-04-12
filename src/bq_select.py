import csv
import os
import pathlib

from classes.bigquery import Bigquery
from classes.config import Config
from classes.util import *

def bq_select(bq: Bigquery, config: Config, result_dir: str) -> None:
    for tbl in config.tables:
        full_tableid = f"{config.project}.{tbl['dataset']}.{tbl['table']}"
        print_with_color(f"\n### {full_tableid}", COLOR_BLUE)

        result_dir_bytable = os.path.join(result_dir, f"{tbl['dataset']}.{tbl['table']}")

        column_names = bq.get_columnslist(full_tableid)

        # カラムがなかったらスキップ
        if len(column_names) == 0:
            continue

        # sql出力
        schemafields = bq.get_schemafields(full_tableid)
        render_content = {
            "full_tableid": full_tableid,
            "unnestcolumns": get_unnestcolumns(schemafields),
            "unnestjoins": get_unnestjoins(schemafields),
        }
        write_used_jinja2template(
            os.path.join(os.path.dirname(__file__), "templates", "sql", "select_unnest.sql"),
            os.path.join(result_dir_bytable, "select_unnest.sql"),
            render_content
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


# 再帰関数
def get_unnestcolumns(schemafields: list, prefix: str = "main") -> list[str]: # type: ignore
    columns = []
    for schemafield in schemafields:
        if schemafield.field_type == "RECORD":
            columns.extend(get_unnestcolumns(schemafield.fields, f"{prefix}__{schemafield.name}"))
        else:
            columns.append(f"{prefix}.{schemafield.name}")
    return columns


# 再帰関数
def get_unnestjoins(schemafields: list, prefix: str = "main") -> list[str]: # type: ignore
    joins = []
    for schemafield in schemafields:
        if schemafield.field_type == "RECORD":
            alias = f"{prefix}__{schemafield.name}"
            joins.append({
                "column_name": f"{prefix}.{schemafield.name}",
                "alias": alias
            })
            joins.extend(get_unnestjoins(schemafield.fields, alias))
    return joins