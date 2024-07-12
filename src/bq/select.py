import csv
import os
import pathlib

from classes.bigquery import Bigquery
from classes.config import Config
from classes.util import *

def bq_select(config: Config, result_dir: str) -> None:
    for tbl in config.tables:
        full_tableid = f"{tbl['project']}.{tbl['dataset']}.{tbl['table']}"
        print_with_color(f"\n### {full_tableid}", COLOR_BLUE)

        bq: Bigquery = Bigquery(tbl['project'])
        result_dir_bytable = os.path.join(result_dir, f"{tbl['dataset']}.{tbl['table']}")

        column_names = bq.get_columnslist(full_tableid)

        # カラムがなかったらスキップ
        if len(column_names) == 0:
            continue

        schemafields = bq.get_schemafields(full_tableid)

        # カラム情報出力
        write_df_to_csv(
            path=os.path.join(result_dir_bytable, "columns.csv"),
            df=polars.DataFrame([{"column": sf.name} for sf in schemafields])
        )

        # sql出力
        write_sql(schemafields, result_dir_bytable, full_tableid)

        # csv出力
        path: str = os.path.join(result_dir_bytable, "template.csv")
        with open(path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)

        # uploadコマンドのサンプル
        upload_command: str = f"bq load --source_format=CSV --replace=false --skip_leading_rows 1 {tbl['project']}:{tbl['dataset']}.{tbl['table']} {os.path.join("src", path)}\n"

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
            upload_command += f"bq load --source_format=NEWLINE_DELIMITED_JSON --replace=false {tbl['project']}:{tbl['dataset']}.{tbl['table']} {os.path.join("src", get_escapedtext_forcommand(result_json["path"]))}\n"

        write_text_file(os.path.join(result_dir_bytable, "upload.sh"), upload_command)

        print_with_color("...Done", COLOR_GREEN)

    return


# 再帰関数
def get_unnestcolumns(schemafields: list, parent: str = "main.") -> list[dict]: # type: ignore
    cols = []
    for schemafield in schemafields:
        full_name: str = f"{parent}{schemafield.name}"
        cols.append({
            "parent": parent,
            "name": schemafield.name,
            "full_name": full_name,
            "type": schemafield.field_type,
            "mode": schemafield.mode
        })
        if schemafield.field_type == "RECORD":
            cols.extend(get_unnestcolumns(schemafield.fields, f"{full_name.replace(".", "__") if schemafield.mode == "REPEATED" else full_name}."))
    return cols


def write_sql(schemafields, result_dir: str, full_tableid: str) -> None:
    unnestcolumns = get_unnestcolumns(schemafields)

    # RECORD型のカラムを展開したSQL
    write_used_jinja2template(
        template_path=os.path.join(os.path.dirname(__file__), "templates", "sql", "select_unnest.sql"),
        write_target_path=os.path.join(result_dir, "select_unnest.sql"),
        render_content={
            "full_tableid": full_tableid,
            "columns": [{"name": col["full_name"], "alias": col["full_name"].replace(".", "__")} for col in unnestcolumns if col["type"] != "RECORD"],
            "joins": [{"name": col["full_name"], "alias": col["full_name"].replace(".", "__")} for col in unnestcolumns if col["mode"] == "REPEATED"]
        }
    )
