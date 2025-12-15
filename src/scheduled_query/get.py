import json
import os
from typing import List

import polars
from google.api_core import exceptions
from google.protobuf import json_format

from classes.config import Config
from classes.scheduled_query import ScheduledQuery
from classes.util import (
    COLOR_BLUE,
    COLOR_GREEN,
    COLOR_YELLOW,
    print_with_color,
    write_df_to_csv,
    write_text_file,
)


OUTPUT_COLUMNS = [
    "project",
    "location",
    "transfer_config_name",
    "display_name",
    "destination_dataset_id",
    "schedule",
    "next_run_time",
    "state",
    "update_time",
    "data_source_id",
    "owner_user_id",
    "owner_email",
    "owner_display_name",
    "params",
]


def scheduled_query_get(config: Config, result_dir: str) -> None:
    """
    指定されたプロジェクト群のスケジュールクエリ一覧を取得し、CSV/JSON で出力する。
    """

    scheduled_query = ScheduledQuery(config)

    for project in config.projects:
        print_with_color(f"\n### {project}", COLOR_BLUE)

        try:
            records, raw_configs = _list_scheduled_queries(scheduled_query, project)
        except exceptions.GoogleAPICallError as exc:
            print_with_color(str(exc), COLOR_YELLOW)
            continue

        output_path = os.path.join(result_dir, f"{project}_scheduled_queries.csv")
        write_df_to_csv(output_path, _records_to_dataframe(records))

        json_output_path = os.path.join(result_dir, f"{project}_scheduled_queries.json")
        write_text_file(json_output_path, json.dumps(raw_configs, ensure_ascii=False, indent=2))

        if len(records) == 0:
            print_with_color("スケジュールクエリは見つかりませんでした", COLOR_YELLOW)
        else:
            print_with_color(f"{len(records)} 件のスケジュールクエリを出力しました", COLOR_GREEN)


def _list_scheduled_queries(
    scheduled_query: ScheduledQuery, project: str
) -> tuple[List[dict], List[dict]]:
    """
    1プロジェクト分のスケジュールクエリ情報を取得し、表示用と生データ用に分けて返す。
    """

    records: List[dict] = []
    raw_configs: List[dict] = []

    for transfer_config in scheduled_query.list_scheduled_queries(project):
        raw_configs.append(json_format.MessageToDict(transfer_config._pb, preserving_proto_field_name=True))
        records.append(
            {
                "project": project,
                "transfer_config_name": transfer_config.name,
                "display_name": transfer_config.display_name,
                "destination_dataset_id": transfer_config.destination_dataset_id,
                "schedule": transfer_config.schedule,
                "next_run_time": str(transfer_config.next_run_time or ""),
                "state": transfer_config.state.name if transfer_config.state else "",
                "update_time": str(transfer_config.update_time or ""),
                "owner_email": getattr(getattr(transfer_config, "owner_info", None), "email", ""),
            }
        )

    return records, raw_configs


def _records_to_dataframe(records: List[dict]) -> polars.DataFrame:
    """
    取得したスケジュールクエリ情報を Polars DataFrame に変換する。
    """
    
    if records:
        df = polars.DataFrame(records)
    else:
        df = polars.DataFrame({column: [] for column in OUTPUT_COLUMNS})

    available_columns = [column for column in OUTPUT_COLUMNS if column in df.columns]
    return df.select(available_columns)
