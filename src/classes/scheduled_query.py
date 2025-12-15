from __future__ import annotations

from typing import Iterable

from google.cloud import bigquery_datatransfer

from classes.config import Config


class ScheduledQuery:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.client = bigquery_datatransfer.DataTransferServiceClient()

    def list_scheduled_queries(self, project: str) -> Iterable[bigquery_datatransfer.TransferConfig]:  # type: ignore[name-defined]
        """
        指定したプロジェクトの scheduled_query 設定を返す。
        """
        
        for location in self.config.locations:
            parent = f"projects/{project}/locations/{location}"
            for transfer_config in self.client.list_transfer_configs(parent=parent):
                if transfer_config.data_source_id != "scheduled_query":
                    continue
                yield self.client.get_transfer_config(name=transfer_config.name)
