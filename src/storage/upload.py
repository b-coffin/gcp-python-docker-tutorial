import os
import pandas
import pathlib
import pytz

from datetime import datetime

from classes.config import Config
from classes.storage import Storage

def storage_upload(config: Config) -> None:
    """
    Google Cloud Storageにファイルをアップロードします。

    Parameters
    ----------
    config (Config): 設定情報

    Returns
    ----------
    None
    """
    storage: Storage = Storage(config)

    # source_folderがNoneの場合はサンプルデータをアップロード
    # source_folderが指定されている場合はそのフォルダ内のファイルをアップロード
    if config.source_folder is None:
        df: pandas.DataFrame = pandas.DataFrame({
            'A': [1, 2, 3, 4, 5],
            'B': ['a', 'b', 'c', 'd', 'e']
        })
        storage.upload_df(config.bucket, df, os.path.join(config.destination_folder, f"sample_{datetime.now(pytz.timezone('Asia/Tokyo')).strftime("%Y%m%d%H%M%S")}.csv"))
    else:
        for path in [p for p in pathlib.Path().glob(f"input/{config.source_folder}/**/*") if p.is_file()]:
            storage.upload_blob(config.bucket, path, os.path.join(config.destination_folder, os.path.basename(path)))
